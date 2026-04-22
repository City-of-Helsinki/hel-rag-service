"""
Azure Blob Storage sink for embedding vectors exported as Parquet files.

Accumulates per-chunk embedding results in memory during a pipeline batch,
then serializes to Parquet (via PyArrow) and uploads the result as a single
blob once the batch completes.  One blob per date-batch; re-running the same
date range overwrites the existing blob (overwrite=True).
"""

import logging
import threading
from datetime import datetime, timezone
from io import BytesIO
from typing import List, Optional

import pyarrow as pa
import pyarrow.parquet as pq
from azure.core.exceptions import ResourceExistsError, ResourceNotFoundError
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient

from ..core.config import settings
from .embedder import EmbeddingResult

logger = logging.getLogger(__name__)

# Explicit Parquet schema for robustness and reproducibility
_PARQUET_SCHEMA = pa.schema(
    [
        pa.field("chunk_id", pa.string()),
        pa.field("native_id", pa.string()),
        pa.field("embedding", pa.list_(pa.float32())),
        pa.field("embedding_model", pa.string()),
        pa.field("embedding_dimensions", pa.int32()),
        pa.field("created_at", pa.timestamp("us", tz="UTC")),
    ]
)


class ParquetEmbeddingSaver:
    """Thread-safe batch accumulator that uploads embedding vectors as Parquet blobs to Azure."""

    def __init__(
        self,
        container_name: Optional[str] = None,
        blob_prefix: Optional[str] = None,
        connection_string: Optional[str] = None,
        account_url: Optional[str] = None,
    ) -> None:
        self._container_name = container_name or settings.AZURE_BLOB_EMBEDDINGS_CONTAINER_NAME
        self._blob_prefix = blob_prefix or settings.AZURE_BLOB_EMBEDDINGS_BLOB_PREFIX.format(dimension=settings.EMBEDDING_DIMENSION)
        self._connection_string = connection_string
        self._account_url = account_url

        self._buffer: list = []
        self._lock = threading.Lock()
        self._batch_start: Optional[datetime] = None
        self._batch_end: Optional[datetime] = None

        # Lazy-initialised service client
        self._blob_service_client: Optional[BlobServiceClient] = None

    def start_batch(self, batch_start: datetime, batch_end: datetime) -> None:
        """Reset the in-memory buffer and record the current batch date range."""
        with self._lock:
            self._buffer = []
            self._batch_start = batch_start
            self._batch_end = batch_end
        logger.debug(
            f"Parquet saver: started batch {batch_start.date()} to {batch_end.date()}"
        )

    def buffer(self, embedding_results: List[EmbeddingResult], native_id: str) -> None:
        """Thread-safe accumulation of embedding results for a document.

        Args:
            embedding_results: List of EmbeddingResult objects to store.
            native_id: The parent decision's native ID (not carried by EmbeddingResult).
        """
        now = datetime.now(timezone.utc)
        rows = [
            {
                "chunk_id": result.chunk_id,
                "native_id": native_id,
                "embedding": result.embedding,
                "embedding_model": result.model,
                "embedding_dimensions": len(result.embedding),
                "created_at": now,
            }
            for result in embedding_results
        ]
        with self._lock:
            self._buffer.extend(rows)

    def flush_batch(self) -> bool:
        """Serialize the buffer to Parquet and upload to Azure Blob Storage.

        Returns:
            True on success or when the buffer is empty; False on failure (error
            is logged but not re-raised so the pipeline continues).
        """
        # Snapshot and clear the buffer under the lock, then do I/O outside it
        with self._lock:
            rows = list(self._buffer)
            self._buffer = []
            batch_start = self._batch_start
            batch_end = self._batch_end

        if not rows:
            logger.debug("Parquet saver: empty batch, skipping upload")
            return True

        if batch_start is None or batch_end is None:
            logger.error("Parquet saver: flush_batch called before start_batch")
            return False

        blob_name = self._build_blob_name(batch_start, batch_end)

        try:
            # Build PyArrow table with explicit schema
            columns = {
                "chunk_id": [r["chunk_id"] for r in rows],
                "native_id": [r["native_id"] for r in rows],
                "embedding": [r["embedding"] for r in rows],
                "embedding_model": [r["embedding_model"] for r in rows],
                "embedding_dimensions": [r["embedding_dimensions"] for r in rows],
                "created_at": [r["created_at"] for r in rows],
            }
            table = pa.table(columns, schema=_PARQUET_SCHEMA)

            # Serialize to in-memory Parquet bytes
            buf = BytesIO()
            pq.write_table(table, buf)
            buf.seek(0)
            parquet_bytes = buf.read()

            # Upload to Azure Blob Storage
            client = self._get_blob_service_client()
            container_client = client.get_container_client(self._container_name)
            self._ensure_container_exists(container_client)

            blob_client = container_client.get_blob_client(blob_name)
            blob_client.upload_blob(parquet_bytes, overwrite=True)

            logger.info(
                f"Parquet saver: uploaded {len(rows)} rows to {blob_name} "
                f"({len(parquet_bytes):,} bytes)"
            )
            return True

        except Exception as exc:
            logger.error(
                f"Parquet saver: failed to upload batch {blob_name}: {exc}",
                exc_info=True,
            )
            return False

    def _get_blob_service_client(self) -> BlobServiceClient:
        """Return a lazily-created BlobServiceClient.

        Prefers connection string auth; falls back to DefaultAzureCredential.
        """
        if self._blob_service_client is not None:
            return self._blob_service_client

        conn_str = self._connection_string or settings.AZURE_BLOB_CONNECTION_STRING
        account_url = self._account_url or settings.AZURE_BLOB_ACCOUNT_URL

        if conn_str:
            self._blob_service_client = BlobServiceClient.from_connection_string(conn_str)
            logger.debug("Parquet saver: using connection string authentication")
        elif account_url:
            credential = DefaultAzureCredential()
            self._blob_service_client = BlobServiceClient(
                account_url=account_url,
                credential=credential,
            )
            logger.debug("Parquet saver: using DefaultAzureCredential authentication")
        else:
            raise ValueError(
                "Azure Blob Storage embeddings export is enabled but neither "
                "AZURE_BLOB_CONNECTION_STRING nor AZURE_BLOB_ACCOUNT_URL is configured."
            )

        return self._blob_service_client

    def _ensure_container_exists(self, container_client) -> None:
        """Create the container if it does not yet exist."""
        try:
            container_client.get_container_properties()
        except ResourceNotFoundError:
            logger.info(
                f"Parquet saver: container '{self._container_name}' not found, creating it"
            )
            try:
                container_client.create_container()
            except ResourceExistsError:
                pass  # Race condition — already created by another process

    def _build_blob_name(self, batch_start: datetime, batch_end: datetime) -> str:
        """Return the blob path: {prefix}/{YYYY-MM-DD}_{YYYY-MM-DD}.parquet"""
        start_str = batch_start.strftime("%Y-%m-%d")
        end_str = batch_end.strftime("%Y-%m-%d")
        return f"{self._blob_prefix}/{start_str}_{end_str}.parquet"
