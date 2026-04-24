"""
Azure Blob Storage sink for raw decision API responses.

Accumulates per-document raw responses in memory during a date-batch fetch,
then gzip-compresses and uploads the entire batch as a single NDJSON blob.
"""

import gzip
import json
import logging
import threading
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from azure.core.exceptions import ResourceExistsError, ResourceNotFoundError
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient

from ..core.config import settings

logger = logging.getLogger(__name__)


class AzureBlobRawResponseSaver:
    """Thread-safe batch accumulator that uploads gzip NDJSON blobs to Azure."""

    def __init__(
        self,
        container_name: Optional[str] = None,
        blob_prefix: Optional[str] = None,
        connection_string: Optional[str] = None,
        account_url: Optional[str] = None,
    ) -> None:
        self._container_name = container_name or settings.AZURE_BLOB_CONTAINER_NAME
        self._blob_prefix = blob_prefix or settings.AZURE_BLOB_BLOB_PREFIX
        self._connection_string = connection_string
        self._account_url = account_url

        self._buffer: list = []
        self._lock = threading.Lock()
        self._batch_start: Optional[datetime] = None
        self._batch_end: Optional[datetime] = None
        self._flush_failures: int = 0

        # Lazy-initialised service client
        self._blob_service_client: Optional[BlobServiceClient] = None

    def start_batch(self, batch_start: datetime, batch_end: datetime) -> None:
        """Reset the buffer and record the current batch key."""
        with self._lock:
            self._buffer = []
            self._batch_start = batch_start
            self._batch_end = batch_end
        logger.debug(
            f"Blob saver: started batch {batch_start.date()} to {batch_end.date()}"
        )

    def buffer(self, native_id: str, raw_data: Dict[str, Any]) -> None:
        """Thread-safe accumulation of one document's raw response."""
        entry = {
            "native_id": native_id,
            "fetched_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "data": raw_data,
        }
        with self._lock:
            self._buffer.append(entry)

    def flush_batch(self) -> bool:
        """Serialize, gzip-compress, and upload all buffered responses for the current batch.

        Returns:
            True on success, False on failure (error is logged but not re-raised).
        """
        with self._lock:
            entries = list(self._buffer)
            self._buffer = []
            batch_start = self._batch_start
            batch_end = self._batch_end

        if not entries:
            logger.debug("Blob saver: empty batch, skipping upload")
            return True

        if batch_start is None or batch_end is None:
            logger.error("Blob saver: flush_batch called before start_batch")
            return False

        blob_name = self._build_blob_name(batch_start, batch_end)

        try:
            ndjson_bytes = "\n".join(json.dumps(e, ensure_ascii=False) for e in entries).encode("utf-8")
            compressed = gzip.compress(ndjson_bytes, compresslevel=6)

            client = self._get_blob_service_client()
            container_client = client.get_container_client(self._container_name)

            try:
                container_client.get_container_properties()
            except ResourceNotFoundError:
                logger.info(f"Container '{self._container_name}' not found, creating it")
                try:
                    container_client.create_container()
                except ResourceExistsError:
                    pass  # Race condition — already created by another process

            blob_client = container_client.get_blob_client(blob_name)
            blob_client.upload_blob(compressed, overwrite=True)

            logger.info(
                f"Blob saver: uploaded {len(entries)} records to {blob_name} "
                f"({len(compressed):,} bytes compressed)"
            )
            return True

        except Exception as exc:
            self._flush_failures += 1
            logger.error(
                f"Blob saver: failed to upload batch {blob_name}: {exc}",
                exc_info=True,
            )
            return False

    def _build_blob_name(self, batch_start: datetime, batch_end: datetime) -> str:
        """Format blob path: {prefix}/{YYYY-MM-DD}_{YYYY-MM-DD}.ndjson.gz"""
        start_str = batch_start.strftime("%Y-%m-%d")
        end_str = batch_end.strftime("%Y-%m-%d")
        return f"{self._blob_prefix}/{start_str}_{end_str}.ndjson.gz"

    def _get_blob_service_client(self) -> BlobServiceClient:
        """Return a lazily-created BlobServiceClient (connection string or DefaultAzureCredential)."""
        if self._blob_service_client is not None:
            return self._blob_service_client

        if self._connection_string:
            self._blob_service_client = BlobServiceClient.from_connection_string(
                self._connection_string
            )
            logger.debug("Blob saver: using connection string authentication")
        elif self._account_url:
            credential = DefaultAzureCredential()
            self._blob_service_client = BlobServiceClient(
                account_url=self._account_url,
                credential=credential,
            )
            logger.debug("Blob saver: using DefaultAzureCredential authentication")
        else:
            raise ValueError(
                "Azure Blob Storage is enabled but neither AZURE_BLOB_CONNECTION_STRING "
                "nor AZURE_BLOB_ACCOUNT_URL is configured."
            )

        return self._blob_service_client
