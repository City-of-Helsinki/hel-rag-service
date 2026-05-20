"""
Azure Blob Storage reader for ingesting archived decision NDJSON blobs.

Reads *.ndjson.gz blobs produced by AzureBlobRawResponseSaver and yields
DecisionDocument objects suitable for batch ingestion.
"""

import gzip
import io
import json
import logging
import re
from datetime import date
from typing import Generator, Iterator, List, Optional, Tuple

from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient

from ..core.config import settings
from ..schemas.decision import DecisionDocument

logger = logging.getLogger(__name__)


class _IterStream(io.RawIOBase):
    """Wrap an iterable of bytes chunks as a readable raw stream."""

    def __init__(self, chunks: Iterator[bytes]) -> None:
        self._chunks = chunks
        self._buf = b""

    def readable(self) -> bool:
        return True

    def readinto(self, b: bytearray) -> int:
        while not self._buf:
            try:
                self._buf = next(self._chunks)
            except StopIteration:
                return 0
        n = min(len(b), len(self._buf))
        b[:n] = self._buf[:n]
        self._buf = self._buf[n:]
        return n


# Blob filename suffix pattern: YYYY-MM-DD_YYYY-MM-DD.ndjson.gz
_BLOB_DATE_PATTERN = re.compile(
    r"(\d{4}-\d{2}-\d{2})_(\d{4}-\d{2}-\d{2})\.ndjson\.gz$"
)


class BlobDecisionReader:
    """Read archived decision NDJSON blobs from Azure Blob Storage."""

    def __init__(
        self,
        container_name: Optional[str] = None,
        blob_prefix: Optional[str] = None,
        connection_string: Optional[str] = None,
        account_url: Optional[str] = None,
    ) -> None:
        self._container_name = container_name or settings.AZURE_BLOB_CONTAINER_NAME
        # Effective prefix: explicit override → INGEST prefix → raw-response prefix
        self._blob_prefix = (
            blob_prefix
            or settings.AZURE_BLOB_INGEST_BLOB_PREFIX
            or settings.AZURE_BLOB_BLOB_PREFIX
        )
        # Use is-not-None check so callers can pass "" to suppress settings fallback
        self._connection_string = (
            connection_string if connection_string is not None
            else settings.AZURE_BLOB_CONNECTION_STRING
        )
        self._account_url = (
            account_url if account_url is not None
            else settings.AZURE_BLOB_ACCOUNT_URL
        )

        self._blob_service_client: Optional[BlobServiceClient] = None

    def list_blobs(
        self,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
    ) -> List[str]:
        """Return blob names under the configured prefix, optionally filtered by date range.

        Only blobs whose date window overlaps [start_date, end_date] are included.
        Results are sorted chronologically (ascending blob start date).

        Args:
            start_date: Inclusive lower bound for the overlap filter.
            end_date: Inclusive upper bound for the overlap filter.

        Returns:
            Sorted list of matching blob names.
        """
        client = self._get_blob_service_client()
        container_client = client.get_container_client(self._container_name)

        prefix = self._blob_prefix.rstrip("/") + "/"
        all_blobs = list(container_client.list_blobs(name_starts_with=prefix))
        ndjson_blobs = [b.name for b in all_blobs if b.name.endswith(".ndjson.gz")]

        if start_date is None and end_date is None:
            # No date filter — return all blobs sorted chronologically
            dated = []
            for name in ndjson_blobs:
                try:
                    blob_start, _ = self.get_blob_date_range(name)
                    dated.append((blob_start, name))
                except ValueError:
                    # Blob name doesn't match expected pattern — include at end
                    dated.append((date.max, name))
            dated.sort(key=lambda x: x[0])
            result = [name for _, name in dated]
        else:
            dated = []
            for name in ndjson_blobs:
                try:
                    blob_start, blob_end = self.get_blob_date_range(name)
                except ValueError:
                    logger.warning(
                        f"Blob '{name}' does not match expected date pattern; skipping."
                    )
                    continue

                overlaps = True
                if end_date is not None and blob_start > end_date:
                    overlaps = False
                if start_date is not None and blob_end < start_date:
                    overlaps = False

                if overlaps:
                    dated.append((blob_start, name))

            dated.sort(key=lambda x: x[0])
            result = [name for _, name in dated]

        logger.info(
            f"BlobDecisionReader: found {len(result)} blob(s) under '{prefix}'"
            + (
                f" overlapping {start_date} – {end_date}"
                if start_date or end_date
                else ""
            )
        )
        return result

    def iter_documents(
        self, blob_name: str
    ) -> Generator[DecisionDocument, None, None]:
        """Download, decompress, and yield DecisionDocument objects from a blob.

        Per-line parse errors are logged and skipped.  Download/decompress errors
        are re-raised so the caller can decide whether to continue with other blobs.

        Args:
            blob_name: Full blob name (including prefix) to download.

        Yields:
            DecisionDocument objects parsed from each NDJSON line.

        Raises:
            Exception: On download or decompression failure.
        """
        client = self._get_blob_service_client()
        container_client = client.get_container_client(self._container_name)
        blob_client = container_client.get_blob_client(blob_name)

        logger.debug(f"BlobDecisionReader: downloading '{blob_name}'")
        downloader = blob_client.download_blob()
        chunk_stream = _IterStream(iter(downloader.chunks()))

        yielded = 0
        errors = 0
        with gzip.open(chunk_stream, "rt", encoding="utf-8") as gz:
            for line in gz:
                line = line.strip()
                if not line:
                    continue
                try:
                    entry = json.loads(line)
                    data = entry["data"]
                    docs: List[DecisionDocument] = []
                    if isinstance(data, dict):
                        if "NativeId" in data:
                            docs = [DecisionDocument(**data)]
                        elif "decisions" in data:
                            docs = [DecisionDocument(**item) for item in data["decisions"]]
                    elif isinstance(data, list):
                        docs = [DecisionDocument(**item) for item in data]
                    if not docs:
                        raise ValueError(f"Unrecognised data shape: {type(data).__name__}")
                    for doc in docs:
                        yield doc
                        yielded += 1
                except Exception as exc:
                    errors += 1
                    logger.warning(
                        f"BlobDecisionReader: skipping malformed line in '{blob_name}': {exc}"
                    )

        logger.info(
            f"BlobDecisionReader: '{blob_name}' — yielded {yielded} documents"
            + (f", skipped {errors} malformed lines" if errors else "")
        )

    def get_blob_date_range(self, blob_name: str) -> Tuple[date, date]:
        """Parse batch_start and batch_end from the blob filename.

        Expects the blob name to end with 'YYYY-MM-DD_YYYY-MM-DD.ndjson.gz'.

        Args:
            blob_name: Blob name (full path or suffix portion).

        Returns:
            Tuple of (batch_start, batch_end) as date objects.

        Raises:
            ValueError: If the filename does not match the expected pattern.
        """
        match = _BLOB_DATE_PATTERN.search(blob_name)
        if not match:
            raise ValueError(
                f"Cannot parse date range from blob name '{blob_name}'. "
                "Expected suffix YYYY-MM-DD_YYYY-MM-DD.ndjson.gz"
            )
        batch_start = date.fromisoformat(match.group(1))
        batch_end = date.fromisoformat(match.group(2))
        return batch_start, batch_end

    def _get_blob_service_client(self) -> BlobServiceClient:
        """Return a lazily-created BlobServiceClient."""
        if self._blob_service_client is not None:
            return self._blob_service_client

        if self._connection_string:
            self._blob_service_client = BlobServiceClient.from_connection_string(
                self._connection_string
            )
            logger.debug("BlobDecisionReader: using connection string authentication")
        elif self._account_url:
            credential = DefaultAzureCredential()
            self._blob_service_client = BlobServiceClient(
                account_url=self._account_url,
                credential=credential,
            )
            logger.debug("BlobDecisionReader: using DefaultAzureCredential authentication")
        else:
            raise ValueError(
                "BlobDecisionReader requires either AZURE_BLOB_CONNECTION_STRING "
                "or AZURE_BLOB_ACCOUNT_URL to be configured."
            )

        return self._blob_service_client
