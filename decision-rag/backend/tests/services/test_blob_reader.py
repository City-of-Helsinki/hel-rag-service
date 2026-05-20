"""
Unit tests for BlobDecisionReader.
"""

import gzip
import json
from datetime import date
from unittest.mock import MagicMock, patch

import pytest

from app.services.blob_reader import BlobDecisionReader

CONTAINER = "test-container"
PREFIX = "api_responses"


def _make_reader(**kwargs) -> BlobDecisionReader:
    return BlobDecisionReader(
        container_name=CONTAINER,
        blob_prefix=PREFIX,
        connection_string="fake-connection-string",
        **kwargs,
    )


def _make_ndjson_gz(entries: list) -> bytes:
    """Encode a list of dicts as gzip-compressed NDJSON bytes."""
    lines = "\n".join(json.dumps(e, ensure_ascii=False) for e in entries)
    return gzip.compress(lines.encode("utf-8"), compresslevel=1)


def _blob_item(name: str):
    item = MagicMock()
    item.name = name
    return item


def _mock_service_client(blob_names: list):
    """Return a mocked BlobServiceClient whose container lists the given blob names."""
    blob_items = [_blob_item(n) for n in blob_names]

    container_client = MagicMock()
    container_client.list_blobs.return_value = blob_items

    service_client = MagicMock()
    service_client.get_container_client.return_value = container_client

    return service_client, container_client


class TestGetBlobDateRange:
    def test_parses_simple_name(self):
        reader = _make_reader()
        start, end = reader.get_blob_date_range(
            "api_responses/2025-01-01_2025-01-07.ndjson.gz"
        )
        assert start == date(2025, 1, 1)
        assert end == date(2025, 1, 7)

    def test_parses_nested_prefix(self):
        reader = _make_reader()
        start, end = reader.get_blob_date_range(
            "some/deep/prefix/2024-06-15_2024-06-30.ndjson.gz"
        )
        assert start == date(2024, 6, 15)
        assert end == date(2024, 6, 30)

    def test_raises_on_bad_name(self):
        reader = _make_reader()
        with pytest.raises(ValueError, match="Cannot parse date range"):
            reader.get_blob_date_range("api_responses/no_dates_here.ndjson.gz")

    def test_raises_on_missing_suffix(self):
        reader = _make_reader()
        with pytest.raises(ValueError):
            reader.get_blob_date_range("api_responses/2025-01-01_2025-01-07.json")


class TestListBlobs:
    def _setup_reader(self, blob_names: list):
        reader = _make_reader()
        service_client, _ = _mock_service_client(blob_names)
        reader._blob_service_client = service_client
        return reader

    def test_returns_all_ndjson_gz_blobs_without_filter(self):
        names = [
            f"{PREFIX}/2025-01-01_2025-01-01.ndjson.gz",
            f"{PREFIX}/2025-01-02_2025-01-02.ndjson.gz",
            f"{PREFIX}/other_file.txt",
        ]
        reader = self._setup_reader(names)
        result = reader.list_blobs()
        assert len(result) == 2
        assert all(n.endswith(".ndjson.gz") for n in result)

    def test_sorted_chronologically(self):
        names = [
            f"{PREFIX}/2025-03-01_2025-03-01.ndjson.gz",
            f"{PREFIX}/2025-01-01_2025-01-01.ndjson.gz",
            f"{PREFIX}/2025-02-01_2025-02-01.ndjson.gz",
        ]
        reader = self._setup_reader(names)
        result = reader.list_blobs()
        assert result[0].startswith(f"{PREFIX}/2025-01")
        assert result[1].startswith(f"{PREFIX}/2025-02")
        assert result[2].startswith(f"{PREFIX}/2025-03")

    def test_date_filter_includes_overlapping_blob(self):
        # Blob covers Jan 1–7, filter asks for Jan 5–10 → overlaps
        names = [f"{PREFIX}/2025-01-01_2025-01-07.ndjson.gz"]
        reader = self._setup_reader(names)
        result = reader.list_blobs(start_date=date(2025, 1, 5), end_date=date(2025, 1, 10))
        assert len(result) == 1

    def test_date_filter_excludes_non_overlapping_before(self):
        # Blob ends Jan 7, filter starts Jan 10 → no overlap
        names = [f"{PREFIX}/2025-01-01_2025-01-07.ndjson.gz"]
        reader = self._setup_reader(names)
        result = reader.list_blobs(start_date=date(2025, 1, 10), end_date=date(2025, 1, 20))
        assert len(result) == 0

    def test_date_filter_excludes_non_overlapping_after(self):
        # Blob starts Jan 10, filter ends Jan 7 → no overlap
        names = [f"{PREFIX}/2025-01-10_2025-01-20.ndjson.gz"]
        reader = self._setup_reader(names)
        result = reader.list_blobs(start_date=date(2025, 1, 1), end_date=date(2025, 1, 7))
        assert len(result) == 0

    def test_date_filter_with_only_start_date(self):
        names = [
            f"{PREFIX}/2025-01-01_2025-01-07.ndjson.gz",
            f"{PREFIX}/2025-01-10_2025-01-16.ndjson.gz",
        ]
        reader = self._setup_reader(names)
        # start_date = Jan 8 means the first blob (ending Jan 7) is excluded
        result = reader.list_blobs(start_date=date(2025, 1, 8))
        assert len(result) == 1
        assert "2025-01-10" in result[0]

    def test_date_filter_with_only_end_date(self):
        names = [
            f"{PREFIX}/2025-01-01_2025-01-07.ndjson.gz",
            f"{PREFIX}/2025-01-10_2025-01-16.ndjson.gz",
        ]
        reader = self._setup_reader(names)
        result = reader.list_blobs(end_date=date(2025, 1, 8))
        assert len(result) == 1
        assert "2025-01-01" in result[0]

    def test_skips_blobs_with_unparseable_names_when_filtering(self):
        names = [
            f"{PREFIX}/2025-01-01_2025-01-07.ndjson.gz",
            f"{PREFIX}/bad_name.ndjson.gz",
        ]
        reader = self._setup_reader(names)
        # bad_name is skipped with a warning, not an exception
        result = reader.list_blobs(start_date=date(2025, 1, 1), end_date=date(2025, 1, 31))
        assert len(result) == 1

    def test_empty_container(self):
        reader = self._setup_reader([])
        result = reader.list_blobs()
        assert result == []


class TestIterDocuments:
    _BLOB_NAME = f"{PREFIX}/2025-01-01_2025-01-07.ndjson.gz"

    def _setup_blob(self, compressed_data: bytes, reader: BlobDecisionReader = None):
        if reader is None:
            reader = _make_reader()

        blob_client = MagicMock()
        blob_client.download_blob.return_value.chunks.return_value = [compressed_data]

        container_client = MagicMock()
        container_client.get_blob_client.return_value = blob_client

        service_client = MagicMock()
        service_client.get_container_client.return_value = container_client

        reader._blob_service_client = service_client
        return reader

    def _sample_entry(self, native_id: str) -> dict:
        return {
            "native_id": native_id,
            "fetched_at": "2025-01-01T12:00:00Z",
            "data": {
                "NativeId": native_id,
                "Title": f"Decision {native_id}",
                "DateDecision": "2025-01-01",
            },
        }

    def test_yields_correct_documents(self):
        entries = [self._sample_entry("ID-001"), self._sample_entry("ID-002")]
        reader = self._setup_blob(_make_ndjson_gz(entries))
        docs = list(reader.iter_documents(self._BLOB_NAME))
        assert len(docs) == 2
        assert docs[0].NativeId == "ID-001"
        assert docs[1].NativeId == "ID-002"

    def test_skips_malformed_lines_and_continues(self):
        good_entry = self._sample_entry("ID-001")
        raw = (
            json.dumps(good_entry) + "\n"
            + "{this is not valid json\n"
            + json.dumps(self._sample_entry("ID-002"))
        ).encode("utf-8")
        compressed = gzip.compress(raw, compresslevel=1)
        reader = self._setup_blob(compressed)
        docs = list(reader.iter_documents(self._BLOB_NAME))
        assert len(docs) == 2
        assert {d.NativeId for d in docs} == {"ID-001", "ID-002"}

    def test_skips_empty_lines(self):
        entries = [self._sample_entry("ID-001")]
        raw = ("\n" + json.dumps(entries[0]) + "\n\n").encode("utf-8")
        compressed = gzip.compress(raw, compresslevel=1)
        reader = self._setup_blob(compressed)
        docs = list(reader.iter_documents(self._BLOB_NAME))
        assert len(docs) == 1

    def test_raises_on_download_error(self):
        reader = _make_reader()

        blob_client = MagicMock()
        blob_client.download_blob.side_effect = RuntimeError("network error")

        container_client = MagicMock()
        container_client.get_blob_client.return_value = blob_client

        service_client = MagicMock()
        service_client.get_container_client.return_value = container_client

        reader._blob_service_client = service_client
        with pytest.raises(RuntimeError, match="network error"):
            list(reader.iter_documents(self._BLOB_NAME))

    def test_empty_blob(self):
        compressed = gzip.compress(b"", compresslevel=1)
        reader = self._setup_blob(compressed)
        docs = list(reader.iter_documents(self._BLOB_NAME))
        assert docs == []


class TestAuthentication:
    def test_connection_string_auth(self):
        reader = BlobDecisionReader(
            container_name=CONTAINER,
            blob_prefix=PREFIX,
            connection_string="DefaultEndpointsProtocol=https;AccountName=test",
        )
        with patch(
            "app.services.blob_reader.BlobServiceClient.from_connection_string"
        ) as mock_from_cs:
            mock_from_cs.return_value = MagicMock()
            client = reader._get_blob_service_client()
            mock_from_cs.assert_called_once()
            assert client is mock_from_cs.return_value

    def test_account_url_auth(self):
        reader = BlobDecisionReader(
            container_name=CONTAINER,
            blob_prefix=PREFIX,
            connection_string="",  # suppress settings fallback
            account_url="https://myaccount.blob.core.windows.net",
        )
        with patch("app.services.blob_reader.DefaultAzureCredential") as mock_cred, \
             patch("app.services.blob_reader.BlobServiceClient") as mock_svc:
            mock_svc.return_value = MagicMock()
            client = reader._get_blob_service_client()
            mock_cred.assert_called_once()
            mock_svc.assert_called_once()
            assert client is mock_svc.return_value

    def test_raises_when_no_auth_configured(self):
        reader = BlobDecisionReader(
            container_name=CONTAINER,
            blob_prefix=PREFIX,
            connection_string="",
            account_url="",
        )
        with pytest.raises(ValueError, match="AZURE_BLOB_CONNECTION_STRING"):
            reader._get_blob_service_client()

    def test_client_is_cached(self):
        reader = _make_reader()
        with patch(
            "app.services.blob_reader.BlobServiceClient.from_connection_string"
        ) as mock_from_cs:
            mock_from_cs.return_value = MagicMock()
            first = reader._get_blob_service_client()
            second = reader._get_blob_service_client()
            mock_from_cs.assert_called_once()
            assert first is second
