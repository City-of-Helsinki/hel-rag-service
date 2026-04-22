"""
Unit tests for AzureBlobRawResponseSaver.
"""

import gzip
import json
import threading
from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest

from app.services.blob_storage import AzureBlobRawResponseSaver

BATCH_START = datetime(2025, 1, 1)
BATCH_END = datetime(2025, 1, 7)


def _make_saver(connection_string: str = "fake-connection-string") -> AzureBlobRawResponseSaver:
    return AzureBlobRawResponseSaver(
        container_name="test-container",
        blob_prefix="raw_responses",
        connection_string=connection_string,
    )


def _mock_blob_service_client():
    """Return a fully mocked BlobServiceClient chain."""
    blob_client = MagicMock()
    container_client = MagicMock()
    container_client.get_blob_client.return_value = blob_client
    service_client = MagicMock()
    service_client.get_container_client.return_value = container_client
    return service_client, container_client, blob_client


class TestBlobName:
    def test_blob_name_format(self):
        saver = _make_saver()
        name = saver._build_blob_name(BATCH_START, BATCH_END)
        assert name == "raw_responses/2025-01-01_2025-01-07.ndjson.gz"

    def test_blob_name_custom_prefix(self):
        saver = AzureBlobRawResponseSaver(
            container_name="c",
            blob_prefix="archive/decisions",
            connection_string="x",
        )
        name = saver._build_blob_name(datetime(2024, 3, 1), datetime(2024, 3, 31))
        assert name == "archive/decisions/2024-03-01_2024-03-31.ndjson.gz"


class TestStartBatch:
    def test_start_batch_sets_dates(self):
        saver = _make_saver()
        saver.buffer("id1", {"key": "value"})
        saver.start_batch(BATCH_START, BATCH_END)
        assert saver._batch_start == BATCH_START
        assert saver._batch_end == BATCH_END

    def test_start_batch_resets_buffer(self):
        saver = _make_saver()
        saver.start_batch(BATCH_START, BATCH_END)
        saver.buffer("id1", {"a": 1})
        saver.buffer("id2", {"b": 2})

        # Second start_batch should clear the buffer
        saver.start_batch(datetime(2025, 1, 8), datetime(2025, 1, 14))
        assert saver._buffer == []


class TestBuffer:
    def test_buffer_accumulates_entries(self):
        saver = _make_saver()
        saver.start_batch(BATCH_START, BATCH_END)
        saver.buffer("id1", {"x": 1})
        saver.buffer("id2", {"x": 2})
        assert len(saver._buffer) == 2

    def test_buffer_entry_shape(self):
        saver = _make_saver()
        saver.start_batch(BATCH_START, BATCH_END)
        saver.buffer("TEST-001", {"field": "value"})
        entry = saver._buffer[0]
        assert entry["native_id"] == "TEST-001"
        assert entry["data"] == {"field": "value"}
        assert "fetched_at" in entry

    def test_buffer_thread_safe(self):
        saver = _make_saver()
        saver.start_batch(BATCH_START, BATCH_END)
        num_threads = 20
        calls_per_thread = 25

        def _write(thread_idx: int) -> None:
            for i in range(calls_per_thread):
                saver.buffer(f"id-{thread_idx}-{i}", {"v": thread_idx * 100 + i})

        threads = [threading.Thread(target=_write, args=(t,)) for t in range(num_threads)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(saver._buffer) == num_threads * calls_per_thread


class TestFlushBatch:
    @patch("app.services.blob_storage.BlobServiceClient.from_connection_string")
    def test_flush_uploads_ndjson_gz(self, mock_from_conn):
        service_client, container_client, blob_client = _mock_blob_service_client()
        mock_from_conn.return_value = service_client

        saver = _make_saver()
        saver.start_batch(BATCH_START, BATCH_END)
        saver.buffer("id1", {"a": 1})
        saver.buffer("id2", {"b": 2})

        result = saver.flush_batch()

        assert result is True
        blob_client.upload_blob.assert_called_once()
        call_kwargs = blob_client.upload_blob.call_args

        # Verify the uploaded data is valid gzip NDJSON
        uploaded_data = call_kwargs[0][0]
        decompressed = gzip.decompress(uploaded_data).decode("utf-8")
        lines = decompressed.strip().split("\n")
        assert len(lines) == 2
        obj1 = json.loads(lines[0])
        assert obj1["native_id"] == "id1"
        assert obj1["data"] == {"a": 1}

    @patch("app.services.blob_storage.BlobServiceClient.from_connection_string")
    def test_flush_empty_batch_skips_upload(self, mock_from_conn):
        service_client, container_client, blob_client = _mock_blob_service_client()
        mock_from_conn.return_value = service_client

        saver = _make_saver()
        saver.start_batch(BATCH_START, BATCH_END)

        result = saver.flush_batch()

        assert result is True
        blob_client.upload_blob.assert_not_called()

    @patch("app.services.blob_storage.BlobServiceClient.from_connection_string")
    def test_flush_creates_container_if_missing(self, mock_from_conn):
        from azure.core.exceptions import ResourceNotFoundError

        service_client, container_client, blob_client = _mock_blob_service_client()
        # Simulate container not found on first call to get_container_properties
        container_client.get_container_properties.side_effect = ResourceNotFoundError("not found")
        mock_from_conn.return_value = service_client

        saver = _make_saver()
        saver.start_batch(BATCH_START, BATCH_END)
        saver.buffer("id1", {"a": 1})

        result = saver.flush_batch()

        assert result is True
        container_client.create_container.assert_called_once()
        blob_client.upload_blob.assert_called_once()

    @patch("app.services.blob_storage.BlobServiceClient.from_connection_string")
    def test_flush_does_not_raise_on_upload_error(self, mock_from_conn):
        service_client, container_client, blob_client = _mock_blob_service_client()
        blob_client.upload_blob.side_effect = Exception("Network error")
        mock_from_conn.return_value = service_client

        saver = _make_saver()
        saver.start_batch(BATCH_START, BATCH_END)
        saver.buffer("id1", {"a": 1})

        # Should not raise; should return False
        result = saver.flush_batch()
        assert result is False
        assert saver._flush_failures == 1


class TestGetBlobServiceClient:
    @patch("app.services.blob_storage.BlobServiceClient.from_connection_string")
    def test_uses_connection_string(self, mock_from_conn):
        saver = _make_saver(connection_string="my-conn-str")
        saver._get_blob_service_client()
        mock_from_conn.assert_called_once_with("my-conn-str")

    @patch("app.services.blob_storage.DefaultAzureCredential")
    @patch("app.services.blob_storage.BlobServiceClient")
    def test_uses_default_credential_with_account_url(self, mock_bsc_cls, mock_cred_cls):
        saver = AzureBlobRawResponseSaver(
            container_name="c",
            blob_prefix="p",
            account_url="https://myaccount.blob.core.windows.net",
        )
        saver._get_blob_service_client()
        mock_cred_cls.assert_called_once()
        mock_bsc_cls.assert_called_once_with(
            account_url="https://myaccount.blob.core.windows.net",
            credential=mock_cred_cls.return_value,
        )

    def test_raises_if_neither_configured(self):
        saver = AzureBlobRawResponseSaver(container_name="c", blob_prefix="p")
        with pytest.raises(ValueError, match="AZURE_BLOB_CONNECTION_STRING"):
            saver._get_blob_service_client()

    @patch("app.services.blob_storage.BlobServiceClient.from_connection_string")
    def test_client_is_reused(self, mock_from_conn):
        saver = _make_saver()
        c1 = saver._get_blob_service_client()
        c2 = saver._get_blob_service_client()
        assert c1 is c2
        mock_from_conn.assert_called_once()
