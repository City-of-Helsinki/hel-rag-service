"""
Unit tests for ParquetEmbeddingSaver.
"""

import threading
from datetime import datetime
from unittest.mock import MagicMock, patch

import pyarrow.parquet as pq

from app.services.embedder import EmbeddingResult
from app.services.parquet_embedding_saver import ParquetEmbeddingSaver

BATCH_START = datetime(2025, 1, 1)
BATCH_END = datetime(2025, 1, 7)


def _make_saver(connection_string: str = "fake-connection-string") -> ParquetEmbeddingSaver:
    return ParquetEmbeddingSaver(
        container_name="test-embeddings",
        blob_prefix="embeddings",
        connection_string=connection_string,
    )


def _make_results(n: int = 2, model: str = "text-embedding-3-large") -> list[EmbeddingResult]:
    return [
        EmbeddingResult(
            chunk_id=f"chunk-{i}",
            embedding=[float(i)] * 4,
            model=model,
            tokens_used=10,
        )
        for i in range(n)
    ]


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
        assert name == "embeddings/2025-01-01_2025-01-07.parquet"

    def test_blob_name_custom_prefix(self):
        saver = ParquetEmbeddingSaver(
            container_name="c",
            blob_prefix="exports/embeddings",
            connection_string="x",
        )
        name = saver._build_blob_name(datetime(2024, 3, 1), datetime(2024, 3, 31))
        assert name == "exports/embeddings/2024-03-01_2024-03-31.parquet"


class TestStartBatch:
    def test_start_batch_sets_dates(self):
        saver = _make_saver()
        saver.start_batch(BATCH_START, BATCH_END)
        assert saver._batch_start == BATCH_START
        assert saver._batch_end == BATCH_END

    def test_start_batch_resets_buffer(self):
        saver = _make_saver()
        saver.start_batch(BATCH_START, BATCH_END)
        saver.buffer(_make_results(3), "decision-001")

        # A second start_batch must clear the buffer
        saver.start_batch(datetime(2025, 1, 8), datetime(2025, 1, 14))
        assert saver._buffer == []


class TestBuffer:
    def test_buffer_accumulates_rows(self):
        saver = _make_saver()
        saver.start_batch(BATCH_START, BATCH_END)
        saver.buffer(_make_results(3), "native-001")
        saver.buffer(_make_results(2), "native-002")
        assert len(saver._buffer) == 5

    def test_buffer_row_shape(self):
        saver = _make_saver()
        saver.start_batch(BATCH_START, BATCH_END)
        results = _make_results(1)
        saver.buffer(results, "native-abc")

        row = saver._buffer[0]
        assert row["chunk_id"] == "chunk-0"
        assert row["native_id"] == "native-abc"
        assert row["embedding"] == results[0].embedding
        assert row["embedding_model"] == "text-embedding-3-large"
        assert row["embedding_dimensions"] == 4
        assert "created_at" in row


class TestThreadSafety:
    def test_concurrent_buffer_calls_no_data_loss(self):
        saver = _make_saver()
        saver.start_batch(BATCH_START, BATCH_END)

        num_threads = 20
        results_per_thread = 5

        def _write(native_id: str) -> None:
            saver.buffer(_make_results(results_per_thread), native_id)

        threads = [
            threading.Thread(target=_write, args=(f"native-{t}",))
            for t in range(num_threads)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(saver._buffer) == num_threads * results_per_thread


class TestFlushBatch:
    @patch("app.services.parquet_embedding_saver.BlobServiceClient.from_connection_string")
    def test_buffer_and_flush_success(self, mock_from_conn):
        service_client, container_client, blob_client = _mock_blob_service_client()
        mock_from_conn.return_value = service_client

        saver = _make_saver()
        saver.start_batch(BATCH_START, BATCH_END)
        saver.buffer(_make_results(3), "native-001")
        saver.buffer(_make_results(2), "native-002")

        result = saver.flush_batch()

        assert result is True
        blob_client.upload_blob.assert_called_once()
        call_args = blob_client.upload_blob.call_args
        assert call_args[1]["overwrite"] is True

        # Buffer must be cleared after successful flush
        assert saver._buffer == []

    @patch("app.services.parquet_embedding_saver.BlobServiceClient.from_connection_string")
    def test_flush_empty_batch(self, mock_from_conn):
        service_client, container_client, blob_client = _mock_blob_service_client()
        mock_from_conn.return_value = service_client

        saver = _make_saver()
        saver.start_batch(BATCH_START, BATCH_END)

        result = saver.flush_batch()

        assert result is True
        blob_client.upload_blob.assert_not_called()

    @patch("app.services.parquet_embedding_saver.BlobServiceClient.from_connection_string")
    def test_parquet_schema(self, mock_from_conn):
        """The uploaded bytes must deserialise to a table with the expected schema."""
        import io

        captured = {}

        def _capture_upload(data, **kwargs):
            captured["data"] = data

        service_client, container_client, blob_client = _mock_blob_service_client()
        blob_client.upload_blob.side_effect = _capture_upload
        mock_from_conn.return_value = service_client

        saver = _make_saver()
        saver.start_batch(BATCH_START, BATCH_END)
        saver.buffer(_make_results(2), "native-xyz")
        saver.flush_batch()

        table = pq.read_table(io.BytesIO(captured["data"]))
        assert table.schema.names == [
            "chunk_id",
            "native_id",
            "embedding",
            "embedding_model",
            "embedding_dimensions",
            "created_at",
        ]
        assert table.num_rows == 2

    @patch("app.services.parquet_embedding_saver.BlobServiceClient.from_connection_string")
    def test_flush_upload_failure_returns_false(self, mock_from_conn):
        service_client, container_client, blob_client = _mock_blob_service_client()
        blob_client.upload_blob.side_effect = RuntimeError("network error")
        mock_from_conn.return_value = service_client

        saver = _make_saver()
        saver.start_batch(BATCH_START, BATCH_END)
        saver.buffer(_make_results(1), "native-001")

        result = saver.flush_batch()

        assert result is False  # must not raise

    @patch("app.services.parquet_embedding_saver.BlobServiceClient.from_connection_string")
    def test_container_created_if_missing(self, mock_from_conn):
        from azure.core.exceptions import ResourceNotFoundError

        service_client, container_client, blob_client = _mock_blob_service_client()
        container_client.get_container_properties.side_effect = ResourceNotFoundError("missing")
        mock_from_conn.return_value = service_client

        saver = _make_saver()
        saver.start_batch(BATCH_START, BATCH_END)
        saver.buffer(_make_results(1), "native-001")
        saver.flush_batch()

        container_client.create_container.assert_called_once()

    @patch("app.services.parquet_embedding_saver.BlobServiceClient.from_connection_string")
    def test_container_race_condition_tolerated(self, mock_from_conn):
        """ResourceExistsError during create_container must not cause flush to fail."""
        from azure.core.exceptions import ResourceExistsError, ResourceNotFoundError

        service_client, container_client, blob_client = _mock_blob_service_client()
        container_client.get_container_properties.side_effect = ResourceNotFoundError("missing")
        container_client.create_container.side_effect = ResourceExistsError("already exists")
        mock_from_conn.return_value = service_client

        saver = _make_saver()
        saver.start_batch(BATCH_START, BATCH_END)
        saver.buffer(_make_results(1), "native-001")

        result = saver.flush_batch()

        assert result is True
        blob_client.upload_blob.assert_called_once()
