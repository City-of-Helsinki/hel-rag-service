"""
Unit tests for PgvectorVectorStore.

All PostgreSQL interactions are mocked; no live database is required.
"""

from unittest.mock import MagicMock, patch

import pytest

from app.services.pgvector_store import PgvectorVectorStore

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def mock_conn():
    """Return a mock psycopg2 connection with a mock cursor via context manager."""
    conn = MagicMock()
    cursor = MagicMock()
    conn.closed = 0  # mark as open so _ensure_connection does not trigger reconnect
    # Support both plain cursor() and cursor(cursor_factory=...) calls
    conn.cursor.return_value.__enter__ = MagicMock(return_value=cursor)
    conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
    return conn, cursor


@pytest.fixture()
def store(mock_conn):
    """Return a PgvectorVectorStore with all DB calls mocked out during init."""
    conn, _ = mock_conn
    with patch("app.services.pgvector_store.psycopg2.connect", return_value=conn):
        s = PgvectorVectorStore(
            host="localhost",
            port=5432,
            db="decisions",
            user="postgres",
            password="postgres",
            table="decision_chunks",
            vector_dims=3,
        )
    s.conn = conn
    return s


def _make_chunk(chunk_id: str = "chunk-1", native_id: str = "native-1") -> dict:
    return {
        "chunk_id": chunk_id,
        "native_id": native_id,
        "chunk_index": 0,
        "text": "Hello world",
        "embedding": [0.1, 0.2, 0.3],
        "token_count": 2,
        "chunk_position": 0,
        "metadata": {"title": "Test"},
        "collection": "decisions",
    }


# ---------------------------------------------------------------------------
# bulk_index_chunks
# ---------------------------------------------------------------------------


class TestBulkIndexChunks:
    def test_empty_list_returns_zeros(self, store):
        result = store.bulk_index_chunks([])
        assert result == {"success": 0, "failed": 0, "errors": []}

    def test_upsert_sql_executed_for_each_chunk(self, store, mock_conn):
        conn, cursor = mock_conn
        chunks = [_make_chunk("c1"), _make_chunk("c2")]
        store.bulk_index_chunks(chunks)

        # cursor.execute should have been called once per chunk
        assert cursor.execute.call_count == 2
        # Check that the INSERT ... ON CONFLICT clause is present
        first_sql = cursor.execute.call_args_list[0][0][0]
        assert "INSERT INTO" in first_sql
        assert "ON CONFLICT" in first_sql

    def test_commit_called_on_success(self, store, mock_conn):
        conn, _ = mock_conn
        store.bulk_index_chunks([_make_chunk()])
        conn.commit.assert_called()

    def test_rollback_on_operational_error(self, store, mock_conn):
        import psycopg2

        conn, cursor = mock_conn
        cursor.execute.side_effect = psycopg2.OperationalError("connection lost")

        with (
            patch.object(store, "_reconnect"),
            patch("time.sleep"),
            pytest.raises(psycopg2.OperationalError),
        ):
            store.bulk_index_chunks([_make_chunk()])

        conn.rollback.assert_called()

    def test_returns_correct_counts(self, store):
        chunks = [_make_chunk("c1"), _make_chunk("c2"), _make_chunk("c3")]
        result = store.bulk_index_chunks(chunks)
        assert result["success"] == 3
        assert result["failed"] == 0
        assert result["errors"] == []


# ---------------------------------------------------------------------------
# document_exists
# ---------------------------------------------------------------------------


class TestDocumentExists:
    def test_returns_true_when_row_found(self, store, mock_conn):
        _, cursor = mock_conn
        cursor.fetchone.return_value = (1,)
        assert store.document_exists("native-1") is True

    def test_returns_false_when_no_row(self, store, mock_conn):
        _, cursor = mock_conn
        cursor.fetchone.return_value = None
        assert store.document_exists("native-1") is False

    def test_query_uses_native_id_and_metadata(self, store, mock_conn):
        _, cursor = mock_conn
        cursor.fetchone.return_value = None
        store.document_exists("native-42")
        sql = cursor.execute.call_args[0][0]
        assert "native_id" in sql
        assert "decision_native_id" in sql


# ---------------------------------------------------------------------------
# delete_document
# ---------------------------------------------------------------------------


class TestDeleteDocument:
    def test_returns_deleted_count(self, store, mock_conn):
        _, cursor = mock_conn
        cursor.rowcount = 5
        assert store.delete_document("native-1") == 5

    def test_executes_delete_query(self, store, mock_conn):
        _, cursor = mock_conn
        cursor.rowcount = 0
        store.delete_document("native-1")
        sql = cursor.execute.call_args[0][0]
        assert "DELETE FROM" in sql

    def test_rollback_on_operational_error(self, store, mock_conn):
        import psycopg2

        conn, cursor = mock_conn
        cursor.execute.side_effect = psycopg2.OperationalError("gone")

        with (
            patch.object(store, "_reconnect"),
            patch("time.sleep"),
            pytest.raises(psycopg2.OperationalError),
        ):
            store.delete_document("native-1")

        conn.rollback.assert_called()


# ---------------------------------------------------------------------------
# delete_attachments
# ---------------------------------------------------------------------------


class TestDeleteAttachments:
    def test_returns_deleted_count(self, store, mock_conn):
        _, cursor = mock_conn
        cursor.rowcount = 3
        assert store.delete_attachments("native-1") == 3

    def test_query_filters_is_attachment(self, store, mock_conn):
        _, cursor = mock_conn
        cursor.rowcount = 0
        store.delete_attachments("native-1")
        sql = cursor.execute.call_args[0][0]
        assert "is_attachment" in sql


# ---------------------------------------------------------------------------
# search
# ---------------------------------------------------------------------------


class TestSearch:
    def test_returns_expected_shape(self, store, mock_conn):
        conn, _ = mock_conn
        # RealDictCursor rows are dict-like; use plain dicts here
        row = {
            "id": "chunk-1",
            "collection_name": "decisions",
            "text": "hello",
            "vmetadata": {"title": "T"},
            "score": 0.95,
        }
        # The search method calls cursor(cursor_factory=...) so mock that path
        real_dict_cursor = MagicMock()
        real_dict_cursor.__enter__ = MagicMock(return_value=real_dict_cursor)
        real_dict_cursor.__exit__ = MagicMock(return_value=False)
        real_dict_cursor.fetchall.return_value = [row]
        conn.cursor.return_value = real_dict_cursor

        results = store.search([0.1, 0.2, 0.3], top_k=1)

        assert len(results) == 1
        r = results[0]
        assert r["chunk_id"] == "chunk-1"
        assert r["native_id"] == ""  # native_id now comes from vmetadata
        assert r["text"] == "hello"
        assert r["score"] == pytest.approx(0.95)
        assert r["metadata"] == {"title": "T"}

    def test_empty_results(self, store, mock_conn):
        conn, _ = mock_conn
        real_dict_cursor = MagicMock()
        real_dict_cursor.__enter__ = MagicMock(return_value=real_dict_cursor)
        real_dict_cursor.__exit__ = MagicMock(return_value=False)
        real_dict_cursor.fetchall.return_value = []
        conn.cursor.return_value = real_dict_cursor

        results = store.search([0.1, 0.2, 0.3])
        assert results == []

    def test_returns_empty_list_on_generic_error(self, store, mock_conn):
        conn, _ = mock_conn
        conn.cursor.side_effect = Exception("boom")
        results = store.search([0.1, 0.2, 0.3])
        assert results == []


# ---------------------------------------------------------------------------
# _reconnect
# ---------------------------------------------------------------------------


class TestReconnect:
    def test_closes_open_connection_and_opens_new_one(self, store):
        old_conn = store.conn
        old_conn.closed = 0  # open
        new_conn = MagicMock()
        with patch("app.services.pgvector_store.psycopg2.connect", return_value=new_conn) as mock_connect:
            store._reconnect()
        old_conn.close.assert_called_once()
        mock_connect.assert_called_once_with(
            host=store.host,
            port=store.port,
            dbname=store.db,
            user=store.user,
            password=store.password,
        )
        assert store.conn is new_conn

    def test_skips_close_when_already_closed(self, store):
        old_conn = store.conn
        old_conn.closed = 1  # already closed
        new_conn = MagicMock()
        with patch("app.services.pgvector_store.psycopg2.connect", return_value=new_conn):
            store._reconnect()
        old_conn.close.assert_not_called()
        assert store.conn is new_conn

    def test_sets_autocommit_false_on_new_connection(self, store):
        store.conn.closed = 0
        new_conn = MagicMock()
        with patch("app.services.pgvector_store.psycopg2.connect", return_value=new_conn):
            store._reconnect()
        assert new_conn.autocommit is False

    def test_propagates_connect_failure(self, store):
        import psycopg2

        store.conn.closed = 0
        with patch(
            "app.services.pgvector_store.psycopg2.connect",
            side_effect=psycopg2.OperationalError("cannot connect"),
        ):
            with pytest.raises(psycopg2.OperationalError):
                store._reconnect()


# ---------------------------------------------------------------------------
# _ensure_connection
# ---------------------------------------------------------------------------


class TestEnsureConnection:
    def test_calls_reconnect_when_connection_is_closed(self, store):
        store.conn.closed = 1
        with patch.object(store, "_reconnect") as mock_reconnect:
            store._ensure_connection()
        mock_reconnect.assert_called_once()

    def test_does_not_reconnect_when_connection_is_open(self, store):
        store.conn.closed = 0
        with patch.object(store, "_reconnect") as mock_reconnect:
            store._ensure_connection()
        mock_reconnect.assert_not_called()


# ---------------------------------------------------------------------------
# _execute_with_retry
# ---------------------------------------------------------------------------


class TestExecuteWithRetry:
    def test_returns_result_on_first_attempt(self, store):
        op = MagicMock(return_value=42)
        result = store._execute_with_retry(op)
        assert result == 42
        op.assert_called_once()

    def test_resets_retry_count_on_success(self, store):
        store._retry_count = 2
        op = MagicMock(return_value="ok")
        store._execute_with_retry(op)
        assert store._retry_count == 0

    def test_reconnects_and_retries_on_operational_error(self, store):
        import psycopg2

        op = MagicMock(side_effect=[psycopg2.OperationalError("SSL drop"), "recovered"])
        with (
            patch.object(store, "_reconnect") as mock_reconnect,
            patch("time.sleep"),
        ):
            result = store._execute_with_retry(op)

        assert result == "recovered"
        assert op.call_count == 2
        mock_reconnect.assert_called_once()

    def test_rollback_called_before_reconnect(self, store, mock_conn):
        import psycopg2

        conn, _ = mock_conn
        op = MagicMock(side_effect=[psycopg2.OperationalError("SSL drop"), "ok"])
        with (
            patch.object(store, "_reconnect"),
            patch("time.sleep"),
        ):
            store._execute_with_retry(op)

        conn.rollback.assert_called()

    def test_sleep_duration_uses_exponential_backoff(self, store):
        import psycopg2

        # Two failures then success: sleep called twice
        op = MagicMock(
            side_effect=[
                psycopg2.OperationalError("drop"),
                psycopg2.OperationalError("drop"),
                "ok",
            ]
        )
        with (
            patch.object(store, "_reconnect"),
            patch("time.sleep") as mock_sleep,
        ):
            store._execute_with_retry(op)

        assert mock_sleep.call_count == 2
        # attempt 0 → base * 2^0 = 1.0, attempt 1 → base * 2^1 = 2.0
        sleep_args = [call[0][0] for call in mock_sleep.call_args_list]
        assert sleep_args[0] == pytest.approx(1.0)
        assert sleep_args[1] == pytest.approx(2.0)

    def test_sleep_capped_at_30_seconds(self, store):
        import psycopg2

        # Force a large attempt number by using a high max_retries mock
        # Simulate attempt 6: base * 2^6 = 64, should be capped at 30
        op = MagicMock(side_effect=[psycopg2.OperationalError("drop")] * 7 + ["ok"])
        with (
            patch("app.services.pgvector_store.settings") as mock_settings,
            patch.object(store, "_reconnect"),
            patch("time.sleep") as mock_sleep,
        ):
            mock_settings.PGVECTOR_OP_MAX_RETRIES = 7
            mock_settings.PGVECTOR_RETRY_BACKOFF_BASE = 1.0
            store._execute_with_retry(op)

        sleep_values = [call[0][0] for call in mock_sleep.call_args_list]
        assert all(v <= 30.0 for v in sleep_values)

    def test_raises_after_max_retries_exhausted(self, store):
        import psycopg2

        op = MagicMock(side_effect=psycopg2.OperationalError("persistent"))
        with (
            patch("app.services.pgvector_store.settings") as mock_settings,
            patch.object(store, "_reconnect"),
            patch("time.sleep"),
        ):
            mock_settings.PGVECTOR_OP_MAX_RETRIES = 2
            mock_settings.PGVECTOR_RETRY_BACKOFF_BASE = 0.0
            with pytest.raises(psycopg2.OperationalError):
                store._execute_with_retry(op)

        # 1 original attempt + 2 retries = 3 total calls
        assert op.call_count == 3

    def test_max_retries_exhausted_increments_global_counter(self, store):
        import psycopg2

        store._retry_count = 0
        op = MagicMock(side_effect=psycopg2.OperationalError("gone"))
        with (
            patch("app.services.pgvector_store.settings") as mock_settings,
            patch.object(store, "_reconnect"),
            patch("time.sleep"),
        ):
            mock_settings.PGVECTOR_OP_MAX_RETRIES = 1
            mock_settings.PGVECTOR_RETRY_BACKOFF_BASE = 0.0
            with pytest.raises(psycopg2.OperationalError):
                store._execute_with_retry(op)

        assert store._retry_count >= 1

    def test_non_operational_error_propagates_immediately(self, store):
        op = MagicMock(side_effect=ValueError("logic error"))
        with patch.object(store, "_reconnect") as mock_reconnect:
            with pytest.raises(ValueError, match="logic error"):
                store._execute_with_retry(op)

        # No reconnect attempted for non-connection errors
        mock_reconnect.assert_not_called()
        op.assert_called_once()
