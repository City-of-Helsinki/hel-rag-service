"""
Unit tests for CompositeVectorStore.
"""

from unittest.mock import MagicMock

import pytest

from app.services.vector_store import CompositeVectorStore


def _mock_backend(name: str = "backend") -> MagicMock:
    """Return a MagicMock that looks like a BaseVectorStore."""
    b = MagicMock()
    b.bulk_index_chunks.return_value = {"success": 2, "failed": 0, "errors": []}
    b.index_chunk.return_value = True
    b.document_exists.return_value = False
    b.delete_document.return_value = 3
    b.delete_attachments.return_value = 1
    b.search.return_value = [{"chunk_id": "c1", "score": 0.9}]
    b.get_statistics.return_value = {"total_chunks": 10}
    b.name = name
    return b


CHUNKS = [{"chunk_id": "c1", "text": "hello", "embedding": [0.1]}]


class TestCompositeVectorStoreConstruction:
    def test_requires_at_least_one_backend(self):
        with pytest.raises(ValueError):
            CompositeVectorStore([])

    def test_single_backend_accepted(self):
        b = _mock_backend()
        store = CompositeVectorStore([b])
        assert store.backends == [b]

    def test_multiplebackends_accepted(self):
        b1, b2 = _mock_backend("b1"), _mock_backend("b2")
        store = CompositeVectorStore([b1, b2])
        assert len(store.backends) == 2




class TestWriteOperations:
    @pytest.fixture()
    def twobackends(self):
        return _mock_backend("b1"), _mock_backend("b2")

    @pytest.fixture()
    def store(self, twobackends):
        b1, b2 = twobackends
        return CompositeVectorStore([b1, b2]), b1, b2

    def test_bulk_index_calls_allbackends(self, store):
        composite, b1, b2 = store
        composite.bulk_index_chunks(CHUNKS)
        b1.bulk_index_chunks.assert_called_once_with(CHUNKS, 100)
        b2.bulk_index_chunks.assert_called_once_with(CHUNKS, 100)

    def test_index_chunk_calls_allbackends(self, store):
        composite, b1, b2 = store
        composite.index_chunk(CHUNKS[0])
        b1.index_chunk.assert_called_once_with(CHUNKS[0])
        b2.index_chunk.assert_called_once_with(CHUNKS[0])

    def test_delete_document_calls_allbackends(self, store):
        composite, b1, b2 = store
        composite.delete_document("native-1")
        b1.delete_document.assert_called_once_with("native-1")
        b2.delete_document.assert_called_once_with("native-1")

    def test_delete_document_returns_total_count(self, store):
        composite, b1, b2 = store
        b1.delete_document.return_value = 3
        b2.delete_document.return_value = 4
        assert composite.delete_document("native-1") == 7

    def test_delete_attachments_calls_allbackends(self, store):
        composite, b1, b2 = store
        composite.delete_attachments("native-1")
        b1.delete_attachments.assert_called_once_with("native-1")
        b2.delete_attachments.assert_called_once_with("native-1")

    def test_delete_attachments_returns_total_count(self, store):
        composite, b1, b2 = store
        b1.delete_attachments.return_value = 2
        b2.delete_attachments.return_value = 1
        assert composite.delete_attachments("native-1") == 3

    def test_index_chunk_returns_true_when_all_succeed(self, store):
        composite, b1, b2 = store
        b1.index_chunk.return_value = True
        b2.index_chunk.return_value = True
        assert composite.index_chunk(CHUNKS[0]) is True

    def test_index_chunk_returns_false_when_one_fails(self, store):
        composite, b1, b2 = store
        b1.index_chunk.return_value = True
        b2.index_chunk.return_value = False
        assert composite.index_chunk(CHUNKS[0]) is False


class TestReadOperations:
    @pytest.fixture()
    def store_pair(self):
        b1 = _mock_backend("primary")
        b2 = _mock_backend("secondary")
        b1.document_exists.return_value = True
        b2.document_exists.return_value = False
        b1.search.return_value = [{"chunk_id": "from-primary"}]
        b2.search.return_value = [{"chunk_id": "from-secondary"}]
        b1.get_statistics.return_value = {"total_chunks": 42}
        b2.get_statistics.return_value = {"total_chunks": 99}
        return CompositeVectorStore([b1, b2]), b1, b2

    def test_document_exists_delegates_to_first_backend(self, store_pair):
        composite, b1, b2 = store_pair
        result = composite.document_exists("native-1")
        assert result is True  # from b1
        b1.document_exists.assert_called_once_with("native-1")
        b2.document_exists.assert_not_called()

    def test_search_delegates_to_first_backend(self, store_pair):
        composite, b1, b2 = store_pair
        results = composite.search([0.1, 0.2])
        assert results[0]["chunk_id"] == "from-primary"
        b2.search.assert_not_called()

    def test_get_statistics_delegates_to_first_backend(self, store_pair):
        composite, b1, b2 = store_pair
        stats = composite.get_statistics()
        assert stats["total_chunks"] == 42
        b2.get_statistics.assert_not_called()


# ---------------------------------------------------------------------------
# Error propagation
# ---------------------------------------------------------------------------


class TestErrorPropagation:
    def test_exception_in_first_backend_propagates(self):
        b1 = _mock_backend("b1")
        b2 = _mock_backend("b2")
        b1.bulk_index_chunks.side_effect = RuntimeError("b1 failed")
        composite = CompositeVectorStore([b1, b2])

        with pytest.raises(RuntimeError, match="b1 failed"):
            composite.bulk_index_chunks(CHUNKS)

        # b2 should NOT have been called after b1 raised
        b2.bulk_index_chunks.assert_not_called()

    def test_exception_in_second_backend_propagates(self):
        b1 = _mock_backend("b1")
        b2 = _mock_backend("b2")
        b2.delete_document.side_effect = ConnectionError("b2 gone")
        composite = CompositeVectorStore([b1, b2])

        with pytest.raises(ConnectionError, match="b2 gone"):
            composite.delete_document("native-1")

    def test_close_calls_allbackends(self):
        b1, b2 = _mock_backend("b1"), _mock_backend("b2")
        composite = CompositeVectorStore([b1, b2])
        composite.close()
        b1.close.assert_called_once()
        b2.close.assert_called_once()
