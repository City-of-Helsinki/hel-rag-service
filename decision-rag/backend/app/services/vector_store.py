"""
Vector store abstractions: base class, exception, and composite store.

The concrete implementations live in elasticsearch_store.py and pgvector_store.py.
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional

from app.core import get_logger

logger = get_logger(__name__)


class MaxRetriesExceededError(Exception):
    """Raised when maximum retry attempts are exceeded."""

    pass


class BaseVectorStore(ABC):
    """
    Abstract base class for all vector store backends.

    Provides shared retry-counter state and helpers; subclasses must implement
    all abstract methods.
    """

    def __init__(self) -> None:
        from app.core import settings

        self._retry_count: int = 0
        self._max_total_retries: int = getattr(settings, "MAX_TOTAL_RETRIES", 10)

    def _increment_retry_count(self) -> None:
        """
        Increment retry counter and raise if maximum is exceeded.

        Raises:
            MaxRetriesExceededError: If maximum total retries are exceeded.
        """
        self._retry_count += 1
        logger.warning(
            f"Vector store operation retry {self._retry_count}/{self._max_total_retries}"
        )
        if self._retry_count >= self._max_total_retries:
            error_msg = (
                f"Maximum total retry attempts ({self._max_total_retries}) exceeded "
                f"for vector store operations."
            )
            logger.error(error_msg)
            raise MaxRetriesExceededError(error_msg)

    def _reset_retry_count(self) -> None:
        """Reset retry counter after a successful operation."""
        if self._retry_count > 0:
            logger.info(
                f"Vector store operation successful after {self._retry_count} retries. "
                "Resetting counter."
            )
            self._retry_count = 0

    @abstractmethod
    def bulk_index_chunks(
        self, chunks_with_embeddings: List[Dict[str, Any]], batch_size: int = 100
    ) -> Dict[str, Any]:
        """Bulk index a list of chunks with embeddings."""
        raise NotImplementedError("bulk_index_chunks must be implemented by subclasses")

    @abstractmethod
    def index_chunk(self, chunk_data: Dict[str, Any]) -> bool:
        """Index a single chunk."""
        raise NotImplementedError("index_chunk must be implemented by subclasses")

    @abstractmethod
    def document_exists(self, native_id: str) -> bool:
        """Return True if any chunk for *native_id* exists in the store."""
        raise NotImplementedError("document_exists must be implemented by subclasses")

    @abstractmethod
    def delete_document(self, native_id: str) -> int:
        """Delete all chunks for a document (decision + attachments). Returns deleted count."""
        raise NotImplementedError("delete_document must be implemented by subclasses")

    @abstractmethod
    def delete_attachments(self, decision_native_id: str) -> int:
        """Delete attachment chunks only for a decision. Returns deleted count."""
        raise NotImplementedError("delete_attachments must be implemented by subclasses")

    @abstractmethod
    def search(
        self,
        query_vector: List[float],
        top_k: int = 10,
        filter_conditions: Optional[Dict[str, Any]] = None,
    ) -> List[Dict[str, Any]]:
        """Return top-k results for the given query vector."""
        raise NotImplementedError("search must be implemented by subclasses")

    @abstractmethod
    def get_statistics(self) -> Dict[str, Any]:
        """Return statistics about the store."""
        raise NotImplementedError("get_statistics must be implemented by subclasses")

    @abstractmethod
    def close(self) -> None:
        """Release any resources held by the store."""
        raise NotImplementedError("close must be implemented by subclasses")


class CompositeVectorStore(BaseVectorStore):
    """
    Fan-out vector store that delegates to multiple backends.

    Write operations are dispatched to **all** backends sequentially.
    Read operations are delegated to the **first** backend in the list.
    Exceptions are propagated rather than suppressed.
    """

    def __init__(self, backends: List[BaseVectorStore]) -> None:
        if not backends:
            raise ValueError("CompositeVectorStore requires at least one backend.")
        # Do NOT call super().__init__() – the concrete backends manage their own
        # retry state independently.
        self.backends = backends
        self._retry_count = 0
        self._max_total_retries = 0

    def bulk_index_chunks(
        self, chunks_with_embeddings: List[Dict[str, Any]], batch_size: int = 100
    ) -> Dict[str, Any]:
        result: Dict[str, Any] = {"success": 0, "failed": 0, "errors": []}
        for backend in self.backends:
            r = backend.bulk_index_chunks(chunks_with_embeddings, batch_size)
            result["success"] = max(result["success"], r.get("success", 0))
            result["failed"] += r.get("failed", 0)
            result["errors"].extend(r.get("errors", []))
        return result

    def index_chunk(self, chunk_data: Dict[str, Any]) -> bool:
        ok = True
        for backend in self.backends:
            ok = backend.index_chunk(chunk_data) and ok
        return ok

    def delete_document(self, native_id: str) -> int:
        total = 0
        for backend in self.backends:
            total += backend.delete_document(native_id)
        return total

    def delete_attachments(self, decision_native_id: str) -> int:
        total = 0
        for backend in self.backends:
            total += backend.delete_attachments(decision_native_id)
        return total

    def document_exists(self, native_id: str) -> bool:
        return self.backends[0].document_exists(native_id)

    def search(
        self,
        query_vector: List[float],
        top_k: int = 10,
        filter_conditions: Optional[Dict[str, Any]] = None,
    ) -> List[Dict[str, Any]]:
        return self.backends[0].search(query_vector, top_k, filter_conditions)

    def get_statistics(self) -> Dict[str, Any]:
        return self.backends[0].get_statistics()

    def close(self) -> None:
        for backend in self.backends:
            backend.close()
