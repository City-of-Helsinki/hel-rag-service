"""
Elasticsearch vector store implementation.
"""

from datetime import datetime
from typing import Any, Dict, List, Optional

from elasticsearch import Elasticsearch, helpers
from elasticsearch.exceptions import ConnectionError, ConnectionTimeout, TransportError

from app.core import get_logger, settings
from app.services.vector_store import BaseVectorStore, MaxRetriesExceededError

logger = get_logger(__name__)


class ElasticsearchVectorStore(BaseVectorStore):
    """
    Service for storing and retrieving document embeddings in Elasticsearch.

    Configuration:
    - Index name: (default decision_documents)
    - Vector dimensions: 3072 (text-embedding-3-large)
    - Similarity: cosine
    """

    def __init__(self, url: str = None, index_name: str = None, vector_dims: int = 3072):
        """
        Initialize Elasticsearch vector store.

        Args:
            url: Elasticsearch URL
            index_name: Name of the index to use
            vector_dims: Dimension of embedding vectors
        """
        super().__init__()

        self.url = url or getattr(settings, "ELASTICSEARCH_URL", "http://localhost:9200")
        self.index_name = index_name or getattr(
            settings, "ELASTICSEARCH_INDEX", "decision_documents"
        )
        self.vector_dims = vector_dims
        self.cert = getattr(settings, "ELASTICSEARCH_CERT", None)

        # Initialize Elasticsearch client
        try:
            if self.cert:
                self.client = Elasticsearch(
                    [self.url],
                    request_timeout=30,
                    retry_on_timeout=True,
                    max_retries=3,
                    ca_certs=self.cert,
                    basic_auth=(
                        getattr(settings, "ELASTICSEARCH_USER", None),
                        getattr(settings, "ELASTICSEARCH_PASSWORD", None),
                    ),
                )
            else:
                self.client = Elasticsearch(
                    [self.url], request_timeout=30, retry_on_timeout=True, max_retries=3
                )

            # Test connection
            if not self.client.ping():
                raise ConnectionError("Failed to connect to Elasticsearch")

            logger.info(f"Connected to Elasticsearch at {self.url}")

            # Create index if it doesn't exist
            self._create_index_if_not_exists()

        except Exception as e:
            logger.error(f"Error initializing Elasticsearch client: {e}")
            raise

    def _create_index_if_not_exists(self):
        """Create the index with proper mappings if it doesn't exist."""
        if self.client.indices.exists(index=self.index_name):
            logger.info(f"Index '{self.index_name}' already exists")
            return

        # Define index mappings compatible with Open WebUI structure
        mappings = {
            "mappings": {
                "dynamic_templates": [
                    {"strings": {"match_mapping_type": "string", "mapping": {"type": "keyword"}}}
                ],
                "properties": {
                    "collection": {"type": "keyword"},
                    "id": {"type": "keyword"},
                    "text": {"type": "text"},
                    "vector": {
                        "type": "dense_vector",
                        "dims": self.vector_dims,
                        "index": True,
                        "similarity": "cosine",
                        "index_options": {"type": "bbq_hnsw", "m": 16, "ef_construction": 100},
                    },
                    "metadata": {
                        "properties": {
                            # Open WebUI standard metadata
                            "collection_name": {"type": "keyword"},
                            "file_id": {"type": "keyword"},
                            "name": {"type": "keyword"},
                            "source": {"type": "keyword"},
                            # Decision-specific metadata
                            "native_id": {"type": "keyword"},
                            "title": {"type": "keyword"},
                            "date_decision": {"type": "keyword"},
                            "section": {"type": "keyword"},
                            "classification_code": {"type": "keyword"},
                            "classification_title": {"type": "keyword"},
                            "case_id": {"type": "keyword"},
                            "organization_name": {"type": "keyword"},
                            # Chunk metadata
                            "chunk_id": {"type": "keyword"},
                            "chunk_index": {"type": "long"},
                            "token_count": {"type": "long"},
                            "chunk_position": {"type": "long"},
                            "start_index": {"type": "long"},
                            # Attachment-specific metadata
                            "is_attachment": {"type": "boolean"},
                            "attachment_native_id": {"type": "keyword"},
                            "attachment_title": {"type": "keyword"},
                            "attachment_number": {"type": "long"},
                            "attachment_type": {"type": "keyword"},
                            "attachment_url": {"type": "keyword"},
                            "decision_native_id": {"type": "keyword"},
                            # Indexing metadata
                            "indexed_at": {"type": "date"},
                        }
                    },
                },
            },
            "settings": {"number_of_shards": 1, "number_of_replicas": 1},
        }

        try:
            self.client.indices.create(index=self.index_name, body=mappings)
            logger.info(
                f"Created index '{self.index_name}' with vector dimensions {self.vector_dims}"
            )
        except Exception as e:
            logger.error(f"Error creating index: {e}")
            raise

    def bulk_index_chunks(
        self, chunks_with_embeddings: List[Dict[str, Any]], batch_size: int = 100
    ) -> Dict[str, Any]:
        """
        Bulk index chunks with embeddings to Elasticsearch.

        Args:
            chunks_with_embeddings: List of dictionaries containing chunk data and embeddings
            batch_size: Number of documents to index per batch

        Returns:
            Dictionary with indexing statistics
        """
        if not chunks_with_embeddings:
            logger.warning("No chunks provided for indexing")
            return {"success": 0, "failed": 0, "errors": []}

        logger.info(f"Bulk indexing {len(chunks_with_embeddings)} chunks to '{self.index_name}'")

        # Prepare documents for bulk indexing with Open WebUI structure
        actions = []
        for chunk_data in chunks_with_embeddings:
            # Extract metadata and prepare nested structure
            metadata = chunk_data.get("metadata", {})
            metadata.update(
                {
                    "chunk_id": chunk_data["chunk_id"],
                    "native_id": chunk_data["native_id"],
                    "chunk_index": chunk_data["chunk_index"],
                    "token_count": chunk_data.get("token_count", 0),
                    "chunk_position": chunk_data.get("chunk_position", 0),
                    "indexed_at": datetime.utcnow().isoformat(),
                }
            )

            doc = {
                "_index": self.index_name,
                "_id": chunk_data["chunk_id"],
                "_source": {
                    "collection": getattr(settings, "COLLECTION_NAME", "decisions"),
                    "id": chunk_data["chunk_id"],
                    "text": chunk_data["text"],
                    "vector": chunk_data["embedding"],
                    "metadata": metadata,
                },
            }
            actions.append(doc)

        # Perform bulk indexing
        success_count = 0
        failed_count = 0
        errors = []

        try:
            success, failed = helpers.bulk(
                self.client,
                actions,
                chunk_size=batch_size,
                request_timeout=60,
                raise_on_error=False,
                stats_only=False,
            )

            success_count = success

            # Reset retry count on successful operation
            self._reset_retry_count()

            # Process failures
            if failed:
                failed_count = len(failed)
                for item in failed:
                    error_msg = str(item)
                    errors.append(error_msg)
                    logger.error(f"Failed to index document: {error_msg}")

            logger.info(
                f"Bulk indexing complete: {success_count} successful, {failed_count} failed"
            )

        except (ConnectionTimeout, TransportError) as e:
            self._increment_retry_count()
            logger.error(f"Connection/timeout error during bulk indexing: {e}")
            errors.append(str(e))
            failed_count = len(chunks_with_embeddings)
            raise
        except MaxRetriesExceededError:
            raise
        except Exception as e:
            logger.error(f"Error during bulk indexing: {e}")
            errors.append(str(e))
            failed_count = len(chunks_with_embeddings)

        return {
            "success": success_count,
            "failed": failed_count,
            "errors": errors[:10],  # Limit errors in response
        }

    def index_chunk(self, chunk_data: Dict[str, Any]) -> bool:
        """
        Index a single chunk with embedding.

        Args:
            chunk_data: Dictionary containing chunk data and embedding

        Returns:
            True if successful, False otherwise
        """
        try:
            metadata = chunk_data.get("metadata", {})
            metadata.update(
                {
                    "chunk_id": chunk_data["chunk_id"],
                    "native_id": chunk_data["native_id"],
                    "chunk_index": chunk_data["chunk_index"],
                    "token_count": chunk_data.get("token_count", 0),
                    "chunk_position": chunk_data.get("chunk_position", 0),
                    "indexed_at": datetime.utcnow().isoformat(),
                }
            )

            doc = {
                "collection": chunk_data.get("collection", "decisions"),
                "id": chunk_data["chunk_id"],
                "text": chunk_data["text"],
                "vector": chunk_data["embedding"],
                "metadata": metadata,
            }

            self.client.index(index=self.index_name, id=chunk_data["chunk_id"], document=doc)

            self._reset_retry_count()

            logger.debug(f"Indexed chunk: {chunk_data['chunk_id']}")
            return True

        except (ConnectionTimeout, TransportError) as e:
            self._increment_retry_count()
            logger.error(
                f"Connection/timeout error indexing chunk {chunk_data.get('chunk_id')}: {e}"
            )
            raise
        except MaxRetriesExceededError:
            raise
        except Exception as e:
            logger.error(f"Error indexing chunk {chunk_data.get('chunk_id')}: {e}")
            return False

    def search(
        self,
        query_vector: List[float],
        top_k: int = 10,
        filter_conditions: Optional[Dict[str, Any]] = None,
    ) -> List[Dict[str, Any]]:
        """
        Search for similar documents using vector similarity.

        Args:
            query_vector: Query embedding vector
            top_k: Number of results to return
            filter_conditions: Optional filters to apply

        Returns:
            List of matching documents with scores
        """
        try:
            query = {
                "knn": {
                    "field": "vector",
                    "query_vector": query_vector,
                    "k": top_k,
                    "num_candidates": top_k * 10,
                }
            }

            if filter_conditions:
                query["filter"] = filter_conditions

            response = self.client.search(index=self.index_name, body=query, size=top_k)

            self._reset_retry_count()

            results = []
            for hit in response["hits"]["hits"]:
                source = hit["_source"]
                metadata = source.get("metadata", {})
                result = {
                    "chunk_id": source.get("id", metadata.get("chunk_id", "")),
                    "native_id": metadata.get("native_id", ""),
                    "text": source.get("text", ""),
                    "score": hit["_score"],
                    "metadata": metadata,
                }
                results.append(result)

            logger.info(f"Search completed: {len(results)} results")
            return results

        except (ConnectionTimeout, TransportError) as e:
            self._increment_retry_count()
            logger.error(f"Connection/timeout error during search: {e}")
            raise
        except MaxRetriesExceededError:
            raise
        except Exception as e:
            logger.error(f"Error searching: {e}")
            return []

    def delete_document(self, native_id: str) -> int:
        """
        Delete all chunks for a document (including attachments).

        Args:
            native_id: Native ID of the document

        Returns:
            Number of chunks deleted
        """
        try:
            response = self.client.delete_by_query(
                index=self.index_name,
                body={
                    "query": {
                        "bool": {
                            "should": [
                                {"term": {"metadata.native_id": native_id}},
                                {"term": {"metadata.decision_native_id": native_id}},
                            ]
                        }
                    }
                },
            )

            self._reset_retry_count()

            deleted_count = response.get("deleted", 0)
            logger.info(f"Deleted {deleted_count} chunks for document {native_id}")
            return deleted_count

        except (ConnectionTimeout, TransportError) as e:
            self._increment_retry_count()
            logger.error(f"Connection/timeout error deleting document {native_id}: {e}")
            raise
        except MaxRetriesExceededError:
            raise
        except Exception as e:
            logger.error(f"Error deleting document {native_id}: {e}")
            return 0

    def delete_attachments(self, decision_native_id: str) -> int:
        """
        Delete all attachment chunks for a decision.

        Args:
            decision_native_id: Native ID of the parent decision

        Returns:
            Number of attachment chunks deleted
        """
        try:
            response = self.client.delete_by_query(
                index=self.index_name,
                body={
                    "query": {
                        "bool": {
                            "must": [
                                {"term": {"metadata.decision_native_id": decision_native_id}},
                                {"term": {"metadata.is_attachment": True}},
                            ]
                        }
                    }
                },
            )

            self._reset_retry_count()

            deleted_count = response.get("deleted", 0)
            logger.info(
                f"Deleted {deleted_count} attachment chunks for decision {decision_native_id}"
            )
            return deleted_count

        except (ConnectionTimeout, TransportError) as e:
            self._increment_retry_count()
            logger.error(
                f"Connection/timeout error deleting attachments for decision {decision_native_id}: {e}"
            )
            raise
        except MaxRetriesExceededError:
            raise
        except Exception as e:
            logger.error(f"Error deleting attachments for decision {decision_native_id}: {e}")
            return 0

    def document_exists(self, native_id: str) -> bool:
        """
        Check if a document has been indexed.

        Args:
            native_id: Native ID of the document

        Returns:
            True if document exists, False otherwise
        """
        try:
            response = self.client.count(
                index=self.index_name,
                body={"query": {"term": {"metadata.native_id": native_id}}},
            )

            self._reset_retry_count()

            count = response.get("count", 0)
            return count > 0

        except (ConnectionTimeout, TransportError) as e:
            self._increment_retry_count()
            logger.error(
                f"Connection/timeout error checking document existence {native_id}: {e}"
            )
            raise
        except MaxRetriesExceededError:
            raise
        except Exception as e:
            logger.error(f"Error checking document existence {native_id}: {e}")
            return False

    def get_statistics(self) -> Dict[str, Any]:
        """
        Get statistics about the index.

        Returns:
            Dictionary with index statistics
        """
        try:
            stats = self.client.indices.stats(index=self.index_name)
            count_response = self.client.count(index=self.index_name)

            native_ids_response = self.client.search(
                index=self.index_name,
                body={
                    "query": {"match_all": {}},
                    "fields": ["metadata.native_id"],
                    "size": 10000,
                    "_source": False,
                },
            )

            self._reset_retry_count()

            unique_native_ids = set()
            for hit in native_ids_response["hits"]["hits"]:
                fields = hit.get("fields", {})
                native_id_values = fields.get("metadata.native_id", [])
                for nid in native_id_values:
                    unique_native_ids.add(nid)

            return {
                "instance": "elasticsearch",
                "index_name": self.index_name,
                "total_chunks": count_response.get("count", 0),
                "size_bytes": stats["indices"][self.index_name]["total"]["store"]["size_in_bytes"],
                "size_mb": stats["indices"][self.index_name]["total"]["store"]["size_in_bytes"]
                / (1024 * 1024),
                "total_decisions": len(unique_native_ids),
            }

        except (ConnectionTimeout, TransportError) as e:
            self._increment_retry_count()
            logger.error(f"Connection/timeout error getting statistics: {e}")
            raise
        except MaxRetriesExceededError:
            raise
        except Exception as e:
            logger.error(f"Error getting statistics: {e}")
            return {"instance": "elasticsearch", "index_name": self.index_name, "error": str(e)}

    def close(self) -> None:
        """Close the Elasticsearch client connection."""
        try:
            self.client.close()
            logger.info("Elasticsearch connection closed")
        except Exception as e:
            logger.error(f"Error closing Elasticsearch connection: {e}")
