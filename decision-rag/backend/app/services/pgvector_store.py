"""
pgvector (PostgreSQL) vector store implementation.
"""

import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import psycopg2
import psycopg2.extras

from app.core import get_logger, settings
from app.services.vector_store import BaseVectorStore, MaxRetriesExceededError

logger = get_logger(__name__)


class PgvectorVectorStore(BaseVectorStore):
    """
    Vector store backed by PostgreSQL with the pgvector extension.

    Uses cosine-similarity HNSW index for fast approximate nearest-neighbour search.
    """

    def __init__(
        self,
        host: str = None,
        port: int = None,
        db: str = None,
        user: str = None,
        password: str = None,
        table: str = None,
        vector_dims: int = None,
    ) -> None:
        """
        Connect to PostgreSQL and create the chunk table / indexes if they do not exist.

        Args:
            host: PostgreSQL host (falls back to PGVECTOR_HOST setting)
            port: PostgreSQL port (falls back to PGVECTOR_PORT setting)
            db: Database name (falls back to PGVECTOR_DB setting)
            user: Database user (falls back to PGVECTOR_USER setting)
            password: Database password (falls back to PGVECTOR_PASSWORD setting)
            table: Table name (falls back to PGVECTOR_TABLE setting)
            vector_dims: Embedding dimensions (falls back to EMBEDDING_DIMENSION setting)
        """
        super().__init__()

        self.host = host or getattr(settings, "PGVECTOR_HOST", "localhost")
        self.port = port or getattr(settings, "PGVECTOR_PORT", 5432)
        self.db = db or getattr(settings, "PGVECTOR_DB", "decisions")
        self.user = user or getattr(settings, "PGVECTOR_USER", "postgres")
        self.password = password if password is not None else getattr(settings, "PGVECTOR_PASSWORD", "")
        self.table = table or getattr(settings, "PGVECTOR_TABLE", "decision_chunks")
        self.vector_dims = vector_dims or getattr(settings, "EMBEDDING_DIMENSION", 3072)

        try:
            self.conn = psycopg2.connect(
                host=self.host,
                port=self.port,
                dbname=self.db,
                user=self.user,
                password=self.password,
            )
            self.conn.autocommit = False

            # Assume the table and pgvector extension are already set up

            logger.info(
                f"Connected to pgvector at {self.host}:{self.port}/{self.db}, "
                f"table '{self.table}'"
            )
        except Exception as e:
            logger.error(f"Error initializing PgvectorVectorStore: {e}")
            raise

    def _reconnect(self, attempt: int = 1) -> None:
        """Close the current connection and open a fresh one."""
        logger.warning(f"Reconnecting to pgvector (attempt {attempt})...")
        try:
            if self.conn.closed == 0:
                self.conn.close()
        except Exception:
            pass
        self.conn = psycopg2.connect(
            host=self.host,
            port=self.port,
            dbname=self.db,
            user=self.user,
            password=self.password,
        )
        self.conn.autocommit = False
        logger.info("Reconnected to pgvector successfully.")

    def _ensure_connection(self) -> None:
        """Proactively reconnect if the connection is already known to be closed."""
        if self.conn.closed != 0:
            logger.warning("pgvector connection is closed; reconnecting before operation.")
            self._reconnect()

    def _execute_with_retry(self, operation):
        """
        Execute a DB operation callable with reconnect-and-retry on OperationalError.

        Retries up to PGVECTOR_OP_MAX_RETRIES times with exponential backoff
        (capped at 30 s).  Non-OperationalError exceptions propagate immediately.
        """
        max_retries = getattr(settings, "PGVECTOR_OP_MAX_RETRIES", 3)
        backoff_base = getattr(settings, "PGVECTOR_RETRY_BACKOFF_BASE", 1.0)

        for attempt in range(max_retries + 1):
            try:
                result = operation()
                self._reset_retry_count()
                return result
            except psycopg2.OperationalError as e:
                try:
                    self.conn.rollback()
                except Exception:
                    pass
                if attempt >= max_retries:
                    self._increment_retry_count()
                    raise
                logger.warning(
                    f"pgvector OperationalError on attempt {attempt + 1}/{max_retries + 1}: {e}. "
                    "Reconnecting..."
                )
                self._reconnect(attempt + 1)
                sleep_time = min(backoff_base * (2 ** attempt), 30.0)
                time.sleep(sleep_time)

    def bulk_index_chunks(
        self, chunks_with_embeddings: List[Dict[str, Any]], batch_size: int = 100
    ) -> Dict[str, Any]:
        """
        Insert or upsert a batch of chunks into the pgvector table.

        Args:
            chunks_with_embeddings: Chunk dicts containing at minimum
                chunk_id, native_id, chunk_index, text, embedding, metadata.
            batch_size: Unused (kept for interface compatibility).

        Returns:
            Dict with success/failed/errors counts.
        """
        if not chunks_with_embeddings:
            logger.warning("No chunks provided for indexing")
            return {"success": 0, "failed": 0, "errors": []}

        logger.info(
            f"Bulk indexing {len(chunks_with_embeddings)} chunks to pgvector table '{self.table}'"
        )

        success_count = 0
        failed_count = 0
        errors: List[str] = []

        try:
            def _do_bulk_insert():
                count = 0
                with self.conn.cursor() as cur:
                    for chunk_data in chunks_with_embeddings:
                        vmetadata = dict(chunk_data.get("metadata", {}))
                        vmetadata.update(
                            {
                                "chunk_id": chunk_data["chunk_id"],
                                "native_id": chunk_data["native_id"],
                                "chunk_index": chunk_data["chunk_index"],
                                "token_count": chunk_data.get("token_count", 0),
                                "chunk_position": chunk_data.get("chunk_position", 0),
                                "indexed_at": datetime.now(timezone.utc).isoformat(),
                            }
                        )
                        vector_str = "[" + ",".join(str(v) for v in chunk_data["embedding"]) + "]"
                        cur.execute(
                            f"""
                            INSERT INTO {self.table}
                                (id, collection_name, text, vector, vmetadata)
                            VALUES (%s, %s, %s, %s::halfvec, %s)
                            ON CONFLICT (id) DO UPDATE SET
                                collection_name = EXCLUDED.collection_name,
                                text            = EXCLUDED.text,
                                vector          = EXCLUDED.vector,
                                vmetadata       = EXCLUDED.vmetadata;
                            """,
                            (
                                chunk_data["chunk_id"],
                                chunk_data.get(
                                    "collection",
                                    getattr(settings, "COLLECTION_NAME", "decisions"),
                                ),
                                chunk_data["text"],
                                vector_str,
                                psycopg2.extras.Json(vmetadata),
                            ),
                        )
                        count += 1
                self.conn.commit()
                return count

            self._ensure_connection()
            success_count = self._execute_with_retry(_do_bulk_insert)
            logger.info(
                f"Bulk indexing complete: {success_count} successful, {failed_count} failed"
            )

        except (psycopg2.OperationalError, MaxRetriesExceededError):
            raise
        except Exception as e:
            try:
                self.conn.rollback()
            except Exception:
                pass
            logger.error(f"Error during pgvector bulk indexing: {e}")
            errors.append(str(e))
            failed_count = len(chunks_with_embeddings) - success_count

        return {
            "success": success_count,
            "failed": failed_count,
            "errors": errors[:10],
        }

    def index_chunk(self, chunk_data: Dict[str, Any]) -> bool:
        """
        Insert or upsert a single chunk.

        Args:
            chunk_data: Chunk dict.

        Returns:
            True if successful, False otherwise.
        """
        try:
            def _do_index():
                vmetadata = dict(chunk_data.get("metadata", {}))
                vmetadata.update(
                    {
                        "chunk_id": chunk_data["chunk_id"],
                        "native_id": chunk_data["native_id"],
                        "chunk_index": chunk_data["chunk_index"],
                        "token_count": chunk_data.get("token_count", 0),
                        "chunk_position": chunk_data.get("chunk_position", 0),
                        "indexed_at": datetime.now(timezone.utc).isoformat(),
                    }
                )
                vector_str = "[" + ",".join(str(v) for v in chunk_data["embedding"]) + "]"
                with self.conn.cursor() as cur:
                    cur.execute(
                        f"""
                        INSERT INTO {self.table}
                            (id, collection_name, text, vector, vmetadata)
                        VALUES (%s, %s, %s, %s::halfvec, %s)
                        ON CONFLICT (id) DO UPDATE SET
                            collection_name = EXCLUDED.collection_name,
                            text            = EXCLUDED.text,
                            vector          = EXCLUDED.vector,
                            vmetadata       = EXCLUDED.vmetadata;
                        """,
                        (
                            chunk_data["chunk_id"],
                            chunk_data.get(
                                "collection",
                                getattr(settings, "COLLECTION_NAME", "decisions"),
                            ),
                            chunk_data["text"],
                            vector_str,
                            psycopg2.extras.Json(vmetadata),
                        ),
                    )
                self.conn.commit()
                return True

            self._ensure_connection()
            result = self._execute_with_retry(_do_index)
            logger.debug(f"Indexed chunk: {chunk_data['chunk_id']}")
            return result

        except (psycopg2.OperationalError, MaxRetriesExceededError):
            raise
        except Exception as e:
            try:
                self.conn.rollback()
            except Exception:
                pass
            logger.error(f"Error indexing chunk {chunk_data.get('chunk_id')}: {e}")
            return False

    def delete_document(self, native_id: str) -> int:
        """
        Delete all chunks for a document (decision + its attachments).

        Args:
            native_id: The native ID of the decision.

        Returns:
            Number of rows deleted.
        """
        try:
            def _do_delete():
                with self.conn.cursor() as cur:
                    cur.execute(
                        f"""
                        DELETE FROM {self.table}
                        WHERE vmetadata->>'native_id' = %s
                           OR vmetadata->>'decision_native_id' = %s;
                        """,
                        (native_id, native_id),
                    )
                    count = cur.rowcount
                self.conn.commit()
                return count

            self._ensure_connection()
            deleted_count = self._execute_with_retry(_do_delete)
            logger.info(f"Deleted {deleted_count} chunks for document {native_id}")
            return deleted_count

        except (psycopg2.OperationalError, MaxRetriesExceededError):
            raise
        except Exception as e:
            try:
                self.conn.rollback()
            except Exception:
                pass
            logger.error(f"Error deleting document {native_id}: {e}")
            return 0

    def delete_attachments(self, decision_native_id: str) -> int:
        """
        Delete attachment chunks for a decision.

        Args:
            decision_native_id: The native ID of the parent decision.

        Returns:
            Number of rows deleted.
        """
        try:
            def _do_delete_attachments():
                with self.conn.cursor() as cur:
                    cur.execute(
                        f"""
                        DELETE FROM {self.table}
                        WHERE vmetadata->>'decision_native_id' = %s
                          AND (vmetadata->>'is_attachment')::boolean = true;
                        """,
                        (decision_native_id,),
                    )
                    count = cur.rowcount
                self.conn.commit()
                return count

            self._ensure_connection()
            deleted_count = self._execute_with_retry(_do_delete_attachments)
            logger.info(
                f"Deleted {deleted_count} attachment chunks for decision {decision_native_id}"
            )
            return deleted_count

        except (psycopg2.OperationalError, MaxRetriesExceededError):
            raise
        except Exception as e:
            try:
                self.conn.rollback()
            except Exception:
                pass
            logger.error(f"Error deleting attachments for {decision_native_id}: {e}")
            return 0

    def document_exists(self, native_id: str) -> bool:
        """
        Return True if any chunk for *native_id* (decision or attachment) exists.

        Args:
            native_id: The native ID of the decision.
        """
        try:
            def _do_check():
                with self.conn.cursor() as cur:
                    cur.execute(
                        f"""
                        SELECT 1 FROM {self.table}
                        WHERE vmetadata->>'native_id' = %s
                           OR vmetadata->>'decision_native_id' = %s
                        LIMIT 1;
                        """,
                        (native_id, native_id),
                    )
                    return cur.fetchone() is not None

            self._ensure_connection()
            return self._execute_with_retry(_do_check)

        except (psycopg2.OperationalError, MaxRetriesExceededError):
            raise
        except Exception as e:
            logger.error(f"Error checking document existence {native_id}: {e}")
            return False

    def search(
        self,
        query_vector: List[float],
        top_k: int = 10,
        filter_conditions: Optional[Dict[str, Any]] = None,
    ) -> List[Dict[str, Any]]:
        """
        Cosine-similarity nearest-neighbour search using the pgvector ``<=>`` operator.

        Args:
            query_vector: Query embedding vector.
            top_k: Number of results to return.
            filter_conditions: Optional dict of exact metadata key→value filters.

        Returns:
            List of result dicts matching the shape returned by ElasticsearchVectorStore.
        """
        try:
            def _do_search():
                vector_str = "[" + ",".join(str(v) for v in query_vector) + "]"
                filter_params: List[Any] = []
                where_clauses: List[str] = []
                if filter_conditions:
                    for key, value in filter_conditions.items():
                        where_clauses.append("vmetadata->>%s = %s")
                        filter_params.extend([key, str(value)])
                where_sql = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""
                with self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                    cur.execute(
                        f"""
                        SELECT id, collection_name, text, vmetadata,
                               1 - (vector <=> %s::halfvec) AS score
                        FROM {self.table}
                        {where_sql}
                        ORDER BY vector <=> %s::halfvec
                        LIMIT %s;
                        """,
                        [vector_str] + filter_params + [vector_str, top_k],
                    )
                    rows = cur.fetchall()
                results = []
                for row in rows:
                    vmetadata = row["vmetadata"] or {}
                    results.append(
                        {
                            "chunk_id": row["id"],
                            "native_id": vmetadata.get("native_id", ""),
                            "text": row["text"] or "",
                            "score": float(row["score"]),
                            "metadata": vmetadata,
                        }
                    )
                return results

            self._ensure_connection()
            results = self._execute_with_retry(_do_search)
            logger.info(f"pgvector search completed: {len(results)} results")
            return results

        except (psycopg2.OperationalError, MaxRetriesExceededError):
            raise
        except Exception as e:
            logger.error(f"Error during pgvector search: {e}")
            return []

    def get_statistics(self) -> Dict[str, Any]:
        """
        Return chunk count and table size statistics.

        Returns:
            Dictionary with total_chunks, size_bytes, size_mb, index_name.
        """
        try:
            def _do_stats():
                with self.conn.cursor() as cur:
                    cur.execute(f"SELECT COUNT(*) FROM {self.table};")
                    total = cur.fetchone()[0]
                    cur.execute(
                        "SELECT pg_total_relation_size(%s);",
                        (self.table,),
                    )
                    size = cur.fetchone()[0]
                return total, size

            self._ensure_connection()
            total_chunks, size_bytes = self._execute_with_retry(_do_stats)
            return {
                "instance": "pgvector",
                "index_name": self.table,
                "total_chunks": total_chunks,
                "size_bytes": size_bytes,
                "size_mb": size_bytes / (1024 * 1024),
            }

        except (psycopg2.OperationalError, MaxRetriesExceededError):
            raise
        except Exception as e:
            logger.error(f"Error getting pgvector statistics: {e}")
            return {"instance": "pgvector", "index_name": self.table, "error": str(e)}

    def close(self) -> None:
        """Close the PostgreSQL connection."""
        try:
            self.conn.close()
            logger.info("pgvector connection closed")
        except Exception as e:
            logger.error(f"Error closing pgvector connection: {e}")
