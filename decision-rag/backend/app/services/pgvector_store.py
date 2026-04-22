"""
pgvector (PostgreSQL) vector store implementation.
"""

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

            self._enable_pgvector()
            #self._create_table_if_not_exists()

            logger.info(
                f"Connected to pgvector at {self.host}:{self.port}/{self.db}, "
                f"table '{self.table}'"
            )
        except Exception as e:
            logger.error(f"Error initializing PgvectorVectorStore: {e}")
            raise

    def _enable_pgvector(self) -> None:
        """Enable the pgvector extension in the database."""
        with self.conn.cursor() as cur:
            cur.execute("CREATE EXTENSION IF NOT EXISTS vector;")
        self.conn.commit()

    def _create_table_if_not_exists(self) -> None:
        """Create the chunk table and indexes if they do not already exist."""
        with self.conn.cursor() as cur:
            cur.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self.table} (
                    id              TEXT PRIMARY KEY,
                    vector          HALFVEC({self.vector_dims}),
                    collection_name TEXT NOT NULL,
                    text            TEXT,
                    vmetadata       JSONB
                );
                """
            )
            cur.execute(
                f"""
                CREATE INDEX IF NOT EXISTS idx_{self.table}_vector
                    ON {self.table} USING hnsw (vector halfvec_cosine_ops)
                    WITH (m='16', ef_construction='64');
                """
            )
            cur.execute(
                f"""
                CREATE INDEX IF NOT EXISTS idx_{self.table}_collection_name
                    ON {self.table} (collection_name);
                """
            )
        self.conn.commit()
        logger.info(f"Table '{self.table}' ready")

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
                    success_count += 1

            self.conn.commit()
            self._reset_retry_count()
            logger.info(
                f"Bulk indexing complete: {success_count} successful, {failed_count} failed"
            )

        except psycopg2.OperationalError as e:
            self.conn.rollback()
            self._increment_retry_count()
            logger.error(f"pgvector connection error during bulk indexing: {e}")
            errors.append(str(e))
            failed_count = len(chunks_with_embeddings) - success_count
            raise
        except MaxRetriesExceededError:
            self.conn.rollback()
            raise
        except Exception as e:
            self.conn.rollback()
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
            self._reset_retry_count()
            logger.debug(f"Indexed chunk: {chunk_data['chunk_id']}")
            return True

        except psycopg2.OperationalError as e:
            self.conn.rollback()
            self._increment_retry_count()
            logger.error(
                f"pgvector connection error indexing chunk {chunk_data.get('chunk_id')}: {e}"
            )
            raise
        except MaxRetriesExceededError:
            self.conn.rollback()
            raise
        except Exception as e:
            self.conn.rollback()
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
            with self.conn.cursor() as cur:
                cur.execute(
                    f"""
                    DELETE FROM {self.table}
                    WHERE vmetadata->>'native_id' = %s
                       OR vmetadata->>'decision_native_id' = %s;
                    """,
                    (native_id, native_id),
                )
                deleted_count = cur.rowcount

            self.conn.commit()
            self._reset_retry_count()
            logger.info(f"Deleted {deleted_count} chunks for document {native_id}")
            return deleted_count

        except psycopg2.OperationalError as e:
            self.conn.rollback()
            self._increment_retry_count()
            logger.error(f"pgvector connection error deleting document {native_id}: {e}")
            raise
        except MaxRetriesExceededError:
            self.conn.rollback()
            raise
        except Exception as e:
            self.conn.rollback()
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
            with self.conn.cursor() as cur:
                cur.execute(
                    f"""
                    DELETE FROM {self.table}
                    WHERE vmetadata->>'decision_native_id' = %s
                      AND (vmetadata->>'is_attachment')::boolean = true;
                    """,
                    (decision_native_id,),
                )
                deleted_count = cur.rowcount

            self.conn.commit()
            self._reset_retry_count()
            logger.info(
                f"Deleted {deleted_count} attachment chunks for decision {decision_native_id}"
            )
            return deleted_count

        except psycopg2.OperationalError as e:
            self.conn.rollback()
            self._increment_retry_count()
            logger.error(
                f"pgvector connection error deleting attachments for {decision_native_id}: {e}"
            )
            raise
        except MaxRetriesExceededError:
            self.conn.rollback()
            raise
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Error deleting attachments for {decision_native_id}: {e}")
            return 0

    def document_exists(self, native_id: str) -> bool:
        """
        Return True if any chunk for *native_id* (decision or attachment) exists.

        Args:
            native_id: The native ID of the decision.
        """
        try:
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
                row = cur.fetchone()

            self._reset_retry_count()
            return row is not None

        except psycopg2.OperationalError as e:
            self._increment_retry_count()
            logger.error(
                f"pgvector connection error checking existence of {native_id}: {e}"
            )
            raise
        except MaxRetriesExceededError:
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

            self._reset_retry_count()

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

            logger.info(f"pgvector search completed: {len(results)} results")
            return results

        except psycopg2.OperationalError as e:
            self._increment_retry_count()
            logger.error(f"pgvector connection error during search: {e}")
            raise
        except MaxRetriesExceededError:
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
            with self.conn.cursor() as cur:
                cur.execute(f"SELECT COUNT(*) FROM {self.table};")
                total_chunks = cur.fetchone()[0]

                cur.execute(
                    "SELECT pg_total_relation_size(%s);",
                    (self.table,),
                )
                size_bytes = cur.fetchone()[0]

            self._reset_retry_count()

            return {
                "instance": "pgvector",
                "index_name": self.table,
                "total_chunks": total_chunks,
                "size_bytes": size_bytes,
                "size_mb": size_bytes / (1024 * 1024),
            }

        except psycopg2.OperationalError as e:
            self._increment_retry_count()
            logger.error(f"pgvector connection error getting statistics: {e}")
            raise
        except MaxRetriesExceededError:
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
