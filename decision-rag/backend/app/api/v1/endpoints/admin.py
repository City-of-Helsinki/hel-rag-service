"""Administrative operations endpoints."""

from pathlib import Path

from fastapi import APIRouter, Depends, HTTPException, Query

from app.api.deps import get_elasticsearch_store, get_pgvector_store, get_repository, verify_api_key
from app.core import get_logger, settings
from app.repositories import DecisionRepository
from app.services import ElasticsearchVectorStore, PgvectorVectorStore
from app.utils import raise_error_with_id

router = APIRouter()
logger = get_logger(__name__)


@router.delete("/checkpoint")
async def clear_checkpoint(
    repository: DecisionRepository = Depends(get_repository),
    _: None = Depends(verify_api_key),
):
    """
    Clear checkpoint file.

    Removes the checkpoint file used for resuming operations.
    This will cause subsequent resume operations to start from the beginning.

    Returns:
        Success status message
    """
    try:
        checkpoint_path = repository.checkpoint_file

        if checkpoint_path.exists():
            checkpoint_path.unlink()
            logger.info("Checkpoint file deleted")
            return {"status": "success", "message": "Checkpoint cleared"}
        else:
            return {"status": "success", "message": "No checkpoint file found"}

    except Exception as e:
        raise_error_with_id(
            logger, e, status_code=500,
            message="Failed to clear checkpoint",
            context={"operation": "clear_checkpoint"},
        )


@router.delete("/repository")
async def clear_repository(
    confirm: bool = Query(
        False,
        description="Must be set to true to confirm deletion",
    ),
    repository: DecisionRepository = Depends(get_repository),
    _: None = Depends(verify_api_key),
):
    """
    Clear all stored documents.

    **WARNING**: This operation is irreversible and will delete all locally stored
    decision documents and the checkpoint file.

    Args:
        confirm: Must be explicitly set to true to perform deletion

    Returns:
        Deletion statistics
    """
    if not confirm:
        raise HTTPException(
            status_code=400,
            detail="Must set confirm=true to perform this operation",
        )

    try:
        # Get stats before deletion
        stats_before = repository.get_statistics()
        total_before = stats_before["total_documents"]

        # Clear repository
        repository.clear_repository()

        logger.info(f"Repository cleared: {total_before} documents deleted")

        return {
            "status": "success",
            "message": "Repository cleared successfully",
            "documents_deleted": total_before,
        }

    except Exception as e:
        raise_error_with_id(
            logger, e, status_code=500,
            message="Failed to clear repository",
            context={"operation": "clear_repository"},
        )


@router.delete("/vector-store/elasticsearch")
async def clear_elasticsearch_vector_store(
    confirm: bool = Query(
        False,
        description="Must be set to true to confirm deletion",
    ),
    vector_store: ElasticsearchVectorStore = Depends(get_elasticsearch_store),
    _: None = Depends(verify_api_key),
):
    """
    Clear all documents from the Elasticsearch vector store.

    **WARNING**: This operation is irreversible and will delete all indexed
    document chunks and embeddings from Elasticsearch.

    Args:
        confirm: Must be explicitly set to true to perform deletion

    Returns:
        Deletion statistics
    """
    if not confirm:
        raise HTTPException(
            status_code=400,
            detail="Must set confirm=true to perform this operation",
        )

    try:
        count_response = vector_store.client.count(index=vector_store.index_name)
        total_before = count_response.get("count", 0)

        logger.info(f"Clearing Elasticsearch vector store: {total_before} chunks to delete")

        delete_response = vector_store.client.delete_by_query(
            index=vector_store.index_name,
            body={"query": {"match_all": {}}},
        )

        deleted = delete_response.get("deleted", 0)

        logger.info(f"Elasticsearch vector store cleared: {deleted} chunks deleted")

        return {
            "status": "success",
            "message": "Elasticsearch vector store cleared successfully",
            "chunks_deleted": deleted,
        }

    except Exception as e:
        raise_error_with_id(
            logger, e, status_code=500,
            message="Failed to clear Elasticsearch vector store",
            context={"operation": "clear_elasticsearch_vector_store"},
        )


@router.delete("/vector-store/pgvector")
async def clear_pgvector_vector_store(
    confirm: bool = Query(
        False,
        description="Must be set to true to confirm deletion",
    ),
    vector_store: PgvectorVectorStore = Depends(get_pgvector_store),
    _: None = Depends(verify_api_key),
):
    """
    Clear all documents from the pgvector (PostgreSQL) vector store.

    **WARNING**: This operation is irreversible and will delete all indexed
    document chunks and embeddings from PostgreSQL.

    Args:
        confirm: Must be explicitly set to true to perform deletion

    Returns:
        Deletion statistics
    """
    if not confirm:
        raise HTTPException(
            status_code=400,
            detail="Must set confirm=true to perform this operation",
        )

    try:
        with vector_store.conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {vector_store.table};")
            total_before = cur.fetchone()[0]

        logger.info(f"Clearing pgvector store: {total_before} chunks to delete")

        with vector_store.conn.cursor() as cur:
            cur.execute(f"DELETE FROM {vector_store.table};")
            deleted = cur.rowcount

        vector_store.conn.commit()

        logger.info(f"pgvector store cleared: {deleted} chunks deleted")

        return {
            "status": "success",
            "message": "pgvector store cleared successfully",
            "chunks_deleted": deleted,
        }

    except Exception as e:
        vector_store.conn.rollback()
        raise_error_with_id(
            logger, e, status_code=500,
            message="Failed to clear pgvector store",
            context={"operation": "clear_pgvector_vector_store"},
        )


@router.get("/config")
async def get_configuration(
    _: None = Depends(verify_api_key),
):
    """
    Get current configuration.

    Returns sanitized configuration (secrets are masked).

    Returns:
        Current configuration settings
    """
    try:
        # Return sanitized config (mask sensitive values)
        config = {
            "api": {
                "host": settings.API_HOST,
                "port": settings.API_PORT,
                "workers": settings.API_WORKERS,
                "reload": settings.API_RELOAD,
                "title": settings.API_TITLE,
                "version": settings.API_VERSION,
                "prefix": settings.API_PREFIX,
            },
            "external_api": {
                "base_url": settings.API_BASE_URL,
                "api_key_configured": bool(settings.API_KEY),
                "page_size": settings.API_PAGE_SIZE,
                "requests_per_second": settings.REQUESTS_PER_SECOND,
                "timeout": settings.REQUEST_TIMEOUT,
            },
            "azure_openai": {
                "endpoint": settings.AZURE_OPENAI_ENDPOINT,
                "api_key_configured": bool(settings.AZURE_OPENAI_API_KEY),
                "model": settings.AZURE_OPENAI_EMBEDDING_MODEL,
                "api_version": settings.AZURE_OPENAI_API_VERSION,
            },
            "elasticsearch": {
                "url": settings.ELASTICSEARCH_URL,
                "index": settings.ELASTICSEARCH_INDEX,
            },
            "storage": {
                "data_dir": settings.DATA_DIR,
                "decisions_dir": settings.DECISIONS_DIR,
            },
            "processing": {
                "max_workers_ingestion": settings.MAX_WORKERS_INGESTION,
                "max_workers_attachments": settings.MAX_WORKERS_ATTACHMENTS,
                "max_workers_attachment_processing": settings.MAX_WORKERS_ATTACHMENT_PROCESSING,
                "embedding_batch_size": settings.EMBEDDING_BATCH_SIZE,
                "embed_metadata_in_chunks": settings.EMBED_METADATA_IN_CHUNKS,
                "process_attachments": settings.PROCESS_ATTACHMENTS,
            },
            "collection": {
                "name": settings.COLLECTION_NAME,
                "description": settings.COLLECTION_DESCRIPTION,
            },
        }

        return config

    except Exception as e:
        raise_error_with_id(
            logger, e, status_code=500,
            message="Failed to retrieve configuration",
            context={"operation": "get_configuration"},
        )


@router.get("/logs")
async def get_recent_logs(
    log_type: str = Query("pipeline", description="Log type: pipeline, api, or error"),
    lines: int = Query(50, ge=1, le=1000, description="Number of recent lines to return"),
    _: None = Depends(verify_api_key),
):
    """
    Get recent log entries.

    Returns the last N lines from the specified log file.

    Args:
        log_type: Type of log to retrieve (pipeline, api, error)
        lines: Number of recent lines to return

    Returns:
        Recent log entries
    """
    try:
        # Determine log file
        log_file_map = {
            "pipeline": settings.LOG_FILE,
            "api": settings.API_LOG_FILE,
            "error": settings.ERROR_LOG_FILE,
        }

        if log_type not in log_file_map:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid log_type. Must be one of: {', '.join(log_file_map.keys())}",
            )

        log_file = Path(settings.LOG_DIR) / log_file_map[log_type]

        if not log_file.exists():
            return {
                "log_type": log_type,
                "lines": [],
                "message": "Log file not found",
            }

        # Read last N lines
        with open(log_file, "r", encoding="utf-8") as f:
            all_lines = f.readlines()
            recent_lines = all_lines[-lines:] if len(all_lines) > lines else all_lines

        return {
            "log_type": log_type,
            "total_lines": len(all_lines),
            "returned_lines": len(recent_lines),
            "lines": [line.rstrip() for line in recent_lines],
        }

    except HTTPException:
        raise
    except Exception as e:
        raise_error_with_id(
            logger, e, status_code=500,
            message="Failed to read log file",
            context={"operation": "get_recent_logs", "log_type": log_type},
        )
