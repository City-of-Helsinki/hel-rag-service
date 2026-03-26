"""Dependency injection for FastAPI endpoints."""

from typing import Generator

from fastapi import Header, HTTPException, status

from app.core import get_logger, settings
from app.repositories import DecisionRepository
from app.services import (
    AzureEmbedder,
    DecisionAPIClient,
    DecisionDataFetcher,
    ElasticsearchVectorStore,
    IngestionPipeline,
    ParagraphChunker,
    SchedulerService,
    job_manager,
)
from app.services.attachment_downloader import AttachmentDownloader

logger = get_logger(__name__)


async def verify_api_key(
    api_key: str = Header(None, alias="X-API-Key")
) -> None:
    """
    Verify API key for authentication.

    This dependency checks if API authentication is enabled and validates
    the API key from the request header.

    Args:
        api_key: API key from the request header

    Raises:
        HTTPException: 401 if API key is invalid
        HTTPException: 403 if API key is missing

    Returns:
        None if authentication succeeds or is disabled
    """
    # If authentication is disabled, allow all requests
    if not settings.API_AUTH_ENABLED:
        return

    # Get header name from settings
    header_name = settings.API_AUTH_KEY_HEADER

    # Check if API key is provided
    if api_key is None:
        logger.warning(f"API request attempted without {header_name} header")
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=f"API key is required. Provide it in the {header_name} header.",
        )

    # Validate API key
    if api_key != settings.API_AUTH_KEY:
        logger.warning("API request attempted with invalid API key")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid API key",
        )

    # Authentication successful
    logger.debug("API request authenticated successfully")


def get_repository() -> DecisionRepository:
    """
    Get DecisionRepository instance.

    Returns:
        DecisionRepository instance
    """
    return DecisionRepository(settings.DATA_DIR)


def get_api_client() -> DecisionAPIClient:
    """
    Get DecisionAPIClient instance.

    Returns:
        DecisionAPIClient instance
    """
    return DecisionAPIClient()


def get_vector_store() -> Generator[ElasticsearchVectorStore, None, None]:
    """
    Get ElasticsearchVectorStore instance.

    Yields:
        ElasticsearchVectorStore instance
    """
    store = ElasticsearchVectorStore()
    try:
        yield store
    finally:
        # Cleanup if needed
        pass


def get_embedder() -> AzureEmbedder:
    """
    Get AzureEmbedder instance.

    Returns:
        AzureEmbedder instance
    """
    return AzureEmbedder()


def get_chunker() -> ParagraphChunker:
    """
    Get ParagraphChunker instance.

    Returns:
        ParagraphChunker instance
    """
    return ParagraphChunker()


def get_ingestion_pipeline() -> IngestionPipeline:
    """
    Get IngestionPipeline instance with all dependencies.

    Returns:
        IngestionPipeline instance
    """
    repository = get_repository()
    vector_store = ElasticsearchVectorStore()
    embedder = get_embedder()
    chunker = get_chunker()
    attachment_downloader = get_attachment_downloader()

    return IngestionPipeline(
        repository=repository,
        vector_store=vector_store,
        embedder=embedder,
        chunker=chunker,
        attachment_downloader=attachment_downloader,
    )


def get_data_fetcher() -> DecisionDataFetcher:
    """
    Get DecisionDataFetcher instance with dependencies.

    Returns:
        DecisionDataFetcher instance
    """
    api_client = get_api_client()
    return DecisionDataFetcher(api_client)

def get_attachment_downloader() -> AttachmentDownloader:
    """
    Get AttachmentDownloader instance.

    Returns:
        AttachmentDownloader instance
    """
    if settings.PROCESS_ATTACHMENTS:
        return AttachmentDownloader()
    return None


def get_job_manager():
    """
    Get global JobManager instance.

    Returns:
        JobManager instance
    """
    return job_manager


def get_scheduler() -> SchedulerService:
    """
    Get SchedulerService instance (singleton).

    Returns:
        SchedulerService instance
    """
    if not hasattr(get_scheduler, "_instance"):
        get_scheduler._instance = SchedulerService(
            job_manager=get_job_manager(),
            fetcher=get_data_fetcher(),
            pipeline=get_ingestion_pipeline(),
        )
    return get_scheduler._instance

