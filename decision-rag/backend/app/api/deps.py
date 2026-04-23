"""Dependency injection for FastAPI endpoints."""

from typing import Generator, Optional

from fastapi import Header, HTTPException, status

from app.core import get_logger, settings
from app.repositories import DecisionRepository
from app.services import (
    AzureEmbedder,
    BaseVectorStore,
    CompositeVectorStore,
    DecisionAPIClient,
    DecisionDataFetcher,
    ElasticsearchVectorStore,
    IngestionPipeline,
    ParagraphChunker,
    PgvectorVectorStore,
    SchedulerService,
    job_manager,
)
from app.services.attachment_downloader import AttachmentDownloader
from app.services.blob_storage import AzureBlobRawResponseSaver
from app.services.parquet_embedding_saver import ParquetEmbeddingSaver

logger = get_logger(__name__)


async def verify_api_key(
    api_key: str = Header(None, alias=settings.API_AUTH_KEY_HEADER),
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


def get_api_client(blob_saver: Optional[AzureBlobRawResponseSaver] = None) -> DecisionAPIClient:
    """
    Get DecisionAPIClient instance.

    Returns:
        DecisionAPIClient instance
    """
    raw_response_saver = blob_saver.buffer if blob_saver is not None else None
    return DecisionAPIClient(raw_response_saver=raw_response_saver)


def _build_vector_store() -> BaseVectorStore:
    """
    Build the appropriate vector store based on VECTOR_STORE_BACKENDS setting.

    Returns:
        BaseVectorStore instance (single backend or CompositeVectorStore).
    """
    backends_config: list[str] = [
        b.strip().lower() for b in settings.VECTOR_STORE_BACKENDS
    ]

    instances: list[BaseVectorStore] = []
    if "elasticsearch" in backends_config:
        instances.append(ElasticsearchVectorStore())
    if "pgvector" in backends_config:
        instances.append(PgvectorVectorStore())

    if not instances:
        raise ValueError(
            f"No valid backends found in VECTOR_STORE_BACKENDS: {settings.VECTOR_STORE_BACKENDS}"
        )

    if len(instances) == 1:
        return instances[0]

    return CompositeVectorStore(instances)


def get_vector_store() -> Generator[BaseVectorStore, None, None]:
    """
    Get vector store instance based on configured backends.

    Yields:
        BaseVectorStore instance
    """
    store = _build_vector_store()
    try:
        yield store
    finally:
        for s in store.backends if isinstance(store, CompositeVectorStore) else [store]:
            s.close()


def get_elasticsearch_store() -> Generator[ElasticsearchVectorStore, None, None]:
    """
    Get ElasticsearchVectorStore instance.

    Yields:
        ElasticsearchVectorStore instance
    """
    store = ElasticsearchVectorStore()
    try:
        yield store
    finally:
        store.close()


def get_pgvector_store() -> Generator[PgvectorVectorStore, None, None]:
    """
    Get PgvectorVectorStore instance.

    Yields:
        PgvectorVectorStore instance
    """
    store = PgvectorVectorStore()
    try:
        yield store
    finally:
        store.close()


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
    vector_store = _build_vector_store()
    embedder = get_embedder()
    chunker = get_chunker()
    attachment_downloader = get_attachment_downloader()
    parquet_saver = get_parquet_saver()

    return IngestionPipeline(
        repository=repository,
        vector_store=vector_store,
        embedder=embedder,
        chunker=chunker,
        attachment_downloader=attachment_downloader,
        parquet_saver=parquet_saver,
    )


def get_parquet_saver() -> Optional[ParquetEmbeddingSaver]:
    """Return a configured ParquetEmbeddingSaver or None if disabled."""
    if not settings.AZURE_BLOB_EMBEDDINGS_ENABLED:
        return None
    return ParquetEmbeddingSaver(
        container_name=settings.AZURE_BLOB_EMBEDDINGS_CONTAINER_NAME,
        blob_prefix=settings.AZURE_BLOB_EMBEDDINGS_BLOB_PREFIX.format(dimension=settings.EMBEDDING_DIMENSION),
        connection_string=settings.AZURE_BLOB_CONNECTION_STRING or None,
        account_url=settings.AZURE_BLOB_ACCOUNT_URL or None,
    )


def get_blob_saver() -> Optional[AzureBlobRawResponseSaver]:
    """Return a configured AzureBlobRawResponseSaver or None if disabled."""
    if not settings.AZURE_BLOB_RAW_RESPONSES_ENABLED:
        return None
    return AzureBlobRawResponseSaver(
        container_name=settings.AZURE_BLOB_CONTAINER_NAME,
        blob_prefix=settings.AZURE_BLOB_BLOB_PREFIX,
        connection_string=settings.AZURE_BLOB_CONNECTION_STRING or None,
        account_url=settings.AZURE_BLOB_ACCOUNT_URL or None,
    )


def get_data_fetcher() -> DecisionDataFetcher:
    """
    Get DecisionDataFetcher instance with dependencies.

    Returns:
        DecisionDataFetcher instance
    """
    blob_saver = get_blob_saver()
    api_client = get_api_client(blob_saver=blob_saver)
    return DecisionDataFetcher(api_client=api_client, blob_saver=blob_saver)

def get_attachment_downloader() -> Optional[AttachmentDownloader]:
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

