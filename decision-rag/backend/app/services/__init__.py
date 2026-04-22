"""Services package initialization."""

from .api_client import DecisionAPIClient
from .attachment_downloader import AttachmentDownloader
from .chunker import DocumentChunk, ParagraphChunker
from .content_converter import (
    HTMLSanitizer,
    MarkdownConverter,
    convert_attachment_content,
    convert_decision_content,
)
from .data_fetcher import DecisionDataFetcher
from .elasticsearch_store import ElasticsearchVectorStore
from .embedder import AzureEmbedder, EmbeddingResult
from .ingestion_pipeline import IngestionPipeline
from .job_manager import Job, JobManager, job_manager
from .parquet_embedding_saver import ParquetEmbeddingSaver
from .pgvector_store import PgvectorVectorStore
from .scheduler import SchedulerService
from .scheduler_state import ExecutionRecord, SchedulerState, SchedulerStateManager
from .vector_store import BaseVectorStore, CompositeVectorStore, MaxRetriesExceededError

__all__ = [
    "DecisionAPIClient",
    "DecisionDataFetcher",
    "AttachmentDownloader",
    "convert_decision_content",
    "convert_attachment_content",
    "MarkdownConverter",
    "HTMLSanitizer",
    "ParagraphChunker",
    "DocumentChunk",
    "AzureEmbedder",
    "EmbeddingResult",
    "BaseVectorStore",
    "ElasticsearchVectorStore",
    "PgvectorVectorStore",
    "CompositeVectorStore",
    "MaxRetriesExceededError",
    "IngestionPipeline",
    "Job",
    "JobManager",
    "job_manager",
    "ParquetEmbeddingSaver",
    "SchedulerService",
    "SchedulerState",
    "SchedulerStateManager",
    "ExecutionRecord",
]
