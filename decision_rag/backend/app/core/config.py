"""
Configuration settings for the decision data pipeline.
"""

from datetime import datetime, timedelta

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Application settings and configuration."""

    # Debug and environment settings
    DEBUG: bool = True

    # API Configuration
    API_KEY: str = ""
    API_BASE_URL: str = ""
    DECISION_IDS_ENDPOINT: str = ""
    DECISION_DOCUMENT_ENDPOINT: str = ""

    # Request parameters
    API_PAGE_SIZE: int = 1000

    # Rate limiting
    REQUESTS_PER_SECOND: float = 1.0
    REQUEST_TIMEOUT: int = 30  # seconds

    # Retry configuration
    MAX_RETRY_ATTEMPTS: int = 3
    MAX_TOTAL_RETRIES: int = 10  # Maximum total retry attempts across all operations
    RETRY_BACKOFF_FACTOR: float = 2.0  # Exponential backoff multiplier
    RETRY_MIN_WAIT: float = 1.0  # Minimum wait time in seconds
    RETRY_MAX_WAIT: float = 60.0  # Maximum wait time in seconds

    # Document fetch retry configuration
    DOCUMENT_FETCH_MAX_RETRIES: int = 3  # Maximum retry attempts per failed document
    DOCUMENT_FETCH_RETRY_MIN_WAIT: float = 5.0  # Minimum wait time between retries (seconds)
    DOCUMENT_FETCH_RETRY_MAX_WAIT: float = 300.0  # Maximum wait time between retries (seconds)
    DOCUMENT_FETCH_RETRY_BACKOFF_MULTIPLIER: float = 3.0  # Exponential backoff multiplier
    DOCUMENT_FETCH_RETRY_STAGGER_DELAY: float = 2.0  # Delay between initiating retries for different documents

    # Date range configuration
    START_DATE: str = "2025-01-01T00:00:00.000"

    # End date is 38 days before current date
    @property
    def END_DATE(self) -> str:
        """Calculate end date as 38 days before current date."""
        end = datetime.now() - timedelta(days=38)
        return end.strftime("%Y-%m-%dT%H:%M:%S")

    BATCH_SIZE_DAYS: int = 7  # Weekly batches

    # Storage configuration
    DATA_DIR: str = "data"
    DECISIONS_DIR: str = "data/decisions"
    CHECKPOINT_FILE: str = "data/checkpoint.json"

    # Checkpoint configuration
    CHECKPOINT_INTERVAL_FETCH: int = 50  # Save checkpoint every N documents during fetch
    CHECKPOINT_INTERVAL_INGEST: int = 10  # Save checkpoint every N documents during ingest
    CHECKPOINT_INTERVAL_FULL_PIPELINE_BATCHES: int = 1  # Save checkpoint every N batches in full pipeline

    # Attachment configuration
    ATTACHMENT_DOWNLOAD_DIR: str = "data/attachments_temp"
    PROCESS_ATTACHMENTS: bool = True
    MAX_ATTACHMENT_SIZE_MB: int = 50
    ATTACHMENT_TIMEOUT: int = 60

    # Logging configuration
    LOG_LEVEL: str = "INFO"
    LOG_DIR: str = "data/logs"
    LOG_FILE: str = "pipeline.log"
    API_LOG_FILE: str = "api.log"
    ERROR_LOG_FILE: str = "errors.log"

    # Log Management
    LOG_RETENTION_DAYS: int = 7
    LOG_ROTATION_WHEN: str = "midnight"
    LOG_ROTATION_INTERVAL: int = 1
    LOG_BACKUP_COUNT: int = 7

    # Processing configuration
    MAX_CONCURRENT_REQUESTS: int = 2

    # Concurrency configuration
    MAX_WORKERS_INGESTION: int = 1  # Parallel document ingestion
    MAX_WORKERS_ATTACHMENTS: int = 5  # Parallel attachment downloads
    MAX_WORKERS_ATTACHMENT_PROCESSING: int = 2  # Parallel attachment processing
    EMBEDDING_BATCH_SIZE: int = 100  # Increased from 16 for better throughput

    # Azure OpenAI configuration
    AZURE_OPENAI_ENDPOINT: str = ""
    AZURE_OPENAI_API_KEY: str = ""
    AZURE_OPENAI_API_VERSION: str = "2024-02-01"
    AZURE_OPENAI_EMBEDDING_MODEL: str = "text-embedding-3-large"
    EMBEDDING_DIMENSION: int = 3072

    # Elasticsearch configuration
    ELASTICSEARCH_URL: str = "https://localhost:9200"
    ELASTICSEARCH_INDEX: str = "decision_documents"
    ELASTICSEARCH_CERT: str = ""  # Path to SSL certificate if needed
    ELASTICSEARCH_USER: str = ""
    ELASTICSEARCH_PASSWORD: str = ""

    # Chunking configuration
    EMBED_METADATA_IN_CHUNKS: bool = True  # Feature flag for metadata embedding
    METADATA_HEADER_OVERHEAD_TOKENS: int = 250  # Expected token overhead for headers

    # Collection configuration for Open WebUI compatibility
    COLLECTION_NAME: str = "decisions"
    COLLECTION_DESCRIPTION: str = "Knowledge for storing decisions for City of Helsinki."

    # API Server Configuration
    API_HOST: str = "0.0.0.0"
    API_PORT: int = 8000
    API_WORKERS: int = 1
    API_RELOAD: bool = False
    API_CORS_ORIGINS: list[str] = ["*"]
    API_TITLE: str = "Helsinki Decision Documents API"
    API_VERSION: str = "1.0.0"
    API_DESCRIPTION: str = "RESTful API for Helsinki Decision Documents pipeline operations"
    API_PREFIX: str = "/api/v1"

    # API Authentication Configuration
    API_AUTH_ENABLED: bool = False  # Feature flag to enable/disable authentication
    API_AUTH_KEY: str = ""  # API key for authentication (use environment variable in production)
    API_AUTH_KEY_HEADER: str = "X-API-Key"  # HTTP header name for API key

    # Scheduler Configuration
    SCHEDULER_ENABLED: bool = False
    SCHEDULER_INTERVAL_HOURS: int = 24
    SCHEDULER_START_TIME: str = ""  # Optional, format: "HH:MM" (e.g., "02:00")
    SCHEDULER_TIMEZONE: str = "Europe/Helsinki"
    SCHEDULER_MAX_INSTANCES: int = 1
    PIPELINE_TIMEOUT_HOURS: int = 72
    SCHEDULER_START_DATE: str = ""  # Default start date for scheduled runs (empty = use START_DATE)
    SCHEDULER_END_DATE: str = ""  # Default end date for scheduled runs (empty = use END_DATE)
    SCHEDULER_BATCH_SIZE: int = 100
    SCHEDULER_SKIP_EXISTING: bool = True
    SCHEDULER_KEEP_FILES: bool = False
    SCHEDULER_STATE_FILE: str = "data/scheduler_state.json"
    SCHEDULER_LOG_FILE: str = "scheduler.log"

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


# Global settings instance
settings = Settings()
