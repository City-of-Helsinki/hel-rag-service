# Helsinki Decision Documents Data Pipeline

A robust data pipeline for retrieving, processing, and indexing Helsinki city decision documents. This pipeline fetches historical decision data from the decision API, converts HTML to Markdown, chunks text intelligently, generates embeddings using Azure OpenAI, and indexes to Elasticsearch for RAG (Retrieval-Augmented Generation) implementation.

## Features

### Pipeline Features
- **Batch Processing**: Fetches data in weekly batches to handle large date ranges efficiently
- **Retry Logic**: Implements exponential backoff for resilient API communication
- **Rate Limiting**: Respects API rate limits to avoid overwhelming the service
- **Checkpointing**: Saves progress periodically and can resume from interruptions
- **Parallel Fetching**: Uses thread pools for concurrent document retrieval
- **Data Validation**: Validates all fetched documents for completeness
- **Comprehensive Logging**: Detailed logs for monitoring and debugging
- **CLI Interface**: User-friendly command-line interface with rich output
- **HTML to Markdown Conversion**: Converts HTML content to clean Markdown using markitdown
- **Intelligent Chunking**: Splits text into optimal chunks (500-1000 tokens) with overlap
- **Azure OpenAI Embeddings**: Generates high-quality embeddings using text-embedding-3-large
- **Vector Store**: Indexes document chunks in Elasticsearch for semantic search

### API & Automation
- **RESTful API**: FastAPI-based backend for pipeline operations and monitoring
- **Automated Scheduler**: Configurable interval or time-based pipeline execution
- **Background Jobs**: Async job execution with progress tracking
- **Health Monitoring**: Component health checks and status reporting
- **Interactive Documentation**: Swagger UI and ReDoc for API exploration

### Operations
- **Comprehensive Logging**: Detailed logs for monitoring and debugging
- **CLI Interface**: User-friendly command-line interface with rich output
- **Docker Support**: Containerized deployment with docker-compose

## Architecture

```
backend/
├── app/
│   ├── core/           # Core configuration and logging
│   ├── schemas/        # Pydantic models for data validation
│   ├── services/       # Services for data processing
│   │   ├── api_client.py          # API client for fetching decisions
│   │   ├── data_fetcher.py        # Data fetching orchestration
│   │   ├── content_converter.py   # HTML to Markdown conversion
│   │   ├── chunker.py             # Text chunking with token counting
│   │   ├── embedder.py            # Azure OpenAI embedding generation
│   │   ├── vector_store.py        # Elasticsearch vector store
│   │   ├── ingestion_pipeline.py  # Full ingestion pipeline
│   │   ├── scheduler.py           # Automated scheduling service
│   │   ├── scheduler_state.py     # Scheduler state management
│   │   └── job_manager.py         # Background job management
│   ├── repositories/   # Data storage and checkpoint management
│   ├── api/            # FastAPI endpoints
│   │   ├── v1/
│   │   │   ├── endpoints/  # API route handlers
│   │   │   └── models/     # Request/response models
│   │   └── router.py       # API router configuration
│   └── utils/          # Utility functions (dates, parsing, validation)
├── tests/              # Unit tests
├── pipeline.py         # Main CLI application
├── main.py             # FastAPI server entrypoint
├── trigger_scheduler.py # Scheduler control script
└── requirements.txt    # Python dependencies
```

## Installation

### Prerequisites

- Python 3.11 or higher
- pip or uv for package management

### Setup

1. **Clone the repository** (if not already done)

2. **Navigate to the backend directory**:
   ```bash
   cd backend
   ```

3. **Create a virtual environment**:
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

4. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

5. **Create environment file** (optional):
   ```bash
   cp .env.example .env
   # Edit .env to customize settings
   ```

## FastAPI Server

The backend includes a FastAPI server for API-based pipeline operations and monitoring.

### Starting the API Server

```bash
# Development mode with auto-reload
python main.py

# Or using uvicorn directly
uvicorn main:app --reload --host 0.0.0.0 --port 8000

# Production mode
uvicorn main:app --workers 4 --host 0.0.0.0 --port 8000
```

### API Documentation

Once running, access:
- **Swagger UI**: http://localhost:8000/docs
- **ReDoc**: http://localhost:8000/redoc
- **Health Check**: http://localhost:8000/api/v1/health

### API Authentication

The API supports optional API key-based authentication for securing endpoints.

**Enable authentication in `.env`:**
```bash
API_AUTH_ENABLED=true
API_AUTH_KEY=your-secure-api-key-here  # Generate with: openssl rand -hex 32
API_AUTH_KEY_HEADER=X-API-Key
```

**Using authentication:**
```bash
# Include API key in requests
curl -H "X-API-Key: your-api-key" http://localhost:8000/api/v1/data/documents
```

**Note:** Health endpoints (`/health`, `/api/v1/health/detailed`, `/api/v1/scheduler/health`) do not require authentication.

For detailed authentication documentation, see [API_README.md](../ai_docs/API_README.md#authentication).

## Scheduler Service

The scheduler provides automated pipeline execution with configurable scheduling.

### Configuration

Configure in `.env`:
```bash
SCHEDULER_ENABLED=true
SCHEDULER_INTERVAL_HOURS=24
SCHEDULER_START_TIME=02:00  # Optional: specific time
SCHEDULER_TIMEZONE=Europe/Helsinki
```

### Managing the Scheduler

```bash
# Check status
curl http://localhost:8000/api/v1/scheduler/status

# Trigger manual execution
curl -X POST http://localhost:8000/api/v1/scheduler/trigger

# Or use the control script
python trigger_scheduler.py status
python trigger_scheduler.py trigger
```

## Usage

The pipeline can be used in three ways:
1. **CLI Commands** - Direct command-line execution
2. **FastAPI Server** - API-based operations
3. **Scheduler** - Automated execution

### CLI Pipeline Commands

The pipeline is controlled through the `pipeline.py` CLI script.

#### Full Pipeline (Recommended)

Run the complete end-to-end pipeline in one streaming operation:

```bash
# Fetch and process all data (uses START_DATE from config)
python pipeline.py full-pipeline

# Specific date range
python pipeline.py full-pipeline --start-date 2024-01-01 --end-date 2024-12-31
```

### Fetch Data Only

Fetch decisions from the API without processing:

```bash
python pipeline.py fetch
```

### Fetch Specific Date Range

```bash
python pipeline.py fetch --start-date 2024-01-01 --end-date 2024-12-31
```

### Resume from Checkpoint

If the fetch was interrupted, resume from the last checkpoint:

```bash
python pipeline.py fetch --resume
```

### View Statistics

Display information about stored data:

```bash
python pipeline.py stats
```

### Validate Data

Validate the integrity of stored documents:

```bash
python pipeline.py validate --sample-size 100
```

### Ingest Documents into Vector Store

Process fetched documents through the full ingestion pipeline (convert, chunk, embed, index):

```bash
# Ingest all fetched documents
python pipeline.py ingest

# Ingest documents from specific date
python pipeline.py ingest --start-date 2024-01-01

# Process in batches of 20 documents
python pipeline.py ingest --batch-size 20

# Force reindexing of existing documents
python pipeline.py ingest --reindex
```

The ingestion pipeline performs the following steps:
1. **Convert HTML to Markdown**: Sanitizes and converts HTML content to clean Markdown
2. **Chunk Text**: Splits text into 500-1000 token chunks with 10-20% overlap
3. **Generate Embeddings**: Creates embeddings using Azure OpenAI text-embedding-3-large
4. **Index to Elasticsearch**: Stores chunks with embeddings in Elasticsearch vector database

### Full Pipeline (Recommended)

Run the complete end-to-end pipeline in a single streaming operation:

```bash
# Basic usage - fetch and process all historical data
python pipeline.py full-pipeline

# Specific date range with custom batch size
python pipeline.py full-pipeline --start-date 2024-01-01 --end-date 2024-12-31 --batch-size 100

# Resume interrupted pipeline
python pipeline.py full-pipeline --resume

# Skip already indexed documents
python pipeline.py full-pipeline --skip-existing

# Debug mode with file preservation
python pipeline.py full-pipeline --log-level DEBUG --keep-files --batch-size 10

# Production run without attachments
python pipeline.py full-pipeline --skip-attachments --batch-size 200
```

**Key Features:**
- **Streaming Processing**: Fetches and processes documents in batches without storing all to disk
- **Memory Management**: Automatically deletes processed files to control disk usage
- **Batch Processing**: Configurable batch size (default: 50 documents)
- **Error Handling**: Continues processing on individual document failures
- **Checkpointing**: Can resume from interruption
- **Progress Display**: Real-time progress for both fetching and processing

**Options:**
- `--batch-size`: Documents to process per batch (default: 50)
- `--skip-existing`: Skip documents already in vector store
- `--skip-attachments`: Skip attachment processing
- `--keep-files`: Keep decision files after processing (default: delete)
- `--resume`: Resume from last checkpoint
- `--log-level`: Logging level (DEBUG, INFO, WARNING, ERROR)

The full-pipeline command combines fetch and ingest operations with optimized memory management.

### Available Commands

```bash
# Pipeline operations
python pipeline.py full-pipeline   # Complete end-to-end pipeline (recommended)
python pipeline.py fetch           # Fetch documents from API only
python pipeline.py ingest          # Process already-fetched documents

# Information & management
python pipeline.py stats           # Show repository statistics
python pipeline.py vector-store-stats  # Show vector store statistics
python pipeline.py validate        # Validate stored documents
python pipeline.py clear-checkpoint    # Clear checkpoint file
python pipeline.py clear-repository    # Clear all stored data
python pipeline.py version         # Display version
```

### Clear Checkpoint

Clear the checkpoint to start fresh:

```bash
python pipeline.py clear-checkpoint
```

### Additional Options

- `--log-level`: Set logging level (DEBUG, INFO, WARNING, ERROR)
- `--skip-existing`: Skip existing documents (default False)

```bash
python pipeline.py fetch --log-level DEBUG --skip-existing
```

## Configuration

Configuration is managed through environment variables or the `.env` file. Key settings include:

### API Configuration
- `API_BASE_URL`: Base URL for the decision API
- `API_KEY`: API key for authentication
- `DECISION_IDS_ENDPOINT`: Endpoint for fetching decision ids
- `DECISION_DOCUMENT_ENDPOINT`: Endpoint for fetching single documents
- `API_PAGE_SIZE`: Amount of fetched decision ids 

### Rate Limiting
- `REQUESTS_PER_SECOND`: Rate limit for API requests (default: 5.0)
- `REQUEST_TIMEOUT`: Request timeout in seconds (default: 30)

### Retry Configuration
- `MAX_RETRY_ATTEMPTS`: Maximum retry attempts (default: 3)
- `RETRY_BACKOFF_FACTOR`: Exponential backoff multiplier (default: 2.0)

### Date Range
- `START_DATE`: Start date for fetching decisions (default: 2017-01-01)
- `END_DATE`: End date for fetching decisions (default:  automatically calculated as 2 months before current date)

### Azure OpenAI Configuration
- `AZURE_OPENAI_ENDPOINT`: Azure OpenAI endpoint URL
- `AZURE_OPENAI_API_KEY`: Azure OpenAI API key
- `AZURE_OPENAI_API_VERSION`: API version (default: 2024-02-01)
- `AZURE_OPENAI_EMBEDDING_MODEL`: Embedding model name (default: text-embedding-3-large)

### Elasticsearch Configuration
- `ELASTICSEARCH_URL`: Elasticsearch URL (default: http://localhost:9200)
- `ELASTICSEARCH_INDEX`: Index name for decision documents (default: decision_documents)

### Scheduler Configuration
- `SCHEDULER_ENABLED`: Enable/disable scheduler (default: false)
- `SCHEDULER_INTERVAL_HOURS`: Run every N hours (default: 24)
- `SCHEDULER_START_TIME`: Optional specific time (HH:MM, e.g., "02:00")
- `SCHEDULER_TIMEZONE`: Timezone for scheduling (default: Europe/Helsinki)
- `SCHEDULER_MAX_INSTANCES`: Max concurrent executions (default: 1)
- `PIPELINE_TIMEOUT_HOURS`: Max execution time (default: 72)
- `SCHEDULER_BATCH_SIZE`: Batch size for scheduled runs (default: 100)
- `SCHEDULER_SKIP_EXISTING`: Skip existing documents (default: true)
- `SCHEDULER_KEEP_FILES`: Keep downloaded files (default: false)

### API Server Configuration
- `API_HOST`: API server host (default: 0.0.0.0)
- `API_PORT`: API server port (default: 8000)
- `API_WORKERS`: Number of worker processes (default: 1)
- `API_RELOAD`: Enable auto-reload for development (default: false)

### Collection Configuration (Open WebUI Compatibility)
- `COLLECTION_NAME`: Collection identifier, set by Open WebUI
- `COLLECTION_DESCRIPTION`: Collection description (default: Knowledge for storing decisions for City of Helsinki.)

### Storage
- `DATA_DIR`: Directory for data storage (default: data)
- `DECISIONS_DIR`: Directory for decision documents (default: data/decisions)

### Processing
- `MAX_CONCURRENT_REQUESTS`: Number of parallel requests (default: 5)
- `BATCH_SIZE_DAYS`: Days per batch (default: 7)

## Data Storage

Documents are stored as JSON files in the `data/decisions/` directory. Each file is named using the sanitized `NativeId` of the decision.

### Storage Structure

```
data/
├── decisions/
│   ├── {NativeId1}.json
│   ├── {NativeId2}.json
│   └── ...
└── checkpoint.json
```

### Checkpoint Format

The checkpoint file tracks progress:

```json
{
  "last_date": "2024-01-15T00:00:00",
  "documents_saved": 1250,
  "documents_skipped": 45,
  "timestamp": "2024-11-29T10:30:00",
  "completed": false
}
```

## Logging

Logs are stored in the `data/logs/` directory:

- `pipeline.log`: Main application logs
- `api.log`: API request/response logs
- `errors.log`: Error logs only

Logs are written to both files and console for real-time monitoring.

### Log Rotation and Retention

The system automatically manages log files with the following features:

- **Daily Rotation**: Logs rotate daily at midnight, creating date-stamped files (e.g., `pipeline_2026-01-28.log`)
- **Automatic Cleanup**: Old logs are automatically deleted after the retention period (default: 7 days)
- **Configurable**: Adjust retention and rotation settings via environment variables

Configuration options in `.env`:
```bash
LOG_RETENTION_DAYS=7      # Keep logs for 7 days (0 = disable cleanup)
LOG_ROTATION_WHEN=midnight  # When to rotate logs
LOG_ROTATION_INTERVAL=1     # Rotate every 1 day
```

Log files older than the retention period are automatically deleted on application startup, freeing disk space.

## Testing

Run the test suite:

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=app --cov-report=html

# Run specific test file
pytest tests/test_repository.py
```

### Code Style

The project uses:
- **Black** for code formatting
- **Ruff** for linting
- **MyPy** for type checking

Run formatting and linting:

```bash
# Format code
black app/ tests/

# Lint code
ruff check app/ tests/

# Type checking
mypy app/
```
