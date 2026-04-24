# Quick Start Guide

## 1. Install and configure

**Navigate to the backend directory** and run the setup script:

```bash
cd backend
chmod +x setup.sh
./setup.sh
```

This creates a virtual environment, installs dependencies, and copies `.env.example` to `.env`.

Or manually:

```bash
python3 -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate
pip install -r requirements.txt
cp .env.example .env
```

**Edit `.env`** to set the required values before starting:

```bash
# Source API
API_BASE_URL=https://...
API_KEY=your-api-key
DECISION_IDS_ENDPOINT=/path/to/ids
DECISION_DOCUMENT_ENDPOINT=/path/to/documents

# Azure OpenAI (for embedding generation)
AZURE_OPENAI_ENDPOINT=https://your-resource.openai.azure.com/
AZURE_OPENAI_API_KEY=your-openai-key

# Vector store (choose one or both)
ELASTICSEARCH_URL=http://localhost:9200
# PGVECTOR_HOST=localhost
```

See `.env.example` for all available options.

## 2. Start the server

```bash
# Development (auto-reload on file changes)
python main.py

# Or with uvicorn directly
uvicorn main:app --reload --host 0.0.0.0 --port 8000
```

Once running, open the interactive API docs to explore all endpoints:

- **Swagger UI**: http://localhost:8000/docs
- **ReDoc**: http://localhost:8000/redoc
- **Health check**: http://localhost:8000/api/v1/health

## 3. Ingest data

All pipeline operations are triggered via the REST API. Jobs run asynchronously in the background — each call returns a `job_id` you can use to monitor progress.

### Full pipeline (recommended)

Fetches decisions from the source API, converts HTML to Markdown, generates embeddings, and indexes to the vector store in one streaming operation:

```bash
# Run with defaults (date range is set dynamically in config)
curl -X POST http://localhost:8000/api/v1/pipeline/full

# Specific date range
curl -X POST http://localhost:8000/api/v1/pipeline/full \
  -H "Content-Type: application/json" \
  -d '{"start_date": "2024-01-01", "end_date": "2024-12-31"}'

# Skip documents already in the vector store
curl -X POST http://localhost:8000/api/v1/pipeline/full \
  -H "Content-Type: application/json" \
  -d '{"skip_existing": true}'

# Resume an interrupted run
curl -X POST http://localhost:8000/api/v1/pipeline/full \
  -H "Content-Type: application/json" \
  -d '{"resume": true}'
```

### Fetch only

Fetches documents from the source API and saves them locally without processing:

```bash
curl -X POST http://localhost:8000/api/v1/pipeline/fetch

# With options
curl -X POST http://localhost:8000/api/v1/pipeline/fetch \
  -H "Content-Type: application/json" \
  -d '{"start_date": "2024-01-01", "end_date": "2024-12-31", "resume": true}'
```

### Ingest only

Processes locally stored documents (convert, chunk, embed, index). Use this after a standalone fetch:

```bash
curl -X POST http://localhost:8000/api/v1/pipeline/ingest

# Force reindex documents already in the vector store
curl -X POST http://localhost:8000/api/v1/pipeline/ingest \
  -H "Content-Type: application/json" \
  -d '{"reindex": true, "batch_size": 100}'
```

### Monitor job progress

Each pipeline call returns a `job_id`. Poll it to check status:

```bash
# Check a specific job
curl http://localhost:8000/api/v1/pipeline/jobs/{job_id}

# List all jobs
curl http://localhost:8000/api/v1/pipeline/jobs
```

## 4. Monitor and manage

```bash
# Detailed health check (vector store, embeddings, API connectivity)
curl http://localhost:8000/api/v1/health/detailed

# Repository statistics (locally stored documents)
curl http://localhost:8000/api/v1/data/stats

# Vector store statistics
curl http://localhost:8000/api/v1/data/vector-store/stats

# List stored documents (paginated)
curl "http://localhost:8000/api/v1/data/documents?page=1&page_size=50"

# Check logs
tail -f data/logs/pipeline.log
```

### Clear data (admin)

```bash
# Clear fetch checkpoint (allows restarting from scratch)
curl -X DELETE http://localhost:8000/api/v1/admin/checkpoint

# Clear all locally stored documents (irreversible — requires confirm=true)
curl -X DELETE "http://localhost:8000/api/v1/admin/repository?confirm=true"

# Clear the Elasticsearch vector store (irreversible — requires confirm=true)
curl -X DELETE "http://localhost:8000/api/v1/admin/vector-store/elasticsearch?confirm=true"
```

## Optional API authentication

To secure endpoints in non-local environments:

```bash
# .env
API_AUTH_ENABLED=true
API_AUTH_KEY=your-secure-key  # Generate with: openssl rand -hex 32
```

Include the key in all requests:

```bash
curl -H "X-API-Key: your-api-key" http://localhost:8000/api/v1/pipeline/full
```

Health endpoints (`/health`, `/health/detailed`, `/scheduler/health`) do not require authentication.

## Automated scheduling

The scheduler runs the full pipeline automatically at a configured interval:

```bash
# .env
SCHEDULER_ENABLED=true
SCHEDULER_INTERVAL_HOURS=24
SCHEDULER_START_TIME=02:00        # Optional: specific time (HH:MM)
SCHEDULER_TIMEZONE=Europe/Helsinki
```

Control the scheduler while the server is running:

```bash
curl http://localhost:8000/api/v1/scheduler/status
curl -X POST http://localhost:8000/api/v1/scheduler/trigger  # Run now
curl -X POST http://localhost:8000/api/v1/scheduler/pause
curl -X POST http://localhost:8000/api/v1/scheduler/resume
```

## Development

```bash
pytest                           # Run all tests
pytest --cov=app                 # Run tests with coverage report

black app/ tests/                # Format code
ruff check app/ tests/           # Lint code
mypy app/                        # Type checking
```

## Troubleshooting

- **Server won't start**: Check that `AZURE_OPENAI_ENDPOINT`, `AZURE_OPENAI_API_KEY`, and `ELASTICSEARCH_URL` are set in `.env`
- **Jobs failing**: Check `data/logs/pipeline.log` and `data/logs/errors.log` for details
- **Rate limit errors**: Decrease `REQUESTS_PER_SECOND` in `.env`
- **Interrupted pipeline**: Trigger a new run with `"resume": true` in the request body

For full documentation, see [README.md](README.md)
