# Quick Start Guide

## Initial Setup

1. **Navigate to backend directory**:
   ```bash
   cd backend
   ```

2. **Run setup script** (Unix/macOS):
   ```bash
   chmod +x setup.sh
   ./setup.sh
   ```

   Or manually:
   ```bash
   python3 -m venv venv
   source venv/bin/activate  # Windows: venv\Scripts\activate
   pip install -r requirements.txt
   ```

3. **Verify installation**:
   ```bash
   python pipeline.py version
   ```

## Basic Usage

### Full Pipeline (Recommended)

The easiest way to get started - fetch, process, and index documents in one command:

```bash
# Run complete pipeline with default settings
python pipeline.py full-pipeline

# Process specific date range
python pipeline.py full-pipeline --start-date 2024-01-01 --end-date 2024-12-31

# Resume interrupted pipeline
python pipeline.py full-pipeline --resume

# Small batch for testing (processes 10 docs, keeps files for inspection)
python pipeline.py full-pipeline --batch-size 10 --keep-files --log-level DEBUG
```

**What it does:**
1. Fetches decisions from the API
2. Converts HTML to Markdown
3. Chunks text intelligently
4. Generates embeddings with Azure OpenAI
5. Indexes to Elasticsearch
6. Automatically cleans up temporary files

### Fetch Data (Alternative)

If you prefer to fetch and process separately:

```bash
# Fetch all data (uses START_DATE from config, typically 2017-01-01)
python pipeline.py fetch

# Fetch specific date range
python pipeline.py fetch --start-date 2024-01-01 --end-date 2024-12-31

# Resume interrupted fetch
python pipeline.py fetch --resume
```

### Ingest Data (After Fetching)

Process already-fetched documents:

```bash
# Ingest all fetched documents
python pipeline.py ingest

# Ingest with custom batch size
python pipeline.py ingest --batch-size 100
```

### Monitor Progress

```bash
# View statistics
python pipeline.py stats

# View vector store statistics
python pipeline.py vector-store-stats

# Validate data
python pipeline.py validate

# Check logs
tail -f data/logs/pipeline.log
```

## Common Commands

```bash
# Development
pytest                          # Run tests
pytest --cov=app               # Run tests with coverage
black app/ tests/              # Format code
ruff check app/                # Lint code

# Data management
python pipeline.py clear-checkpoint  # Clear checkpoint
python pipeline.py clear-repository  # Clear checkpoint and fetched decisions
python pipeline.py validate          # Validate stored data
python pipeline.py version           # Display version
```

## Configuration

Edit `.env` file to customize:
- API rate limits
- Batch sizes
- Storage locations
- Logging levels

## Troubleshooting

- **Connection issues**: Check network and API availability
- **Rate limiting**: Decrease `REQUESTS_PER_SECOND` in .env
- **Interrupted fetch**: Use `--resume` flag
- **Logs**: Check `logs/` directory for detailed information

For detailed documentation, see [README.md](README.md)
