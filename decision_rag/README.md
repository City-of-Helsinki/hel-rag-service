# Helsinki Decision Documents RAG Pipeline

A complete data pipeline for ingesting Helsinki city decision documents into a vector database for Retrieval-Augmented Generation (RAG) applications with Open WebUI.

## Overview

This project provides an automated pipeline that:
- Fetches decision documents from City of Helsinki decision API
- Converts HTML content to clean Markdown
- Intelligently chunks text for optimal embedding
- Generates embeddings using Azure OpenAI
- Indexes documents to Elasticsearch for semantic search
- Integrates with Open WebUI for conversational AI interactions

## Architecture

The system consists of four main components:

### 1. Data Pipeline (Python)
Located in [`backend/`](backend/), this component includes:
- **CLI Application**: Complete data ingestion workflow from API to vector database
- **FastAPI Backend**: RESTful API for pipeline operations, monitoring, and management
- **Scheduler Service**: Automated pipeline execution with configurable scheduling

### 2. Elasticsearch Vector Database
Stores document chunks with embeddings for semantic search capabilities.

### 3. Open WebUI Interface
Provides a conversational interface for querying the decision documents using RAG.

### 4. Docker Infrastructure
Container orchestration with docker-compose for all services.

## Quick Start

### Option 1: Using Docker (Recommended)

```bash
# Start all services (Elasticsearch, Backend API with Scheduler, Open WebUI)
docker-compose up -d

# The scheduler will automatically run the pipeline based on configured interval
# Access the API at http://localhost:8000/docs
```

### Option 2: CLI Pipeline (Manual)

```bash
# Start Elasticsearch only
docker-compose up -d rag-elastic-vectordb

# Set up and run the pipeline manually
cd backend
./setup.sh
python pipeline.py full-pipeline
```

For detailed instructions, see:
- [Backend QUICKSTART](backend/QUICKSTART.md) - CLI usage

## Documentation

### Core Documentation
- **[Backend README](backend/README.md)** - Comprehensive pipeline documentation
- **[Quick Start Guide](backend/QUICKSTART.md)** - Get up and running quickly

## Key Features

### Pipeline Features
- **Batch Processing** - Handles large date ranges efficiently with weekly batches
- **Resilient** - Automatic retry with exponential backoff and checkpoint recovery
- **Parallel Processing** - Concurrent document fetching and processing
- **Rate Limited** - Respects API constraints to ensure reliability
- **Comprehensive Logging** - Detailed logs for monitoring and debugging
- **Streaming Pipeline** - Memory-efficient processing of large document sets
- **Attachment Support** - Processes public PDF attachments alongside decisions

### API & Automation
- **RESTful API** - FastAPI-based backend for pipeline operations and monitoring
- **Automated Scheduler** - Configurable interval or time-based execution
- **Background Jobs** - Async job execution with progress tracking
- **Health Monitoring** - Component health checks and status reporting

### Operations & Monitoring
- **Comprehensive Logging** - Detailed logs for monitoring and debugging
- **Interactive Documentation** - Swagger UI for API exploration
- **Docker Support** - Complete containerized deployment

## Requirements

- Python 3.11+
- Docker & Docker Compose (for Elasticsearch & Open WebUI)
- Azure OpenAI API access
- City of Helsinki decision API access

## Configuration

The pipeline is configured via environment variables in [`backend/.env`](backend/.env.example). Key settings include:

- API credentials and endpoints
- Azure OpenAI configuration
- Elasticsearch connection
- Processing parameters (batch sizes, concurrency, etc.)
- Date ranges for data fetching

## Project Structure

```
decision_rag/
├── backend/              # Data ingestion pipeline
│   ├── app/
│   │   ├── core/         # Configuration & logging
│   │   ├── schemas/      # Data models
│   │   ├── services/     # Business logic
│   │   ├── repositories/ # Data storage
│   │   └── utils/        # Helper functions
│   ├── tests/            # Unit tests
│   ├── pipeline.py       # CLI application
│   └── README.md         # Detailed documentation
├── docker-compose.yml    # Infrastructure setup
└── README.md             # This file
```
