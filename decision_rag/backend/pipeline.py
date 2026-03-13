"""
Main pipeline script for fetching Helsinki decision documents.
"""

import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import typer
from rich.console import Console
from rich.progress import BarColumn, Progress, SpinnerColumn, TaskProgressColumn, TextColumn

from app import __version__
from app.core import get_logger, settings, setup_logging
from app.repositories import DecisionRepository
from app.services import (
    AttachmentDownloader,
    AzureEmbedder,
    DecisionAPIClient,
    DecisionDataFetcher,
    ElasticsearchVectorStore,
    IngestionPipeline,
    ParagraphChunker,
)
from app.utils.date_utils import parse_date
from app.utils.validators import validate_decision_document

# Initialize CLI
app = typer.Typer(
    name="decision-pipeline",
    help="Helsinki Decision Documents Data Pipeline",
    add_completion=False,
)
console = Console()


def initialize_pipeline(log_level: str = "INFO") -> tuple[DecisionDataFetcher, DecisionRepository]:
    """
    Initialize pipeline components.

    Args:
        log_level: Logging level

    Returns:
        Tuple of (fetcher, repository)
    """
    # Setup logging
    setup_logging(
        log_level=log_level,
        log_dir=settings.LOG_DIR,
        log_file=settings.LOG_FILE,
        api_log_file=settings.API_LOG_FILE,
        error_log_file=settings.ERROR_LOG_FILE,
        retention_days=settings.LOG_RETENTION_DAYS,
        rotation_when=settings.LOG_ROTATION_WHEN,
        rotation_interval=settings.LOG_ROTATION_INTERVAL,
    )

    logger = get_logger(__name__)
    logger.info("Initializing pipeline...")

    # Initialize components
    api_client = DecisionAPIClient()
    fetcher = DecisionDataFetcher(api_client)
    repository = DecisionRepository(settings.DATA_DIR)

    return fetcher, repository


@app.command()
def fetch(
    start_date: Optional[str] = typer.Option(
        None,
        "--start-date",
        "-s",
        help="Start date in YYYY-MM-DD format (defaults to 2017-01-01)",
    ),
    end_date: Optional[str] = typer.Option(
        None,
        "--end-date",
        "-e",
        help="End date in YYYY-MM-DD format (defaults to 2 months ago)",
    ),
    resume: Optional[bool] = typer.Option(
        False,
        "--resume",
        "-r",
        help="Resume from last checkpoint",
    ),
    log_level: Optional[str] = typer.Option(
        "INFO",
        "--log-level",
        "-l",
        help="Logging level (DEBUG, INFO, WARNING, ERROR)",
    ),
    skip_existing: Optional[bool] = typer.Option(
        False,
        "--skip-existing",
        help="Skip documents that already exist in storage",
    ),
):
    """
    Fetch decision documents from the API.

    Examples:
        # Fetch all historical data (default range)
        python pipeline.py fetch

        # Fetch specific date range
        python pipeline.py fetch --start-date 2024-01-01 --end-date 2024-12-31

        # Resume from checkpoint
        python pipeline.py fetch --resume
    """
    logger = get_logger(__name__)

    try:
        # Initialize pipeline
        fetcher, repository = initialize_pipeline(log_level)

        # Validate settings
        if (
            not settings.API_KEY
            or not settings.API_BASE_URL
            or not settings.DECISION_IDS_ENDPOINT
            or not settings.DECISION_DOCUMENT_ENDPOINT
        ):
            console.print(
                "[bold red]Error: Invalid configuration settings. Please check your .env file.[/bold red]"
            )
            logger.error("Invalid configuration settings")
            sys.exit(1)

        # Get API key from settings
        api_key = settings.API_KEY

        # Determine date range
        if resume:
            checkpoint = repository.load_checkpoint()
            if checkpoint:
                console.print(
                    f"[green]Resuming from checkpoint: {checkpoint.get('last_date')}[/green]"
                )
                start_dt = parse_date(checkpoint["last_date"])
            else:
                console.print("[yellow]No checkpoint found, starting from beginning[/yellow]")
                start_dt = parse_date(start_date or settings.START_DATE)
        else:
            start_dt = parse_date(start_date or settings.START_DATE)

        end_dt = parse_date(end_date or settings.END_DATE)

        console.print("[bold blue]Starting data fetch[/bold blue]")
        console.print(f"Date range: {start_dt.date()} to {end_dt.date()}")
        console.print(f"Data directory: {settings.DATA_DIR}")
        console.print(f"Skip existing: {skip_existing}\n")

        # Fetch data
        documents_saved = 0
        documents_skipped = 0

        for document in fetcher.fetch_all_decisions(api_key, start_dt, end_dt):
            # Check if already exists
            if skip_existing and repository.decision_exists(document.NativeId):
                documents_skipped += 1
                logger.debug(f"Skipping existing document: {document.NativeId}")
                continue

            # Save document
            if repository.save_decision(document):
                documents_saved += 1

                # Save checkpoint periodically
                if documents_saved % 50 == 0:
                    checkpoint_data = {
                        "last_date": document.DateDecision or end_dt.isoformat(),
                        "documents_saved": documents_saved,
                        "documents_skipped": documents_skipped,
                    }
                    repository.save_checkpoint(checkpoint_data)

        # Final summary
        console.print("\n[bold green]✓ Fetch completed successfully![/bold green]")
        console.print(f"Documents saved: {documents_saved}")
        console.print(f"Documents skipped: {documents_skipped}")

        # Save final checkpoint
        checkpoint_data = {
            "last_date": end_dt.isoformat(),
            "documents_saved": documents_saved,
            "documents_skipped": documents_skipped,
            "completed": True,
        }
        repository.save_checkpoint(checkpoint_data)

    except KeyboardInterrupt:
        console.print("\n[yellow]Fetch interrupted by user[/yellow]")
        logger.info("Fetch interrupted by user")
        sys.exit(1)
    except Exception as e:
        console.print(f"\n[bold red]Error: {e}[/bold red]")
        logger.exception("Fatal error during fetch")
        sys.exit(1)


@app.command()
def ingest(
    start_date: Optional[str] = typer.Option(
        None,
        "--start-date",
        "-s",
        help="Process documents from this date onwards (YYYY-MM-DD)",
    ),
    batch_size: int = typer.Option(
        50,
        "--batch-size",
        "-b",
        help="Number of documents to process per batch",
    ),
    resume: bool = typer.Option(
        False,
        "--resume",
        "-r",
        help="Resume from last ingestion checkpoint",
    ),
    reindex: bool = typer.Option(
        False,
        "--reindex",
        help="Force reindexing of existing documents",
    ),
    skip_attachments: bool = typer.Option(
        False,
        "--skip-attachments",
        help="Skip processing attachments",
    ),
    log_level: Optional[str] = typer.Option(
        "INFO",
        "--log-level",
        "-l",
        help="Logging level (DEBUG, INFO, WARNING, ERROR)",
    ),
):
    """
    Ingest decision documents into the vector store.

    This command processes fetched decisions through the full pipeline:
    - Converts HTML to Markdown
    - Chunks text into embeddable segments
    - Generates embeddings using Azure OpenAI
    - Indexes to Elasticsearch
    - Processes public attachments (if not skipped)

    Examples:
        # Ingest all fetched documents
        python pipeline.py ingest

        # Ingest documents from specific date
        python pipeline.py ingest --start-date 2024-01-01

        # Resume from checkpoint
        python pipeline.py ingest --resume

        # Force reindexing
        python pipeline.py ingest --reindex

        # Skip attachment processing
        python pipeline.py ingest --skip-attachments
    """
    logger = get_logger(__name__)

    try:
        # Setup logging
        setup_logging(
            log_level=log_level,
            log_dir=settings.LOG_DIR,
            log_file=settings.LOG_FILE,
            api_log_file=settings.API_LOG_FILE,
            error_log_file=settings.ERROR_LOG_FILE,
            retention_days=settings.LOG_RETENTION_DAYS,
            rotation_when=settings.LOG_ROTATION_WHEN,
            rotation_interval=settings.LOG_ROTATION_INTERVAL,
        )

        console.print("[bold blue]Initializing ingestion pipeline...[/bold blue]")

        # Initialize components
        repository = DecisionRepository(settings.DATA_DIR)

        # Initialize chunker
        chunker = ParagraphChunker(
            target_tokens=750,
            min_tokens=500,
            max_tokens=1000,
            overlap_tokens=100,
            header_overhead_tokens=settings.METADATA_HEADER_OVERHEAD_TOKENS,
            embed_metadata=settings.EMBED_METADATA_IN_CHUNKS,
        )

        # Initialize embedder
        try:
            embedder = AzureEmbedder()
            console.print("[green]✓ Connected to Azure OpenAI[/green]")
        except Exception as e:
            console.print(
                f"[bold red]Error: Failed to initialize Azure OpenAI embedder: {e}[/bold red]"
            )
            console.print(
                "[yellow]Make sure AZURE_OPENAI_ENDPOINT and AZURE_OPENAI_API_KEY are set in .env[/yellow]"
            )
            sys.exit(1)

        # Initialize vector store
        try:
            vector_store = ElasticsearchVectorStore()
            console.print("[green]✓ Connected to Elasticsearch[/green]")
        except Exception as e:
            console.print(f"[bold red]Error: Failed to connect to Elasticsearch: {e}[/bold red]")
            console.print(
                "[yellow]Make sure Elasticsearch is running at {settings.ELASTICSEARCH_URL}[/yellow]"
            )
            sys.exit(1)

        # Initialize attachment downloader if not skipped
        attachment_downloader = None
        if not skip_attachments and settings.PROCESS_ATTACHMENTS:
            try:
                attachment_downloader = AttachmentDownloader()
                console.print("[green]✓ Attachment downloader initialized[/green]")
            except Exception as e:
                console.print(f"[yellow]Warning: Failed to initialize attachment downloader: {e}[/yellow]")
                console.print("[yellow]Continuing without attachment processing[/yellow]")

        # Initialize ingestion pipeline
        pipeline = IngestionPipeline(
            repository=repository,
            chunker=chunker,
            embedder=embedder,
            vector_store=vector_store,
            attachment_downloader=attachment_downloader,
        )

        console.print("[green]✓ Pipeline initialized[/green]\n")

        # Get list of documents to process
        all_native_ids = repository.get_all_native_ids()

        if not all_native_ids:
            console.print(
                "[yellow]No documents found in repository. Run 'fetch' command first.[/yellow]"
            )
            sys.exit(0)

        # Filter by date if specified
        if start_date:
            start_dt = parse_date(start_date)
            console.print(f"Filtering documents from {start_dt.date()}")
            # Filter documents by loading and checking date
            filtered_ids = []
            for native_id in all_native_ids:
                doc = repository.get_decision(native_id)
                if doc and doc.DateDecision:
                    doc_date = parse_date(doc.DateDecision)
                    if doc_date >= start_dt:
                        filtered_ids.append(native_id)
            all_native_ids = filtered_ids

        console.print(f"[bold blue]Processing {len(all_native_ids)} documents[/bold blue]")
        console.print(f"Batch size: {batch_size}")
        console.print(f"Reindex mode: {reindex}")
        console.print(f"Process attachments: {not skip_attachments and attachment_downloader is not None}")
        console.print(f"Logging level: {log_level}\n")

        # Process in batches
        total_stats = {
            "total": len(all_native_ids),
            "processed": 0,
            "successful": 0,
            "failed": 0,
            "skipped": 0,
            "total_chunks": 0,
            "total_attachments": 0,
            "total_attachment_chunks": 0,
        }

        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
            console=console,
        ) as progress:
            task = progress.add_task("Ingesting documents...", total=len(all_native_ids))

            for i in range(0, len(all_native_ids), batch_size):
                batch = all_native_ids[i : i + batch_size]

                # Process batch
                batch_stats = pipeline.process_batch(batch, reindex=reindex)

                # Update totals
                total_stats["processed"] += batch_stats["processed"]
                total_stats["successful"] += batch_stats["successful"]
                total_stats["failed"] += batch_stats["failed"]
                total_stats["skipped"] += batch_stats["skipped"]
                total_stats["total_chunks"] += batch_stats["total_chunks"]
                total_stats["total_attachments"] += batch_stats.get("total_attachments", 0)
                total_stats["total_attachment_chunks"] += batch_stats.get(
                    "total_attachment_chunks", 0
                )

                # Update progress
                progress.advance(task, len(batch))

        # Final summary
        console.print("\n[bold green]✓ Ingestion completed![/bold green]")
        console.print(f"Total documents: {total_stats['total']}")
        console.print(f"Successful: {total_stats['successful']}")
        console.print(f"Failed: {total_stats['failed']}")
        console.print(f"Skipped: {total_stats['skipped']}")
        console.print(f"Total decision chunks indexed: {total_stats['total_chunks']}")
        if not skip_attachments:
            console.print(f"Total attachments processed: {total_stats['total_attachments']}")
            console.print(
                f"Total attachment chunks indexed: {total_stats['total_attachment_chunks']}"
            )

        # Show vector store statistics
        vs_stats = vector_store.get_statistics()
        console.print("\n[bold blue]Vector Store Statistics[/bold blue]")
        console.print(f"Index: {vs_stats.get('index_name')}")
        console.print(f"Total chunks: {vs_stats.get('total_chunks', 0)}")
        console.print(f"Size: {vs_stats.get('size_mb', 0):.2f} MB\n")

    except KeyboardInterrupt:
        console.print("\n[yellow]Ingestion interrupted by user[/yellow]")
        logger.info("Ingestion interrupted by user")
        sys.exit(1)
    except Exception as e:
        console.print(f"\n[bold red]Error: {e}[/bold red]")
        logger.exception("Fatal error during ingestion")
        sys.exit(1)


@app.command()
def full_pipeline(
    start_date: Optional[str] = typer.Option(
        None,
        "--start-date",
        "-s",
        help="Start date in YYYY-MM-DD format (defaults to settings.START_DATE)",
    ),
    end_date: Optional[str] = typer.Option(
        None,
        "--end-date",
        "-e",
        help="End date in YYYY-MM-DD format (defaults to settings.END_DATE)",
    ),
    batch_size: int = typer.Option(
        50,
        "--batch-size",
        "-b",
        help="Documents to process per batch",
    ),
    resume: bool = typer.Option(
        False,
        "--resume",
        "-r",
        help="Resume from last checkpoint",
    ),
    skip_existing: bool = typer.Option(
        False,
        "--skip-existing",
        help="Skip documents already in vector store",
    ),
    skip_attachments: bool = typer.Option(
        False,
        "--skip-attachments",
        help="Skip attachment processing",
    ),
    keep_files: bool = typer.Option(
        False,
        "--keep-files",
        help="Keep decision files after processing (default: delete)",
    ),
    log_level: Optional[str] = typer.Option(
        "INFO",
        "--log-level",
        "-l",
        help="Logging level (DEBUG, INFO, WARNING, ERROR)",
    ),
):
    """
    Run the full data ingestion pipeline in one streaming operation.

    This command combines fetching, processing, and indexing in a single
    streaming operation with memory-efficient batch processing:
    - Fetches decision documents from API
    - Processes them through conversion, chunking, embedding
    - Indexes to Elasticsearch
    - Deletes processed files to control disk usage

    Examples:
        # Basic usage - full historical data
        python pipeline.py full-pipeline

        # Specific date range with custom batch size
        python pipeline.py full-pipeline --start-date 2024-01-01 --end-date 2024-12-31 --batch-size 100

        # Resume interrupted pipeline
        python pipeline.py full-pipeline --resume

        # Skip already indexed documents
        python pipeline.py full-pipeline --skip-existing

        # Debug mode with file preservation
        python pipeline.py full-pipeline --log-level DEBUG --keep-files --batch-size 10
    """
    logger = get_logger(__name__)

    try:
        # Setup logging
        setup_logging(
            log_level=log_level,
            log_dir=settings.LOG_DIR,
            log_file=settings.LOG_FILE,
            api_log_file=settings.API_LOG_FILE,
            error_log_file=settings.ERROR_LOG_FILE,
            retention_days=settings.LOG_RETENTION_DAYS,
            rotation_when=settings.LOG_ROTATION_WHEN,
            rotation_interval=settings.LOG_ROTATION_INTERVAL,
        )

        console.print("[bold blue]Initializing full pipeline...[/bold blue]")

        # Validate configuration
        if (
            not settings.API_KEY
            or not settings.API_BASE_URL
            or not settings.DECISION_IDS_ENDPOINT
            or not settings.DECISION_DOCUMENT_ENDPOINT
        ):
            console.print(
                "[bold red]Error: Invalid API configuration. Check your .env file.[/bold red]"
            )
            sys.exit(1)

        # Initialize fetcher components
        api_client = DecisionAPIClient()
        fetcher = DecisionDataFetcher(api_client)
        repository = DecisionRepository(settings.DATA_DIR)

        # Initialize processing components
        chunker = ParagraphChunker(
            target_tokens=1000,
            min_tokens=500,
            max_tokens=1050,
            overlap_tokens=100,
            header_overhead_tokens=settings.METADATA_HEADER_OVERHEAD_TOKENS,
            embed_metadata=settings.EMBED_METADATA_IN_CHUNKS,
        )

        try:
            embedder = AzureEmbedder()
            console.print("[green]✓ Connected to Azure OpenAI[/green]")
        except Exception as e:
            console.print(f"[bold red]Error: Failed to initialize Azure OpenAI: {e}[/bold red]")
            console.print(
                "[yellow]Make sure AZURE_OPENAI_ENDPOINT and AZURE_OPENAI_API_KEY are set[/yellow]"
            )
            sys.exit(1)

        try:
            vector_store = ElasticsearchVectorStore()
            console.print("[green]✓ Connected to Elasticsearch[/green]")
        except Exception as e:
            console.print(f"[bold red]Error: Failed to connect to Elasticsearch: {e}[/bold red]")
            sys.exit(1)

        # Initialize attachment downloader if needed
        attachment_downloader = None
        if not skip_attachments and settings.PROCESS_ATTACHMENTS:
            try:
                attachment_downloader = AttachmentDownloader()
                console.print("[green]✓ Attachment downloader initialized[/green]")
            except Exception as e:
                console.print(f"[yellow]Warning: Failed to initialize attachment downloader: {e}[/yellow]")
                console.print("[yellow]Continuing without attachment processing[/yellow]")

        # Initialize ingestion pipeline
        pipeline = IngestionPipeline(
            repository=repository,
            chunker=chunker,
            embedder=embedder,
            vector_store=vector_store,
            attachment_downloader=attachment_downloader,
        )

        console.print("[green]✓ Pipeline initialized[/green]\n")

        # Determine date range
        if resume:
            checkpoint = repository.load_checkpoint()
            if checkpoint and "full_pipeline" in checkpoint:
                console.print(
                    f"[green]Resuming from checkpoint: {checkpoint['full_pipeline'].get('last_date')}[/green]"
                )
                start_dt = parse_date(checkpoint["full_pipeline"]["last_date"])
            else:
                console.print("[yellow]No checkpoint found, starting from beginning[/yellow]")
                start_dt = parse_date(start_date or settings.START_DATE)
        else:
            start_dt = parse_date(start_date or settings.START_DATE)

        end_dt = parse_date(end_date or settings.END_DATE)

        console.print("[bold blue]Full Pipeline Configuration[/bold blue]")
        console.print(f"Date range: {start_dt.date()} to {end_dt.date()}")
        console.print(f"Batch size: {batch_size} documents")
        console.print(f"Skip existing: {skip_existing}")
        console.print(f"Process attachments: {not skip_attachments and attachment_downloader is not None}")
        console.print(f"Keep files: {keep_files}")
        console.print(f"Logging level: {log_level}\n")

        # Initialize statistics
        stats = {
            "total_fetched": 0,
            "total_processed": 0,
            "total_successful": 0,
            "total_failed": 0,
            "total_skipped": 0,
            "total_chunks": 0,
            "total_attachments": 0,
            "total_attachment_chunks": 0,
            "total_deleted": 0,
            "batches_processed": 0,
        }

        # Batch accumulator
        batch_buffer = []
        batch_native_ids = []

        # Create progress display
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
            console=console,
        ) as progress:
            fetch_task = progress.add_task("Fetching documents...", total=None)
            process_task = progress.add_task("Processing documents...", total=0)

            # Stream documents from fetcher
            for document in fetcher.fetch_all_decisions(
                settings.API_KEY, start_dt, end_dt
            ):
                stats["total_fetched"] += 1

                # Check if already in vector store
                if skip_existing and vector_store.document_exists(document.NativeId):
                    stats["total_skipped"] += 1
                    logger.debug(f"Skipping already indexed document: {document.NativeId}")
                    continue

                # Save document temporarily
                if repository.save_decision(document):
                    batch_buffer.append(document)
                    batch_native_ids.append(document.NativeId)
                    progress.update(fetch_task, description=f"Fetching documents... ({stats['total_fetched']} fetched)")

                # Process batch when size reached
                if len(batch_buffer) >= batch_size:
                    logger.info(f"Processing batch of {len(batch_buffer)} documents")

                    # Update progress task total
                    progress.update(process_task, total=len(batch_native_ids))

                    # Process each document in batch
                    batch_stats = _process_batch(
                        pipeline,
                        batch_native_ids,
                        skip_existing,
                        progress,
                        process_task,
                    )

                    # Update totals
                    stats["total_processed"] += batch_stats["processed"]
                    stats["total_successful"] += batch_stats["successful"]
                    stats["total_failed"] += batch_stats["failed"]
                    stats["total_skipped"] += batch_stats["skipped"]
                    stats["total_chunks"] += batch_stats["total_chunks"]
                    stats["total_attachments"] += batch_stats.get("total_attachments", 0)
                    stats["total_attachment_chunks"] += batch_stats.get("total_attachment_chunks", 0)
                    stats["batches_processed"] += 1

                    # Delete processed files
                    if not keep_files:
                        delete_stats = repository.delete_decisions(batch_native_ids)
                        stats["total_deleted"] += delete_stats["deleted"]
                        logger.info(f"Deleted {delete_stats['deleted']} processed files")

                    # Save checkpoint
                    last_doc = batch_buffer[-1] if batch_buffer else None
                    fetcher_stats = fetcher.stats  # Get current fetcher stats
                    checkpoint_data = {
                        "full_pipeline": {
                            "timestamp": datetime.now().isoformat(),
                            "last_date": last_doc.DateDecision if last_doc else end_dt.isoformat(),
                            "native_ids_fetched": fetcher_stats.get("ids_fetched", 0),
                            "documents_fetched": stats["total_fetched"],
                            "fetch_errors": fetcher_stats.get("errors", 0),
                            "documents_processed": stats["total_processed"],
                            "documents_successful": stats["total_successful"],
                            "documents_failed": stats["total_failed"],
                            "documents_skipped": stats["total_skipped"],
                            "last_native_id": last_doc.NativeId if last_doc else None,
                            "completed": False,
                        }
                    }
                    repository.save_checkpoint(checkpoint_data)

                    # Clear batch buffer
                    batch_buffer = []
                    batch_native_ids = []
                    progress.reset(process_task)

            # Process remaining documents in final batch
            if batch_buffer:
                logger.info(f"Processing final batch of {len(batch_buffer)} documents")
                progress.update(process_task, total=len(batch_native_ids))

                batch_stats = _process_batch(
                    pipeline,
                    batch_native_ids,
                    skip_existing,
                    progress,
                    process_task,
                )

                stats["total_processed"] += batch_stats["processed"]
                stats["total_successful"] += batch_stats["successful"]
                stats["total_failed"] += batch_stats["failed"]
                stats["total_skipped"] += batch_stats["skipped"]
                stats["total_chunks"] += batch_stats["total_chunks"]
                stats["total_attachments"] += batch_stats.get("total_attachments", 0)
                stats["total_attachment_chunks"] += batch_stats.get("total_attachment_chunks", 0)
                stats["batches_processed"] += 1

                # Delete processed files
                if not keep_files:
                    delete_stats = repository.delete_decisions(batch_native_ids)
                    stats["total_deleted"] += delete_stats["deleted"]
                    logger.info(f"Deleted {delete_stats['deleted']} processed files")

        # Save final checkpoint
        # Include fetcher statistics
        fetcher_stats = fetcher.stats
        checkpoint_data = {
            "full_pipeline": {
                "timestamp": datetime.now().isoformat(),
                "last_date": end_dt.isoformat(),
                "native_ids_fetched": fetcher_stats.get("ids_fetched", 0),
                "documents_fetched": stats["total_fetched"],
                "fetch_errors": fetcher_stats.get("errors", 0),
                "fetch_retry_attempts": fetcher_stats.get("retry_attempts", 0),
                "fetch_documents_recovered": fetcher_stats.get("documents_recovered", 0),
                "fetch_permanently_failed": fetcher_stats.get("permanently_failed", 0),
                "documents_processed": stats["total_processed"],
                "documents_successful": stats["total_successful"],
                "documents_failed": stats["total_failed"],
                "documents_skipped": stats["total_skipped"],
                "batches_processed": stats["batches_processed"],
                "completed": True,
            }
        }
        repository.save_checkpoint(checkpoint_data)

        # Final summary
        console.print("\n[bold green]✓ Full pipeline completed successfully![/bold green]")
        console.print(f"Native IDs fetched: {fetcher_stats.get('ids_fetched', 0)}")
        console.print(f"Documents fetched: {stats['total_fetched']}")
        console.print(f"Fetch errors: {fetcher_stats.get('errors', 0)}")
        if fetcher_stats.get("retry_attempts", 0) > 0:
            console.print(f"Retry attempts: {fetcher_stats.get('retry_attempts', 0)}")
            console.print(f"Documents recovered: {fetcher_stats.get('documents_recovered', 0)}")
            console.print(f"Permanently failed: {fetcher_stats.get('permanently_failed', 0)}")
        console.print(f"Documents processed: {stats['total_processed']}")
        console.print(f"Successful: {stats['total_successful']}")
        console.print(f"Failed: {stats['total_failed']}")
        console.print(f"Skipped: {stats['total_skipped']}")
        console.print(f"Total chunks indexed: {stats['total_chunks']}")
        if not skip_attachments:
            console.print(f"Attachments processed: {stats['total_attachments']}")
            console.print(f"Attachment chunks indexed: {stats['total_attachment_chunks']}")
        if not keep_files:
            console.print(f"Files deleted: {stats['total_deleted']}")
        console.print(f"Batches processed: {stats['batches_processed']}")

        # Show vector store statistics
        vs_stats = vector_store.get_statistics()
        console.print("\n[bold blue]Vector Store Statistics[/bold blue]")
        console.print(f"Index: {vs_stats.get('index_name')}")
        console.print(f"Total chunks: {vs_stats.get('total_chunks', 0)}")
        console.print(f"Size: {vs_stats.get('size_mb', 0):.2f} MB\n")

    except KeyboardInterrupt:
        console.print("\n[yellow]Pipeline interrupted by user[/yellow]")
        logger.info("Pipeline interrupted by user")
        sys.exit(1)
    except Exception as e:
        console.print(f"\n[bold red]Error: {e}[/bold red]")
        logger.exception("Fatal error during full pipeline")
        sys.exit(1)


def _process_batch(
    pipeline: IngestionPipeline,
    native_ids: List[str],
    skip_existing: bool,
    progress: Progress,
    task_id,
) -> Dict[str, Any]:
    """
    Process a batch of documents through the ingestion pipeline.

    Args:
        pipeline: IngestionPipeline instance
        native_ids: List of NativeIds to process
        skip_existing: Skip documents already in vector store
        progress: Rich Progress instance
        task_id: Progress task ID

    Returns:
        Dictionary with batch statistics
    """
    logger = get_logger(__name__)

    batch_stats = {
        "processed": 0,
        "successful": 0,
        "failed": 0,
        "skipped": 0,
        "total_chunks": 0,
        "total_attachments": 0,
        "total_attachment_chunks": 0,
    }

    for native_id in native_ids:
        try:
            doc_stats = pipeline.process_document(native_id, reindex=not skip_existing)

            batch_stats["processed"] += 1

            if doc_stats.get("skipped"):
                batch_stats["skipped"] += 1
            elif doc_stats.get("success"):
                batch_stats["successful"] += 1
                batch_stats["total_chunks"] += doc_stats.get("chunks_indexed", 0)
                batch_stats["total_attachments"] += doc_stats.get("attachments_processed", 0)
                batch_stats["total_attachment_chunks"] += doc_stats.get(
                    "attachment_chunks_indexed", 0
                )
            else:
                batch_stats["failed"] += 1

            progress.advance(task_id)

        except Exception as e:
            logger.error(f"Error processing document {native_id}: {e}")
            batch_stats["processed"] += 1
            batch_stats["failed"] += 1
            progress.advance(task_id)

    return batch_stats


@app.command()
def stats():
    """Display statistics about stored data."""
    try:
        repository = DecisionRepository(settings.DATA_DIR)
        stats = repository.get_statistics()

        console.print("\n[bold blue]Repository Statistics[/bold blue]")
        console.print(f"Total documents: {stats['total_documents']}")
        console.print(f"Storage size: {stats['storage_size_mb']:.2f} MB")
        console.print(f"Storage path: {stats['storage_path']}\n")

        # Check checkpoint
        checkpoint = repository.load_checkpoint()
        if checkpoint:
            console.print("[bold blue]Last Checkpoint[/bold blue]")
            console.print(f"Timestamp: {checkpoint.get('timestamp')}")
            console.print(f"Last date: {checkpoint.get('last_date')}")
            console.print(f"Documents saved: {checkpoint.get('documents_saved')}")
            console.print(f"Documents skipped: {checkpoint.get('documents_skipped')}")
            console.print(f"Completed: {checkpoint.get('completed', False)}\n")

    except Exception as e:
        console.print(f"[bold red]Error: {e}[/bold red]")
        sys.exit(1)

@app.command()
def vector_store_stats():
    """Display statistics about the vector store."""
    try:
        vector_store = ElasticsearchVectorStore()
        stats = vector_store.get_statistics()

        console.print("\n[bold blue]Vector Store Statistics[/bold blue]")
        console.print(f"Index name: {stats.get('index_name')}")
        console.print(f"Total chunks: {stats.get('total_chunks', 0)}")
        console.print(f"Total decisions: {stats.get('total_decisions', 0)}")
        console.print(f"Size: {stats.get('size_mb', 0):.2f} MB\n")

    except Exception as e:
        console.print(f"[bold red]Error: {e}[/bold red]")
        sys.exit(1)


@app.command()
def validate(
    sample_size: int = typer.Option(
        100,
        "--sample-size",
        "-n",
        help="Number of documents to validate",
    ),
):
    """Validate stored documents."""
    try:
        repository = DecisionRepository(settings.DATA_DIR)

        console.print(f"[bold blue]Validating up to {sample_size} documents...[/bold blue]\n")

        native_ids = repository.get_all_native_ids()[:sample_size]

        valid_count = 0
        invalid_count = 0
        missing_content = 0

        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
            console=console,
        ) as progress:
            task = progress.add_task("Validating...", total=len(native_ids))

            for native_id in native_ids:
                document = repository.get_decision(native_id)

                if document:

                    is_valid, error = validate_decision_document(document)

                    if is_valid:
                        valid_count += 1
                        if not document.Content:
                            missing_content += 1
                    else:
                        invalid_count += 1
                        console.print(f"[red]Invalid: {native_id} - {error}[/red]")

                progress.advance(task)

        console.print("\n[bold green]Validation complete![/bold green]")
        console.print(f"Valid documents: {valid_count}")
        console.print(f"Invalid documents: {invalid_count}")
        console.print(f"Documents without content: {missing_content}\n")

    except Exception as e:
        console.print(f"[bold red]Error: {e}[/bold red]")
        sys.exit(1)


@app.command()
def clear_checkpoint():
    """Clear the checkpoint file."""
    try:
        checkpoint_file = Path(settings.DATA_DIR) / "checkpoint.json"
        if checkpoint_file.exists():
            checkpoint_file.unlink()
            console.print("[green]Checkpoint cleared[/green]")
        else:
            console.print("[yellow]No checkpoint found[/yellow]")
    except Exception as e:
        console.print(f"[bold red]Error: {e}[/bold red]")
        sys.exit(1)


@app.command()
def clear_repository():
    """Clear all stored decision documents and checkpoint."""
    try:
        repository = DecisionRepository(settings.DATA_DIR)
        repository.clear_repository()
        console.print("[green]Repository cleared[/green]")
    except Exception as e:
        console.print(f"[bold red]Error: {e}[/bold red]")
        sys.exit(1)


@app.command()
def version():
    """Display version information."""

    console.print(f"Decision Pipeline v{__version__}")


if __name__ == "__main__":
    app()
