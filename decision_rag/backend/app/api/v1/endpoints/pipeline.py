"""Pipeline operation endpoints."""

import threading
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query

from app.api.deps import (
    get_data_fetcher,
    get_ingestion_pipeline,
    get_job_manager,
    get_repository,
    verify_api_key,
)
from app.api.v1.models.requests import FetchRequest, FullPipelineRequest, IngestRequest
from app.api.v1.models.responses import JobStatusResponse
from app.core import get_logger, settings
from app.repositories import DecisionRepository
from app.services import DecisionDataFetcher, IngestionPipeline, JobManager
from app.services.vector_store import MaxRetriesExceededError
from app.utils.checkpoint_manager import FetchCheckpoint, FullPipelineCheckpoint, IngestCheckpoint
from app.utils.date_utils import parse_date

router = APIRouter()
logger = get_logger(__name__)


def run_fetch_job(
    job_id: str,
    job_manager: JobManager,
    fetcher: DecisionDataFetcher,
    repository: DecisionRepository,
    start_date: Optional[str],
    end_date: Optional[str],
    resume: bool,
    skip_existing: bool,
):
    """
    Run fetch job in background.

    Args:
        job_id: Job identifier
        job_manager: Job manager instance
        fetcher: Data fetcher instance
        repository: Decision repository instance
        start_date: Start date for fetching
        end_date: End date for fetching
        resume: Resume from checkpoint
        skip_existing: Skip existing documents
    """
    try:
        job_manager.start_job(job_id)
        job_manager.update_progress(job_id, 0, "Starting document fetch...")

        # Initialize checkpoint manager
        checkpoint_mgr = FetchCheckpoint(repository)

        # Determine date range
        if resume:
            saved_checkpoint = checkpoint_mgr.load_checkpoint()
            if saved_checkpoint:
                logger.info(f"Resuming from checkpoint: {saved_checkpoint.get('last_date')}")
                start = parse_date(saved_checkpoint["last_date"])
                documents_saved = saved_checkpoint.get("documents_saved", 0)
                documents_skipped = saved_checkpoint.get("documents_skipped", 0)
                documents_failed = saved_checkpoint.get("documents_failed", 0)
            else:
                logger.info("No checkpoint found, starting from beginning")
                start = parse_date(start_date or settings.START_DATE)
                documents_saved = 0
                documents_skipped = 0
                documents_failed = 0
        else:
            start = parse_date(start_date or settings.START_DATE)
            documents_saved = 0
            documents_skipped = 0
            documents_failed = 0

        end = parse_date(end_date or settings.END_DATE)

        # Initialize checkpoint
        checkpoint_mgr.initialize(start.isoformat(), end.isoformat())
        checkpoint_mgr.update_progress(
            start.isoformat(), documents_saved, documents_skipped, documents_failed
        )

        logger.info(f"Starting fetch job {job_id} from {start} to {end}")

        # Fetch data - iterate through generator
        for document in fetcher.fetch_all_decisions(
            api_key=settings.API_KEY,
            start_date=start,
            end_date=end,
        ):
            # Check for shutdown request
            if job_manager.is_shutdown_requested():
                logger.warning(f"Shutdown requested, stopping fetch job {job_id}")
                # Save checkpoint before exiting
                checkpoint_mgr.save()
                job_manager.fail_job(
                    job_id,
                    "Job stopped due to shutdown request",
                    statistics={
                        "documents_saved": documents_saved,
                        "documents_skipped": documents_skipped,
                        "documents_failed": documents_failed,
                    },
                )
                return

            try:
                # Check if already exists
                if skip_existing and repository.decision_exists(document.NativeId):
                    documents_skipped += 1
                    logger.debug(f"Skipping existing document: {document.NativeId}")
                    continue

                # Save document
                if repository.save_decision(document):
                    documents_saved += 1

                    # Update and save checkpoint periodically
                    if checkpoint_mgr.should_save(
                        documents_saved, settings.CHECKPOINT_INTERVAL_FETCH
                    ):
                        checkpoint_mgr.update_progress(
                            document.DateDecision or end.isoformat(),
                            documents_saved,
                            documents_skipped,
                            documents_failed,
                        )
                        checkpoint_mgr.save()
                        job_manager.update_progress(
                            job_id,
                            min(90, (documents_saved / max(documents_saved + 100, 1)) * 100),
                            f"Fetched {documents_saved} documents",
                        )
                else:
                    documents_failed += 1

            except Exception as e:
                logger.error(f"Error saving document {document.NativeId}: {e}")
                documents_failed += 1

        # Save final checkpoint
        checkpoint_mgr.update_progress(
            end.isoformat(), documents_saved, documents_skipped, documents_failed
        )
        checkpoint_mgr.mark_completed(
            last_date=end.isoformat(),
            documents_saved=documents_saved,
            documents_skipped=documents_skipped,
            documents_failed=documents_failed,
        )

        # Complete job
        statistics = {
            "documents_saved": documents_saved,
            "documents_skipped": documents_skipped,
            "documents_failed": documents_failed,
        }

        job_manager.complete_job(
            job_id,
            statistics=statistics,
            message=f"Fetched {documents_saved} documents successfully",
        )

        logger.info(f"Fetch job {job_id} completed: {statistics}")

    except Exception as e:
        logger.error(f"Fetch job {job_id} failed: {e}")
        job_manager.fail_job(job_id, str(e))


def run_ingest_job(
    job_id: str,
    job_manager: JobManager,
    pipeline: IngestionPipeline,
    start_date: Optional[str],
    batch_size: int,
    resume: bool,
    reindex: bool,
):
    """
    Run ingestion job in background.

    Args:
        job_id: Job identifier
        job_manager: Job manager instance
        pipeline: Ingestion pipeline instance
        start_date: Start date for filtering
        batch_size: Batch size for processing
        resume: Resume from checkpoint
        reindex: Reindex existing documents
    """
    try:
        job_manager.start_job(job_id)
        job_manager.update_progress(job_id, 0, "Starting document ingestion...")

        logger.info(f"Starting ingest job {job_id}")

        # Initialize checkpoint manager
        checkpoint_mgr = IngestCheckpoint(pipeline.repository)

        # Check for resume
        resume_index = 0
        resumed_stats = {
            "processed": 0,
            "successful": 0,
            "failed": 0,
            "skipped": 0,
            "total_chunks": 0,
            "total_attachments": 0,
            "total_attachment_chunks": 0,
        }

        if resume:
            saved_checkpoint = checkpoint_mgr.load_checkpoint()
            if saved_checkpoint and not saved_checkpoint.get("completed", False):
                logger.info(
                    f"Resuming from checkpoint: processed {saved_checkpoint.get('processed', 0)} documents"
                )
                resume_index = saved_checkpoint.get("last_processed_index", 0)
                resumed_stats = {
                    "processed": saved_checkpoint.get("processed", 0),
                    "successful": saved_checkpoint.get("successful", 0),
                    "failed": saved_checkpoint.get("failed", 0),
                    "skipped": saved_checkpoint.get("skipped", 0),
                    "total_chunks": saved_checkpoint.get("total_chunks", 0),
                    "total_attachments": saved_checkpoint.get("total_attachments", 0),
                    "total_attachment_chunks": saved_checkpoint.get("total_attachment_chunks", 0),
                }
            else:
                logger.info("No valid checkpoint found, starting from beginning")

        # Get list of documents to process
        all_native_ids = pipeline.repository.get_all_native_ids()

        if not all_native_ids:
            job_manager.complete_job(
                job_id,
                statistics={"total_documents": 0, "successful": 0, "failed": 0, "skipped": 0},
                message="No documents found in repository",
            )
            return

        # Filter by date if specified
        if start_date:
            start_dt = parse_date(start_date)
            logger.info(f"Filtering documents from {start_dt.date()}")
            filtered_ids = []
            for native_id in all_native_ids:
                doc = pipeline.repository.get_decision(native_id)
                if doc and doc.DateDecision:
                    doc_date = parse_date(doc.DateDecision)
                    if doc_date >= start_dt:
                        filtered_ids.append(native_id)
            all_native_ids = filtered_ids

        logger.info(f"Processing {len(all_native_ids)} documents in batches of {batch_size}")

        # Initialize checkpoint
        checkpoint_mgr.initialize(
            total_documents=len(all_native_ids), start_date=start_date, batch_size=batch_size
        )

        # Process in batches
        total_stats = {
            "total": len(all_native_ids),
            "processed": resumed_stats["processed"],
            "successful": resumed_stats["successful"],
            "failed": resumed_stats["failed"],
            "skipped": resumed_stats["skipped"],
            "total_chunks": resumed_stats["total_chunks"],
            "total_attachments": resumed_stats["total_attachments"],
            "total_attachment_chunks": resumed_stats["total_attachment_chunks"],
        }

        # Start from resume_index if resuming
        start_index = resume_index if resume else 0
        if start_index > 0:
            logger.info(f"Resuming from index {start_index}")

        for i in range(start_index, len(all_native_ids), batch_size):
            # Check for shutdown request
            if job_manager.is_shutdown_requested():
                logger.warning(f"Shutdown requested, stopping ingest job {job_id}")
                # Save checkpoint before exiting
                checkpoint_mgr.update_progress(
                    total_stats["processed"],
                    total_stats["successful"],
                    total_stats["failed"],
                    total_stats["skipped"],
                    total_stats["total_chunks"],
                    total_stats["total_attachments"],
                    total_stats["total_attachment_chunks"],
                    i,
                )
                checkpoint_mgr.save()
                job_manager.fail_job(
                    job_id,
                    "Job stopped due to shutdown request",
                    statistics=total_stats,
                )
                return

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

            # Update and save checkpoint periodically
            if checkpoint_mgr.should_save(
                total_stats["processed"], settings.CHECKPOINT_INTERVAL_INGEST
            ):
                checkpoint_mgr.update_progress(
                    total_stats["processed"],
                    total_stats["successful"],
                    total_stats["failed"],
                    total_stats["skipped"],
                    total_stats["total_chunks"],
                    total_stats["total_attachments"],
                    total_stats["total_attachment_chunks"],
                    i + batch_size,
                )
                checkpoint_mgr.save()

            # Update progress
            progress = min(95, int((total_stats["processed"] / total_stats["total"]) * 100))
            job_manager.update_progress(
                job_id,
                progress,
                f"Processed {total_stats['processed']}/{total_stats['total']} documents",
            )

        # Complete job and mark checkpoint as completed
        checkpoint_mgr.mark_completed(
            processed=total_stats["processed"],
            successful=total_stats["successful"],
            failed=total_stats["failed"],
            skipped=total_stats["skipped"],
            total_chunks=total_stats["total_chunks"],
            total_attachments=total_stats["total_attachments"],
            total_attachment_chunks=total_stats["total_attachment_chunks"],
        )

        statistics = {
            "total_documents": total_stats["total"],
            "successful": total_stats["successful"],
            "failed": total_stats["failed"],
            "skipped": total_stats["skipped"],
            "total_chunks": total_stats["total_chunks"],
            "attachments_processed": total_stats["total_attachments"],
            "attachment_chunks": total_stats["total_attachment_chunks"],
        }

        job_manager.complete_job(
            job_id,
            statistics=statistics,
            message=f"Ingested {statistics['successful']} documents successfully",
        )

        logger.info(f"Ingest job {job_id} completed: {statistics}")

    except MaxRetriesExceededError as e:
        logger.error(f"Ingest job {job_id} failed due to max retries exceeded: {e}")
        # Request shutdown to prevent further operations
        job_manager.request_shutdown()
        job_manager.fail_job(
            job_id,
            f"Maximum Elasticsearch retry attempts exceeded. Shutdown requested. Error: {str(e)}",
        )
    except Exception as e:
        logger.error(f"Ingest job {job_id} failed: {e}")
        job_manager.fail_job(job_id, str(e))


def run_full_pipeline_job(
    job_id: str,
    job_manager: JobManager,
    fetcher: DecisionDataFetcher,
    pipeline: IngestionPipeline,
    request: FullPipelineRequest,
):
    """
    Run full pipeline in background (fetch + ingest in streaming batches).

    Args:
        job_id: Job identifier
        job_manager: Job manager instance
        fetcher: Data fetcher instance
        pipeline: Ingestion pipeline instance
        request: Pipeline request parameters
    """
    try:
        job_manager.start_job(job_id)
        job_manager.update_progress(job_id, 0, "Starting full pipeline...")

        # Initialize checkpoint manager
        checkpoint_mgr = FullPipelineCheckpoint(pipeline.repository)

        # Determine date range
        if request.resume:
            saved_checkpoint = checkpoint_mgr.load_checkpoint()
            if saved_checkpoint and not saved_checkpoint.get("completed", False):
                logger.info(
                    f"Resuming from checkpoint: {saved_checkpoint.get('last_date')}"
                )
                start = parse_date(saved_checkpoint["last_date"])
            else:
                logger.info("No valid checkpoint found, starting from beginning")
                start = parse_date(request.start_date or settings.START_DATE)
        else:
            start = parse_date(request.start_date or settings.START_DATE)

        end = parse_date(request.end_date or settings.END_DATE)

        logger.info(f"Starting full pipeline job {job_id} from {start} to {end}")
        logger.info(f"Batch size: {request.batch_size}, Skip existing: {request.skip_existing}")

        # Initialize checkpoint
        checkpoint_mgr.initialize(start.isoformat(), end.isoformat(), request.batch_size)

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

        job_manager.update_progress(job_id, 5, "Fetching documents...")

        # Create filter function if skip_existing is enabled
        id_filter = None
        if request.skip_existing:
            # Filter function checks if document should be fetched (returns True if NOT in vector store)
            def id_filter(native_id: str) -> bool:
                return not pipeline.vector_store.document_exists(native_id)

        # Stream documents from fetcher with optional filtering
        for document in fetcher.fetch_all_decisions(
            settings.API_KEY, start, end, id_filter=id_filter
        ):
            # Check for shutdown request
            if job_manager.is_shutdown_requested():
                logger.warning(f"Shutdown requested, stopping full pipeline job {job_id}")
                # Get fetcher stats and save checkpoint
                fetcher_stats = fetcher.stats
                checkpoint_mgr.update_progress(
                    document.DateDecision or end.isoformat(),
                    fetcher_stats.get("ids_fetched", 0),
                    fetcher_stats.get("ids_skipped_existing", 0),
                    stats["total_fetched"],
                    fetcher_stats.get("errors", 0),
                    fetcher_stats.get("retry_attempts", 0),
                    fetcher_stats.get("documents_recovered", 0),
                    fetcher_stats.get("permanently_failed", 0),
                    stats["total_processed"],
                    stats["total_successful"],
                    stats["total_failed"],
                    stats["total_skipped"],
                    stats["total_chunks"],
                    stats["total_attachments"],
                    stats["total_attachment_chunks"],
                    stats["total_deleted"],
                    stats["batches_processed"],
                )
                checkpoint_mgr.save()
                job_manager.fail_job(
                    job_id,
                    "Job stopped due to shutdown request",
                    statistics=stats,
                )
                return

            stats["total_fetched"] += 1

            # Save document temporarily
            if pipeline.repository.save_decision(document):
                batch_buffer.append(document)
                batch_native_ids.append(document.NativeId)

            # Process batch when size reached
            if len(batch_buffer) >= request.batch_size:
                logger.info(f"Processing batch of {len(batch_buffer)} documents")
                job_manager.update_progress(
                    job_id,
                    min(90, 10 + (stats["total_fetched"] / max(stats["total_fetched"] + 100, 1)) * 80),
                    f"Processing batch {stats['batches_processed'] + 1}...",
                )

                # Process the batch
                batch_stats = pipeline.process_batch(batch_native_ids, reindex=not request.skip_existing)

                stats["total_processed"] += batch_stats["processed"]
                stats["total_successful"] += batch_stats["successful"]
                stats["total_failed"] += batch_stats["failed"]
                stats["total_skipped"] += batch_stats["skipped"]
                stats["total_chunks"] += batch_stats["total_chunks"]
                stats["total_attachments"] += batch_stats.get("total_attachments", 0)
                stats["total_attachment_chunks"] += batch_stats.get("total_attachment_chunks", 0)
                stats["batches_processed"] += 1

                # Delete processed files if not keeping them
                if not request.keep_files:
                    for native_id in batch_native_ids:
                        try:
                            pipeline.repository.delete_decision(native_id)
                            stats["total_deleted"] += 1
                        except Exception as e:
                            logger.warning(f"Failed to delete {native_id}: {e}")

                # Save checkpoint periodically
                if checkpoint_mgr.should_save(
                    stats["batches_processed"],
                    settings.CHECKPOINT_INTERVAL_FULL_PIPELINE_BATCHES,
                ):
                    fetcher_stats = fetcher.stats
                    checkpoint_mgr.update_progress(
                        document.DateDecision or end.isoformat(),
                        fetcher_stats.get("ids_fetched", 0),
                        fetcher_stats.get("ids_skipped_existing", 0),
                        stats["total_fetched"],
                        fetcher_stats.get("errors", 0),
                        fetcher_stats.get("retry_attempts", 0),
                        fetcher_stats.get("documents_recovered", 0),
                        fetcher_stats.get("permanently_failed", 0),
                        stats["total_processed"],
                        stats["total_successful"],
                        stats["total_failed"],
                        stats["total_skipped"],
                        stats["total_chunks"],
                        stats["total_attachments"],
                        stats["total_attachment_chunks"],
                        stats["total_deleted"],
                        stats["batches_processed"],
                    )
                    checkpoint_mgr.save()

                # Clear batch
                batch_buffer = []
                batch_native_ids = []

        # Process remaining documents in final batch
        if batch_buffer:
            logger.info(f"Processing final batch of {len(batch_buffer)} documents")
            job_manager.update_progress(job_id, 95, "Processing final batch...")

            batch_stats = pipeline.process_batch(batch_native_ids, reindex=not request.skip_existing)

            stats["total_processed"] += batch_stats["processed"]
            stats["total_successful"] += batch_stats["successful"]
            stats["total_failed"] += batch_stats["failed"]
            stats["total_skipped"] += batch_stats["skipped"]
            stats["total_chunks"] += batch_stats["total_chunks"]
            stats["total_attachments"] += batch_stats.get("total_attachments", 0)
            stats["total_attachment_chunks"] += batch_stats.get("total_attachment_chunks", 0)
            stats["batches_processed"] += 1

            # Delete processed files
            if not request.keep_files:
                for native_id in batch_native_ids:
                    try:
                        pipeline.repository.delete_decision(native_id)
                        stats["total_deleted"] += 1
                    except Exception as e:
                        logger.warning(f"Failed to delete {native_id}: {e}")

        # Mark checkpoint as completed
        fetcher_stats = fetcher.stats
        checkpoint_mgr.mark_completed(
            last_date=end.isoformat(),
            native_ids_fetched=fetcher_stats.get("ids_fetched", 0),
            native_ids_skipped_existing=fetcher_stats.get("ids_skipped_existing", 0),
            documents_fetched=stats["total_fetched"],
            fetch_errors=fetcher_stats.get("errors", 0),
            fetch_retry_attempts=fetcher_stats.get("retry_attempts", 0),
            fetch_documents_recovered=fetcher_stats.get("documents_recovered", 0),
            fetch_permanently_failed=fetcher_stats.get("permanently_failed", 0),
            documents_processed=stats["total_processed"],
            documents_successful=stats["total_successful"],
            documents_failed=stats["total_failed"],
            documents_skipped=stats["total_skipped"],
            total_chunks=stats["total_chunks"],
            total_attachments=stats["total_attachments"],
            total_attachment_chunks=stats["total_attachment_chunks"],
            files_deleted=stats["total_deleted"],
            batches_processed=stats["batches_processed"],
        )

        # Complete job
        # Include fetcher statistics for tracking fetch failures and retries
        fetcher_stats = fetcher.stats
        statistics = {
            "native_ids_fetched": fetcher_stats.get("ids_fetched", 0),
            "native_ids_skipped_existing": fetcher_stats.get("ids_skipped_existing", 0),
            "documents_fetched": stats["total_fetched"],
            "fetch_errors": fetcher_stats.get("errors", 0),
            "fetch_retry_attempts": fetcher_stats.get("retry_attempts", 0),
            "fetch_documents_recovered": fetcher_stats.get("documents_recovered", 0),
            "fetch_permanently_failed": fetcher_stats.get("permanently_failed", 0),
            "documents_processed": stats["total_processed"],
            "successful": stats["total_successful"],
            "failed": stats["total_failed"],
            "skipped": stats["total_skipped"],
            "total_chunks": stats["total_chunks"],
            "attachments_processed": stats["total_attachments"],
            "attachment_chunks": stats["total_attachment_chunks"],
            "files_deleted": stats["total_deleted"],
            "batches_processed": stats["batches_processed"],
        }

        job_manager.complete_job(
            job_id,
            statistics=statistics,
            message=f"Full pipeline completed: {stats['total_successful']} documents processed successfully",
        )

        logger.info(f"Full pipeline job {job_id} completed: {statistics}")

    except MaxRetriesExceededError as e:
        logger.error(f"Full pipeline job {job_id} failed due to max retries exceeded: {e}")
        # Request shutdown to prevent further operations
        job_manager.request_shutdown()
        job_manager.fail_job(
            job_id,
            f"Maximum Elasticsearch retry attempts exceeded. Shutdown requested. Error: {str(e)}",
        )
    except Exception as e:
        logger.error(f"Full pipeline job {job_id} failed: {e}")
        job_manager.fail_job(job_id, str(e))


@router.post("/fetch", response_model=JobStatusResponse, status_code=202)
async def start_fetch(
    request: FetchRequest,
    fetcher: DecisionDataFetcher = Depends(get_data_fetcher),
    repository: DecisionRepository = Depends(get_repository),
    job_manager: JobManager = Depends(get_job_manager),
    _: None = Depends(verify_api_key),
):
    """
    Start document fetching process.

    Fetches decision documents from the external API and stores them locally.
    The operation runs in the background and returns a job ID for tracking.

    Returns:
        Job status with job_id for tracking progress
    """
    # Check if shutdown is requested
    if job_manager.is_shutdown_requested():
        raise HTTPException(
            status_code=503,
            detail="Pipeline shutdown has been requested. Cannot start new jobs. Use /pipeline/shutdown/reset to resume operations.",
        )

    # Create job
    job_id = job_manager.create_job("fetch")

    # Start background task
    thread = threading.Thread(
        target=run_fetch_job,
        args=(
            job_id,
            job_manager,
            fetcher,
            repository,
            request.start_date,
            request.end_date,
            request.resume,
            request.skip_existing,
        ),
    )
    thread.daemon = True
    thread.start()

    job = job_manager.get_job(job_id)

    return JobStatusResponse(
        job_id=job.job_id,
        type=job.type,
        status=job.status,
        message="Fetch job started",
    )


@router.post("/ingest", response_model=JobStatusResponse, status_code=202)
async def start_ingest(
    request: IngestRequest,
    pipeline: IngestionPipeline = Depends(get_ingestion_pipeline),
    job_manager: JobManager = Depends(get_job_manager),
    _: None = Depends(verify_api_key),
):
    """
    Start document ingestion process.

    Processes fetched documents through the full pipeline:
    - Converts HTML to Markdown
    - Chunks text into embeddable segments
    - Generates embeddings using Azure OpenAI
    - Indexes to Elasticsearch
    - Processes public attachments (if not skipped)

    Returns:
        Job status with job_id for tracking progress
    """
    # Check if shutdown is requested
    if job_manager.is_shutdown_requested():
        raise HTTPException(
            status_code=503,
            detail="Pipeline shutdown has been requested. Cannot start new jobs. Use /pipeline/shutdown/reset to resume operations.",
        )

    # Create job
    job_id = job_manager.create_job("ingest")

    # Start background task
    thread = threading.Thread(
        target=run_ingest_job,
        args=(
            job_id,
            job_manager,
            pipeline,
            request.start_date,
            request.batch_size,
            request.resume,
            request.reindex,
        ),
    )
    thread.daemon = True
    thread.start()

    job = job_manager.get_job(job_id)

    return JobStatusResponse(
        job_id=job.job_id,
        type=job.type,
        status=job.status,
        message="Ingest job started",
    )


@router.post("/full", response_model=JobStatusResponse, status_code=202)
async def start_full_pipeline(
    request: FullPipelineRequest,
    fetcher: DecisionDataFetcher = Depends(get_data_fetcher),
    pipeline: IngestionPipeline = Depends(get_ingestion_pipeline),
    job_manager: JobManager = Depends(get_job_manager),
    _: None = Depends(verify_api_key),
):
    """
    Start full pipeline execution (fetch + ingest).

    Executes both fetching and ingestion in sequence:
    1. Fetches documents from external API
    2. Processes and indexes them to vector store

    Returns:
        Job status with job_id for tracking progress
    """
    # Check if shutdown is requested
    if job_manager.is_shutdown_requested():
        raise HTTPException(
            status_code=503,
            detail="Pipeline shutdown has been requested. Cannot start new jobs. Use /pipeline/shutdown/reset to resume operations.",
        )

    # Create job
    job_id = job_manager.create_job("full_pipeline")

    # Start background task
    thread = threading.Thread(
        target=run_full_pipeline_job,
        args=(job_id, job_manager, fetcher, pipeline, request),
    )
    thread.daemon = True
    thread.start()

    job = job_manager.get_job(job_id)

    return JobStatusResponse(
        job_id=job.job_id,
        type=job.type,
        status=job.status,
        message="Full pipeline job started",
    )


@router.get("/jobs/{job_id}", response_model=JobStatusResponse)
async def get_job_status(
    job_id: str,
    job_manager: JobManager = Depends(get_job_manager),
    _: None = Depends(verify_api_key),
):
    """
    Get status of a running or completed job.

    Returns detailed information about job progress, status, and statistics.

    Args:
        job_id: Job identifier

    Returns:
        Job status with progress and statistics
    """
    job = job_manager.get_job(job_id)

    if not job:
        raise HTTPException(status_code=404, detail=f"Job {job_id} not found")

    return JobStatusResponse(
        job_id=job.job_id,
        type=job.type,
        status=job.status,
        progress=job.progress,
        message=job.message or job.error,
        start_time=job.start_time,
        end_time=job.end_time,
        statistics=job.statistics,
    )


@router.get("/jobs", response_model=List[JobStatusResponse])
async def list_jobs(
    job_type: Optional[str] = Query(None, description="Filter by job type"),
    status: Optional[str] = Query(None, description="Filter by status"),
    job_manager: JobManager = Depends(get_job_manager),
    _: None = Depends(verify_api_key),
):
    """
    List all jobs with optional filters.

    Args:
        job_type: Filter by job type (fetch, ingest, full_pipeline)
        status: Filter by status (created, running, completed, failed, cancelled)

    Returns:
        List of jobs matching the filters
    """
    jobs = job_manager.list_jobs(job_type=job_type, status=status)

    return [
        JobStatusResponse(
            job_id=job.job_id,
            type=job.type,
            status=job.status,
            progress=job.progress,
            message=job.message or job.error,
            start_time=job.start_time,
            end_time=job.end_time,
            statistics=job.statistics,
        )
        for job in jobs
    ]


@router.delete("/jobs/{job_id}", response_model=JobStatusResponse)
async def cancel_job(
    job_id: str,
    job_manager: JobManager = Depends(get_job_manager),
    _: None = Depends(verify_api_key),
):
    """
    Cancel a running job.

    Note: Cancellation may not be immediate as it depends on the job's current state.

    Args:
        job_id: Job identifier

    Returns:
        Updated job status
    """
    job = job_manager.get_job(job_id)

    if not job:
        raise HTTPException(status_code=404, detail=f"Job {job_id} not found")

    if job.status not in ("created", "running"):
        raise HTTPException(
            status_code=409,
            detail=f"Cannot cancel job in status: {job.status}",
        )

    success = job_manager.cancel_job(job_id)

    if not success:
        raise HTTPException(status_code=500, detail="Failed to cancel job")

    job = job_manager.get_job(job_id)

    return JobStatusResponse(
        job_id=job.job_id,
        type=job.type,
        status=job.status,
        progress=job.progress,
        message=job.message,
        start_time=job.start_time,
        end_time=job.end_time,
    )


@router.post("/shutdown")
async def request_shutdown(
    job_manager: JobManager = Depends(get_job_manager),
    _: None = Depends(verify_api_key),
):
    """
    Request graceful shutdown of all pipeline operations.

    This endpoint triggers a graceful shutdown flag that will:
    - Stop all running pipeline jobs at safe checkpoint
    - Prevent new jobs from starting
    - Allow the application to be shut down safely

    Use this when:
    - Elasticsearch connection errors persist and cannot be recovered
    - Critical errors occur that require manual intervention
    - Need to stop the pipeline without losing data

    Returns:
        Shutdown confirmation with status of running jobs
    """
    logger.warning("Shutdown requested via API endpoint")

    # Request shutdown
    job_manager.request_shutdown()

    # Get currently running jobs
    running_jobs = job_manager.list_jobs(status="running")

    response = {
        "message": "Graceful shutdown requested",
        "shutdown_requested": True,
        "running_jobs_count": len(running_jobs),
        "running_jobs": [
            {
                "job_id": job.job_id,
                "type": job.type,
                "progress": job.progress,
                "message": job.message,
            }
            for job in running_jobs
        ],
        "note": "Running jobs will be stopped at the next safe checkpoint",
    }

    return response


@router.post("/shutdown/reset")
async def reset_shutdown_flag(
    job_manager: JobManager = Depends(get_job_manager),
    _: None = Depends(verify_api_key),
):
    """
    Reset the shutdown flag to allow pipeline operations to resume.

    Use this after:
    - Resolving the issues that required shutdown
    - Verifying Elasticsearch connection is stable
    - Ready to restart pipeline operations

    Returns:
        Confirmation of shutdown flag reset
    """
    logger.info("Shutdown flag reset via API endpoint")

    job_manager.reset_shutdown_flag()

    return {
        "message": "Shutdown flag has been reset",
        "shutdown_requested": False,
        "note": "Pipeline operations can now be started again",
    }
