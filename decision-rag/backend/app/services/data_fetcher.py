"""
Data fetcher service for orchestrating decision data retrieval.

There are capabilities for fetching decision IDs in batches, filtering IDs to avoid fetching existing documents, and robust retry logic for failed document fetches with exponential backoff and staggered delays.
"""

import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Generator, List, Optional

from ..core.config import settings
from ..schemas.decision import DecisionDocument
from ..utils.date_utils import format_date_for_api, generate_date_range, weeks_between
from .api_client import DecisionAPIClient

logger = logging.getLogger(__name__)


class DecisionDataFetcher:
    """Service for fetching decision data in batches."""

    def __init__(self, api_client: Optional[DecisionAPIClient] = None):
        """
        Initialize the data fetcher.

        Args:
            api_client: Optional API client instance
        """
        self.api_client = api_client or DecisionAPIClient()
        self.stats = {
            "total_batches": 0,
            "batches_completed": 0,
            "ids_fetched": 0,
            "ids_skipped_existing": 0,
            "documents_fetched": 0,
            "errors": 0,
            "retry_attempts": 0,
            "documents_recovered": 0,
            "permanently_failed": 0,
            "start_time": None,
        }

    def fetch_all_decisions(
        self,
        api_key: str,
        start_date: datetime,
        end_date: datetime,
        id_filter: Optional[callable] = None,
    ) -> Generator[DecisionDocument, None, None]:
        """
        Fetch all decisions for the given date range.

        This method can optionally filter IDs before fetching to avoid fetching
        documents that already exist in the vector store.

        Args:
            api_key: API key for authentication
            start_date: Start date
            end_date: End date
            id_filter: Optional callable that takes a native_id and returns True if
                      the document should be fetched. If None, all documents are fetched.

        Yields:
            DecisionDocument objects
        """
        # Reset stats for this fetch operation
        self.stats = {
            "total_batches": 0,
            "batches_completed": 0,
            "ids_fetched": 0,
            "ids_skipped_existing": 0,
            "documents_fetched": 0,
            "errors": 0,
            "retry_attempts": 0,
            "documents_recovered": 0,
            "permanently_failed": 0,
            "start_time": time.time(),
        }

        # Generate date batches
        date_batches = generate_date_range(start_date, end_date, settings.BATCH_SIZE_DAYS)
        self.stats["total_batches"] = len(date_batches)

        total_weeks = weeks_between(start_date, end_date)
        filter_msg = " with ID filtering" if id_filter else ""
        logger.info(f"Starting data fetch{filter_msg} from {start_date.date()} to {end_date.date()}")
        logger.info(f"Total batches: {len(date_batches)} ({total_weeks} weeks)")

        # Process each batch
        for batch_start, batch_end in date_batches:
            logger.info(f"\nProcessing batch: {batch_start.date()} to {batch_end.date()}")

            try:
                # Fetch documents for this batch
                documents = self._fetch_batch(batch_start, batch_end, api_key, id_filter)

                # Yield each document
                for doc in documents:
                    self.stats["documents_fetched"] += 1
                    yield doc

                self.stats["batches_completed"] += 1

                # Log progress
                self._log_progress()

            except Exception as e:
                logger.error(
                    f"Error processing batch {batch_start.date()} to {batch_end.date()}: {e}"
                )
                self.stats["errors"] += 1

        # Log final summary
        self._log_summary()

    def _fetch_batch(
        self,
        start_date: datetime,
        end_date: datetime,
        api_key: str,
        id_filter: Optional[callable] = None,
    ) -> List[DecisionDocument]:
        """
        Fetch all decisions for a single date batch.

        Args:
            start_date: Batch start date
            end_date: Batch end date
            api_key: API key for authentication
            id_filter: Optional callable that takes a native_id and returns True if
                      the document should be fetched. If None, all documents are fetched.

        Returns:
            List of DecisionDocument objects
        """
        # Format dates for API
        start_str = format_date_for_api(start_date)
        end_str = format_date_for_api(end_date)

        # Fetch all decision IDs for this batch
        decision_ids = self.api_client.fetch_decision_ids(api_key, start_str, end_str)
        total_ids = len(decision_ids.decisions)
        self.stats["ids_fetched"] += total_ids

        logger.info(f"Found {total_ids} decision IDs in batch")

        if not decision_ids.decisions:
            return []

        # Extract NativeIds
        native_ids = [d.NativeId for d in decision_ids.decisions]

        # Filter IDs if filter function provided
        if id_filter:
            filtered_ids = []
            for native_id in native_ids:
                if id_filter(native_id):
                    filtered_ids.append(native_id)
                else:
                    self.stats["ids_skipped_existing"] += 1

            skipped_count = len(native_ids) - len(filtered_ids)
            logger.info(
                f"Filtered {len(filtered_ids)} IDs to fetch "
                f"(skipped {skipped_count} existing documents)"
            )
            native_ids = filtered_ids

        # If no documents to fetch after filtering, return empty list
        if not native_ids:
            return []

        # Fetch documents for filtered IDs
        documents, failed_ids = self._fetch_documents_for_ids(native_ids, api_key)

        # Retry failed documents if any
        if failed_ids:
            logger.info(f"Retrying {len(failed_ids)} failed documents...")
            retry_documents = self._retry_failed_documents(failed_ids, api_key)
            documents.extend(retry_documents)

        return documents

    def _fetch_documents_for_ids(
        self, native_ids: List[str], api_key: str
    ) -> tuple[List[DecisionDocument], List[str]]:
        """
        Fetch decision documents for a list of NativeIds.

        Args:
            native_ids: List of NativeId strings
            api_key: API key for authentication

        Returns:
            Tuple of (List of DecisionDocument objects, List of failed native_ids)
        """
        documents = []
        failed_ids = []

        # Use ThreadPoolExecutor for parallel fetching
        with ThreadPoolExecutor(max_workers=settings.MAX_CONCURRENT_REQUESTS) as executor:
            # Submit all fetch tasks with thread_safe=True for parallel execution
            future_to_id = {
                executor.submit(
                    self.api_client.fetch_decision_document, native_id, api_key
                ): native_id
                for native_id in native_ids
            }

            # Collect results as they complete
            for future in as_completed(future_to_id):
                native_id = future_to_id[future]
                try:
                    document = future.result()
                    if document:
                        documents.append(document)
                    else:
                        logger.warning(f"No document returned for {native_id}")
                        failed_ids.append(native_id)
                        self.stats["errors"] += 1
                except Exception as e:
                    logger.error(f"Error fetching document {native_id}: {e}", exc_info=True)
                    failed_ids.append(native_id)
                    self.stats["errors"] += 1

        if failed_ids:
            logger.warning(f"Failed to fetch {len(failed_ids)} documents: {failed_ids[:5]}{'...' if len(failed_ids) > 5 else ''}")

        logger.info(f"Successfully fetched {len(documents)}/{len(native_ids)} documents (errors: {len(failed_ids)})")
        return documents, failed_ids

    def _retry_failed_documents(
        self, failed_ids: List[str], api_key: str
    ) -> List[DecisionDocument]:
        """
        Retry fetching failed documents with exponential backoff and staggered delays.

        Args:
            failed_ids: List of native IDs that failed initial fetch
            api_key: API key for authentication

        Returns:
            List of successfully fetched DecisionDocument objects from retries
        """
        recovered_documents = []
        remaining_failed = failed_ids.copy()
        max_retries = settings.DOCUMENT_FETCH_MAX_RETRIES

        logger.info(
            f"Starting retry process for {len(failed_ids)} failed documents "
            f"(max {max_retries} retries per document)"
        )

        # Iterate through retry attempts
        for retry_attempt in range(1, max_retries + 1):
            if not remaining_failed:
                break

            # Calculate exponential backoff wait time for this retry round
            wait_time = min(
                settings.DOCUMENT_FETCH_RETRY_MIN_WAIT
                * (settings.DOCUMENT_FETCH_RETRY_BACKOFF_MULTIPLIER ** (retry_attempt - 1)),
                settings.DOCUMENT_FETCH_RETRY_MAX_WAIT,
            )

            logger.info(
                f"Retry attempt {retry_attempt}/{max_retries}: "
                f"Retrying {len(remaining_failed)} documents after {wait_time:.1f}s wait"
            )

            # Wait before starting this retry round
            time.sleep(wait_time)

            # Process retries sequentially with stagger delay
            newly_failed = []
            for i, native_id in enumerate(remaining_failed):
                # Add stagger delay between different document retries (except first)
                if i > 0:
                    time.sleep(settings.DOCUMENT_FETCH_RETRY_STAGGER_DELAY)

                self.stats["retry_attempts"] += 1

                try:
                    # Attempt to fetch the document
                    document = self.api_client.fetch_decision_document(native_id, api_key)

                    if document:
                        recovered_documents.append(document)
                        self.stats["documents_recovered"] += 1
                        logger.info(
                            f"Successfully recovered document {native_id} "
                            f"on retry attempt {retry_attempt}"
                        )
                    else:
                        logger.warning(
                            f"No document returned for {native_id} on retry attempt {retry_attempt}"
                        )
                        newly_failed.append(native_id)

                except Exception as e:
                    logger.error(
                        f"Error fetching document {native_id} on retry attempt {retry_attempt}: {e}"
                    )
                    newly_failed.append(native_id)

            # Update remaining failed list for next retry round
            remaining_failed = newly_failed

            if remaining_failed:
                logger.info(
                    f"Retry attempt {retry_attempt} complete: "
                    f"Recovered {len(recovered_documents)} documents, "
                    f"{len(remaining_failed)} still failing"
                )

        # Log permanently failed documents
        if remaining_failed:
            self.stats["permanently_failed"] += len(remaining_failed)
            logger.warning(
                f"{len(remaining_failed)} documents permanently failed after {max_retries} retry attempts: "
                f"{remaining_failed[:5]}{'...' if len(remaining_failed) > 5 else ''}"
            )

        logger.info(
            f"Retry process complete: Recovered {len(recovered_documents)}/{len(failed_ids)} documents, "
            f"{len(remaining_failed)} permanently failed"
        )

        return recovered_documents

    def _log_progress(self):
        """Log current progress."""
        elapsed = time.time() - self.stats["start_time"]
        batches_done = self.stats["batches_completed"]
        total_batches = self.stats["total_batches"]

        progress_pct = (batches_done / total_batches * 100) if total_batches > 0 else 0

        # Estimate remaining time
        if batches_done > 0:
            avg_time_per_batch = elapsed / batches_done
            remaining_batches = total_batches - batches_done
            estimated_remaining = avg_time_per_batch * remaining_batches

            logger.info(
                f"Progress: {batches_done}/{total_batches} batches ({progress_pct:.1f}%) | "
                f"IDs: {self.stats['ids_fetched']} | "
                f"IDs skipped: {self.stats.get('ids_skipped_existing', 0)} | "
                f"Documents: {self.stats['documents_fetched']} | "
                f"Errors: {self.stats['errors']} | "
                f"Retries: {self.stats['retry_attempts']} | "
                f"Recovered: {self.stats['documents_recovered']} | "
                f"Elapsed: {elapsed/60:.1f}m | "
                f"Est. remaining: {estimated_remaining/60:.1f}m"
            )

    def _log_summary(self):
        """Log final summary."""
        elapsed = time.time() - self.stats["start_time"]

        logger.info("\n" + "=" * 80)
        logger.info("FETCH SUMMARY")
        logger.info("=" * 80)
        logger.info(
            f"Total batches processed: {self.stats['batches_completed']}/{self.stats['total_batches']}"
        )
        logger.info(f"Total IDs fetched: {self.stats['ids_fetched']}")
        if self.stats.get('ids_skipped_existing', 0) > 0:
            logger.info(f"IDs skipped (existing): {self.stats['ids_skipped_existing']}")
        logger.info(f"Total documents fetched: {self.stats['documents_fetched']}")
        logger.info(f"Total errors: {self.stats['errors']}")
        logger.info(f"Retry attempts: {self.stats['retry_attempts']}")
        logger.info(f"Documents recovered via retry: {self.stats['documents_recovered']}")
        logger.info(f"Documents permanently failed: {self.stats['permanently_failed']}")
        if self.stats["retry_attempts"] > 0:
            recovery_rate = (
                self.stats["documents_recovered"] / self.stats["retry_attempts"] * 100
            )
            logger.info(f"Retry recovery rate: {recovery_rate:.1f}%")
        logger.info(f"Total time: {elapsed/60:.1f} minutes")
        if self.stats["batches_completed"] > 0:
            logger.info(
                f"Average time per batch: {elapsed/self.stats['batches_completed']:.1f} seconds"
            )
        logger.info("=" * 80)
