"""
Ingestion pipeline for processing decision documents.

Implements the full flow from loading documents, converting content, chunking, embedding, and indexing to Elasticsearch. Also includes attachment processing with parallelization and robust error handling.
"""

import shutil
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from app.core import get_logger, settings
from app.repositories import DecisionRepository
from app.schemas.decision import Attachment
from app.services.attachment_downloader import AttachmentDownloader
from app.services.chunker import ParagraphChunker
from app.services.content_converter import convert_attachment_content, convert_decision_content
from app.services.embedder import AzureEmbedder
from app.services.vector_store import ElasticsearchVectorStore

logger = get_logger(__name__)


class IngestionPipeline:
    """
    Orchestrates the full ingestion pipeline:
    1. Load decision documents
    2. Convert HTML to Markdown
    3. Chunk markdown text
    4. Generate embeddings
    5. Index to Elasticsearch
    """

    def __init__(
        self,
        repository: DecisionRepository,
        chunker: ParagraphChunker,
        embedder: AzureEmbedder,
        vector_store: ElasticsearchVectorStore,
        attachment_downloader: Optional[AttachmentDownloader] = None,
    ):
        """
        Initialize ingestion pipeline.

        Args:
            repository: Decision document repository
            chunker: Text chunking service
            embedder: Embedding generation service
            vector_store: Vector store service
            attachment_downloader: Attachment download service (optional)
        """
        self.repository = repository
        self.chunker = chunker
        self.embedder = embedder
        self.vector_store = vector_store
        self.attachment_downloader = attachment_downloader
        self._lock = threading.Lock()  # Thread safety for logging and stats

        logger.info("Initialized IngestionPipeline")

    def process_document(self, native_id: str, reindex: bool = False) -> Dict[str, Any]:
        """
        Process a single decision document.

        Args:
            native_id: Native ID of the document
            reindex: Force reindexing even if document exists

        Returns:
            Dictionary with processing statistics
        """
        stats = {
            "native_id": native_id,
            "success": False,
            "chunks_created": 0,
            "chunks_indexed": 0,
            "attachments_processed": 0,
            "attachment_chunks_created": 0,
            "attachment_chunks_indexed": 0,
            "error": None,
        }

        try:
            # Check if already indexed
            if not reindex and self.vector_store.document_exists(native_id):
                logger.info(f"Document {native_id} already indexed, skipping")
                stats["skipped"] = True
                return stats

            # Load decision document
            decision = self.repository.get_decision(native_id)
            if not decision:
                stats["error"] = "Document not found"
                logger.warning(f"Document not found: {native_id}")
                return stats

            # Check if document has content
            if not decision.Content:
                stats["error"] = "No content"
                logger.warning(f"Document has no content: {native_id}")
                return stats

            # Convert HTML to Markdown
            logger.debug(f"Converting HTML to Markdown for {native_id}")
            markdown_text = convert_decision_content(decision.Content)

            if not markdown_text or not markdown_text.strip():
                stats["error"] = "Conversion failed or empty result"
                logger.warning(f"Markdown conversion resulted in empty text: {native_id}")
                return stats

            # Prepare metadata
            metadata = self._extract_metadata(decision)
            metadata["is_attachment"] = False  # Mark as decision chunk

            # Chunk text
            logger.debug(f"Chunking text for {native_id}")
            chunks = self.chunker.chunk_text(
                text=markdown_text, native_id=native_id, metadata=metadata
            )

            if not chunks:
                stats["error"] = "No chunks created"
                logger.warning(f"No chunks created for {native_id}")
                return stats

            stats["chunks_created"] = len(chunks)

            # Generate embeddings
            logger.debug(f"Generating embeddings for {len(chunks)} chunks")
            chunk_dicts = [
                {"chunk_id": chunk.chunk_id, "text": chunk.text, "native_id": chunk.native_id}
                for chunk in chunks
            ]

            embedding_results = self.embedder.create_embeddings(chunk_dicts)

            if not embedding_results:
                stats["error"] = "Embedding generation failed"
                logger.error(f"Failed to generate embeddings for {native_id}")
                return stats

            # Prepare chunks with embeddings for indexing
            chunks_with_embeddings = []
            embedding_map = {er.chunk_id: er.embedding for er in embedding_results}

            for chunk in chunks:
                if chunk.chunk_id in embedding_map:
                    chunks_with_embeddings.append(
                        {
                            "chunk_id": chunk.chunk_id,
                            "native_id": chunk.native_id,
                            "chunk_index": chunk.chunk_index,
                            "text": chunk.text,
                            "embedding": embedding_map[chunk.chunk_id],
                            "token_count": chunk.token_count,
                            "chunk_position": chunk.chunk_index,
                            "metadata": chunk.metadata,
                        }
                    )

            # Delete existing chunks if reindexing
            if reindex:
                deleted_count = self.vector_store.delete_document(native_id)
                logger.info(f"Deleted {deleted_count} existing chunks for {native_id}")

            # Index to Elasticsearch
            logger.debug(f"Indexing {len(chunks_with_embeddings)} chunks to Elasticsearch")
            index_result = self.vector_store.bulk_index_chunks(chunks_with_embeddings)

            stats["chunks_indexed"] = index_result["success"]
            stats["success"] = index_result["success"] > 0

            if index_result["failed"] > 0:
                stats["error"] = f"{index_result['failed']} chunks failed to index"

            logger.info(
                f"Processed document {native_id}: "
                f"{stats['chunks_created']} chunks created, "
                f"{stats['chunks_indexed']} chunks indexed"
            )

            # Process attachments if enabled and downloader is available
            if settings.PROCESS_ATTACHMENTS and self.attachment_downloader and decision.Attachments:
                try:
                    attachment_stats = self.process_attachments(decision)
                    stats["attachments_processed"] = attachment_stats.get(
                        "attachments_processed", 0
                    )
                    stats["attachment_chunks_created"] = attachment_stats.get("chunks_created", 0)
                    stats["attachment_chunks_indexed"] = attachment_stats.get("chunks_indexed", 0)
                except Exception as e:
                    logger.error(
                        f"Error processing attachments for {native_id}: {e}", exc_info=True
                    )
                    # Don't fail the entire document processing

        except Exception as e:
            stats["error"] = str(e)
            logger.error(f"Error processing document {native_id}: {e}", exc_info=True)

        return stats

    def process_batch(self, native_ids: List[str], reindex: bool = False) -> Dict[str, Any]:
        """
        Process a batch of documents with optional parallelization.

        Args:
            native_ids: List of native IDs to process
            reindex: Force reindexing even if documents exist

        Returns:
            Dictionary with batch processing statistics
        """
        batch_stats = {
            "total": len(native_ids),
            "processed": 0,
            "successful": 0,
            "failed": 0,
            "skipped": 0,
            "total_chunks": 0,
            "total_attachments": 0,
            "total_attachment_chunks": 0,
            "errors": [],
        }

        logger.info(f"Processing batch of {len(native_ids)} documents")

        max_workers = settings.MAX_WORKERS_INGESTION

        if max_workers > 1:
            # Parallel processing with ThreadPoolExecutor
            logger.info(f"Using {max_workers} workers for parallel document processing")

            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                # Submit all document processing tasks
                future_to_id = {
                    executor.submit(self.process_document, native_id, reindex): native_id
                    for native_id in native_ids
                }

                # Collect results as they complete
                for future in as_completed(future_to_id):
                    native_id = future_to_id[future]
                    try:
                        doc_stats = future.result()
                        self._update_batch_stats(batch_stats, doc_stats, native_id)
                    except Exception as e:
                        logger.error(
                            f"Exception processing document {native_id}: {e}", exc_info=True
                        )
                        batch_stats["processed"] += 1
                        batch_stats["failed"] += 1
                        batch_stats["errors"].append({"native_id": native_id, "error": str(e)})
        else:
            # Serial processing (original behavior)
            logger.info("Using serial document processing (MAX_WORKERS_INGESTION=1)")
            for native_id in native_ids:
                try:
                    doc_stats = self.process_document(native_id, reindex)
                    self._update_batch_stats(batch_stats, doc_stats, native_id)
                except Exception as e:
                    logger.error(f"Exception processing document {native_id}: {e}", exc_info=True)
                    batch_stats["processed"] += 1
                    batch_stats["failed"] += 1
                    batch_stats["errors"].append({"native_id": native_id, "error": str(e)})

        logger.info(
            f"Batch processing complete: "
            f"{batch_stats['successful']} successful, "
            f"{batch_stats['failed']} failed, "
            f"{batch_stats['skipped']} skipped, "
            f"{batch_stats['total_chunks']} decision chunks, "
            f"{batch_stats['total_attachments']} attachments, "
            f"{batch_stats['total_attachment_chunks']} attachment chunks"
        )

        return batch_stats

    def _update_batch_stats(
        self, batch_stats: Dict[str, Any], doc_stats: Dict[str, Any], native_id: str
    ) -> None:
        """
        Update batch statistics with document processing results.
        Thread-safe method for updating shared batch_stats.

        Args:
            batch_stats: Batch statistics dictionary to update
            doc_stats: Document processing statistics
            native_id: Native ID of the document
        """
        with self._lock:
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
                if doc_stats.get("error"):
                    batch_stats["errors"].append(
                        {"native_id": native_id, "error": doc_stats["error"]}
                    )

    def process_attachments(self, decision) -> Dict[str, Any]:
        """
        Process all attachments for a decision with optional parallelization.

        Args:
            decision: Decision document object

        Returns:
            Dictionary with attachment processing statistics
        """
        stats = {
            "attachments_processed": 0,
            "chunks_created": 0,
            "chunks_indexed": 0,
            "failed": 0,
        }

        if not decision.Attachments:
            logger.debug(f"No attachments for decision {decision.NativeId}")
            return stats

        # Get temporary directory for downloads
        temp_dir = Path(settings.ATTACHMENT_DOWNLOAD_DIR) / decision.NativeId
        temp_dir.mkdir(parents=True, exist_ok=True)

        # If there's attachments without NativeId, we generate one
        attachment_number = 0
        for attachment in decision.Attachments:
            attachment_number += 1
            if not attachment.NativeId:
                attachment.NativeId = f"att_{attachment.AttachmentNumber or attachment_number}"

        try:
            # Download attachments
            downloaded = self.attachment_downloader.download_attachments(
                decision.Attachments, temp_dir, decision.NativeId
            )

            if not downloaded:
                logger.info(f"No attachments downloaded for decision {decision.NativeId}")
                return stats

            max_workers = settings.MAX_WORKERS_ATTACHMENT_PROCESSING

            if max_workers > 1 and len(downloaded) > 1:
                # Parallel attachment processing
                logger.debug(f"Using {max_workers} workers for parallel attachment processing")

                # Prepare attachment processing tasks
                attachment_tasks = []
                for attachment in decision.Attachments:
                    native_id = attachment.NativeId
                    if native_id in downloaded:
                        attachment_tasks.append((attachment, downloaded[native_id]))

                with ThreadPoolExecutor(max_workers=max_workers) as executor:
                    # Submit all attachment processing tasks
                    future_to_attachment = {
                        executor.submit(
                            self._process_single_attachment, decision, attachment, file_path
                        ): attachment.NativeId
                        for attachment, file_path in attachment_tasks
                    }

                    # Collect results as they complete
                    for future in as_completed(future_to_attachment):
                        logger.info(
                            f"Attachment processing task completed: {future_to_attachment[future]}"
                        )
                        att_native_id = future_to_attachment[future]
                        try:
                            att_stats = future.result()
                            with self._lock:  # Thread-safe stats update
                                stats["chunks_created"] += att_stats.get("chunks_created", 0)
                                stats["chunks_indexed"] += att_stats.get("chunks_indexed", 0)
                                if att_stats.get("success"):
                                    logger.info(
                                        f"Attachment {att_native_id} processed successfully"
                                    )
                                    stats["attachments_processed"] += 1
                                else:
                                    logger.info(f"Attachment {att_native_id} processing failed")
                                    stats["failed"] += 1
                        except Exception as e:
                            logger.error(
                                f"Exception processing attachment {att_native_id}: {e}",
                                exc_info=True,
                            )
                            with self._lock:
                                stats["failed"] += 1

                # ThreadPoolExecutor context has exited - all threads are complete
                logger.debug(
                    f"All attachment processing threads completed for decision {decision.NativeId}"
                )
            else:
                # Serial attachment processing (original behavior)
                logger.debug("Using serial attachment processing")
                for attachment in decision.Attachments:
                    native_id = attachment.NativeId

                    if native_id not in downloaded:
                        continue

                    file_path = downloaded[native_id]

                    try:
                        att_stats = self._process_single_attachment(decision, attachment, file_path)
                        stats["chunks_created"] += att_stats.get("chunks_created", 0)
                        stats["chunks_indexed"] += att_stats.get("chunks_indexed", 0)
                        if att_stats.get("success"):
                            logger.info(f"Attachment {native_id} processed successfully")
                            stats["attachments_processed"] += 1
                        else:
                            logger.info(f"Attachment {native_id} processing failed")
                            stats["failed"] += 1
                    except Exception as e:
                        logger.error(
                            f"Error processing attachment {native_id} for decision {decision.NativeId}: {e}",
                            exc_info=True,
                        )
                        stats["failed"] += 1

            # All processing complete - safe to proceed to cleanup

        except Exception as e:
            logger.error(
                f"Error in attachment processing for decision {decision.NativeId}: {e}",
                exc_info=True,
            )
            # Continue to cleanup even on error

        finally:
            # Clean up temporary files - all threads have completed at this point
            try:

                if temp_dir.exists():
                    shutil.rmtree(temp_dir)
                    logger.info(f"Cleaned up temporary directory: {temp_dir}")
            except Exception as e:
                logger.warning(f"Failed to clean up temporary directory {temp_dir}: {e}")

        logger.info(
            f"Attachment processing complete for decision {decision.NativeId}: "
            f"{stats['attachments_processed']} processed, "
            f"{stats['chunks_indexed']} chunks indexed, "
            f"{stats['failed']} failed"
        )

        return stats

    def _process_single_attachment(
        self, decision, attachment: Attachment, file_path: Path
    ) -> Dict[str, Any]:
        """
        Process a single attachment (convert, chunk, embed, index).

        Args:
            decision: Decision document object
            attachment: Attachment object
            file_path: Path to downloaded attachment file

        Returns:
            Dictionary with processing statistics
        """
        stats = {
            "success": False,
            "chunks_created": 0,
            "chunks_indexed": 0,
        }

        # Record time for processing
        start_time = datetime.now()

        native_id = attachment.NativeId

        try:
            # Convert to markdown
            logger.info(f"Converting attachment {native_id} to Markdown")
            markdown_text = convert_attachment_content(file_path)

            if not markdown_text or not markdown_text.strip():
                logger.warning(f"Attachment {native_id} conversion resulted in empty text")
                return stats

            # Prepare metadata
            metadata = self._extract_attachment_metadata(decision, attachment)

            # Chunk text
            logger.info(f"Chunking attachment {native_id}")
            # Use a unique chunk ID prefix for attachments
            chunk_native_id = f"{decision.NativeId}_att_{native_id}"
            chunks = self.chunker.chunk_text(
                text=markdown_text, native_id=chunk_native_id, metadata=metadata
            )

            if not chunks:
                logger.warning(f"No chunks created for attachment {native_id}")
                return stats

            stats["chunks_created"] = len(chunks)

            # Generate embeddings
            logger.info(f"Generating embeddings for {len(chunks)} attachment chunks")
            chunk_dicts = [
                {
                    "chunk_id": chunk.chunk_id,
                    "text": chunk.text,
                    "native_id": chunk.native_id,
                }
                for chunk in chunks
            ]

            embedding_results = self.embedder.create_embeddings(chunk_dicts)

            if not embedding_results:
                logger.error(f"Failed to generate embeddings for attachment {native_id}")
                return stats

            # Prepare chunks with embeddings
            chunks_with_embeddings = []
            embedding_map = {er.chunk_id: er.embedding for er in embedding_results}

            for chunk in chunks:
                if chunk.chunk_id in embedding_map:
                    chunks_with_embeddings.append(
                        {
                            "chunk_id": chunk.chunk_id,
                            "native_id": chunk.native_id,
                            "chunk_index": chunk.chunk_index,
                            "text": chunk.text,
                            "embedding": embedding_map[chunk.chunk_id],
                            "token_count": chunk.token_count,
                            "chunk_position": chunk.chunk_index,
                            "metadata": chunk.metadata,
                        }
                    )

            # Index to Elasticsearch
            logger.info(
                f"Indexing {len(chunks_with_embeddings)} attachment chunks to Elasticsearch"
            )
            index_result = self.vector_store.bulk_index_chunks(chunks_with_embeddings)

            stats["chunks_indexed"] = index_result["success"]
            stats["success"] = index_result["success"] > 0

            if index_result["failed"] > 0:
                logger.warning(f"{index_result['failed']} attachment chunks failed to index")

            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()

            logger.info(f"Attachment {native_id} processing time: {duration:.2f} seconds")

            logger.info(
                f"Processed attachment {native_id} for decision {decision.NativeId}: "
                f"{len(chunks)} chunks created, {index_result['success']} chunks indexed"
            )

        except Exception as e:
            logger.error(f"Error in _process_single_attachment for {native_id}: {e}", exc_info=True)
            # Return stats as is (success=False)

        return stats

    def _extract_attachment_metadata(self, decision, attachment: Attachment) -> Dict[str, Any]:
        """
        Extract metadata for attachment chunks.

        Args:
            decision: Decision document object
            attachment: Attachment object

        Returns:
            Dictionary of metadata including both decision and attachment info
        """
        # Start with decision metadata
        metadata = self._extract_metadata(decision)

        # Add attachment-specific metadata
        attachment_native_id = attachment.NativeId or f"att_{attachment.AttachmentNumber}"
        metadata.update(
            {
                "is_attachment": True,
                "attachment_native_id": attachment_native_id,
                "attachment_title": attachment.Title or "",
                "attachment_number": attachment.AttachmentNumber or 0,
                "attachment_type": attachment.Type or "",
                "attachment_url": attachment.FileURI or "",
                "decision_native_id": decision.NativeId,
            }
        )

        return metadata

    def _extract_metadata(self, decision) -> Dict[str, Any]:
        """
        Extract metadata from decision document.

        Args:
            decision: Decision document object

        Returns:
            Dictionary of metadata with Open WebUI required fields
        """
        # Open WebUI standard metadata
        metadata = {
            "collection_name": "Helsinki Decisions",
            "source": f"{decision.Title}.md" if decision.Title else "Unknown Source",
            "name": f"{decision.Title}.md" if decision.Title else "Unknown Source",
            "file_id": decision.NativeId or "",
            # Decision-specific metadata
            "native_id": decision.NativeId or "",
            "title": decision.Title or "",
            "case_id": decision.CaseID or "",
            "section": decision.Section or "",
            "classification_code": decision.ClassificationCode or "",
            "classification_title": decision.ClassificationTitle or "",
        }

        # Add date if available
        if decision.DateDecision:
            try:
                # Parse and format date
                if isinstance(decision.DateDecision, str):
                    # Assuming ISO format
                    metadata["date_decision"] = decision.DateDecision
                else:
                    metadata["date_decision"] = decision.DateDecision.isoformat()
            except Exception as e:
                logger.warning(f"Error parsing date: {e}")

        # Add organization info if available
        if hasattr(decision, "Organization") and decision.Organization:
            if hasattr(decision.Organization, "Name"):
                metadata["organization_name"] = decision.Organization.Name or ""

        return metadata
