"""
Text chunking service for breaking down documents into embeddable chunks.

The ParagraphChunker class implements a paragraph-based chunking strategy with token monitoring and metadata embedding. It uses tiktoken for accurate token counting and supports configurable chunk sizes, overlaps, and metadata headers.
"""

from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import tiktoken

from app.core import get_logger

logger = get_logger(__name__)


@dataclass
class DocumentChunk:
    """Represents a chunk of a document with metadata."""

    chunk_id: str
    native_id: str
    chunk_index: int
    text: str
    token_count: int
    metadata: Dict[str, Any]


class ParagraphChunker:
    """
    Service for chunking text into paragraph-based chunks with token monitoring.

    Strategy:
    - Split on paragraph boundaries (double newlines)
    - Target: 500-1500 tokens per chunk
    - Overlap: 10-20% (50-200 tokens)
    - Use tiktoken for accurate token counting
    """

    def __init__(
        self,
        target_tokens: int = 1000,
        min_tokens: int = 500,
        max_tokens: int = 1500,
        overlap_tokens: int = 100,
        encoding_name: str = "cl100k_base",
        header_overhead_tokens: int = 250,
        embed_metadata: bool = True,
    ):
        """
        Initialize paragraph chunker.

        Args:
            target_tokens: Target number of tokens per chunk (default: 750)
            min_tokens: Minimum tokens per chunk (default: 500)
            max_tokens: Maximum tokens per chunk (default: 1000)
            overlap_tokens: Number of tokens to overlap between chunks (default: 100)
            encoding_name: Tiktoken encoding to use (default: cl100k_base for GPT-4/3.5)
            header_overhead_tokens: Expected token overhead for metadata headers (default: 75)
            embed_metadata: Whether to embed metadata in chunk text (default: True)
        """
        self.target_tokens = target_tokens
        self.min_tokens = min_tokens
        self.max_tokens = max_tokens
        self.overlap_tokens = overlap_tokens
        self.header_overhead_tokens = header_overhead_tokens
        self.embed_metadata = embed_metadata

        try:
            self.encoding = tiktoken.get_encoding(encoding_name)
        except Exception as e:
            logger.error(f"Error loading tiktoken encoding {encoding_name}: {e}")
            raise

        logger.info(
            f"Initialized ParagraphChunker with target={target_tokens}, "
            f"range=[{min_tokens}, {max_tokens}], overlap={overlap_tokens}, "
            f"header_overhead={header_overhead_tokens}, embed_metadata={embed_metadata}"
        )

    def count_tokens(self, text: str) -> int:
        """
        Count tokens in text using tiktoken.

        Args:
            text: Text to count tokens for

        Returns:
            Number of tokens
        """
        if not text:
            return 0
        return len(self.encoding.encode(text))

    def chunk_text(
        self, text: str, native_id: str, metadata: Optional[Dict[str, Any]] = None
    ) -> List[DocumentChunk]:
        """
        Chunk text into paragraph-based chunks.

        Args:
            text: Text to chunk
            native_id: Native ID of the document
            metadata: Additional metadata to attach to chunks

        Returns:
            List of DocumentChunk objects
        """
        if not text or not text.strip():
            logger.warning(f"Empty text provided for chunking: {native_id}")
            return []

        # Solution 2: Add size limit for text processing
        MAX_TEXT_LENGTH = 1_000_000  # 1M characters
        if len(text) > MAX_TEXT_LENGTH:
            logger.warning(
                f"[{native_id}] Text too large ({len(text)} chars), truncating to {MAX_TEXT_LENGTH}"
            )
            text = text[:MAX_TEXT_LENGTH]

        metadata = metadata or {}

        # Split into paragraphs
        paragraphs = self._split_paragraphs(text)

        # Group paragraphs into chunks
        chunks = self._create_chunks(paragraphs, native_id, metadata)

        logger.info(
            f"Chunked document {native_id}: {len(paragraphs)} paragraphs -> {len(chunks)} chunks"
        )

        # Log chunk size distribution
        if chunks:
            token_counts = [chunk.token_count for chunk in chunks]
            avg_tokens = sum(token_counts) / len(token_counts)
            logger.debug(
                f"Chunk distribution for {native_id}: "
                f"avg={avg_tokens:.0f}, min={min(token_counts)}, max={max(token_counts)}"
            )

        return chunks

    def _split_paragraphs(self, text: str) -> List[str]:
        """
        Split text into paragraphs.

        Args:
            text: Text to split

        Returns:
            List of paragraph strings
        """
        # Split on double newlines
        paragraphs = text.split("\n\n")

        # Clean and filter empty paragraphs
        paragraphs = [p.strip() for p in paragraphs if p.strip()]

        return paragraphs

    def _create_chunks(
        self, paragraphs: List[str], native_id: str, metadata: Dict[str, Any]
    ) -> List[DocumentChunk]:
        """
        Create chunks from paragraphs.

        Args:
            paragraphs: List of paragraph strings
            native_id: Native ID of the document
            metadata: Document metadata

        Returns:
            List of DocumentChunk objects
        """
        chunks: List[DocumentChunk] = []
        current_chunk_text = ""
        current_chunk_tokens = 0
        chunk_index = 0

        # Calculate effective token limits accounting for header overhead
        effective_max_tokens = self.max_tokens
        effective_target_tokens = self.target_tokens
        effective_min_tokens = self.min_tokens

        if self.embed_metadata:
            # Reserve space for metadata headers
            effective_max_tokens = max(
                self.max_tokens - self.header_overhead_tokens, self.min_tokens
            )
            effective_target_tokens = max(
                self.target_tokens - self.header_overhead_tokens, self.min_tokens // 2
            )
            effective_min_tokens = max(self.min_tokens - self.header_overhead_tokens, 50)

        for i, paragraph in enumerate(paragraphs):
            paragraph_tokens = self.count_tokens(paragraph)

            # If single paragraph exceeds max tokens, split it
            if paragraph_tokens > effective_max_tokens:
                # First, save current chunk if any
                if current_chunk_text:
                    chunks.append(
                        self._create_chunk(
                            current_chunk_text,
                            native_id,
                            chunk_index,
                            current_chunk_tokens,
                            metadata,
                        )
                    )
                    chunk_index += 1
                    current_chunk_text = ""
                    current_chunk_tokens = 0

                # Split large paragraph into smaller chunks
                large_para_chunks = self._split_large_paragraph(
                    paragraph, native_id, chunk_index, metadata, effective_max_tokens
                )
                chunks.extend(large_para_chunks)
                chunk_index += len(large_para_chunks)
                continue

            # Check if adding paragraph would exceed max tokens
            potential_tokens = current_chunk_tokens + paragraph_tokens

            if potential_tokens > effective_max_tokens and current_chunk_text:
                # Save current chunk
                chunks.append(
                    self._create_chunk(
                        current_chunk_text, native_id, chunk_index, current_chunk_tokens, metadata
                    )
                )
                chunk_index += 1

                # Add overlap from previous chunk
                overlap_text = self._get_overlap_text(current_chunk_text)
                current_chunk_text = (
                    overlap_text + "\n\n" + paragraph if overlap_text else paragraph
                )
                current_chunk_tokens = self.count_tokens(current_chunk_text)

            elif potential_tokens >= effective_min_tokens or i == len(paragraphs) - 1:
                # Add paragraph to current chunk
                if current_chunk_text:
                    current_chunk_text += "\n\n" + paragraph
                else:
                    current_chunk_text = paragraph
                current_chunk_tokens = self.count_tokens(current_chunk_text)

                # If we've reached target size or this is the last paragraph, save chunk
                if current_chunk_tokens >= effective_target_tokens or i == len(paragraphs) - 1:
                    chunks.append(
                        self._create_chunk(
                            current_chunk_text,
                            native_id,
                            chunk_index,
                            current_chunk_tokens,
                            metadata,
                        )
                    )
                    chunk_index += 1

                    # Prepare next chunk with overlap
                    if i < len(paragraphs) - 1:
                        overlap_text = self._get_overlap_text(current_chunk_text)
                        current_chunk_text = overlap_text
                        current_chunk_tokens = (
                            self.count_tokens(current_chunk_text) if overlap_text else 0
                        )
                    else:
                        current_chunk_text = ""
                        current_chunk_tokens = 0
            else:
                # Accumulate paragraphs until we reach minimum size
                if current_chunk_text:
                    current_chunk_text += "\n\n" + paragraph
                else:
                    current_chunk_text = paragraph
                current_chunk_tokens = self.count_tokens(current_chunk_text)

        # Save any remaining text as final chunk
        if current_chunk_text and self.count_tokens(current_chunk_text) > 0:
            chunks.append(
                self._create_chunk(
                    current_chunk_text, native_id, chunk_index, current_chunk_tokens, metadata
                )
            )

        return chunks

    def _split_large_paragraph(
        self,
        paragraph: str,
        native_id: str,
        start_index: int,
        metadata: Dict[str, Any],
        max_tokens: Optional[int] = None,
    ) -> List[DocumentChunk]:
        """
        Split a large paragraph that exceeds max tokens.

        Args:
            paragraph: Large paragraph to split
            native_id: Native ID of the document
            start_index: Starting chunk index
            metadata: Document metadata
            max_tokens: Optional maximum token limit (uses self.max_tokens if None)

        Returns:
            List of DocumentChunk objects
        """
        chunks: List[DocumentChunk] = []

        # Use provided max_tokens or default
        if max_tokens is None:
            max_tokens = self.max_tokens

        # Solution 2: Add size limit for paragraph processing
        MAX_PARAGRAPH_LENGTH = 100_000
        if len(paragraph) > MAX_PARAGRAPH_LENGTH:
            logger.warning(
                f"[{native_id}] Paragraph too large ({len(paragraph)} chars), using token-based splitting only"
            )
            return self._split_by_tokens(paragraph, native_id, start_index, metadata)

        # Split on sentences (basic sentence splitting)
        sentences = self._split_sentences(paragraph)

        current_text = ""
        current_tokens = 0
        chunk_index = start_index

        for sentence in sentences:
            sentence_tokens = self.count_tokens(sentence)

            # If single sentence is too large, split by tokens directly
            if sentence_tokens > max_tokens:
                if current_text:
                    chunks.append(
                        self._create_chunk(
                            current_text, native_id, chunk_index, current_tokens, metadata
                        )
                    )
                    chunk_index += 1

                # Split sentence by tokens
                token_chunks = self._split_by_tokens(
                    sentence, native_id, chunk_index, metadata, max_tokens
                )
                chunks.extend(token_chunks)
                chunk_index += len(token_chunks)
                current_text = ""
                current_tokens = 0
                continue

            potential_tokens = current_tokens + sentence_tokens

            if potential_tokens > max_tokens and current_text:
                chunks.append(
                    self._create_chunk(
                        current_text, native_id, chunk_index, current_tokens, metadata
                    )
                )
                chunk_index += 1

                overlap_text = self._get_overlap_text(current_text)
                current_text = overlap_text + " " + sentence if overlap_text else sentence
                current_tokens = self.count_tokens(current_text)
            else:
                if current_text:
                    current_text += " " + sentence
                else:
                    current_text = sentence
                current_tokens = self.count_tokens(current_text)

        if current_text:
            chunks.append(
                self._create_chunk(current_text, native_id, chunk_index, current_tokens, metadata)
            )

        return chunks

    def _split_sentences(self, text: str) -> List[str]:
        """
        Simple sentence splitter without regex (Solution 3: faster alternative).

        Args:
            text: Text to split into sentences

        Returns:
            List of sentences
        """
        sentences = []
        current = []

        # Simple character-by-character parsing
        for i, char in enumerate(text):
            current.append(char)
            if char in ".!?" and (i == len(text) - 1 or text[i + 1] in " \n\t"):
                sentences.append("".join(current).strip())
                current = []

        # Add any remaining text
        if current:
            sentences.append("".join(current).strip())

        # Filter empty sentences and return
        result = [s for s in sentences if s]
        return result if result else [text]

    def _split_by_tokens(
        self,
        text: str,
        native_id: str,
        start_index: int,
        metadata: Dict[str, Any],
        max_tokens: Optional[int] = None,
    ) -> List[DocumentChunk]:
        """
        Split text by token count when it's too large.

        Args:
            text: Text to split
            native_id: Native ID of the document
            start_index: Starting chunk index
            metadata: Document metadata
            max_tokens: Optional maximum token limit (uses self.max_tokens if None)

        Returns:
            List of DocumentChunk objects
        """
        chunks: List[DocumentChunk] = []
        tokens = self.encoding.encode(text)

        # Use provided max_tokens or default
        if max_tokens is None:
            max_tokens = self.max_tokens

        chunk_index = start_index
        start = 0

        # Solution 1: Add safety limit to prevent infinite loops
        MAX_CHUNKS = 1000  # Safety limit
        chunk_count = 0

        while start < len(tokens) and chunk_count < MAX_CHUNKS:
            end = min(start + max_tokens, len(tokens))
            chunk_tokens = tokens[start:end]
            chunk_text = self.encoding.decode(chunk_tokens)

            chunks.append(
                self._create_chunk(chunk_text, native_id, chunk_index, len(chunk_tokens), metadata)
            )

            chunk_index += 1

            # Solution 1: Ensure we always make progress
            if end - self.overlap_tokens <= start:
                logger.warning(
                    f"[{native_id}] Overlap too large, adjusting: overlap={self.overlap_tokens}, max={max_tokens}"
                )
                start = end  # Skip overlap if it would cause infinite loop
            else:
                start = end - self.overlap_tokens  # Add overlap

            chunk_count += 1

        if chunk_count >= MAX_CHUNKS:
            logger.error(
                f"[{native_id}] Hit maximum chunk limit of {MAX_CHUNKS}, text may be truncated"
            )

        return chunks

    def _get_overlap_text(self, text: str) -> str:
        """
        Get overlap text from the end of a chunk.

        Args:
            text: Text to get overlap from

        Returns:
            Overlap text
        """
        if not text:
            return ""

        tokens = self.encoding.encode(text)

        if len(tokens) <= self.overlap_tokens:
            return text

        overlap_tokens = tokens[-self.overlap_tokens :]
        return self.encoding.decode(overlap_tokens)

    def _generate_metadata_header(
        self, metadata: Dict[str, Any], is_attachment: bool = False
    ) -> str:
        """
        Generate formatted metadata header for chunk text.

        Args:
            metadata: Document metadata dictionary
            is_attachment: Whether this is an attachment chunk

        Returns:
            Formatted metadata header string in Finnish
        """
        if not self.embed_metadata:
            return ""

        header_lines = ["--- Dokumentin konteksti ---"]

        # Decision title and ID (always include)
        title = metadata.get("title", "")
        native_id = metadata.get("native_id", "")
        if title and native_id:
            header_lines.append(f'Päätös: "{title}" (ID: {native_id})')
        elif title:
            header_lines.append(f'Päätös: "{title}"')
        elif native_id:
            header_lines.append(f"Päätös ID: {native_id}")

        # Attachment information (for attachment chunks)
        if is_attachment:
            attachment_title = metadata.get("attachment_title", "")
            attachment_url = metadata.get("attachment_url", "")
            if attachment_title and attachment_url:
                header_lines.append(f'Liite: "{attachment_title}" ({attachment_url})')
            elif attachment_title:
                header_lines.append(f'Liite: "{attachment_title}"')
            elif attachment_url:
                header_lines.append(f"Liite: {attachment_url}")

            # Parent decision reference for attachments
            decision_native_id = metadata.get("decision_native_id", "")
            if decision_native_id and decision_native_id != native_id:
                header_lines.append(f"Päätöksen ID: {decision_native_id}")

        # Classification information
        classification_title = metadata.get("classification_title", "")
        classification_code = metadata.get("classification_code", "")
        if classification_title and classification_code:
            header_lines.append(f"Luokitus: {classification_title} ({classification_code})")
        elif classification_title:
            header_lines.append(f"Luokitus: {classification_title}")

        # Organization
        organization_name = metadata.get("organization_name", "")
        if organization_name:
            header_lines.append(f"Organisaatio: {organization_name}")

        # Decision date
        date_decision = metadata.get("date_decision", "")
        if date_decision:
            # Format date if it's ISO format
            try:
                if "T" in date_decision:
                    date_part = date_decision.split("T")[0]
                    header_lines.append(f"Päivämäärä: {date_part}")
                else:
                    header_lines.append(f"Päivämäärä: {date_decision}")
            except Exception:
                header_lines.append(f"Päivämäärä: {date_decision}")

        # Case ID (for cross-referencing)
        case_id = metadata.get("case_id", "")
        if case_id:
            header_lines.append(f"Diaarinumero: {case_id}")

        # Section information
        section = metadata.get("section", "")
        if section:
            header_lines.append(f"Pykälä: {section}")

        header_lines.append("---")

        header_text = "\n".join(header_lines)

        # Log warning if header is unexpectedly large
        header_tokens = self.count_tokens(header_text)
        if header_tokens > self.header_overhead_tokens:
            logger.warning(
                f"Metadata header exceeds expected overhead: {header_tokens} > {self.header_overhead_tokens}"
            )

        return header_text

    def _create_chunk(
        self,
        text: str,
        native_id: str,
        chunk_index: int,
        token_count: int,
        metadata: Dict[str, Any],
    ) -> DocumentChunk:
        """
        Create a DocumentChunk object with optional metadata header.

        Args:
            text: Chunk text
            native_id: Native ID of the document
            chunk_index: Index of the chunk in the document
            token_count: Number of tokens in the chunk
            metadata: Document metadata

        Returns:
            DocumentChunk object
        """
        chunk_id = f"{native_id}_chunk_{chunk_index}"

        # Generate and prepend metadata header if enabled
        if self.embed_metadata:
            is_attachment = metadata.get("is_attachment", False)
            header = self._generate_metadata_header(metadata, is_attachment)

            if header:
                # Prepend header to text
                enhanced_text = header + "\n\n" + text
                # Recalculate token count with header
                token_count = self.count_tokens(enhanced_text)
                text = enhanced_text

        # Add chunking metadata
        chunk_metadata = {**metadata, "chunk_position": chunk_index, "total_tokens": token_count}

        return DocumentChunk(
            chunk_id=chunk_id,
            native_id=native_id,
            chunk_index=chunk_index,
            text=text,
            token_count=token_count,
            metadata=chunk_metadata,
        )
