"""
Embedding generation service using Azure OpenAI and implementing processing embeddings in batches with retry logic.
"""

import time
from dataclasses import dataclass
from typing import Any, Dict, List

from openai import AzureOpenAI
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from app.core import get_logger, settings

logger = get_logger(__name__)


@dataclass
class EmbeddingResult:
    """Result of an embedding generation."""

    chunk_id: str
    embedding: List[float]
    model: str
    tokens_used: int


class AzureEmbedder:
    """
    Service for generating embeddings using Azure OpenAI.

    Configuration:
    - Model: text-embedding-3-large
    - Batch size: 16 embeddings per request
    - Embedding dimension: 3072
    - Rate limiting and retry logic
    """

    def __init__(
        self,
        endpoint: str = None,
        api_key: str = None,
        api_version: str = None,
        model: str = None,
        dimension: int = None,
        batch_size: int = None,
        max_retries: int = 3,
    ):
        """
        Initialize Azure OpenAI embedder.

        Args:
            endpoint: Azure OpenAI endpoint URL
            api_key: Azure OpenAI API key
            api_version: Azure OpenAI API version
            model: Embedding model name (default: text-embedding-3-large)
            dimension: Dimension of the embeddings
            batch_size: Number of texts to embed per API request (default from settings)
            max_retries: Maximum number of retries on failure
        """
        self.endpoint = endpoint or getattr(settings, "AZURE_OPENAI_ENDPOINT", "")
        self.api_key = api_key or getattr(settings, "AZURE_OPENAI_API_KEY", "")
        self.api_version = api_version or getattr(
            settings, "AZURE_OPENAI_API_VERSION", "2024-02-01"
        )
        self.model = model or getattr(
            settings, "AZURE_OPENAI_EMBEDDING_MODEL", "text-embedding-3-large"
        )
        self.dimension = dimension or getattr(settings, "EMBEDDING_DIMENSION", 3072)
        # Use configured batch size from settings, default to 100
        self.batch_size = batch_size or getattr(settings, "EMBEDDING_BATCH_SIZE", 100)
        self.max_retries = max_retries

        # Validate configuration
        if not self.endpoint or not self.api_key:
            raise ValueError("Azure OpenAI endpoint and API key are required")

        # Initialize Azure OpenAI client
        try:
            self.client = AzureOpenAI(
                azure_endpoint=self.endpoint,
                api_key=self.api_key,
                api_version=self.api_version,
                timeout=60.0,  # Add explicit timeout
                max_retries=0,  # Disable internal retries, rely on tenacity
            )
            logger.info(
                f"Initialized AzureEmbedder with model={self.model}, batch_size={self.batch_size}"
            )
        except Exception as e:
            logger.error(f"Error initializing Azure OpenAI client: {e}")
            raise

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=60),
        retry=retry_if_exception_type((Exception,)),
        reraise=True,
    )
    def _create_embeddings_batch(self, texts: List[str]) -> List[List[float]]:
        """
        Create embeddings for a batch of texts with retry logic.

        Args:
            texts: List of text strings to embed

        Returns:
            List of embedding vectors

        Raises:
            Exception: If embedding generation fails after retries
        """
        try:
            response = self.client.embeddings.create(input=texts, model=self.model, dimensions=self.dimension)

            # Extract embeddings in order
            embeddings = [item.embedding for item in response.data]

            logger.debug(
                f"Generated {len(embeddings)} embeddings, tokens used: {response.usage.total_tokens}"
            )

            return embeddings

        except Exception as e:
            logger.error(f"Error generating embeddings: {e}")
            raise

    def create_embeddings(self, chunks: List[Dict[str, Any]]) -> List[EmbeddingResult]:
        """
        Generate embeddings for a list of document chunks.

        Args:
            chunks: List of chunk dictionaries with 'chunk_id' and 'text' keys

        Returns:
            List of EmbeddingResult objects
        """
        if not chunks:
            logger.warning("No chunks provided for embedding generation")
            return []

        results: List[EmbeddingResult] = []
        total_chunks = len(chunks)
        total_tokens = 0

        logger.info(
            f"Generating embeddings for {total_chunks} chunks in batches of {self.batch_size}"
        )

        # Process in batches
        for i in range(0, total_chunks, self.batch_size):
            batch = chunks[i : i + self.batch_size]
            batch_texts = [chunk["text"] for chunk in batch]
            batch_ids = [chunk["chunk_id"] for chunk in batch]

            try:
                # Generate embeddings for batch
                embeddings = self._create_embeddings_batch(batch_texts)

                # Create EmbeddingResult objects
                for chunk_id, embedding, text in zip(batch_ids, embeddings, batch_texts, strict=False):
                    # Estimate tokens (rough approximation)
                    tokens_used = len(text.split()) * 1.3  # Rough estimate

                    results.append(
                        EmbeddingResult(
                            chunk_id=chunk_id,
                            embedding=embedding,
                            model=self.model,
                            tokens_used=int(tokens_used),
                        )
                    )
                    total_tokens += tokens_used

                logger.info(
                    f"Processed batch {i // self.batch_size + 1}/{(total_chunks + self.batch_size - 1) // self.batch_size}: "
                    f"{len(batch)} chunks"
                )

                # Rate limiting: small delay between batches
                if i + self.batch_size < total_chunks:
                    time.sleep(0.1)

            except Exception as e:
                logger.error(f"Error processing batch {i // self.batch_size + 1}: {e}")
                # Continue with next batch instead of failing completely
                continue

        logger.info(
            f"Embedding generation complete: {len(results)}/{total_chunks} successful, "
            f"~{total_tokens:.0f} tokens used"
        )

        return results

    def create_embedding(self, text: str) -> List[float]:
        """
        Generate embedding for a single text.

        Args:
            text: Text to embed

        Returns:
            Embedding vector
        """
        if not text or not text.strip():
            logger.warning("Empty text provided for embedding generation")
            return []

        try:
            embeddings = self._create_embeddings_batch([text])
            return embeddings[0] if embeddings else []
        except Exception as e:
            logger.error(f"Error generating single embedding: {e}")
            return []

