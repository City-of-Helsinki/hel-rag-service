"""
Attachment downloader service for fetching decision attachments.

Attachments are downloaded from public URLs provided in the decision documents, with filtering based on publicity and personal data criteria. The service includes rate limiting, retry logic, and optional parallelization for efficient downloading of multiple attachments.
"""

import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Dict, List, Optional

import httpx
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from app.core import get_logger, settings
from app.schemas.decision import Attachment

logger = get_logger(__name__)


class AttachmentDownloader:
    """Service for downloading decision attachments from public URLs."""

    def __init__(
        self,
        timeout: int = None,
        rate_limit: float = None,
    ):
        """
        Initialize attachment downloader.

        Args:
            timeout: HTTP request timeout in seconds
            rate_limit: Requests per second limit
        """
        self.timeout = timeout or getattr(settings, "ATTACHMENT_TIMEOUT", 60)
        if rate_limit is not None and rate_limit <= 0:
            raise ValueError("Rate limit must be a positive number")
        if getattr(settings, "REQUESTS_PER_SECOND", 5.0) <= 0:
            raise ValueError("REQUESTS_PER_SECOND must be greater than 0")
        self.rate_limit_delay = 1.0 / (rate_limit or getattr(settings, "REQUESTS_PER_SECOND", 5.0))
        self.last_request_time = 0
        self._rate_limit_lock = threading.Lock()  # Thread-safe rate limiting

        self.client = httpx.Client(timeout=self.timeout, follow_redirects=True)

        logger.info(
            f"Initialized AttachmentDownloader with timeout={self.timeout}s, "
            f"rate_limit={1.0/self.rate_limit_delay:.1f} req/s"
        )

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()

    def close(self):
        """Close the HTTP client."""
        if self.client:
            self.client.close()

    def _rate_limit(self):
        """Apply rate limiting between requests. Thread-safe."""
        with self._rate_limit_lock:
            current_time = time.time()
            time_since_last = current_time - self.last_request_time
            if time_since_last < self.rate_limit_delay:
                sleep_time = self.rate_limit_delay - time_since_last
                time.sleep(sleep_time)
            self.last_request_time = time.time()

    def should_fetch_attachment(self, attachment: Attachment) -> bool:
        """
        Determine if an attachment should be fetched.

        Filters attachments based on:
        - PublicityClass must be "Julkinen" (public)
        - PersonalData must be "Ei sisällä henkilötietoja" (no personal data)
        - FileURI must be present

        Args:
            attachment: Attachment object to check

        Returns:
            True if attachment should be fetched, False otherwise
        """
        if not attachment.FileURI:
            logger.debug(f"Skipping attachment '{attachment.Title}': no FileURI")
            return False

        if attachment.PublicityClass and attachment.PublicityClass != "Julkinen":
            logger.debug(
                f"Skipping attachment '{attachment.Title}': "
                f"PublicityClass={attachment.PublicityClass}"
            )
            return False

        if attachment.PersonalData and attachment.PersonalData != "Ei sisällä henkilötietoja":
            logger.debug(
                f"Skipping attachment '{attachment.Title}': "
                f"PersonalData={attachment.PersonalData}"
            )
            return False

        return True

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=2.0, min=1.0, max=60.0),
        retry=retry_if_exception_type((httpx.TimeoutException, httpx.NetworkError)),
    )
    def download_attachment(
        self, file_uri: str, target_path: Path, attachment_title: str = ""
    ) -> Optional[Path]:
        """
        Download a single attachment.

        Args:
            file_uri: URL to download from
            target_path: Path to save the file
            attachment_title: Title for logging purposes

        Returns:
            Path to downloaded file on success, None on failure
        """
        try:
            self._rate_limit()

            logger.debug(f"Downloading attachment '{attachment_title}' from {file_uri}")

            # Make request
            response = self.client.get(file_uri)
            response.raise_for_status()

            # Check file size
            content_length = response.headers.get("content-length")
            if content_length:
                size_mb = int(content_length) / (1024 * 1024)
                max_size = getattr(settings, "MAX_ATTACHMENT_SIZE_MB", 50)
                if size_mb > max_size:
                    logger.warning(
                        f"Attachment '{attachment_title}' exceeds max size: "
                        f"{size_mb:.1f}MB > {max_size}MB"
                    )
                    return None

            # Ensure target directory exists
            target_path.parent.mkdir(parents=True, exist_ok=True)

            # Write to file
            with open(target_path, "wb") as f:
                f.write(response.content)

            logger.info(
                f"Downloaded attachment '{attachment_title}' "
                f"({len(response.content)} bytes) to {target_path}"
            )
            return target_path

        except httpx.HTTPStatusError as e:
            logger.error(
                f"HTTP error downloading '{attachment_title}' from {file_uri}: "
                f"{e.response.status_code}"
            )
            return None
        except httpx.TimeoutException:
            logger.error(f"Timeout downloading '{attachment_title}' from {file_uri}")
            raise  # Let retry handle it
        except httpx.NetworkError as e:
            logger.error(f"Network error downloading '{attachment_title}' from {file_uri}: {e}")
            raise  # Let retry handle it
        except Exception as e:
            logger.error(
                f"Unexpected error downloading '{attachment_title}' from {file_uri}: {e}",
                exc_info=True,
            )
            return None

    def download_attachments(
        self, attachments: List[Attachment], temp_dir: Path, decision_native_id: str = ""
    ) -> Dict[str, Path]:
        """
        Download multiple attachments with optional parallelization.

        Args:
            attachments: List of attachment objects
            temp_dir: Temporary directory for downloads
            decision_native_id: Native ID of parent decision (for logging)

        Returns:
            Dictionary mapping attachment NativeId to file path
        """
        downloaded = {}

        # Filter attachments
        filtered = [att for att in attachments if self.should_fetch_attachment(att)]

        if not filtered:
            logger.info(
                f"No public attachments to download for decision {decision_native_id} "
                f"({len(attachments)} total attachments)"
            )
            return downloaded

        logger.info(
            f"Downloading {len(filtered)} public attachments for decision {decision_native_id} "
            f"(filtered from {len(attachments)} total)"
        )

        # Ensure temp directory exists
        temp_dir.mkdir(parents=True, exist_ok=True)

        max_workers = getattr(settings, "MAX_WORKERS_ATTACHMENTS", 5)

        if max_workers > 1 and len(filtered) > 1:
            # Parallel download with ThreadPoolExecutor
            logger.debug(f"Using {max_workers} workers for parallel attachment downloads")

            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                # Submit all download tasks
                future_to_attachment = {}
                for attachment in filtered:
                    if not attachment.NativeId:
                        logger.warning(
                            f"Attachment '{attachment.Title}' is missing NativeId; skipping download."
                        )
                        continue

                    native_id = attachment.NativeId
                    ext = self._get_extension_from_uri(attachment.FileURI)
                    filename = f"{decision_native_id}_{native_id}{ext}"
                    target_path = temp_dir / filename

                    future = executor.submit(
                        self.download_attachment,
                        attachment.FileURI,
                        target_path,
                        attachment.Title or native_id,
                    )
                    future_to_attachment[future] = (native_id, attachment.Title)

                # Collect results as they complete
                for future in as_completed(future_to_attachment):
                    native_id, title = future_to_attachment[future]
                    try:
                        result = future.result()
                        if result:
                            downloaded[native_id] = result
                        else:
                            logger.warning(
                                f"Failed to download attachment '{title}' "
                                f"({native_id}) for decision {decision_native_id}"
                            )
                    except Exception as e:
                        logger.error(
                            f"Exception downloading attachment '{title}' ({native_id}): {e}",
                            exc_info=True,
                        )
        else:
            # Serial download (original behavior)
            logger.debug("Using serial attachment downloads")
            for attachment in filtered:
                # Generate filename
                if not attachment.NativeId:
                    logger.warning(
                        f"Attachment '{attachment.Title}' is missing NativeId; skipping download."
                    )
                    continue
                native_id = attachment.NativeId
                ext = self._get_extension_from_uri(attachment.FileURI)
                filename = f"{decision_native_id}_{native_id}{ext}"
                target_path = temp_dir / filename

                # Download
                result = self.download_attachment(
                    attachment.FileURI, target_path, attachment.Title or native_id
                )

                if result:
                    downloaded[native_id] = result
                else:
                    logger.warning(
                        f"Failed to download attachment '{attachment.Title}' "
                        f"({native_id}) for decision {decision_native_id}"
                    )

        logger.info(
            f"Successfully downloaded {len(downloaded)}/{len(filtered)} attachments "
            f"for decision {decision_native_id}"
        )

        return downloaded

    def _get_extension_from_uri(self, file_uri: str) -> str:
        """
        Extract file extension from URI.

        Args:
            file_uri: File URI

        Returns:
            File extension with leading dot (e.g., '.pdf')
        """
        try:
            path = file_uri.split("?")[0]  # Remove query parameters
            ext = Path(path).suffix.lower()
            return ext if ext else ".bin"
        except Exception:
            return ".bin"
