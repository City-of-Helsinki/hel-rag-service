"""
API client for fetching decision ids and singular decisions from City of Helsinki decisions API.

The DecisionAPIClient class provides methods for fetching decision IDs based on date ranges and retrieving individual decision documents by their NativeId. The client implements robust error handling and retry logic to ensure reliable communication with the API, while respecting rate limits.
"""

import logging
import threading
import time
from typing import Any, Callable, Dict, Optional

import httpx
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from ..core.config import settings
from ..schemas.decision import (
    DecisionDocument,
    DecisionDocumentResponse,
    DecisionIdResponse,
)

logger = logging.getLogger(__name__)
api_logger = logging.getLogger("api")


class APIOutageError(Exception):
    """Exception raised when API-wide outage is detected (404 on decision IDs endpoint)."""
    pass


class DecisionAPIClient:
    """Client for interacting with the Helsinki Paatos API."""

    def __init__(
        self,
        raw_response_saver: Optional[Callable[[str, Dict[str, Any]], None]] = None,
    ):
        """Initialize the API client."""
        self.base_url = settings.API_BASE_URL
        self.timeout = settings.REQUEST_TIMEOUT
        self.client = httpx.Client(timeout=self.timeout)
        if settings.REQUESTS_PER_SECOND <= 0:
            raise ValueError("REQUESTS_PER_SECOND must be greater than 0")
        self.rate_limit_delay = 1.0 / settings.REQUESTS_PER_SECOND
        self.last_request_time = 0
        self.raw_response_saver = raw_response_saver

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

    def _rate_limit(self) -> None:
        """Apply rate limiting between requests.

        Uses a lock to ensure thread-safe rate limiting across concurrent requests.
        """
        if not hasattr(self, '_rate_limit_lock'):
            self._rate_limit_lock = threading.Lock()

        with self._rate_limit_lock:
            current_time = time.time()
            time_since_last = current_time - self.last_request_time
            if time_since_last < self.rate_limit_delay:
                sleep_time = self.rate_limit_delay - time_since_last
                time.sleep(sleep_time)
            self.last_request_time = time.time()

    @retry(
        stop=stop_after_attempt(settings.MAX_RETRY_ATTEMPTS),
        wait=wait_exponential(
            multiplier=settings.RETRY_BACKOFF_FACTOR,
            min=settings.RETRY_MIN_WAIT,
            max=settings.RETRY_MAX_WAIT,
        ),
        retry=retry_if_exception_type((httpx.TimeoutException, httpx.NetworkError)),
    )
    def _make_request(self, url: str, params: Optional[Dict[str, Any]] = None) -> httpx.Response:
        """
        Make HTTP request with retry logic.

        Args:
            url: Request URL
            params: Query parameters

        Returns:
            HTTP response
        """
        self._rate_limit()

        # Sanitize params before logging — never expose the API key.
        _SENSITIVE_PARAM_KEYS = {"api-key", "apikey", "api_key", "key", "token", "secret"}
        safe_params = (
            {
                k: ("***" if k.lower() in _SENSITIVE_PARAM_KEYS else v)
                for k, v in params.items()
            }
            if params
            else params
        )
        api_logger.debug(f"Making request to {url} with params: {safe_params}")

        try:
            response = self.client.get(url, params=params)
            response.raise_for_status()
            api_logger.debug(f"Request successful: {url}")
            return response
        except httpx.HTTPStatusError as e:
            api_logger.error(f"HTTP error for {url}: {e.response.status_code} - {e.response.text}")
            raise
        except httpx.TimeoutException:
            api_logger.error(f"Timeout for {url}")
            raise
        except httpx.NetworkError as e:
            api_logger.error(f"Network error for {url}: {e}")
            raise
        except Exception as e:
            api_logger.error(f"Unexpected error for {url}: {e}")
            raise

    def fetch_decision_ids(
        self,
        api_key: str,
        start_date: str,
        end_date: str,
        size: int = None,
    ) -> DecisionIdResponse:
        """
        Fetch decision IDs for a date range.

        Args:
            api_key: API key for authentication
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            size: Number of results

        Returns:
            DecisionIdResponse containing list of decision IDs
        """
        if size is None:
            size = settings.API_PAGE_SIZE

        url = f"{self.base_url}{settings.DECISION_IDS_ENDPOINT}"
        params = {
            "api-key": api_key,
            "handledsince": start_date,
            "handledbefore": end_date,
            "size": size,
        }

        logger.info(f"Fetching decision IDs from {start_date} to {end_date}")

        try:
            response = self._make_request(url, params)
            data = response.json()

            # Parse response
            decision_response = DecisionIdResponse(**data)

            logger.info(
                f"Retrieved {len(decision_response.decisions)} decision IDs "
                f"(total count: {decision_response.count})"
            )

            return decision_response

        except httpx.HTTPStatusError as e:
            # 404, 500, or 504 on decision IDs endpoint indicates API-wide outage
            if e.response.status_code == 404:
                logger.error(f"API outage detected: 404 error fetching decision IDs from {start_date} to {end_date}")
                raise APIOutageError(f"API returned 404 for decision IDs endpoint: {url}") from e
            elif e.response.status_code == 500:
                logger.error(f"API outage detected: 500 error fetching decision IDs from {start_date} to {end_date}")
                raise APIOutageError(f"API returned 500 for decision IDs endpoint: {url}") from e
            elif e.response.status_code == 504:
                logger.error(f"API outage detected: 504 error fetching decision IDs from {start_date} to {end_date}")
                raise APIOutageError(f"API returned 504 for decision IDs endpoint: {url}") from e
            logger.error(f"HTTP error fetching decision IDs: {e}")
            raise
        except Exception as e:
            logger.error(f"Error fetching decision IDs: {e}")
            raise

    def fetch_decision_document(self, native_id: str, api_key: str) -> Optional[DecisionDocument]:
        """
        Fetch a single decision document by NativeId.

        Args:
            native_id: The NativeId of the decision
            api_key: API key for authentication

        Returns:
            DecisionDocument object or None if not found
        """
        url = f"{self.base_url}{settings.DECISION_DOCUMENT_ENDPOINT.format(native_id=native_id)}"
        params = {
            "api-key": api_key,
        }

        logger.debug(f"Fetching decision document: {native_id}")

        try:
            response = self._make_request(url, params)
            data = response.json()

            # Save full raw response if a saver is configured
            if self.raw_response_saver is not None:
                try:
                    self.raw_response_saver(native_id, data)
                except Exception as e:
                    logger.error(f"Failed to save raw response for {native_id}: {e}")

            # The endpoint returns a single decision, not a list
            # Compatible with v1 of the API
            if isinstance(data, dict):
                # Try to parse as single decision
                if "NativeId" in data:
                    decision = DecisionDocument(**data)
                    logger.debug(f"Successfully fetched decision: {native_id}")
                    return decision
                # Or as a response with decisions list
                elif "decisions" in data:
                    response_obj = DecisionDocumentResponse(**data)
                    if response_obj.decisions:
                        return response_obj.decisions[0]
            # Compatible with v2 of the API
            elif isinstance(data, list) and len(data) > 0:
                response_obj = DecisionDocumentResponse(decisions=[DecisionDocument(**item) for item in data])
                if response_obj.decisions:
                    return response_obj.decisions[0]

            logger.warning(f"No decision found for NativeId: {native_id}")
            return None

        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                logger.warning(f"Decision not found: {native_id}")
                return None
            logger.error(f"HTTP error fetching decision {native_id}: {e}")
            raise
        except Exception as e:
            logger.error(f"Error fetching decision {native_id}: {e}")
            raise
