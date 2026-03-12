"""
Error tracking utilities for secure error handling.

Provides functions to generate error IDs and sanitize error responses
to prevent leaking sensitive information to API consumers.
"""

import uuid
from typing import Any, Dict, Optional

from fastapi import HTTPException, Request


def generate_error_id() -> str:
    """
    Generate a unique error identifier.

    Returns:
        UUID-based error ID as string
    """
    return str(uuid.uuid4())


def log_error_with_id(
    logger,
    error: Exception,
    error_id: str,
    request: Optional[Request] = None,
    context: Optional[Dict[str, Any]] = None,
) -> None:
    """
    Log error with full context including error ID for tracking.

    Args:
        logger: Logger instance
        error: Exception that occurred
        error_id: Unique error identifier
        request: Optional FastAPI request object
        context: Optional additional context dictionary
    """
    # Build log message with context
    log_parts = [f"Error ID: {error_id}"]

    if request:
        log_parts.append(f"Method: {request.method}")
        log_parts.append(f"Path: {request.url.path}")
        if request.client:
            log_parts.append(f"Client: {request.client.host}")

    if context:
        for key, value in context.items():
            log_parts.append(f"{key}: {value}")

    log_parts.append(f"Error: {type(error).__name__}: {str(error)}")

    # Log with full traceback
    logger.error(" | ".join(log_parts), exc_info=True)


def create_error_response(
    error_id: str,
    error_type: str = "internal_server_error",
    message: str = "An unexpected error occurred",
    include_support_instructions: bool = True,
) -> Dict[str, Any]:
    """
    Create a sanitized error response for API consumers.

    Args:
        error_id: Unique error identifier
        error_type: Error type identifier
        message: User-friendly error message
        include_support_instructions: Whether to include support contact message

    Returns:
        Dictionary with sanitized error response
    """
    response = {
        "error": error_type,
        "error_id": error_id,
        "message": message,
    }

    if include_support_instructions:
        response["support_message"] = (
            f"If you need assistance, please contact support with error ID: {error_id}"
        )

    return response


def raise_error_with_id(
    logger,
    error: Exception,
    status_code: int = 500,
    message: str = "An unexpected error occurred",
    context: Optional[Dict[str, Any]] = None,
) -> None:
    """
    Log an error and raise an HTTPException with error ID.

    This is a convenience function for endpoint error handling.

    Args:
        logger: Logger instance
        error: Exception that occurred
        status_code: HTTP status code to return
        message: User-friendly error message
        context: Optional additional context dictionary

    Raises:
        HTTPException with sanitized error response
    """
    error_id = generate_error_id()
    log_error_with_id(logger, error, error_id, context=context)

    response = create_error_response(
        error_id=error_id,
        error_type="error",
        message=message,
    )

    raise HTTPException(status_code=status_code, detail=response["support_message"])
