"""
Validation utilities for decision data.

Includes functions to validate decision documents, date ranges, and NativeId formats.
"""

from datetime import datetime
from typing import Optional

from ..schemas.decision import DecisionDocument


def validate_native_id(native_id: str) -> bool:
    """
    Validate decision NativeId format.

    Args:
        native_id: NativeId to validate

    Returns:
        True if valid format
    """
    if not native_id or not isinstance(native_id, str):
        return False

    # NativeId typically follows a pattern, adjust based on actual format
    # For now, just check it's not empty and has reasonable length
    return len(native_id.strip()) > 0 and len(native_id) < 200


def validate_decision_document(document: DecisionDocument) -> tuple[bool, Optional[str]]:
    """
    Ensure required fields are present in decision document.

    Args:
        document: DecisionDocument to validate

    Returns:
        Tuple of (is_valid, error_message)
    """
    # Check NativeId
    if not validate_native_id(document.NativeId):
        return False, f"Invalid NativeId: {document.NativeId}"

    # Check that at least Title or Content exists
    if not document.Title and not document.Content:
        return False, f"Document {document.NativeId} has no Title or Content"

    # Validate date format if present
    if document.DateDecision:
        try:
            datetime.fromisoformat(document.DateDecision.replace("Z", "+00:00"))
        except (ValueError, AttributeError):
            # Try alternative format
            try:
                datetime.strptime(document.DateDecision, "%Y-%m-%d")
            except ValueError:
                return False, f"Invalid DateDecision format: {document.DateDecision}"

    return True, None


def validate_date_range(start_date: datetime, end_date: datetime) -> tuple[bool, Optional[str]]:
    """
    Ensure valid date range.

    Args:
        start_date: Start date
        end_date: End date

    Returns:
        Tuple of (is_valid, error_message)
    """
    if start_date >= end_date:
        return False, f"Start date {start_date} must be before end date {end_date}"

    # Check if dates are reasonable (not too far in past or future)
    min_date = datetime(2000, 1, 1)
    max_date = datetime.now()

    if start_date < min_date:
        return False, f"Start date {start_date} is before minimum date {min_date}"

    if end_date > max_date:
        return False, f"End date {end_date} is in the future"

    return True, None
