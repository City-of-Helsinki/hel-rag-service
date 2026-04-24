"""
Date utility functions for the pipeline.

Includes parsing, formatting, and generating date ranges.
"""

from datetime import datetime, timedelta
from typing import List, Tuple

from dateutil import parser as date_parser


def parse_date(date_string: str) -> datetime:
    """
    Parse various date formats into datetime object.

    Args:
        date_string: Date string in various formats

    Returns:
        Parsed datetime object
    """
    return date_parser.parse(date_string)


def format_date_for_api(date: datetime, format_str: str = "%Y-%m-%dT%H:%M:%S") -> str:
    """
    Format datetime object for API requests (format YYYY-MM-DDTHH:MM:SS).

    Args:
        date: Datetime object
        format_str: Output format string

    Returns:
        Formatted date string
    """
    return date.strftime(format_str)


def generate_date_range(
    start_date: datetime, end_date: datetime, step_days: int = 7, backwards: bool = False
) -> List[Tuple[datetime, datetime]]:
    """
    Generate date ranges in batches.

    Args:
        start_date: Start date
        end_date: End date
        step_days: Number of days per batch
        backwards: Whether to generate ranges backwards from end_date
    Returns:
        List of (start, end) datetime tuples
    """
    date_ranges = []
    if backwards:
        current = start_date
        while current > end_date:
            batch_start = max(current - timedelta(days=step_days), end_date)
            date_ranges.append((batch_start, current))
            current = batch_start
    else:
        current = start_date
        while current < end_date:
            batch_end = min(current + timedelta(days=step_days), end_date)
            date_ranges.append((current, batch_end))
            current = batch_end

    return date_ranges


def weeks_between(start_date: datetime, end_date: datetime, backwards: bool = False) -> int:
    """
    Calculate number of weeks between two dates.

    Args:
        start_date: Start date
        end_date: End date
        backwards: Whether to calculate weeks backwards from end_date

    Returns:
        Number of weeks (rounded up)
    """
    if backwards:
        delta = start_date - end_date
    else:
        delta = end_date - start_date
    return (delta.days + 6) // 7  # Round up
