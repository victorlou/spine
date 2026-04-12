"""
Utilities for datetime parsing, formatting, and manipulation.
Provides centralized datetime operations to avoid duplication.
"""

from datetime import UTC, datetime, timedelta
from typing import List, Optional

from src.utils.logger import get_logger

logger = get_logger(__name__)


# Standard datetime formats used across the pipeline
DATETIME_FORMATS = [
    "%Y-%m-%dT%H:%M:%S.%fZ",  # ISO format with microseconds and Z
    "%Y-%m-%dT%H:%M:%SZ",  # ISO format with Z
    "%Y-%m-%dT%H:%M:%S.%f",  # ISO format with microseconds
    "%Y-%m-%dT%H:%M:%S",  # ISO format
    "%Y-%m-%d %H:%M:%S.%f",  # Standard with microseconds
    "%Y-%m-%d %H:%M:%S",  # Standard datetime
    "%Y-%m-%d",  # Date only
]


def parse_datetime(
    value: Optional[str], formats: Optional[List[str]] = None, required: bool = False
) -> Optional[datetime]:
    """
    Parse a datetime string into a datetime object.

    Handles various formats and ensures UTC timezone. This is the canonical
    datetime parsing function for the entire pipeline.

    Args:
        value: Datetime string to parse
        formats: List of formats to try (defaults to DATETIME_FORMATS)
        required: Whether the datetime is required

    Returns:
        Optional[datetime]: Parsed datetime with UTC timezone, or None

    Raises:
        ValueError: If parsing fails and datetime is required

    Examples:
        >>> parse_datetime("2025-07-16T03:39:59.665417+00:00")
        datetime(2025, 7, 16, 3, 39, 59, 665417, tzinfo=timezone.utc)
        >>> parse_datetime("2025-07-16")
        datetime(2025, 7, 16, 0, 0, tzinfo=timezone.utc)
        >>> parse_datetime(None, required=False)
        None
    """
    if not value:
        if required:
            raise ValueError("Required datetime value is missing")
        return None

    # Use default formats if none provided
    formats = formats or DATETIME_FORMATS

    # Try to parse with each format
    for fmt in formats:
        try:
            # Parse the datetime
            dt = datetime.strptime(value, fmt)

            # Ensure UTC timezone
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=UTC)

            return dt
        except ValueError:
            continue

    # If all formats fail, raise or return None
    # Note: Caller will log if needed; exception already has context
    if required:
        raise ValueError(f"Unable to parse datetime value: {value}")

    # Log at TRACE level only for non-required attempts (debugging)
    logger.trace(
        "Unable to parse datetime value",
        extra_fields={"value": value, "formats_tried": len(formats)},
    )
    return None


def format_datetime(dt: datetime, format_string: str = "%Y-%m-%d %H:%M:%S.%f") -> str:
    """
    Format a datetime object to a string.

    Args:
        dt: Datetime to format
        format_string: Format string (default includes microseconds)

    Returns:
        str: Formatted datetime string

    Raises:
        ValueError: If datetime formatting fails

    Examples:
        >>> dt = datetime(2025, 7, 16, 3, 39, 59, 665417, tzinfo=UTC)
        >>> format_datetime(dt)
        '2025-07-16 03:39:59.665417'
        >>> format_datetime(dt, "%Y-%m-%d")
        '2025-07-16'
    """
    try:
        return dt.strftime(format_string)
    except (AttributeError, ValueError) as e:
        raise ValueError(f"Failed to format datetime: {e!s}") from e


def get_date_offset(
    days: int = 0, base_date: Optional[datetime] = None, date_format: str = "%Y-%m-%d"
) -> str:
    """
    Get a date string with an offset from today or a base date.

    Args:
        days: Number of days to offset (positive for future, negative for past)
        base_date: Base date to offset from (defaults to today)
        date_format: Format for the output date string

    Returns:
        str: Formatted date string

    Examples:
        >>> # Assuming today is 2025-07-16
        >>> get_date_offset(0)
        '2025-07-16'
        >>> get_date_offset(-7)  # 7 days ago
        '2025-07-09'
        >>> get_date_offset(7)   # 7 days from now
        '2025-07-23'
    """
    if base_date is None:
        base_date = datetime.now(UTC)

    target_date = base_date + timedelta(days=days)
    return target_date.strftime(date_format)


def days_between(start: datetime, end: datetime) -> int:
    """
    Calculate the number of days between two datetimes.

    Args:
        start: Start datetime
        end: End datetime

    Returns:
        int: Number of days between the datetimes
    """
    return (end - start).days


def ensure_utc(dt: datetime) -> datetime:
    """
    Ensure a datetime has UTC timezone.

    If the datetime is naive (no timezone), it's assumed to be UTC.
    If it has a different timezone, it's converted to UTC.

    Args:
        dt: Datetime to ensure is UTC

    Returns:
        datetime: Datetime with UTC timezone
    """
    if dt.tzinfo is None:
        return dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)
