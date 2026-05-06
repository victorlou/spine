"""Tests for datetime helper utilities."""

from datetime import UTC, datetime, timezone

import pytest

from src.utils.datetime_utils import (
    days_between,
    ensure_utc,
    format_datetime,
    get_date_offset,
    parse_datetime,
)


def test_parse_datetime_success_and_required_failure() -> None:
    parsed = parse_datetime("2025-07-16T03:39:59")
    assert parsed is not None
    assert parsed.tzinfo == UTC

    assert parse_datetime(None, required=False) is None
    with pytest.raises(ValueError, match="Required datetime value is missing"):
        parse_datetime(None, required=True)


def test_parse_datetime_with_custom_formats() -> None:
    parsed = parse_datetime("16/07/2025", formats=["%d/%m/%Y"], required=True)
    assert parsed == datetime(2025, 7, 16, tzinfo=UTC)

    with pytest.raises(ValueError, match="Unable to parse datetime value"):
        parse_datetime("invalid", formats=["%Y-%m-%d"], required=True)


def test_format_date_offset_days_between_and_ensure_utc() -> None:
    dt = datetime(2025, 7, 16, 3, 39, 59, 665417, tzinfo=UTC)
    assert format_datetime(dt, "%Y-%m-%d") == "2025-07-16"
    with pytest.raises(ValueError, match="Failed to format datetime"):
        format_datetime(None)  # type: ignore[arg-type]

    base = datetime(2025, 7, 16, tzinfo=UTC)
    assert get_date_offset(-2, base_date=base) == "2025-07-14"
    assert days_between(datetime(2025, 7, 1, tzinfo=UTC), datetime(2025, 7, 16, tzinfo=UTC)) == 15

    naive = datetime(2025, 7, 16, 12, 0, 0)
    aware = datetime(2025, 7, 16, 12, 0, 0, tzinfo=timezone.utc)
    assert ensure_utc(naive).tzinfo == UTC
    assert ensure_utc(aware).tzinfo == UTC
