"""Tests for incremental cursor helpers."""

import pytest

from src.handler.incremental_cursor import apply_incremental_cursor_tolerance
from src.utils.exceptions import HandlerError


def test_apply_tolerance_zero_returns_unchanged() -> None:
    assert (
        apply_incremental_cursor_tolerance(
            " 20260512 ",
            tolerance_calendar_days=0,
            reference_format="none",
        )
        == "20260512"
    )


def test_apply_tolerance_subtract_days() -> None:
    assert (
        apply_incremental_cursor_tolerance(
            "20260512",
            tolerance_calendar_days=1,
            reference_format="yyyymmdd",
        )
        == "20260511"
    )


def test_apply_tolerance_month_boundary() -> None:
    assert (
        apply_incremental_cursor_tolerance(
            "20260301",
            tolerance_calendar_days=1,
            reference_format="yyyymmdd",
        )
        == "20260228"
    )


def test_apply_tolerance_none_stays_none() -> None:
    assert (
        apply_incremental_cursor_tolerance(
            None,
            tolerance_calendar_days=1,
            reference_format="yyyymmdd",
        )
        is None
    )


def test_apply_tolerance_invalid_length() -> None:
    with pytest.raises(HandlerError, match="YYYYMMDD"):
        apply_incremental_cursor_tolerance(
            "2026051",
            tolerance_calendar_days=1,
            reference_format="yyyymmdd",
        )


def test_apply_tolerance_invalid_date() -> None:
    with pytest.raises(HandlerError, match="parse"):
        apply_incremental_cursor_tolerance(
            "20260231",
            tolerance_calendar_days=1,
            reference_format="yyyymmdd",
        )
