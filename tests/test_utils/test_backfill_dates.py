"""Unit tests for backfill date pair generation."""

from unittest.mock import MagicMock

import pytest

from src.config.backfill_config import (
    BackfillConfig,
    BackfillReferenceConfig,
    BackfillStaticDateConfig,
)
from src.utils.backfill_dates import generate_backfill_date_pairs


def _mock_redis_context():
    """Return a mock RedisContextManager (not used when start/end/limit are static strings)."""
    return MagicMock()


def _make_config(inclusive: bool, limit_str: str) -> BackfillConfig:
    """Build BackfillConfig with static dates for testing."""
    driver = BackfillStaticDateConfig(
        start="2026-01-01",
        end=limit_str,
        increment="15 DAY",
        inclusive=inclusive,
    )
    reference = BackfillReferenceConfig(
        field="startDate",
        increment="15 DAY",
        limit=limit_str,
    )
    return BackfillConfig(
        driver_key="startDate",
        reference_key="endDate",
        driver_config=driver,
        reference_config=reference,
        request_body_keys=["startDate", "endDate"],
    )


def _pairs_to_tuples(pairs: list) -> list[tuple[str, str]]:
    """Convert list of dicts to list of (start, end) tuples."""
    return [(p["startDate"], p["endDate"]) for p in pairs]


@pytest.mark.parametrize(
    "inclusive,limit,expected",
    [
        (
            False,
            "2026-02-20",
            [
                ("2026-01-01", "2026-01-15"),
                ("2026-01-16", "2026-01-30"),
                ("2026-01-31", "2026-02-14"),
                ("2026-02-15", "2026-02-20"),
            ],
        ),
        (
            False,
            "2026-02-14",
            [
                ("2026-01-01", "2026-01-15"),
                ("2026-01-16", "2026-01-30"),
                ("2026-01-31", "2026-02-14"),
            ],
        ),
        (
            False,
            "2026-02-15",
            [
                ("2026-01-01", "2026-01-15"),
                ("2026-01-16", "2026-01-30"),
                ("2026-01-31", "2026-02-14"),
                ("2026-02-15", "2026-02-15"),
            ],
        ),
        (
            False,
            "2026-02-16",
            [
                ("2026-01-01", "2026-01-15"),
                ("2026-01-16", "2026-01-30"),
                ("2026-01-31", "2026-02-14"),
                ("2026-02-15", "2026-02-16"),
            ],
        ),
        # inclusive=true: 15-day windows with overlapping boundary (next start = this end)
        (
            True,
            "2026-02-20",
            [
                ("2026-01-01", "2026-01-15"),
                ("2026-01-15", "2026-01-29"),
                ("2026-01-29", "2026-02-12"),
                ("2026-02-12", "2026-02-20"),
            ],
        ),
        (
            True,
            "2026-02-14",
            [
                ("2026-01-01", "2026-01-15"),
                ("2026-01-15", "2026-01-29"),
                ("2026-01-29", "2026-02-12"),
                ("2026-02-12", "2026-02-14"),
            ],
        ),
        (
            True,
            "2026-02-15",
            [
                ("2026-01-01", "2026-01-15"),
                ("2026-01-15", "2026-01-29"),
                ("2026-01-29", "2026-02-12"),
                ("2026-02-12", "2026-02-15"),
            ],
        ),
        (
            True,
            "2026-02-16",
            [
                ("2026-01-01", "2026-01-15"),
                ("2026-01-15", "2026-01-29"),
                ("2026-01-29", "2026-02-12"),
                ("2026-02-12", "2026-02-16"),
            ],
        ),
    ],
)
def test_generate_backfill_date_pairs(inclusive: bool, limit: str, expected: list) -> None:
    """Verify date pairs match expected output for inclusive and limit combinations."""
    config = _make_config(inclusive=inclusive, limit_str=limit)
    redis_context = _mock_redis_context()
    pairs = generate_backfill_date_pairs(config, redis_context)
    assert _pairs_to_tuples(pairs) == expected
