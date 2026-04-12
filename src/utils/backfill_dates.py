"""
Generate paired date ranges for backfill from backfill config.

Produces a list of {driver_key: start, reference_key: end} dicts. Each window
spans at most ref_increment days; endDate is the last day included (inclusive
semantics). When inclusive=false, windows are contiguous; when inclusive=true,
the boundary day overlaps (next start = this end).
"""

import re
from datetime import datetime, timedelta
from typing import Any, Dict, List

from src.config.backfill_config import (
    BackfillConfig,
    parse_increment,
)
from src.utils.dynamic_values import get_resolver
from src.utils.redis_context import RedisContextManager

DATE_FMT = "%Y-%m-%d"
DATE_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def _parse_date(s: str) -> datetime:
    """Parse YYYY-MM-DD string to datetime at midnight UTC."""
    return datetime.strptime(s, DATE_FMT)


def _format_date(dt: datetime) -> str:
    """Format datetime to YYYY-MM-DD."""
    return dt.strftime(DATE_FMT)


def _resolve_end_or_limit(
    value: Any,
    redis_context: RedisContextManager,
) -> str:
    """
    Resolve end/limit to YYYY-MM-DD string.
    Accepts static string (YYYY-MM-DD) or dict for dynamic (e.g. type: DATE, operation: TODAY).
    """
    if value is None:
        raise ValueError("end/limit cannot be None")
    if isinstance(value, str) and DATE_PATTERN.match(value.strip()):
        return value.strip()
    resolved = get_resolver(redis_context).resolve(value)
    if isinstance(resolved, str) and DATE_PATTERN.match(resolved.strip()):
        return resolved.strip()
    if hasattr(resolved, "strftime"):
        return resolved.strftime(DATE_FMT)
    raise ValueError(f"Unsupported end/limit value: {value!r} -> {resolved!r}")


def generate_backfill_date_pairs(
    backfill_config: BackfillConfig,
    redis_context: RedisContextManager,
) -> List[Dict[str, str]]:
    """
    Generate list of {driver_key: start_date, reference_key: end_date} pairs.

    Each window spans at most ref_increment days; endDate is the last day
    included. When inclusive=false, windows are contiguous (next start =
    endDate + 1). When inclusive=true, the boundary day overlaps (next start =
    endDate). The last window may be shorter when capped at limit.

    Args:
        backfill_config: Parsed backfill config (driver + reference).
        redis_context: Used to resolve dynamic start/end/limit.

    Returns:
        List of dicts e.g. [{"startDate": "2026-01-01", "endDate": "2026-01-15"}, ...].
        Empty list if start > end or limit is before first start.
    """
    driver = backfill_config.driver_config
    reference = backfill_config.reference_config
    driver_key = backfill_config.driver_key
    reference_key = backfill_config.reference_key

    # Resolve start/end/limit (static YYYY-MM-DD or dynamic dict)
    start_str = _resolve_end_or_limit(driver.start, redis_context)
    end_str = _resolve_end_or_limit(driver.end, redis_context)
    limit_str = _resolve_end_or_limit(reference.limit, redis_context)

    try:
        start_dt = _parse_date(start_str)
        end_dt = _parse_date(end_str)
        limit_dt = _parse_date(limit_str)
    except ValueError as e:
        raise ValueError(
            f"Backfill dates must be YYYY-MM-DD: start={start_str!r}, end={end_str!r}, limit={limit_str!r}"
        ) from e

    if start_dt > end_dt:
        return []
    if start_dt > limit_dt:
        return []

    ref_increment_rd = parse_increment(reference.increment)
    parse_increment(driver.increment)  # validate

    inclusive = driver.inclusive
    pairs: List[Dict[str, str]] = []
    current = start_dt
    effective_end = min(end_dt, limit_dt)
    if current > effective_end:
        return []

    while current <= effective_end:
        window_start = current
        # endDate = last day of N-[unit] window (inclusive semantics)
        window_end_dt = current + ref_increment_rd - timedelta(days=1)
        if window_end_dt > limit_dt:
            window_end_dt = limit_dt
        if window_end_dt < window_start:
            break
        pairs.append(
            {
                driver_key: _format_date(window_start),
                reference_key: _format_date(window_end_dt),
            }
        )
        if inclusive:
            current = window_end_dt
            if window_end_dt >= limit_dt:
                break  # next start would be limit, already covered
        else:
            current = window_end_dt + timedelta(days=1)
        if current > effective_end:
            break

    return pairs
