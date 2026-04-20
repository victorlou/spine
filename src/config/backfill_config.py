"""
Backfill configuration parsing for date-range backfill.

Backfill is detected from request inputs (path, query, or body) whose ``value`` is a
dict containing ``backfill``. Callers pass a flat name -> ``value`` map (e.g. from
``ResourceConfig.get_request_input_values_for_backfill()``).

Each resource supports a single driver/reference pair: one ``STATIC_DATE`` and one
``REFERENCE``. Extra valid blocks raise ``ValueError`` (wrapped as ``PlanningError``
during plan build).
"""

from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from dateutil.relativedelta import relativedelta

BACKFILL_TYPE_STATIC_DATE = "STATIC_DATE"
BACKFILL_TYPE_REFERENCE = "REFERENCE"


@dataclass
class BackfillStaticDateConfig:
    """Driver backfill: fixed or dynamic start/end with increment."""

    start: Any  # str (YYYY-MM-DD) or dict for dynamic (e.g. type: DATE, operation: TODAY)
    end: Any  # str or dict for dynamic
    increment: str  # e.g. '15 DAY'
    inclusive: bool = (
        False  # if False, windows contiguous (next_start=endDate+1); if True, boundary overlaps (next_start=endDate)
    )


@dataclass
class BackfillReferenceConfig:
    """Reference backfill: value = driver_field + increment, capped at limit."""

    field: str  # request input name of the driver (e.g. startDate)
    increment: str  # e.g. '15 DAY'
    limit: Any  # str or dict for dynamic (e.g. type: DATE, operation: TODAY)


@dataclass
class BackfillConfig:
    """Parsed backfill configuration for a resource."""

    driver_key: str  # request input name that drives the ranges (e.g. startDate)
    reference_key: str  # request input name tied to driver (e.g. endDate)
    driver_config: BackfillStaticDateConfig
    reference_config: BackfillReferenceConfig
    field_keys: List[str]  # [driver_key, reference_key]; injected into request context


def parse_increment(increment_str: str) -> relativedelta:
    """
    Parse increment string to relativedelta.
    Supports 'N DAY', 'N WEEK', and 'N MONTH'.

    Args:
        increment_str: e.g. '15 DAY', '2 WEEK', '1 MONTH'

    Returns:
        relativedelta object representing the increment.

    Raises:
        ValueError: If format is not supported.
    """
    if not increment_str or not isinstance(increment_str, str):
        raise ValueError("increment must be a non-empty string")
    parts = increment_str.strip().upper().split()
    if len(parts) != 2:
        raise ValueError(
            f"increment must be of form 'N DAY', 'N WEEK', or 'N MONTH', got: {increment_str!r}"
        )
    try:
        n = int(parts[0])
    except ValueError as e:
        raise ValueError(f"increment number must be integer, got: {parts[0]!r}") from e
    if n <= 0:
        raise ValueError(f"increment must be positive, got: {n}")
    unit = parts[1]
    if unit in ("DAY", "DAYS"):
        return relativedelta(days=n)
    if unit in ("WEEK", "WEEKS"):
        return relativedelta(weeks=n)
    if unit in ("MONTH", "MONTHS"):
        return relativedelta(months=n)
    raise ValueError(f"increment unit must be DAY, WEEK, or MONTH, got: {unit!r}")


def get_backfill_config(input_values: Optional[Dict[str, Any]]) -> Optional[BackfillConfig]:
    """
    Detect and parse backfill configuration from resource request input values.

    Each resource supports **at most one** date-range backfill: one ``STATIC_DATE``
    driver and one ``REFERENCE`` tied to that driver. The driver and reference may
    live on different ``request_inputs`` locations (path, query, body); that still
    produces a single stream of date windows, not a Cartesian product of multiple
    backfills.

    Raises:
        ValueError: If more than one valid driver or more than one valid reference
            is present (duplicate ``backfill`` blocks).

    Returns:
        BackfillConfig if valid backfill is configured, else None.
    """
    if not input_values or not isinstance(input_values, dict):
        return None

    driver_key: Optional[str] = None
    driver_config: Optional[BackfillStaticDateConfig] = None
    reference_key: Optional[str] = None
    reference_config: Optional[BackfillReferenceConfig] = None

    for key, value in input_values.items():
        if not isinstance(value, dict) or "backfill" not in value:
            continue
        backfill = value.get("backfill")
        if not isinstance(backfill, dict):
            continue
        bf_type = (backfill.get("type") or "").strip().upper()
        if bf_type == BACKFILL_TYPE_STATIC_DATE:
            start = backfill.get("start")
            end = backfill.get("end")
            increment = backfill.get("increment")
            if start is None or end is None or not increment:
                continue
            inclusive = backfill.get("inclusive", False)
            try:
                candidate_driver = BackfillStaticDateConfig(
                    start=start,
                    end=end,
                    increment=str(increment).strip(),
                    inclusive=bool(inclusive),
                )
                parse_increment(candidate_driver.increment)
            except (ValueError, TypeError):
                continue
            if driver_key is not None:
                raise ValueError(
                    "Backfill allows at most one STATIC_DATE driver per resource; "
                    f"found another on input {key!r} (first driver: {driver_key!r}). "
                    "Splitting start/end across path, query, or body is fine; defining "
                    "two independent driver blocks is not."
                )
            driver_config = candidate_driver
            driver_key = key
        elif bf_type == BACKFILL_TYPE_REFERENCE:
            ref_field = backfill.get("field")
            ref_increment = backfill.get("increment")
            limit = backfill.get("limit")
            if not ref_field or not ref_increment or limit is None:
                continue
            try:
                candidate_reference = BackfillReferenceConfig(
                    field=str(ref_field).strip(),
                    increment=str(ref_increment).strip(),
                    limit=limit,
                )
                parse_increment(candidate_reference.increment)
            except (ValueError, TypeError):
                continue
            if reference_key is not None:
                raise ValueError(
                    "Backfill allows at most one REFERENCE field per resource; "
                    f"found another on input {key!r} (first reference: {reference_key!r}). "
                    "Use exactly one driver/reference pair."
                )
            reference_config = candidate_reference
            reference_key = key

    if (
        driver_key is None
        or driver_config is None
        or reference_key is None
        or reference_config is None
    ):
        return None
    if reference_config.field != driver_key:
        return None

    return BackfillConfig(
        driver_key=driver_key,
        reference_key=reference_key,
        driver_config=driver_config,
        reference_config=reference_config,
        field_keys=[driver_key, reference_key],
    )
