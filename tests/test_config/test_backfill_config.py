"""Tests for backfill config detection from request input values."""

import pytest

from src.config.backfill_config import get_backfill_config
from src.config.config_models import ResourceConfig


def test_get_backfill_config_query_style_flat_dict() -> None:
    """GET-style query parameters (snake_case) with nested value + backfill."""
    input_values = {
        "start_date": {
            "value": "{{ date('PREVIOUS_SUNDAY') }}",
            "backfill": {
                "type": "STATIC_DATE",
                "start": "2021-01-03",
                "end": "2026-04-21",
                "inclusive": False,
                "increment": "1 WEEK",
            },
        },
        "end_date": {
            "value": "{{ date('PREVIOUS_SATURDAY') }}",
            "backfill": {
                "type": "REFERENCE",
                "field": "start_date",
                "increment": "7 DAY",
                "limit": "2026-04-14",
            },
        },
    }
    cfg = get_backfill_config(input_values)
    assert cfg is not None
    assert cfg.driver_key == "start_date"
    assert cfg.reference_key == "end_date"
    assert cfg.reference_config.field == "start_date"
    assert cfg.field_keys == ["start_date", "end_date"]


def test_get_backfill_config_body_style_flat_dict() -> None:
    """POST body camelCase keys (doc example shape)."""
    input_values = {
        "startDate": {
            "value": "{{ date('DAYS_AGO', days=9) }}",
            "backfill": {
                "type": "STATIC_DATE",
                "start": "2026-01-01",
                "end": "2026-04-21",
                "inclusive": False,
                "increment": "15 DAY",
            },
        },
        "endDate": {
            "value": "{{ date('DAYS_AGO', days=2) }}",
            "backfill": {
                "type": "REFERENCE",
                "field": "startDate",
                "increment": "15 DAY",
                "limit": "2026-04-14",
            },
        },
    }
    cfg = get_backfill_config(input_values)
    assert cfg is not None
    assert cfg.driver_key == "startDate"
    assert cfg.reference_key == "endDate"


def test_get_backfill_config_mixed_query_without_backfill_and_body_backfill() -> None:
    """Non-backfill query keys must not prevent detecting body backfill."""
    input_values = {
        "advertiser_id": {"type": "SOURCE"},
        "startDate": {
            "value": "x",
            "backfill": {
                "type": "STATIC_DATE",
                "start": "2026-01-01",
                "end": "2026-01-31",
                "increment": "15 DAY",
            },
        },
        "endDate": {
            "value": "y",
            "backfill": {
                "type": "REFERENCE",
                "field": "startDate",
                "increment": "15 DAY",
                "limit": "2026-01-31",
            },
        },
    }
    cfg = get_backfill_config(input_values)
    assert cfg is not None
    assert cfg.driver_key == "startDate"


def test_get_request_input_values_for_backfill_merges_path_query_body() -> None:
    """ResourceConfig merges locations in path → query → body order."""
    # Use dict-shaped inputs (as YAML does); passing RequestInputConfig instances as
    # top-level values is incorrect because normalize_request_inputs wraps non-dicts.
    resource = ResourceConfig(
        method="GET",
        path="/accounts/{account_id}/reports",
        request_inputs={
            "account_id": {"value": "123456", "location": "path"},
            "start_date": {
                "value": {
                    "value": "{{ date('PREVIOUS_SUNDAY') }}",
                    "backfill": {
                        "type": "STATIC_DATE",
                        "start": "2021-01-03",
                        "end": "2026-04-21",
                        "inclusive": False,
                        "increment": "1 WEEK",
                    },
                },
                "location": "query",
            },
            "end_date": {
                "value": {
                    "value": "{{ date('PREVIOUS_SATURDAY') }}",
                    "backfill": {
                        "type": "REFERENCE",
                        "field": "start_date",
                        "increment": "7 DAY",
                        "limit": "2026-04-14",
                    },
                },
                "location": "query",
            },
        },
    )
    merged = resource.get_request_input_values_for_backfill()
    assert set(merged.keys()) == {"account_id", "start_date", "end_date"}
    cfg = get_backfill_config(merged)
    assert cfg is not None
    assert cfg.driver_key == "start_date"


def test_get_backfill_config_empty_returns_none() -> None:
    assert get_backfill_config({}) is None
    assert get_backfill_config(None) is None


def _valid_static_backfill(start: str = "2026-01-01") -> dict:
    return {
        "type": "STATIC_DATE",
        "start": start,
        "end": "2026-01-31",
        "increment": "15 DAY",
    }


def _valid_reference(field: str = "start_date") -> dict:
    return {
        "type": "REFERENCE",
        "field": field,
        "increment": "15 DAY",
        "limit": "2026-01-31",
    }


def test_get_backfill_config_rejects_second_static_date_driver() -> None:
    with pytest.raises(ValueError, match="at most one STATIC_DATE"):
        get_backfill_config(
            {
                "start_date": {"value": "a", "backfill": _valid_static_backfill("2026-01-01")},
                "other_start": {"value": "b", "backfill": _valid_static_backfill("2026-02-01")},
                "end_date": {"value": "c", "backfill": _valid_reference("start_date")},
            }
        )


def test_get_backfill_config_rejects_second_reference() -> None:
    with pytest.raises(ValueError, match="at most one REFERENCE"):
        get_backfill_config(
            {
                "start_date": {"value": "a", "backfill": _valid_static_backfill()},
                "end_date": {"value": "b", "backfill": _valid_reference("start_date")},
                "alt_end": {"value": "c", "backfill": _valid_reference("start_date")},
            }
        )


def test_get_backfill_config_allows_invalid_second_static_without_raise() -> None:
    """A second STATIC_DATE block that fails validation is ignored (not a duplicate driver)."""
    cfg = get_backfill_config(
        {
            "start_date": {"value": "a", "backfill": _valid_static_backfill()},
            "broken": {
                "value": "b",
                "backfill": {
                    "type": "STATIC_DATE",
                    "start": "2026-01-01",
                    "end": "2026-01-31",
                    "increment": "not-a-valid-increment",
                },
            },
            "end_date": {"value": "c", "backfill": _valid_reference("start_date")},
        }
    )
    assert cfg is not None
    assert cfg.driver_key == "start_date"
