"""Plan-time validation for database-backed resources and request-context expansion."""

import pytest

from src.config.config_models import ResourceConfig, SourceType
from src.planner.database_request_context import (
    validate_plan_time_static_database_request_context_expansion,
)
from src.planner.execution_plan import ResourceMetadata
from src.utils.dynamic_values import (
    ComplexDynamicValue,
    DynamicSourceReference,
    DynamicValueType,
)
from src.utils.exceptions import PlanningError


def test_plan_time_rejects_static_multi_context_database_resource() -> None:
    rc = ResourceConfig(
        method="GET",
        database_schema="public",
        database_table="users",
        request_inputs={
            "ids": {
                "value": [1, 2, 3],
                "location": "query",
                "batch_size": 1,
            }
        },
    )
    meta = ResourceMetadata(
        source_name="pg",
        resource_name="users",
        dependencies=set(),
        batch_inputs={"ids": 1},
        config=rc,
    )
    with pytest.raises(PlanningError) as exc_info:
        validate_plan_time_static_database_request_context_expansion(
            meta, source_type=SourceType.POSTGRESQL
        )
    assert "database-backed resource cannot expand" in exc_info.value.message
    assert exc_info.value.details["estimated_request_context_count"] == 3


def test_plan_time_allows_single_context_database_with_static_batch() -> None:
    rc = ResourceConfig(
        method="GET",
        database_schema="public",
        database_table="users",
        request_inputs={
            "ids": {
                "value": [42],
                "location": "query",
                "batch_size": 1,
            }
        },
    )
    meta = ResourceMetadata(
        source_name="pg",
        resource_name="users",
        dependencies=set(),
        batch_inputs={"ids": 1},
        config=rc,
    )
    validate_plan_time_static_database_request_context_expansion(
        meta, source_type=SourceType.POSTGRESQL
    )


def test_plan_time_skips_non_database_source() -> None:
    rc = ResourceConfig(
        method="GET",
        path="/x",
        request_inputs={
            "ids": {
                "value": [1, 2],
                "location": "query",
                "batch_size": 1,
            }
        },
    )
    meta = ResourceMetadata(
        source_name="api",
        resource_name="r",
        dependencies=set(),
        batch_inputs={"ids": 1},
        config=rc,
    )
    validate_plan_time_static_database_request_context_expansion(
        meta, source_type=SourceType.REST_API
    )


def test_plan_time_skips_when_estimate_unknown_dynamic_batch() -> None:
    """Dependencies make estimate_request_count None — planner cannot prove context count."""

    rc = ResourceConfig(
        method="GET",
        database_schema="public",
        database_table="users",
        request_inputs={
            "parent_id": {
                "value": ComplexDynamicValue(
                    type=DynamicValueType.SOURCE,
                    source_config=DynamicSourceReference(source="other", field="id"),
                ),
                "location": "query",
                "batch_size": 1,
            }
        },
    )
    meta = ResourceMetadata(
        source_name="pg",
        resource_name="users",
        dependencies={"pg.other"},
        batch_inputs={"parent_id": 1},
        config=rc,
    )
    validate_plan_time_static_database_request_context_expansion(
        meta, source_type=SourceType.POSTGRESQL
    )
