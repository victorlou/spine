"""Runtime guards when database resources expand to multiple request contexts."""

import pytest

from src.handler.base_handler import HandlerError
from src.planner.database_request_context import (
    reject_if_database_has_multiple_runtime_request_contexts,
)


def test_reject_multiple_contexts_for_database_raises() -> None:
    with pytest.raises(HandlerError) as exc_info:
        reject_if_database_has_multiple_runtime_request_contexts(
            is_db=True,
            request_context_count=2,
            resource_name="users",
            source_name="pg",
            batch_input_keys=["ids"],
            use_backfill=False,
        )
    err = exc_info.value
    assert err.operation == "process_resource"
    assert err.details["request_context_count"] == 2
    assert err.details["batch_input_keys"] == ["ids"]
    assert err.details["use_backfill"] is False
    assert "Database resources cannot expand" in str(err)


def test_reject_multiple_contexts_skipped_when_not_database() -> None:
    reject_if_database_has_multiple_runtime_request_contexts(
        is_db=False,
        request_context_count=5,
        resource_name="api",
        source_name="rest",
        batch_input_keys=["x"],
        use_backfill=True,
    )


def test_reject_multiple_contexts_skipped_when_single_context() -> None:
    reject_if_database_has_multiple_runtime_request_contexts(
        is_db=True,
        request_context_count=1,
        resource_name="users",
        source_name="pg",
        batch_input_keys=[],
        use_backfill=False,
    )


def test_reject_multiple_contexts_skipped_when_zero_contexts() -> None:
    reject_if_database_has_multiple_runtime_request_contexts(
        is_db=True,
        request_context_count=0,
        resource_name="users",
        source_name="pg",
        batch_input_keys=[],
        use_backfill=False,
    )
