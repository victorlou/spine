"""Focused tests for pipeline exception formatting and semantics."""

import pytest

from src.utils.exceptions import (
    ContextError,
    HandlerError,
    LoaderError,
    PipelineError,
    ServiceError,
)


def test_pipeline_error_requires_component_context() -> None:
    with pytest.raises(ValueError, match="Component must be specified"):
        PipelineError("missing component")


def test_pipeline_error_format_error_shape() -> None:
    err = PipelineError(
        "boom",
        component="service",
        operation="fetch",
        details={"k": "v"},
    )
    data = err.format_error()
    assert data["type"] == "PipelineError"
    assert data["component"] == "service"
    assert data["operation"] == "fetch"
    assert data["details"] == {"k": "v"}
    assert "timestamp" in data
    assert "message" in data


def test_pipeline_error_from_error_preserves_cause_and_metadata() -> None:
    cause = RuntimeError("root cause")
    wrapped = PipelineError.from_error(
        cause,
        message="wrapper",
        component="handler",
        operation="process",
        is_retryable=True,
        details={"attempt": 1},
    )
    assert wrapped.__cause__ is cause
    assert wrapped.original_error is cause
    out = wrapped.format_error()
    assert out["cause"]["type"] == "RuntimeError"
    assert out["details"]["attempt"] == 1


def test_format_unknown_error_stable_output() -> None:
    try:
        raise RuntimeError("unexpected")
    except RuntimeError as err:
        out = PipelineError.format_unknown_error(err)
    assert out["type"] == "RuntimeError"
    assert out["message"] == "unexpected"
    assert "timestamp" in out
    assert isinstance(out["traceback"], list)


def test_get_detailed_message_without_traceback_still_includes_details() -> None:
    err = PipelineError("x", component="parser", details={"record_id": "1"})
    msg = err.get_detailed_message(include_traceback=False)
    assert "Error occurred at:" not in msg
    assert "record_id" in msg


def test_subclass_detail_composition() -> None:
    h = HandlerError("h fail", operation="run")
    s = ServiceError("s fail", service_name="rest")
    l = LoaderError("l fail", destination="s3")
    c = ContextError("c fail", details={"scope": "redis"})
    assert h.component == "handler"
    assert s.details["service_name"] == "rest"
    assert l.details["destination"] == "s3"
    assert "Details: scope=redis" in c.base_message
