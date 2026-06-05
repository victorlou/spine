"""Exercise ``run_pipeline`` branches beyond CLI smoke tests."""

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

import src.main as main_module
from src.config.telemetry import TelemetryConfig


def _fake_settings(sources: dict) -> SimpleNamespace:
    """Settings stub including disabled telemetry, matching what run_pipeline reads."""
    return SimpleNamespace(
        pipeline_config=SimpleNamespace(
            sources=sources,
            defaults=SimpleNamespace(telemetry=TelemetryConfig()),
        )
    )


def test_run_pipeline_success_logs_nested_resources(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(main_module, "set_root_log_level", lambda _lvl: None)

    settings = _fake_settings({"api": object()})
    monkeypatch.setattr(main_module, "get_settings", lambda selection=None: settings)

    handler = MagicMock()
    handler.execution_plan.summarize.return_value = {"total_resources": 1}
    handler.handle.return_value = {
        "status": "success",
        "sources": {
            "api": {
                "status": "success",
                "resources": {"users": {"count": 3, "location": "s3://b/p"}},
            }
        },
    }
    monkeypatch.setattr(main_module, "DynamicHandler", lambda *a, **k: handler)

    out = main_module.run_pipeline()
    assert out["status"] == "success"


def test_run_pipeline_failed_source_logs_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(main_module, "set_root_log_level", lambda _lvl: None)
    monkeypatch.setattr(
        main_module, "get_settings", lambda selection=None: _fake_settings({"x": object()})
    )
    handler = MagicMock()
    handler.execution_plan.summarize.return_value = {}
    handler.handle.return_value = {
        "status": "failed",
        "sources": {
            "x": {
                "status": "failed",
                "error": {"code": 1},
            }
        },
    }
    monkeypatch.setattr(main_module, "DynamicHandler", lambda *a, **k: handler)
    out = main_module.run_pipeline()
    assert out["status"] == "failed"


def test_run_pipeline_keyboard_interrupt(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(main_module, "set_root_log_level", lambda _lvl: None)
    monkeypatch.setattr(
        main_module,
        "get_settings",
        lambda selection=None: _fake_settings({}),
    )

    def boom():
        raise KeyboardInterrupt()

    monkeypatch.setattr(
        main_module,
        "DynamicHandler",
        lambda *a, **k: MagicMock(
            validate=lambda: None,
            execution_plan=MagicMock(summarize=lambda: {}),
            handle=boom,
        ),
    )
    out = main_module.run_pipeline()
    assert out["status"] == "interrupted"


def test_run_pipeline_graceful_shutdown(monkeypatch: pytest.MonkeyPatch) -> None:
    from src.utils.exceptions import GracefulShutdownError

    monkeypatch.setattr(main_module, "set_root_log_level", lambda _lvl: None)
    monkeypatch.setattr(
        main_module,
        "get_settings",
        lambda selection=None: _fake_settings({}),
    )

    def boom():
        raise GracefulShutdownError("sigterm")

    monkeypatch.setattr(
        main_module,
        "DynamicHandler",
        lambda *a, **k: MagicMock(
            validate=lambda: None,
            execution_plan=MagicMock(summarize=lambda: {}),
            handle=boom,
        ),
    )
    out = main_module.run_pipeline()
    assert out["status"] == "interrupted"


def test_run_pipeline_validate_only_and_show_plan(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(main_module, "set_root_log_level", lambda _lvl: None)
    monkeypatch.setattr(
        main_module,
        "get_settings",
        lambda selection=None: _fake_settings({"a": 1}),
    )
    handler = MagicMock()
    handler.execution_plan.summarize.return_value = {"stages": []}
    monkeypatch.setattr(main_module, "DynamicHandler", lambda *a, **k: handler)

    v = main_module.run_pipeline(validate_only=True)
    assert v["message"] == "Configuration validation successful"
    handler.validate.assert_called_once()

    p = main_module.run_pipeline(show_plan=True)
    assert "Execution plan generated" in p["message"]


def test_run_pipeline_pipeline_error_format(monkeypatch: pytest.MonkeyPatch) -> None:
    from src.utils.exceptions import PipelineError

    monkeypatch.setattr(main_module, "set_root_log_level", lambda _lvl: None)
    monkeypatch.setattr(
        main_module,
        "get_settings",
        lambda selection=None: _fake_settings({}),
    )

    def boom():
        raise PipelineError("bad", operation="op")

    monkeypatch.setattr(
        main_module,
        "DynamicHandler",
        lambda *a, **k: MagicMock(
            validate=lambda: None,
            execution_plan=MagicMock(summarize=lambda: {}),
            handle=boom,
        ),
    )
    out = main_module.run_pipeline()
    assert out["status"] == "failed"


def test_run_pipeline_unknown_error_uses_format_unknown_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(main_module, "set_root_log_level", lambda _lvl: None)
    monkeypatch.setattr(
        main_module,
        "get_settings",
        lambda selection=None: _fake_settings({}),
    )

    def boom():
        raise RuntimeError("unexpected")

    monkeypatch.setattr(
        main_module,
        "DynamicHandler",
        lambda *a, **k: MagicMock(
            validate=lambda: None,
            execution_plan=MagicMock(summarize=lambda: {}),
            handle=boom,
        ),
    )
    out = main_module.run_pipeline()
    assert out["status"] == "failed"
    assert out["error"]["type"] == "RuntimeError"
