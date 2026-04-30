"""Tests for DynamicHandler loading resolution behavior."""

from types import SimpleNamespace
from typing import Callable, List, Optional
from unittest.mock import MagicMock

import pytest

from src.config.config_models import LoadingConfig
from src.handler.dynamic_handler import DynamicHandler
from src.utils.exceptions import HandlerError
from src.utils.logger import get_logger


def _make_handler(default_loading: LoadingConfig) -> DynamicHandler:
    handler = DynamicHandler.__new__(DynamicHandler)
    handler.config = SimpleNamespace(defaults=SimpleNamespace(loading=default_loading))
    return handler


def test_resolve_resource_loading_sets_default_prefix_for_gcs() -> None:
    handler = _make_handler(
        LoadingConfig(destination="gcs", gcs_bucket="gcs-bucket", prefix=None, format="delta")
    )
    resource_config = SimpleNamespace(loading=None)

    resolved = handler._resolve_resource_loading("my_source", "my_resource", resource_config)

    assert resolved is not None
    assert resolved.prefix == "my_source/my_resource"


def test_resolve_resource_loading_sets_default_prefix_for_azure_blob_alias() -> None:
    handler = _make_handler(
        LoadingConfig(
            destination="blob",
            azure_container="container",
            azure_account="account",
            prefix=None,
            format="delta",
        )
    )
    resource_config = SimpleNamespace(loading=None)

    resolved = handler._resolve_resource_loading("my_source", "my_resource", resource_config)

    assert resolved is not None
    assert resolved.destination == "azure_blob"
    assert resolved.prefix == "my_source/my_resource"


def _stage_with(source_name: str, resource_name: str) -> SimpleNamespace:
    return SimpleNamespace(
        stage_number=1,
        resources=[SimpleNamespace(source_name=source_name, resource_name=resource_name)],
    )


def _build_handler_for_handle(default_loading: LoadingConfig) -> DynamicHandler:
    handler = _make_handler(default_loading)
    handler.logger = get_logger("test_dynamic_handler_preflight")
    handler.spark = SimpleNamespace(name="fake_spark")
    handler._audit_recorder = None

    resource_config = SimpleNamespace(loading=None)
    source_config = SimpleNamespace(resources={"users": resource_config})

    handler.execution_plan = SimpleNamespace(
        stages=[_stage_with("svc", "users")],
        get_source_config=lambda name, sc=source_config: sc if name == "svc" else None,
        summarize=lambda: {"total_stages": 1, "total_resources": 1, "stages": []},
    )
    handler.config = SimpleNamespace(
        defaults=SimpleNamespace(loading=default_loading),
        sources={"svc": source_config},
    )
    return handler


def test_handle_runs_destination_preflight_before_service_creation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The preflight must fire on the regular run path, not only ``--validate-only``."""
    handler = _build_handler_for_handle(
        LoadingConfig(destination="s3", s3_bucket="my-bucket", format="delta")
    )

    create_calls: List[str] = []

    def _fail_create_service(*args, **kwargs):
        create_calls.append("called")
        raise AssertionError("_create_service must not run when preflight fails")

    handler._create_service = _fail_create_service

    captured = {}

    def _fake_preflight(spark, configs, *, write_probe=False):
        captured["spark"] = spark
        captured["configs"] = list(configs)
        captured["write_probe"] = write_probe
        raise HandlerError(
            "Cannot reach loading destination 's3' (s3a://my-bucket): forced",
            operation="destination_preflight",
            details={"destination": "s3", "s3_bucket": "my-bucket"},
        )

    monkeypatch.setattr("src.handler.dynamic_handler.preflight_destinations", _fake_preflight)
    monkeypatch.setattr(
        "src.handler.dynamic_handler.AuditRecorder", lambda: SimpleNamespace(close=lambda: None)
    )

    results = handler.handle()

    assert results["status"] == "failed"
    assert "destination_preflight" in str(results.get("error", {}))
    assert captured["write_probe"] is False
    assert captured["spark"] is handler.spark
    assert any(c.destination == "s3" for c in captured["configs"])
    assert create_calls == [], "service creation must be short-circuited by preflight failure"


def _patch_handle_preflight_and_audit(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("src.handler.dynamic_handler.preflight_destinations", lambda *a, **k: None)
    monkeypatch.setattr(
        "src.handler.dynamic_handler.AuditRecorder", lambda: SimpleNamespace(close=lambda: None)
    )


def _stage_with_resources(
    stage_number: int, *source_resource_pairs: tuple[str, str]
) -> SimpleNamespace:
    resources = [SimpleNamespace(source_name=s, resource_name=r) for s, r in source_resource_pairs]
    return SimpleNamespace(stage_number=stage_number, resources=resources)


def _build_handler_for_handle_stage_loop(
    *,
    stages: List[SimpleNamespace],
    get_source_config: Callable[[str], Optional[SimpleNamespace]],
    sources_by_name: dict[str, SimpleNamespace],
    default_loading: Optional[LoadingConfig] = None,
) -> DynamicHandler:
    """Minimal handler for exercising ``handle()`` after preflight (see `_patch_handle_preflight_and_audit`)."""
    loading = default_loading or LoadingConfig(
        destination="local",
        format="delta",
        write_mode="overwrite",
        storage_root="/tmp/spine-handle-loop-test",
        prefix=None,
    )
    handler = _make_handler(loading)
    handler.logger = get_logger("test_handle_stage_loop")
    handler.spark = MagicMock(name="spark")
    handler.spark_manager = MagicMock()
    handler.spark_manager.stop_session = MagicMock()
    handler.redis_context = MagicMock()
    handler.redis_context.cleanup = MagicMock()
    handler.settings = SimpleNamespace(loading_destinations=set())
    handler._audit_recorder = None
    handler.config = SimpleNamespace(
        defaults=SimpleNamespace(loading=loading),
        sources=sources_by_name,
    )
    handler.execution_plan = SimpleNamespace(
        stages=stages,
        get_source_config=get_source_config,
        summarize=lambda: {"total_stages": len(stages), "stub": True},
    )
    return handler


def test_handle_stage_loop_success(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_handle_preflight_and_audit(monkeypatch)
    resource_config = SimpleNamespace(loading=None)
    source_config = SimpleNamespace(resources={"users": resource_config})
    handler = _build_handler_for_handle_stage_loop(
        stages=[_stage_with("svc", "users")],
        get_source_config=lambda n, sc=source_config: sc if n == "svc" else None,
        sources_by_name={"svc": source_config},
    )
    svc = MagicMock(name="service")
    handler._create_service = MagicMock(return_value=svc)
    handler._process_resource = MagicMock(
        return_value={"status": "success", "count": 3, "location": "redis:svc:users"}
    )

    results = handler.handle()

    assert results["status"] == "success"
    assert results["sources"]["svc"]["status"] == "success"
    assert results["sources"]["svc"]["resources"]["users"]["count"] == 3
    handler._create_service.assert_called_once_with("svc", source_config)
    handler._process_resource.assert_called_once()


def test_handle_stage_loop_missing_source_config_skips_service(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_handle_preflight_and_audit(monkeypatch)
    handler = _build_handler_for_handle_stage_loop(
        stages=[_stage_with("missing_src", "r1")],
        get_source_config=lambda _n: None,
        sources_by_name={},
    )
    handler._create_service = MagicMock()
    handler._process_resource = MagicMock()

    results = handler.handle()

    assert results["status"] == "failed"
    assert results["sources"]["missing_src"]["status"] == "failed"
    assert "Source configuration not found" in str(
        results["sources"]["missing_src"].get("error", {})
    )
    handler._create_service.assert_not_called()
    handler._process_resource.assert_not_called()


def test_handle_stage_loop_failed_resource_result_sets_source_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_handle_preflight_and_audit(monkeypatch)
    resource_config = SimpleNamespace(loading=None)
    source_config = SimpleNamespace(resources={"users": resource_config})
    handler = _build_handler_for_handle_stage_loop(
        stages=[_stage_with("svc", "users")],
        get_source_config=lambda n, sc=source_config: sc if n == "svc" else None,
        sources_by_name={"svc": source_config},
    )
    handler._create_service = MagicMock(return_value=MagicMock())
    handler._process_resource = MagicMock(
        return_value={"status": "failed", "warning": "upstream rejected"}
    )

    results = handler.handle()

    assert results["status"] == "failed"
    sr = results["sources"]["svc"]
    assert sr["status"] == "failed"
    assert sr["error"]["error"] == "upstream rejected"


def test_handle_stage_loop_process_resource_runtime_error_continues(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_handle_preflight_and_audit(monkeypatch)
    rc_u = SimpleNamespace(loading=None)
    rc_v = SimpleNamespace(loading=None)
    source_config = SimpleNamespace(resources={"u": rc_u, "v": rc_v})
    handler = _build_handler_for_handle_stage_loop(
        stages=[_stage_with_resources(1, ("svc", "u"), ("svc", "v"))],
        get_source_config=lambda n, sc=source_config: sc if n == "svc" else None,
        sources_by_name={"svc": source_config},
    )
    handler._create_service = MagicMock(return_value=MagicMock())

    def _proc(**kwargs):
        name = kwargs["resource_meta"].resource_name
        if name == "u":
            raise RuntimeError("boom")
        return {"status": "success", "count": 1}

    handler._process_resource = MagicMock(side_effect=_proc)

    results = handler.handle()

    assert results["status"] == "failed"
    assert results["sources"]["svc"]["status"] == "failed"
    assert "boom" in results["sources"]["svc"]["error"]["error"]
    assert handler._process_resource.call_count == 2


def test_handle_stage_loop_process_resource_handler_error_enriches_error_details(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_handle_preflight_and_audit(monkeypatch)
    resource_config = SimpleNamespace(loading=None)
    source_config = SimpleNamespace(resources={"users": resource_config})
    handler = _build_handler_for_handle_stage_loop(
        stages=[_stage_with("svc", "users")],
        get_source_config=lambda n, sc=source_config: sc if n == "svc" else None,
        sources_by_name={"svc": source_config},
    )
    handler._create_service = MagicMock(return_value=MagicMock())
    inner = ValueError("root cause")
    handler._process_resource = MagicMock(
        side_effect=HandlerError(
            "wrapped",
            operation="process_resource",
            details={"k": "v"},
            original_error=inner,
        )
    )

    results = handler.handle()

    err = results["sources"]["svc"]["error"]
    assert err["operation"] == "process_resource"
    assert err["details"] == {"k": "v"}
    assert "root cause" in (err.get("original_error") or "")


def test_handle_stage_loop_marks_multiple_sources_success(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_handle_preflight_and_audit(monkeypatch)
    cfg_a = SimpleNamespace(resources={"r1": SimpleNamespace(loading=None)})
    cfg_b = SimpleNamespace(resources={"r2": SimpleNamespace(loading=None)})
    handler = _build_handler_for_handle_stage_loop(
        stages=[_stage_with_resources(1, ("src_a", "r1"), ("src_b", "r2"))],
        get_source_config=lambda n, ca=cfg_a, cb=cfg_b: (
            ca if n == "src_a" else cb if n == "src_b" else None
        ),
        sources_by_name={"src_a": cfg_a, "src_b": cfg_b},
    )
    handler._create_service = MagicMock(return_value=MagicMock())
    handler._process_resource = MagicMock(return_value={"status": "success", "count": 0})

    results = handler.handle()

    assert results["status"] == "success"
    assert results["sources"]["src_a"]["status"] == "success"
    assert results["sources"]["src_b"]["status"] == "success"


def test_handle_stage_loop_outer_except_when_stages_not_iterable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_handle_preflight_and_audit(monkeypatch)
    resource_config = SimpleNamespace(loading=None)
    source_config = SimpleNamespace(resources={"users": resource_config})
    handler = _build_handler_for_handle_stage_loop(
        stages=[_stage_with("svc", "users")],
        get_source_config=lambda n, sc=source_config: sc if n == "svc" else None,
        sources_by_name={"svc": source_config},
    )
    handler.execution_plan = SimpleNamespace(
        stages=None,
        get_source_config=handler.execution_plan.get_source_config,
        summarize=lambda: {"broken": True},
    )
    handler._create_service = MagicMock()

    results = handler.handle()

    assert results["status"] == "failed"
    assert "error" in results
    handler._create_service.assert_not_called()
