"""Tests for DynamicHandler loading resolution behavior."""

from types import SimpleNamespace
from typing import List

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
