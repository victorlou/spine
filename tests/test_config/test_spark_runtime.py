"""Tests for Spark runtime detection and connector resolution."""

import pytest

from src.config.config_models import ConnectorProvisionMode, SparkRuntimeConfig
from src.config.spark_runtime import (
    ManagedSparkPlatform,
    detect_managed_spark_platform,
    normalize_spark_event_log_uri,
    resolve_spark_runtime,
)


def test_detect_databricks(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DATABRICKS_RUNTIME_VERSION", "14.3")
    platform, src = detect_managed_spark_platform()
    assert platform == ManagedSparkPlatform.DATABRICKS
    assert "DATABRICKS" in src


def test_detect_emr_step(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("EMR_STEP_ID", "s-123")
    platform, _ = detect_managed_spark_platform()
    assert platform == ManagedSparkPlatform.EMR


def test_auto_gcs_external_on_databricks(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DATABRICKS_RUNTIME_VERSION", "14.3")
    monkeypatch.delenv("SPARK_GCS_CONNECTOR_MODE", raising=False)
    monkeypatch.delenv("SPARK_S3_CONNECTOR_MODE", raising=False)
    r = resolve_spark_runtime(SparkRuntimeConfig())
    assert r.gcs_connector_mode == "external"
    assert r.s3_connector_mode == "external"
    assert r.effective_profile == "cluster_managed"


def test_auto_gcs_packages_locally(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("SPARK_GCS_CONNECTOR_MODE", raising=False)
    monkeypatch.delenv("SPARK_S3_CONNECTOR_MODE", raising=False)
    r = resolve_spark_runtime(SparkRuntimeConfig())
    assert r.gcs_connector_mode == "packages"
    assert r.s3_connector_mode == "packages"
    assert r.effective_profile == "local_dev"


def test_env_overrides_yaml_for_gcs_mode(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DATABRICKS_RUNTIME_VERSION", "14.3")
    monkeypatch.setenv("SPARK_GCS_CONNECTOR_MODE", "packages")
    monkeypatch.delenv("SPARK_S3_CONNECTOR_MODE", raising=False)
    r = resolve_spark_runtime(
        SparkRuntimeConfig(gcs_connector_mode=ConnectorProvisionMode.EXTERNAL)
    )
    assert r.gcs_connector_mode == "packages"


def test_explicit_yaml_external_on_emr(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("SPARK_AZURE_CONNECTOR_MODE", raising=False)
    monkeypatch.setenv("EMR_CLUSTER_ID", "j-1")
    r = resolve_spark_runtime(
        SparkRuntimeConfig(azure_connector_mode=ConnectorProvisionMode.EXTERNAL)
    )
    assert r.azure_connector_mode == "external"


def test_env_overrides_yaml_for_s3_mode(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DATABRICKS_RUNTIME_VERSION", "14.3")
    monkeypatch.setenv("SPARK_S3_CONNECTOR_MODE", "packages")
    r = resolve_spark_runtime(SparkRuntimeConfig(s3_connector_mode=ConnectorProvisionMode.EXTERNAL))
    assert r.s3_connector_mode == "packages"


def test_explicit_yaml_s3_external_on_emr(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("SPARK_S3_CONNECTOR_MODE", raising=False)
    monkeypatch.setenv("EMR_CLUSTER_ID", "j-1")
    r = resolve_spark_runtime(SparkRuntimeConfig(s3_connector_mode=ConnectorProvisionMode.EXTERNAL))
    assert r.s3_connector_mode == "external"


def test_normalize_spark_event_log_uri_preserves_s3a() -> None:
    u = "s3a://bucket/prefix/events"
    assert normalize_spark_event_log_uri(u) == u


def test_resolve_spark_runtime_event_log_uri_when_enabled() -> None:
    r = resolve_spark_runtime(
        SparkRuntimeConfig(spark_event_log_enabled=True, spark_event_log_dir="/tmp/evt")
    )
    assert r.spark_event_log_dir_uri is not None
    assert r.spark_event_log_dir_uri.startswith("file:")
    assert r.spark_ui_enabled is False


def test_summary_for_log_includes_ui_and_event_flags() -> None:
    r = resolve_spark_runtime(
        SparkRuntimeConfig(
            spark_ui_enabled=True,
            spark_event_log_enabled=True,
            spark_event_log_dir="/tmp/e",
        )
    )
    s = r.summary_for_log()
    assert "spark_ui=True" in s
    assert "spark_event_log=True" in s
