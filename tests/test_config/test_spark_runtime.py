"""Tests for Spark runtime detection and connector resolution."""

import pytest

from src.config.config_models import ConnectorProvisionMode, SparkRuntimeConfig
from src.config.spark_runtime import (
    ManagedSparkPlatform,
    detect_managed_spark_platform,
    resolve_spark_runtime,
)


def test_detect_databricks(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DATABRICKS_RUNTIME_VERSION", "14.3")
    monkeypatch.delenv("EMR_STEP_ID", raising=False)
    monkeypatch.delenv("EMR_CLUSTER_ID", raising=False)
    monkeypatch.delenv("ECS_CONTAINER_METADATA_URI", raising=False)
    monkeypatch.delenv("KUBERNETES_SERVICE_HOST", raising=False)
    platform, src = detect_managed_spark_platform()
    assert platform == ManagedSparkPlatform.DATABRICKS
    assert "DATABRICKS" in src


def test_detect_emr_step(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("DATABRICKS_RUNTIME_VERSION", raising=False)
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
    for key in (
        "DATABRICKS_RUNTIME_VERSION",
        "EMR_STEP_ID",
        "EMR_CLUSTER_ID",
        "ECS_CONTAINER_METADATA_URI",
        "KUBERNETES_SERVICE_HOST",
    ):
        monkeypatch.delenv(key, raising=False)
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
