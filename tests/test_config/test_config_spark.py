"""Tests for destination-aware Spark config composition."""

import os

import pytest

from src.config.config_models import ConnectorProvisionMode, SparkRuntimeConfig
from src.config.config_spark import _HADOOP_AWS_PKG, SparkSessionConf
from src.config.spark_runtime import resolve_spark_runtime


@pytest.fixture(autouse=True)
def _clear_managed_spark_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Default each test to a local-dev signal so GCS readiness branches deterministically."""
    for key in (
        "DATABRICKS_RUNTIME_VERSION",
        "EMR_STEP_ID",
        "EMR_CLUSTER_ID",
        "ECS_CONTAINER_METADATA_URI",
        "KUBERNETES_SERVICE_HOST",
    ):
        monkeypatch.delenv(key, raising=False)


def test_managed_configs_sets_driver_bind_for_embedded_spark() -> None:
    """IAM/managed path must set driver bind; otherwise Spark can fail on cloud VMs/containers."""
    resolved = resolve_spark_runtime(SparkRuntimeConfig())
    cfg = SparkSessionConf.get_configs_for_destinations(
        destinations={"local"},
        use_explicit_credentials=False,
        resolved=resolved,
    )
    assert cfg.get("spark.driver.bindAddress") == "127.0.0.1"
    assert cfg.get("spark.driver.host") == "127.0.0.1"


def test_get_configs_local_only_has_no_s3a_keys() -> None:
    cfg = SparkSessionConf.get_configs_for_destinations(
        destinations={"local"},
        use_explicit_credentials=False,
    )
    assert "spark.hadoop.fs.s3a.impl" not in cfg
    assert "spark.hadoop.fs.s3a.endpoint" not in cfg


def test_get_configs_s3_has_s3a_keys() -> None:
    cfg = SparkSessionConf.get_configs_for_destinations(
        destinations={"s3"},
        use_explicit_credentials=False,
        aws_region="us-east-1",
    )
    assert cfg["spark.hadoop.fs.s3a.impl"] == "org.apache.hadoop.fs.s3a.S3AFileSystem"
    assert cfg["spark.hadoop.fs.s3a.endpoint"] == "s3.us-east-1.amazonaws.com"


def test_get_configs_gcs_and_azure_have_connector_hooks() -> None:
    cfg = SparkSessionConf.get_configs_for_destinations(
        destinations={"gcs", "azure_blob"},
        use_explicit_credentials=False,
    )
    assert cfg["spark.hadoop.fs.gs.impl"] == "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem"
    assert cfg["spark.hadoop.fs.gs.auth.type"] == "APPLICATION_DEFAULT"
    assert cfg["spark.hadoop.fs.abfs.impl"] == "org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem"


def test_get_configs_gcs_auth_type_env_override(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SPINE_GCS_AUTH_TYPE", "COMPUTE_ENGINE")
    cfg = SparkSessionConf.get_configs_for_destinations(
        destinations={"gcs"},
        use_explicit_credentials=False,
    )
    assert cfg["spark.hadoop.fs.gs.auth.type"] == "COMPUTE_ENGINE"
    monkeypatch.delenv("SPINE_GCS_AUTH_TYPE", raising=False)


def test_get_configs_gcs_application_default_uses_gac_keyfile(
    monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    cred = tmp_path / "gac.json"
    cred.write_text("{}", encoding="utf-8")
    monkeypatch.setenv("GOOGLE_APPLICATION_CREDENTIALS", str(cred))
    monkeypatch.delenv("SPINE_GCS_AUTH_TYPE", raising=False)

    cfg = SparkSessionConf.get_configs_for_destinations(
        destinations={"gcs"},
        use_explicit_credentials=False,
    )
    assert cfg["spark.hadoop.google.cloud.auth.service.account.json.keyfile"] == str(cred)


def test_get_configs_gcs_application_default_uses_adc_keyfile(
    monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.delenv("GOOGLE_APPLICATION_CREDENTIALS", raising=False)
    monkeypatch.delenv("SPINE_GCS_AUTH_TYPE", raising=False)
    adc_dir = tmp_path / ".config" / "gcloud"
    adc_dir.mkdir(parents=True)
    adc = adc_dir / "application_default_credentials.json"
    adc.write_text("{}", encoding="utf-8")

    cfg = SparkSessionConf.get_configs_for_destinations(
        destinations={"gcs"},
        use_explicit_credentials=False,
    )
    assert cfg["spark.hadoop.google.cloud.auth.service.account.json.keyfile"] == str(adc)


def test_resolve_spark_packages_deduplicates_packages() -> None:
    resolved = resolve_spark_runtime(SparkRuntimeConfig())
    packages = SparkSessionConf._resolve_spark_packages({"s3", "local"}, resolved)
    assert len(packages) == len(set(packages))


def test_runtime_readiness_errors_for_empty_gcs_jar_url(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("SPARK_GCS_CONNECTOR_JAR_URL", "")
    cfg = SparkSessionConf.get_configs_for_destinations(
        destinations={"gcs"},
        use_explicit_credentials=False,
    )
    assert "spark.jars" not in cfg
    os.environ.pop("SPARK_GCS_CONNECTOR_JAR_URL", None)


def test_startup_summary_gcs_default_auth_and_jar() -> None:
    summary = SparkSessionConf.startup_summary(
        destinations={"gcs"},
        use_explicit_credentials=False,
    )
    assert "gcs_auth_type=APPLICATION_DEFAULT" in summary
    assert "gcs_connector_jar=" in summary


def test_startup_summary_gcs_auth_override(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SPINE_GCS_AUTH_TYPE", "COMPUTE_ENGINE")
    summary = SparkSessionConf.startup_summary(
        destinations={"gcs"},
        use_explicit_credentials=False,
    )
    assert "gcs_auth_type=COMPUTE_ENGINE" in summary
    monkeypatch.delenv("SPINE_GCS_AUTH_TYPE", raising=False)


def test_get_configs_gcs_packages_adds_shaded_connector_jar() -> None:
    cfg = SparkSessionConf.get_configs_for_destinations(
        destinations={"gcs"},
        use_explicit_credentials=False,
    )
    jars = cfg.get("spark.jars") or ""
    assert "gcs-connector-hadoop3-2.2.17-shaded.jar" in jars
    assert "gcs-connector" not in (cfg.get("spark.jars.packages") or "")


def test_resolve_spark_packages_omits_gcs_ivy_coordinate() -> None:
    resolved = resolve_spark_runtime(SparkRuntimeConfig())
    pkgs = SparkSessionConf._resolve_spark_packages({"gcs"}, resolved)
    assert not any("gcs-connector" in p for p in pkgs)


def test_resolve_spark_packages_omits_gcs_when_external() -> None:
    cfg = SparkRuntimeConfig(gcs_connector_mode=ConnectorProvisionMode.EXTERNAL)
    resolved = resolve_spark_runtime(cfg)
    pkgs = SparkSessionConf._resolve_spark_packages({"gcs"}, resolved)
    assert not any("gcs-connector" in p for p in pkgs)


def test_resolve_spark_packages_omits_hadoop_aws_when_s3_external() -> None:
    cfg = SparkRuntimeConfig(s3_connector_mode=ConnectorProvisionMode.EXTERNAL)
    resolved = resolve_spark_runtime(cfg)
    pkgs = SparkSessionConf._resolve_spark_packages({"s3"}, resolved)
    assert _HADOOP_AWS_PKG not in pkgs


def test_runtime_readiness_notes_include_s3_when_present() -> None:
    summary = SparkSessionConf.startup_summary(
        destinations={"s3", "local"},
        use_explicit_credentials=False,
    )
    assert "s3_auth=default_chain" in summary
    assert "Spark destinations=" in summary
