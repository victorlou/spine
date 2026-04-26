"""Tests for destination-aware Spark config composition."""

import os

from src.config.config_models import ConnectorProvisionMode, SparkRuntimeConfig
from src.config.config_spark import _GCS_CONNECTOR_PKG, _HADOOP_AWS_PKG, SparkSessionConf
from src.config.spark_runtime import resolve_spark_runtime


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
    assert cfg["spark.hadoop.fs.abfs.impl"] == "org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem"


def test_resolve_spark_packages_deduplicates_packages() -> None:
    resolved = resolve_spark_runtime(SparkRuntimeConfig())
    packages = SparkSessionConf._resolve_spark_packages({"s3", "local"}, resolved)
    assert len(packages) == len(set(packages))


def test_runtime_readiness_errors_for_empty_gcs_package(monkeypatch) -> None:
    monkeypatch.setenv("SPARK_GCS_CONNECTOR_MODE", "packages")
    monkeypatch.setenv("SPARK_GCS_CONNECTOR_PACKAGE", "")
    errors = SparkSessionConf.get_runtime_readiness_errors({"gcs"})
    assert errors and any("packages" in error for error in errors)
    os.environ.pop("SPARK_GCS_CONNECTOR_PACKAGE", None)


def test_resolve_spark_packages_omits_gcs_when_external() -> None:
    cfg = SparkRuntimeConfig(gcs_connector_mode=ConnectorProvisionMode.EXTERNAL)
    resolved = resolve_spark_runtime(cfg)
    pkgs = SparkSessionConf._resolve_spark_packages({"gcs"}, resolved)
    assert _GCS_CONNECTOR_PKG not in pkgs


def test_resolve_spark_packages_omits_hadoop_aws_when_s3_external() -> None:
    cfg = SparkRuntimeConfig(s3_connector_mode=ConnectorProvisionMode.EXTERNAL)
    resolved = resolve_spark_runtime(cfg)
    pkgs = SparkSessionConf._resolve_spark_packages({"s3"}, resolved)
    assert _HADOOP_AWS_PKG not in pkgs


def test_runtime_readiness_notes_include_s3_when_present() -> None:
    notes = SparkSessionConf.get_runtime_readiness_notes(
        {"s3", "local"}, use_explicit_credentials=False
    )
    assert any("S3 destination" in n for n in notes)
    assert any("Local destination" in n for n in notes)
