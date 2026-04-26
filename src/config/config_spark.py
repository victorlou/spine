"""
Spark session bootstrap: Ivy package lists, Hadoop ``fs.*`` overlays, and PySpark env.

This module composes ``SparkSession`` builder settings. Runtime policy (YAML + host detection)
lives in :mod:`src.config.spark_runtime`; process lifecycle and AWS credential loading live in
:mod:`src.utils.spark_manager`.

**Side effect:** :meth:`SparkSessionConf.get_java_options` sets ``os.environ`` entries consumed
by the PySpark JVM launcher (``PYSPARK_SUBMIT_ARGS``, ``SPARK_SUBMIT_OPTS``). Call it only from
session initialization, not at import time; tests that assert on env should reset or isolate.
"""

import logging
import os
from typing import Any, Dict, Optional, Set

from src.config.config_models import SparkRuntimeConfig
from src.config.spark_runtime import ResolvedSparkRuntime, resolve_spark_runtime

# Maven coordinates Spark resolves at startup (Ivy). Keep ngdbc pin aligned with smoke testing.
_HADOOP_AWS_PKG = "org.apache.hadoop:hadoop-aws:3.3.4"
_HADOOP_AZURE_PKG = "org.apache.hadoop:hadoop-azure:3.3.4"
_GCS_CONNECTOR_PKG = "com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.17"
_DELTA_PKG = "io.delta:delta-spark_2.12:3.1.0"
_ICEBERG_PKG = "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.10.1"
_SAP_NGDBC_PKG = "com.sap.cloud.db.jdbc:ngdbc:2.23.10"
SPARK_BASE_JARS_PACKAGES = ",".join([_DELTA_PKG, _ICEBERG_PKG, _SAP_NGDBC_PKG])

_ICEBERG_EXTENSIONS = "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
_DELTA_EXTENSIONS = "io.delta.sql.DeltaSparkSessionExtension"
SPARK_EXTENSIONS = ",".join([_ICEBERG_EXTENSIONS, _DELTA_EXTENSIONS])

# (loading destination id, ResolvedSparkRuntime field, static Maven coord or None for GCS dynamic)
_CONNECTOR_IVY_SPECS: tuple[tuple[str, str, Optional[str]], ...] = (
    ("s3", "s3_connector_mode", _HADOOP_AWS_PKG),
    ("azure_blob", "azure_connector_mode", _HADOOP_AZURE_PKG),
    ("gcs", "gcs_connector_mode", None),
)


def _gcs_package_coordinate() -> str:
    return os.getenv("SPARK_GCS_CONNECTOR_PACKAGE", _GCS_CONNECTOR_PKG).strip()


def _effective_resolved(resolved: Optional[ResolvedSparkRuntime]) -> ResolvedSparkRuntime:
    return resolved or resolve_spark_runtime(SparkRuntimeConfig())


def _hadoop_filesystem_impl_layer(destinations: Set[str]) -> Dict[str, str]:
    """Shared ``fs.*.impl`` entries for GCS and ABFS (identical for local and managed Spark)."""
    layer: Dict[str, str] = {}
    if "azure_blob" in destinations:
        layer["spark.hadoop.fs.abfs.impl"] = "org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem"
    if "gcs" in destinations:
        layer["spark.hadoop.fs.gs.impl"] = "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem"
    return layer


def _s3a_endpoint_and_filesystem(s3_region: str) -> Dict[str, str]:
    """S3A settings shared between local and managed modes (excluding credentials)."""
    return {
        "spark.hadoop.fs.s3a.endpoint": f"s3.{s3_region}.amazonaws.com",
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "spark.hadoop.fs.s3a.path.style.access": "true",
    }


def _readiness_note_s3(resolved: ResolvedSparkRuntime, use_explicit_credentials: bool) -> str:
    auth = (
        "explicit Spark S3A keys from the configured credential path"
        if use_explicit_credentials
        else "DefaultAWSCredentialsProviderChain (IAM role, profile, environment, or web identity)"
    )
    return (
        "S3 destination requires S3A filesystem settings and AWS auth in Spark "
        f"({auth}). Resolved hadoop-aws connector mode={resolved.s3_connector_mode!r} "
        "(``packages`` pulls Ivy coordinates; ``external`` assumes the cluster provides hadoop-aws). "
        "S3A endpoint region follows the AWS credential chain or standard AWS environment variables, not "
        "``defaults.spark_runtime``."
    )


def _readiness_note_gcs(resolved: ResolvedSparkRuntime) -> str:
    return (
        "GCS destination requires Hadoop GCS connector and Google auth in the Spark runtime "
        f"(resolved mode={resolved.gcs_connector_mode!r})."
    )


def _readiness_note_azure(resolved: ResolvedSparkRuntime) -> str:
    return (
        "Azure Blob destination requires ABFS connector and storage auth in the Spark runtime "
        f"(resolved mode={resolved.azure_connector_mode!r})."
    )


def _readiness_note_local() -> str:
    return "Local destination uses file://; ensure storage_root exists and is writable."


_READINESS_NOTE_BUILDERS = {
    "s3": lambda _d, r, u: _readiness_note_s3(r, u),
    "gcs": lambda _d, r, _u: _readiness_note_gcs(r),
    "azure_blob": lambda _d, r, _u: _readiness_note_azure(r),
    "local": lambda _d, _r, _u: _readiness_note_local(),
}


class SparkSessionConf:
    """Compose Spark builder configs, Ivy packages, and operator readiness messaging."""

    @staticmethod
    def get_java_options(
        destinations: Optional[Set[str]] = None, resolved: Optional[ResolvedSparkRuntime] = None
    ) -> None:
        """
        Set Java / PySpark launcher environment variables for Ivy package resolution.

        Mutates ``os.environ`` (``PYSPARK_SUBMIT_ARGS``, ``SPARK_SUBMIT_OPTS``); see module docstring.
        """
        os.environ["SPARK_SUBMIT_OPTS"] = "-Dlog4j.logger.org.apache.spark.repl.Main=ERROR"

        destinations = destinations or {"local"}
        eff = _effective_resolved(resolved)
        spark_packages = SparkSessionConf._resolve_spark_packages(destinations, eff)
        os.environ["PYSPARK_SUBMIT_ARGS"] = (
            f"--packages {','.join(spark_packages)} "
            "--conf spark.ui.showConsoleProgress=false "
            "pyspark-shell"
        )

        logging.getLogger("py4j").setLevel(logging.ERROR)

    @staticmethod
    def _base_configs() -> Dict[str, Any]:
        return {
            "spark.app.name": "DataIngestion",
            "spark.driver.memory": "4g",
            "spark.memory.fraction": "0.8",
            "spark.memory.storageFraction": "0.3",
            "spark.sql.parquet.compression.codec": "snappy",
            "spark.metrics.enabled": "false",
            "spark.sql.extensions": SPARK_EXTENSIONS,
            "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            "spark.sql.catalog.iceberg": "org.apache.iceberg.spark.SparkCatalog",
            "spark.sql.catalog.iceberg.type": "hadoop",
            "spark.driver.extraJavaOptions": "-Djava.security.manager=allow",
            "spark.log.level": "ERROR",
        }

    @staticmethod
    def _resolve_spark_packages(
        destinations: Set[str], resolved: ResolvedSparkRuntime
    ) -> list[str]:
        packages = [_DELTA_PKG, _ICEBERG_PKG, _SAP_NGDBC_PKG]
        for dest, mode_attr, static_coord in _CONNECTOR_IVY_SPECS:
            if dest not in destinations:
                continue
            if getattr(resolved, mode_attr) != "packages":
                continue
            if static_coord is not None:
                packages.append(static_coord)
            else:
                coord = _gcs_package_coordinate()
                if coord:
                    packages.append(coord)

        deduped: list[str] = []
        seen = set()
        for pkg in packages:
            if pkg and pkg not in seen:
                deduped.append(pkg)
                seen.add(pkg)
        return deduped

    @staticmethod
    def get_runtime_readiness_errors(
        destinations: Set[str], resolved: Optional[ResolvedSparkRuntime] = None
    ) -> list[str]:
        errors: list[str] = []
        eff = _effective_resolved(resolved)
        if "gcs" in destinations and eff.gcs_connector_mode == "packages":
            pkg = _gcs_package_coordinate()
            if not pkg:
                errors.append(
                    "GCS destination requires a non-empty Maven coordinate when resolved connector mode is "
                    "'packages' (set defaults.spark_runtime / env SPARK_GCS_CONNECTOR_PACKAGE)."
                )
        return errors

    @staticmethod
    def get_local_configs(
        destinations: Set[str],
        aws_access_key: str,
        aws_secret_key: str,
        s3_region: str = "us-east-1",
        aws_session_token: Optional[str] = None,
        resolved: Optional[ResolvedSparkRuntime] = None,
    ) -> Dict[str, Any]:
        """
        Spark configuration for local-mode master with optional explicit S3A keys.

        ``s3_region`` is only applied when ``s3`` is present in ``destinations``.
        """
        eff = _effective_resolved(resolved)
        config = {
            "spark.master": "local[*]",
            "spark.driver.bindAddress": "127.0.0.1",
            "spark.driver.host": "127.0.0.1",
            "spark.ui.enabled": "false",
            **SparkSessionConf._base_configs(),
            "spark.jars.packages": ",".join(
                SparkSessionConf._resolve_spark_packages(destinations, eff)
            ),
        }

        if "s3" in destinations:
            config.update(_s3a_endpoint_and_filesystem(s3_region))
            if aws_access_key and aws_secret_key:
                config["spark.hadoop.fs.s3a.access.key"] = aws_access_key
                config["spark.hadoop.fs.s3a.secret.key"] = aws_secret_key
            if aws_session_token:
                config["spark.hadoop.fs.s3a.session.token"] = aws_session_token

        config.update(_hadoop_filesystem_impl_layer(destinations))
        return config

    @staticmethod
    def get_runtime_readiness_notes(
        destinations: Set[str],
        resolved: Optional[ResolvedSparkRuntime] = None,
        use_explicit_credentials: bool = False,
    ) -> list[str]:
        eff = _effective_resolved(resolved)
        notes: list[str] = []
        for dest in sorted(destinations):
            builder = _READINESS_NOTE_BUILDERS.get(dest)
            if builder:
                notes.append(builder(dest, eff, use_explicit_credentials))
        return notes

    @staticmethod
    def get_configs_for_destinations(
        destinations: Set[str],
        use_explicit_credentials: bool,
        aws_access_key: str = "",
        aws_secret_key: str = "",
        aws_region: str = "",
        aws_session_token: Optional[str] = None,
        resolved: Optional[ResolvedSparkRuntime] = None,
    ) -> Dict[str, Any]:
        """
        Return Spark configs for the effective destination set and credential mode.

        When ``s3`` is in ``destinations``, ``aws_region`` should be the region for the S3A endpoint
        (from credentials or environment); it is ignored when ``s3`` is absent.
        """
        eff = _effective_resolved(resolved)
        region_for_s3 = (aws_region or "").strip() or "us-east-1"
        if use_explicit_credentials:
            return SparkSessionConf.get_local_configs(
                destinations,
                aws_access_key,
                aws_secret_key,
                region_for_s3,
                aws_session_token,
                resolved=eff,
            )
        return SparkSessionConf._compose_managed_configs(destinations, region_for_s3, eff)

    @staticmethod
    def _compose_managed_configs(
        destinations: Set[str], s3_region: str, resolved: ResolvedSparkRuntime
    ) -> Dict[str, Any]:
        config = {
            **SparkSessionConf._base_configs(),
            "spark.jars.packages": ",".join(
                SparkSessionConf._resolve_spark_packages(destinations, resolved)
            ),
        }
        if "s3" in destinations:
            config["spark.hadoop.fs.s3a.aws.credentials.provider"] = (
                "com.amazonaws.auth.DefaultAWSCredentialsProviderChain"
            )
            config.update(_s3a_endpoint_and_filesystem(s3_region))
        config.update(_hadoop_filesystem_impl_layer(destinations))
        return config
