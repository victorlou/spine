"""
Spark session bootstrap: package coordinates, Hadoop ``fs.*`` overlays, and launcher env.

This module composes ``SparkSession`` builder settings. Runtime policy (YAML + host detection)
lives in :mod:`src.config.spark_runtime`; Spark session lifecycle and per-destination credential
loading (S3 via :mod:`src.utils.spark_manager`) live alongside that module.

GCS is wired through a single path: when connector mode is ``packages`` we append the official
shaded connector JAR URL to ``spark.jars`` and default to
``spark.hadoop.fs.gs.auth.type=APPLICATION_DEFAULT``. This matches local ADC and service-account
flows while still allowing metadata auth via ``SPINE_GCS_AUTH_TYPE`` on managed runtimes.
"""

import logging
import os
from pathlib import Path
from typing import Any, Dict, Optional, Set

from src.config.config_models import SparkRuntimeConfig
from src.config.spark_runtime import (
    ManagedSparkPlatform,
    ResolvedSparkRuntime,
    resolve_spark_runtime,
)

# Maven coordinates Spark resolves at startup (Ivy). Keep ngdbc pin aligned with smoke testing.
_HADOOP_AWS_PKG = "org.apache.hadoop:hadoop-aws:3.3.4"
_HADOOP_AZURE_PKG = "org.apache.hadoop:hadoop-azure:3.3.4"
_DELTA_PKG = "io.delta:delta-spark_2.12:3.1.0"
_ICEBERG_PKG = "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.10.1"
_SAP_NGDBC_PKG = "com.sap.cloud.db.jdbc:ngdbc:2.23.10"
SPARK_BASE_JARS_PACKAGES = ",".join([_DELTA_PKG, _ICEBERG_PKG, _SAP_NGDBC_PKG])

_ICEBERG_EXTENSIONS = "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
_DELTA_EXTENSIONS = "io.delta.sql.DeltaSparkSessionExtension"
SPARK_EXTENSIONS = ",".join([_ICEBERG_EXTENSIONS, _DELTA_EXTENSIONS])

# (loading destination id, ResolvedSparkRuntime field, Ivy Maven coordinate for --packages)
_CONNECTOR_IVY_SPECS: tuple[tuple[str, str, str], ...] = (
    ("s3", "s3_connector_mode", _HADOOP_AWS_PKG),
    ("azure_blob", "azure_connector_mode", _HADOOP_AZURE_PKG),
)

# Shaded GCS connector: avoids Guava skew vs Spark when the non-shaded Ivy artifact is used.
# Spark does not accept Maven classifiers in --packages; use spark.jars + HTTPS URL instead.
_GCS_CONNECTOR_SHADED_JAR_DEFAULT = (
    "https://repo1.maven.org/maven2/com/google/cloud/bigdataoss/gcs-connector/"
    "hadoop3-2.2.17/gcs-connector-hadoop3-2.2.17-shaded.jar"
)


def _gcs_connector_jar_url() -> str:
    """HTTPS URL for the GCS connector JAR appended to ``spark.jars`` when mode is ``packages``."""
    return os.getenv("SPARK_GCS_CONNECTOR_JAR_URL", _GCS_CONNECTOR_SHADED_JAR_DEFAULT).strip()


def _merge_gcs_shaded_jar_into_config(
    config: Dict[str, Any], destinations: Set[str], resolved: ResolvedSparkRuntime
) -> None:
    if "gcs" not in destinations or resolved.gcs_connector_mode != "packages":
        return
    jar = _gcs_connector_jar_url()
    if not jar:
        return
    existing = (config.get("spark.jars") or "").strip()
    pieces = [p.strip() for p in existing.split(",") if p.strip()]
    if jar not in pieces:
        pieces.append(jar)
    config["spark.jars"] = ",".join(pieces)


def _effective_resolved(resolved: Optional[ResolvedSparkRuntime]) -> ResolvedSparkRuntime:
    return resolved or resolve_spark_runtime(SparkRuntimeConfig())


def _hadoop_filesystem_impl_layer(destinations: Set[str]) -> Dict[str, str]:
    """Shared ``fs.*.impl`` and GCS auth entries for ABFS/GCS (local and managed Spark)."""
    layer: Dict[str, str] = {}
    if "azure_blob" in destinations:
        layer["spark.hadoop.fs.abfs.impl"] = "org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem"
    if "gcs" in destinations:
        layer["spark.hadoop.fs.gs.impl"] = "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem"
        # Connector default auth is COMPUTE_ENGINE (GCE metadata). Off GCP that often blocks
        # indefinitely while probing metadata. APPLICATION_DEFAULT uses ADC: service account
        # JSON from GOOGLE_APPLICATION_CREDENTIALS, user creds from gcloud, or metadata on GCE/GKE.
        auth_type = (os.getenv("SPINE_GCS_AUTH_TYPE") or "APPLICATION_DEFAULT").strip()
        if auth_type:
            layer["spark.hadoop.fs.gs.auth.type"] = auth_type
        if auth_type.upper() == "APPLICATION_DEFAULT":
            gac = (os.getenv("GOOGLE_APPLICATION_CREDENTIALS") or "").strip()
            if gac and Path(gac).expanduser().is_file():
                layer["spark.hadoop.google.cloud.auth.service.account.json.keyfile"] = str(
                    Path(gac).expanduser()
                )
            else:
                adc = Path.home() / ".config" / "gcloud" / "application_default_credentials.json"
                if adc.is_file():
                    layer["spark.hadoop.google.cloud.auth.service.account.json.keyfile"] = str(adc)
    return layer


def _embedded_driver_overlays(resolved: ResolvedSparkRuntime) -> Dict[str, Any]:
    """
    In-process Spark (implicit ``local[*]``) needs a bindable driver address.

    The explicit-key path already sets ``spark.driver.*`` in :meth:`SparkSessionConf.get_local_configs`.
    The managed IAM path historically omitted them, which breaks on some Linux cloud VMs and
    containers where Spark would otherwise pick a non-bindable hostname.

    Databricks and EMR provide cluster-managed Spark; do not inject these there.
    """
    if resolved.managed_platform in (
        ManagedSparkPlatform.DATABRICKS,
        ManagedSparkPlatform.EMR,
    ):
        return {}
    overlays: Dict[str, Any] = {"spark.ui.enabled": "false"}
    if resolved.managed_platform in (
        ManagedSparkPlatform.KUBERNETES,
        ManagedSparkPlatform.ECS,
    ):
        bind = os.getenv("SPINE_SPARK_DRIVER_BIND_ADDRESS", "0.0.0.0")
        host = os.getenv(
            "SPINE_SPARK_DRIVER_HOST",
            os.getenv("SPARK_LOCAL_IP", "127.0.0.1"),
        )
    else:
        bind = os.getenv("SPINE_SPARK_DRIVER_BIND_ADDRESS", "127.0.0.1")
        host = os.getenv("SPINE_SPARK_DRIVER_HOST", "127.0.0.1")
    overlays["spark.driver.bindAddress"] = bind
    overlays["spark.driver.host"] = host
    return overlays


def _s3a_endpoint_and_filesystem(s3_region: str) -> Dict[str, str]:
    """S3A settings shared between local and managed modes (excluding credentials)."""
    return {
        "spark.hadoop.fs.s3a.endpoint": f"s3.{s3_region}.amazonaws.com",
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "spark.hadoop.fs.s3a.path.style.access": "true",
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
            packages.append(static_coord)

        deduped: list[str] = []
        seen = set()
        for pkg in packages:
            if pkg and pkg not in seen:
                deduped.append(pkg)
                seen.add(pkg)
        return deduped

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
        _merge_gcs_shaded_jar_into_config(config, destinations, eff)
        return config

    @staticmethod
    def get_runtime_readiness_notes(
        destinations: Set[str],
        resolved: Optional[ResolvedSparkRuntime] = None,
        use_explicit_credentials: bool = False,
    ) -> list[str]:
        # Backward-compatible shim while callers transition to startup_summary().
        _ = (destinations, resolved, use_explicit_credentials)
        return []

    @staticmethod
    def startup_summary(
        destinations: Set[str],
        use_explicit_credentials: bool,
        resolved: Optional[ResolvedSparkRuntime] = None,
    ) -> str:
        eff = _effective_resolved(resolved)
        parts = [f"Spark destinations={sorted(destinations)}"]
        if "gcs" in destinations:
            parts.append(f"gcs_mode={eff.gcs_connector_mode}")
            parts.append(
                f"gcs_auth_type={(os.getenv('SPINE_GCS_AUTH_TYPE') or 'APPLICATION_DEFAULT').strip()}"
            )
            if eff.gcs_connector_mode == "packages":
                parts.append(f"gcs_connector_jar={_gcs_connector_jar_url() or '<empty>'}")
        if "s3" in destinations:
            parts.append(
                "s3_auth=explicit_keys" if use_explicit_credentials else "s3_auth=default_chain"
            )
            parts.append(f"s3_mode={eff.s3_connector_mode}")
        if "azure_blob" in destinations:
            parts.append(f"azure_mode={eff.azure_connector_mode}")
        parts.append(f"managed_platform={eff.managed_platform.value}")
        return "; ".join(parts)

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
            **_embedded_driver_overlays(resolved),
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
        _merge_gcs_shaded_jar_into_config(config, destinations, resolved)
        return config
