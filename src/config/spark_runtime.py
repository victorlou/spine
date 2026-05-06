"""
Spark runtime profile detection and connector resolution.

Operators configure ``defaults.spark_runtime`` in pipeline YAML; environment
variables remain optional overrides for CI and custom images.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import Literal, Optional

from src.config.config_models import ConnectorProvisionMode, SparkRuntimeConfig, SparkRuntimeProfile

ProvisionLiteral = Literal["packages", "external"]

_CONNECTOR_RESOLUTION: tuple[tuple[str, str, str], ...] = (
    ("s3_connector_mode", "SPARK_S3_CONNECTOR_MODE", "s3_connector_mode"),
    ("gcs_connector_mode", "SPARK_GCS_CONNECTOR_MODE", "gcs_connector_mode"),
    ("azure_connector_mode", "SPARK_AZURE_CONNECTOR_MODE", "azure_connector_mode"),
)


class ManagedSparkPlatform(StrEnum):
    """Signals from the host environment (cloud-agnostic, extensible)."""

    NONE = "none"
    DATABRICKS = "databricks"
    EMR = "emr"
    ECS = "ecs"
    KUBERNETES = "kubernetes"


def detect_managed_spark_platform() -> tuple[ManagedSparkPlatform, str]:
    """
    Inspect well-known environment variables. Does not call cloud APIs.

    Returns:
        (platform, human-readable detection source for logs)
    """
    if os.getenv("DATABRICKS_RUNTIME_VERSION"):
        return ManagedSparkPlatform.DATABRICKS, "DATABRICKS_RUNTIME_VERSION"
    if os.getenv("EMR_STEP_ID") or os.getenv("EMR_CLUSTER_ID"):
        return ManagedSparkPlatform.EMR, "EMR_STEP_ID or EMR_CLUSTER_ID"
    if os.getenv("ECS_CONTAINER_METADATA_URI"):
        return ManagedSparkPlatform.ECS, "ECS_CONTAINER_METADATA_URI"
    if os.getenv("KUBERNETES_SERVICE_HOST"):
        return ManagedSparkPlatform.KUBERNETES, "KUBERNETES_SERVICE_HOST"
    return ManagedSparkPlatform.NONE, "no managed platform env signals"


EffectiveSparkProfile = Literal["local_dev", "cluster_managed"]


def _effective_profile(
    config: SparkRuntimeConfig, platform: ManagedSparkPlatform
) -> EffectiveSparkProfile:
    if config.profile == SparkRuntimeProfile.LOCAL_DEV:
        return "local_dev"
    if config.profile == SparkRuntimeProfile.CLUSTER_MANAGED:
        return "cluster_managed"
    if platform == ManagedSparkPlatform.NONE:
        return "local_dev"
    return "cluster_managed"


def _resolve_connector_mode(
    env_key: str,
    config_mode: ConnectorProvisionMode,
    platform: ManagedSparkPlatform,
    external_platforms: frozenset[ManagedSparkPlatform],
) -> ProvisionLiteral:
    raw = os.getenv(env_key)
    if raw is not None and str(raw).strip() != "":
        v = str(raw).strip().lower()
        if v in ("packages", "external"):
            return v  # type: ignore[return-value]
    if config_mode == ConnectorProvisionMode.PACKAGES:
        return "packages"
    if config_mode == ConnectorProvisionMode.EXTERNAL:
        return "external"
    if platform in external_platforms:
        return "external"
    return "packages"


def normalize_spark_event_log_uri(dir_str: str) -> str:
    """
    Normalize operator-supplied ``spark_event_log_dir`` to a URI Spark accepts.

    Absolute ``file:`` / cloud URIs are returned unchanged; bare filesystem paths become ``file:`` URIs.
    """
    s = str(dir_str).strip()
    lower = s.lower()
    if lower.startswith(
        ("file:", "s3a:", "s3:", "hdfs:", "gs:", "abfss:", "abfs:", "wasb:", "wasbs:")
    ):
        return s
    return str(Path(s).expanduser().resolve().as_uri())


@dataclass(frozen=True)
class ResolvedSparkRuntime:
    """Effective Spark bootstrap decisions after YAML + env + detection."""

    managed_platform: ManagedSparkPlatform
    effective_profile: EffectiveSparkProfile
    s3_connector_mode: ProvisionLiteral
    gcs_connector_mode: ProvisionLiteral
    azure_connector_mode: ProvisionLiteral
    detection_source: str
    spark_ui_enabled: bool
    spark_ui_port: Optional[int]
    spark_ui_show_console_progress: bool
    spark_event_log_enabled: bool
    spark_event_log_dir_uri: Optional[str]
    spark_event_log_compress: bool

    def summary_for_log(self) -> str:
        parts = [
            f"spark_runtime profile={self.effective_profile} platform={self.managed_platform.value} "
            f"({self.detection_source}) s3={self.s3_connector_mode} gcs={self.gcs_connector_mode} "
            f"azure={self.azure_connector_mode}",
            f"spark_ui={self.spark_ui_enabled}",
            f"spark_event_log={self.spark_event_log_enabled}",
        ]
        return "; ".join(parts)


def resolve_spark_runtime(config: SparkRuntimeConfig) -> ResolvedSparkRuntime:
    """
    Resolve effective profile and per-destination connector provisioning.

    S3, GCS, and Azure each get the same ``auto`` rule: ``external`` only on Databricks and EMR
    where distributions typically bundle Hadoop cloud connectors; other environments default to
    ``packages`` unless set explicitly in YAML or env.
    """
    platform, detection_source = detect_managed_spark_platform()
    effective = _effective_profile(config, platform)
    external_defaults = frozenset({ManagedSparkPlatform.DATABRICKS, ManagedSparkPlatform.EMR})
    connector_modes: dict[str, ProvisionLiteral] = {}
    for resolved_field, env_key, cfg_attr in _CONNECTOR_RESOLUTION:
        connector_modes[resolved_field] = _resolve_connector_mode(
            env_key, getattr(config, cfg_attr), platform, external_defaults
        )
    event_uri: Optional[str] = None
    if config.spark_event_log_enabled and config.spark_event_log_dir:
        event_uri = normalize_spark_event_log_uri(config.spark_event_log_dir)
    return ResolvedSparkRuntime(
        managed_platform=platform,
        effective_profile=effective,
        detection_source=detection_source,
        spark_ui_enabled=config.spark_ui_enabled,
        spark_ui_port=config.spark_ui_port,
        spark_ui_show_console_progress=config.spark_ui_show_console_progress,
        spark_event_log_enabled=config.spark_event_log_enabled,
        spark_event_log_dir_uri=event_uri,
        spark_event_log_compress=config.spark_event_log_compress,
        **connector_modes,
    )
