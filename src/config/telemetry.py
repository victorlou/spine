"""
OpenTelemetry configuration model.

Spine is an OTLP *producer*: it emits standard OpenTelemetry traces and metrics and defers to the
standard ``OTEL_*`` environment variables. It is deliberately backend-agnostic — it does not know
about any specific collector, storage format, or vendor attribute namespace. Anything vendor-specific
(product, workflow, business-event, cost attributes, …) is supplied by the operator through the
generic ``resource_attributes`` map or the standard ``OTEL_RESOURCE_ATTRIBUTES`` env var, never
hardcoded here.

Config-first, but standard ``OTEL_*`` env vars take precedence at init time (see
``src/utils/telemetry_manager.py``) — mirroring how ``SparkRuntimeConfig`` lets ``SPARK_*`` env vars
override YAML.
"""

from typing import Dict, Literal, Optional

from pydantic import BaseModel, Field, model_validator

OTLPProtocol = Literal["grpc", "http/protobuf"]


class TelemetryConfig(BaseModel):
    """
    OpenTelemetry (OTLP) producer settings.

    Disabled by default. When enabled, Spine initializes a TracerProvider (and, when
    ``metrics_enabled``, a MeterProvider) exporting over OTLP to the configured endpoint. Standard
    ``OTEL_*`` env vars override the values here at init; an unreachable or absent endpoint degrades
    telemetry to a no-op with a warning rather than failing the pipeline — telemetry must never break
    ingestion.
    """

    enabled: bool = Field(
        default=False,
        description=(
            "Master switch. When false (default) Spine installs no OTEL providers and all "
            "instrumentation is a zero-cost no-op. The standard OTEL_SDK_DISABLED=true env var also "
            "forces no-op regardless of this value."
        ),
    )

    service_name: str = Field(
        default="spine",
        description="Maps to the OTEL resource ``service.name``. Overridden by OTEL_SERVICE_NAME.",
    )
    service_version: Optional[str] = Field(
        default=None,
        description="Maps to the OTEL resource ``service.version`` (e.g. a git SHA) when set.",
    )
    deployment_environment: Optional[str] = Field(
        default=None,
        description="Maps to the OTEL resource ``deployment.environment`` (e.g. dev, prod) when set.",
    )
    resource_attributes: Dict[str, str] = Field(
        default_factory=dict,
        description=(
            "Arbitrary additional OTEL resource attributes. Operators add deployment- or "
            "vendor-specific attributes here (Spine does not name them). Standard "
            "OTEL_RESOURCE_ATTRIBUTES env entries take precedence on key collision."
        ),
    )

    endpoint: Optional[str] = Field(
        default=None,
        description=(
            "OTLP collector endpoint (e.g. http://localhost:4317). May reference env vars in YAML "
            "via ${OTEL_EXPORTER_OTLP_ENDPOINT:-...}. Ignored when the OTEL_EXPORTER_OTLP_ENDPOINT "
            "env var is set, which takes precedence."
        ),
    )
    protocol: OTLPProtocol = Field(
        default="grpc",
        description=(
            "OTLP transport: ``grpc`` (default, port 4317) or ``http/protobuf`` (port 4318). "
            "Overridden by OTEL_EXPORTER_OTLP_PROTOCOL."
        ),
    )
    insecure: bool = Field(
        default=True,
        description=(
            "For the grpc exporter: connect without TLS (typical for an in-cluster collector). "
            "Set false to use TLS. Ignored when OTEL_EXPORTER_OTLP_* env config is in effect."
        ),
    )
    headers: Dict[str, str] = Field(
        default_factory=dict,
        description=(
            "Extra OTLP export headers (e.g. an auth token). Kept generic; overridden by "
            "OTEL_EXPORTER_OTLP_HEADERS."
        ),
    )

    traces_enabled: bool = Field(default=True, description="Export trace spans when enabled.")
    metrics_enabled: bool = Field(
        default=True,
        description="Export metrics (resource counter + duration histogram) when enabled.",
    )
    log_correlation: bool = Field(
        default=True,
        description=(
            "Inject the active trace_id/span_id into the structured logger so logs correlate to "
            "spans. No-op when there is no active span; does not change existing log content."
        ),
    )

    export_timeout_ms: int = Field(
        default=30000,
        ge=1,
        description="Per-export and shutdown flush timeout in milliseconds.",
    )
    max_export_batch_size: int = Field(
        default=512,
        ge=1,
        description="Maximum spans per OTLP batch export (BatchSpanProcessor).",
    )

    @model_validator(mode="after")
    def at_least_one_signal_when_enabled(self) -> "TelemetryConfig":
        if self.enabled and not (self.traces_enabled or self.metrics_enabled):
            raise ValueError(
                "telemetry.enabled is true but both traces_enabled and metrics_enabled are false; "
                "enable at least one signal or set enabled to false"
            )
        return self
