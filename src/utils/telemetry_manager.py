"""
OpenTelemetry lifecycle management.

Spine is an OTLP *producer*. ``TelemetryManager`` is the single place that installs OpenTelemetry
providers and an OTLP exporter, and flushes them before the one-shot CLI exits. It is intentionally
backend-agnostic: it configures the standard OTEL SDK and defers to standard ``OTEL_*`` env vars; it
knows nothing about any specific collector, storage format, or vendor attribute namespace.

Design:
- Instrumentation call sites only ever touch the OpenTelemetry *API* (``opentelemetry.trace`` /
  ``opentelemetry.metrics``), which is a zero-cost no-op until a provider is installed here. So no
  ``if enabled:`` guards are needed anywhere in the pipeline flow.
- ``init`` installs providers only when telemetry is enabled, an endpoint is resolvable, and the SDK
  is importable. Otherwise it leaves the global API no-op in place — telemetry never breaks ingestion.
- A flush/shutdown hook is registered with ``atexit`` so a one-shot process exports buffered spans
  before ``sys.exit`` (mirrors ``SparkManager``).
"""

import atexit
import functools
import os
from typing import Any, Callable, Dict

from opentelemetry import trace

from src.config.telemetry import TelemetryConfig
from src.utils.logger import get_logger
from src.utils.telemetry_logging import set_correlation_enabled

# Metric instrument names (single source of truth).
_METRIC_RESOURCE_PROCESSED = "spine.resource.processed"
_METRIC_RESOURCE_DURATION = "spine.resource.duration"

# Env vars that, when present, mean the operator has configured the exporter destination out-of-band
# and Spine should not override it from YAML.
_ENDPOINT_ENV_VARS = (
    "OTEL_EXPORTER_OTLP_ENDPOINT",
    "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT",
    "OTEL_EXPORTER_OTLP_METRICS_ENDPOINT",
)


def _env_truthy(name: str) -> bool:
    return os.environ.get(name, "").strip().lower() in ("true", "1", "yes")


class TelemetryManager:
    """
    Singleton manager for OpenTelemetry providers.

    Like ``SparkManager``, one instance per process. Re-init is a no-op once initialized; tests reset
    ``_instance`` to start fresh.
    """

    _instance = None
    _initialized: bool = False
    _active: bool = False
    _shutdown_done: bool = False

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._logger = get_logger(cls.__name__)
            cls._instance._tracer_provider = None
            cls._instance._meter_provider = None
            cls._instance._resource_counter = None
            cls._instance._resource_duration = None
        return cls._instance

    @property
    def active(self) -> bool:
        """True when providers were installed and telemetry is exporting."""
        return self._active

    def init(self, cfg: TelemetryConfig) -> None:
        """
        Install OpenTelemetry providers from config. Idempotent.

        Degrades to a no-op (leaves the global API no-op in place) when telemetry is disabled, when
        ``OTEL_SDK_DISABLED=true``, when no endpoint is resolvable, or when the OTEL SDK is not
        importable. Never raises for telemetry-only failures — ingestion must not depend on it.
        """
        if self._initialized:
            return
        self._initialized = True

        if _env_truthy("OTEL_SDK_DISABLED"):
            self._logger.debug("Telemetry disabled via OTEL_SDK_DISABLED")
            return
        if not cfg.enabled:
            self._logger.debug("Telemetry disabled (defaults.telemetry.enabled is false)")
            return

        env_endpoint = any(os.environ.get(v, "").strip() for v in _ENDPOINT_ENV_VARS)
        if not env_endpoint and not (cfg.endpoint and cfg.endpoint.strip()):
            self._logger.warning(
                "Telemetry enabled but no OTLP endpoint resolved; telemetry is a no-op",
                extra_fields={
                    "hint": "set defaults.telemetry.endpoint or OTEL_EXPORTER_OTLP_ENDPOINT"
                },
            )
            return

        try:
            self._install(cfg, use_env_endpoint=env_endpoint)
        except ImportError as e:
            self._logger.warning(
                "OpenTelemetry SDK not available; telemetry is a no-op",
                extra_fields={"error": str(e)},
            )
        except Exception as e:  # telemetry must never break ingestion
            self._logger.warning(
                "Failed to initialize telemetry; continuing without it",
                extra_fields={"error": str(e), "error_type": type(e).__name__},
            )

    def _install(self, cfg: TelemetryConfig, use_env_endpoint: bool) -> None:
        from opentelemetry.sdk.resources import OTELResourceDetector, Resource

        resource = self._build_resource(cfg, Resource, OTELResourceDetector)

        if cfg.traces_enabled:
            self._install_traces(cfg, resource, use_env_endpoint)
        if cfg.metrics_enabled:
            self._install_metrics(cfg, resource, use_env_endpoint)

        self._active = self._tracer_provider is not None or self._meter_provider is not None
        if self._active:
            if cfg.log_correlation:
                set_correlation_enabled(True)
            atexit.register(self.shutdown)
            self._logger.info(
                "Telemetry initialized",
                extra_fields={
                    "service_name": cfg.service_name,
                    "protocol": self._effective_protocol(cfg),
                    "traces": cfg.traces_enabled,
                    "metrics": cfg.metrics_enabled,
                    "endpoint_source": "env" if use_env_endpoint else "config",
                },
            )

    def _build_resource(
        self, cfg: TelemetryConfig, Resource: Any, OTELResourceDetector: Any
    ) -> Any:
        attrs: Dict[str, Any] = {"service.name": cfg.service_name}
        if cfg.service_version:
            attrs["service.version"] = cfg.service_version
        if cfg.deployment_environment:
            attrs["deployment.environment"] = cfg.deployment_environment
        attrs.update(cfg.resource_attributes)
        # Resource.create applies env detection but lets passed attrs win; merge env again on top so
        # standard OTEL_SERVICE_NAME / OTEL_RESOURCE_ATTRIBUTES take precedence over YAML.
        base = Resource.create(attrs)
        return base.merge(OTELResourceDetector().detect())

    def _effective_protocol(self, cfg: TelemetryConfig) -> str:
        return (
            os.environ.get("OTEL_EXPORTER_OTLP_TRACES_PROTOCOL")
            or os.environ.get("OTEL_EXPORTER_OTLP_PROTOCOL")
            or cfg.protocol
        )

    def _exporter_kwargs(self, cfg: TelemetryConfig, use_env_endpoint: bool) -> Dict[str, Any]:
        """Pass endpoint/headers from config only when the operator hasn't set OTEL_* env vars."""
        kwargs: Dict[str, Any] = {}
        if not use_env_endpoint and cfg.endpoint:
            kwargs["endpoint"] = cfg.endpoint.strip()
            if self._effective_protocol(cfg) == "grpc":
                kwargs["insecure"] = cfg.insecure
        if cfg.headers and not os.environ.get("OTEL_EXPORTER_OTLP_HEADERS", "").strip():
            kwargs["headers"] = dict(cfg.headers)
        kwargs["timeout"] = max(1, cfg.export_timeout_ms // 1000)
        return kwargs

    def _install_traces(self, cfg: TelemetryConfig, resource: Any, use_env_endpoint: bool) -> None:
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.trace.export import BatchSpanProcessor

        exporter = self._make_span_exporter(cfg, use_env_endpoint)
        provider = TracerProvider(resource=resource)
        provider.add_span_processor(
            BatchSpanProcessor(exporter, max_export_batch_size=cfg.max_export_batch_size)
        )
        trace.set_tracer_provider(provider)
        self._tracer_provider = provider

    def _make_span_exporter(self, cfg: TelemetryConfig, use_env_endpoint: bool) -> Any:
        kwargs = self._exporter_kwargs(cfg, use_env_endpoint)
        if self._effective_protocol(cfg) == "grpc":
            from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
        else:
            from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter

            kwargs.pop("insecure", None)
        return OTLPSpanExporter(**kwargs)

    def _install_metrics(self, cfg: TelemetryConfig, resource: Any, use_env_endpoint: bool) -> None:
        from opentelemetry import metrics
        from opentelemetry.sdk.metrics import MeterProvider
        from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader

        exporter = self._make_metric_exporter(cfg, use_env_endpoint)
        reader = PeriodicExportingMetricReader(exporter)
        provider = MeterProvider(resource=resource, metric_readers=[reader])
        metrics.set_meter_provider(provider)
        self._meter_provider = provider

        meter = metrics.get_meter("spine.handler")
        self._resource_counter = meter.create_counter(
            _METRIC_RESOURCE_PROCESSED,
            unit="1",
            description="Resources processed, by outcome status.",
        )
        self._resource_duration = meter.create_histogram(
            _METRIC_RESOURCE_DURATION,
            unit="ms",
            description="Wall-clock duration to process one resource.",
        )

    def _make_metric_exporter(self, cfg: TelemetryConfig, use_env_endpoint: bool) -> Any:
        kwargs = self._exporter_kwargs(cfg, use_env_endpoint)
        if self._effective_protocol(cfg) == "grpc":
            from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter
        else:
            from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter

            kwargs.pop("insecure", None)
        return OTLPMetricExporter(**kwargs)

    def get_tracer(self, name: str) -> trace.Tracer:
        """Return a tracer. No-op when no provider is installed."""
        return trace.get_tracer(name)

    def record_resource(self, status: str, duration_ms: float, attributes: Dict[str, str]) -> None:
        """Record the resource-processed counter and duration histogram. No-op when metrics off."""
        attrs = {**attributes, "status": status}
        if self._resource_counter is not None:
            self._resource_counter.add(1, attrs)
        if self._resource_duration is not None:
            self._resource_duration.record(duration_ms, attributes)

    def shutdown(self) -> None:
        """Flush and shut down providers. Idempotent (atexit and explicit calls may both fire)."""
        if self._shutdown_done:
            return
        self._shutdown_done = True
        for provider in (self._tracer_provider, self._meter_provider):
            if provider is None:
                continue
            try:
                provider.force_flush()
            except Exception as e:
                self._logger.debug("Telemetry force_flush failed", extra_fields={"error": str(e)})
            try:
                provider.shutdown()
            except Exception as e:
                self._logger.debug("Telemetry shutdown failed", extra_fields={"error": str(e)})


def traced(span_name: str) -> Callable:
    """
    Decorator that wraps an instance method in a span named ``span_name``.

    Uses the calling instance's ``_tracer`` when present, else a module tracer. The span inherits the
    active context, so nested decorated/instrumented calls form the expected tree. When no provider is
    installed the tracer is the API no-op and the wrapper adds negligible overhead.
    """

    def decorator(fn: Callable) -> Callable:
        @functools.wraps(fn)
        def wrapper(self, *args, **kwargs):
            tracer = getattr(self, "_tracer", None)
            if tracer is None:
                tracer = trace.get_tracer("spine")
            with tracer.start_as_current_span(span_name):
                return fn(self, *args, **kwargs)

        return wrapper

    return decorator


def reset_for_tests() -> None:
    """Reset the singleton so a test can install a fresh provider. Test-only helper."""
    TelemetryManager._instance = None
    TelemetryManager._initialized = False
    TelemetryManager._active = False
    TelemetryManager._shutdown_done = False
    set_correlation_enabled(False)
