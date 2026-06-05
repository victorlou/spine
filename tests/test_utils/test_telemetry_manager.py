"""Tests for TelemetryManager, the traced decorator, and log correlation."""

from unittest.mock import MagicMock

from opentelemetry import trace
from opentelemetry.sdk.resources import OTELResourceDetector, Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from src.config.telemetry import TelemetryConfig
from src.utils.telemetry_logging import set_correlation_enabled
from src.utils.telemetry_manager import TelemetryManager, traced


def _inmemory_tracer():
    """A real tracer backed by an in-memory exporter, independent of the global provider."""
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    return provider.get_tracer("test"), exporter


# --------------------------------------------------------------------------- #
# init: disabled / no-op paths
# --------------------------------------------------------------------------- #


def test_disabled_does_not_activate():
    m = TelemetryManager()
    m.init(TelemetryConfig(enabled=False))
    assert m.active is False
    # No global provider installed -> tracer is the API no-op.
    assert trace.get_tracer("x").start_as_current_span("s") is not None


def test_enabled_without_endpoint_is_noop():
    m = TelemetryManager()
    m.init(TelemetryConfig(enabled=True))
    assert m.active is False


def test_otel_sdk_disabled_env_forces_noop(monkeypatch):
    monkeypatch.setenv("OTEL_SDK_DISABLED", "true")
    m = TelemetryManager()
    m.init(TelemetryConfig(enabled=True, endpoint="http://localhost:4317"))
    assert m.active is False


def test_init_is_idempotent():
    m = TelemetryManager()
    m.init(TelemetryConfig(enabled=False))
    m.init(TelemetryConfig(enabled=True, endpoint="http://localhost:4317"))
    # Second call is ignored because the manager already initialized as disabled.
    assert m.active is False


def test_init_degrades_on_install_error(monkeypatch):
    m = TelemetryManager()

    def _boom(*_a, **_k):
        raise ImportError("no sdk")

    monkeypatch.setattr(m, "_install", _boom)
    m.init(TelemetryConfig(enabled=True, endpoint="http://localhost:4317"))
    assert m.active is False  # swallowed, ingestion continues


# --------------------------------------------------------------------------- #
# init: active path installs providers
# --------------------------------------------------------------------------- #


def test_enabled_with_endpoint_installs_providers():
    m = TelemetryManager()
    m.init(TelemetryConfig(enabled=True, endpoint="http://localhost:4317", metrics_enabled=True))
    assert m.active is True
    assert isinstance(trace.get_tracer_provider(), TracerProvider)


def test_enabled_with_traces_only_installs_no_meter_provider():
    m = TelemetryManager()
    m.init(TelemetryConfig(enabled=True, endpoint="http://localhost:4317", metrics_enabled=False))
    assert m.active is True
    assert m._meter_provider is None


# --------------------------------------------------------------------------- #
# resource attributes: env precedence
# --------------------------------------------------------------------------- #


def test_resource_uses_config_when_no_env():
    m = TelemetryManager()
    resource = m._build_resource(
        TelemetryConfig(service_name="spine", resource_attributes={"a": "1"}),
        Resource,
        OTELResourceDetector,
    )
    assert resource.attributes.get("service.name") == "spine"
    assert resource.attributes.get("a") == "1"


def test_env_overrides_config_resource_attributes(monkeypatch):
    monkeypatch.setenv("OTEL_SERVICE_NAME", "from-env")
    monkeypatch.setenv("OTEL_RESOURCE_ATTRIBUTES", "deployment.environment=prod,a=2")
    m = TelemetryManager()
    resource = m._build_resource(
        TelemetryConfig(service_name="spine", resource_attributes={"a": "1"}),
        Resource,
        OTELResourceDetector,
    )
    assert resource.attributes.get("service.name") == "from-env"
    assert resource.attributes.get("a") == "2"
    assert resource.attributes.get("deployment.environment") == "prod"


def test_config_service_version_and_environment_map_to_semconv():
    m = TelemetryManager()
    resource = m._build_resource(
        TelemetryConfig(service_version="abc123", deployment_environment="dev"),
        Resource,
        OTELResourceDetector,
    )
    assert resource.attributes.get("service.version") == "abc123"
    assert resource.attributes.get("deployment.environment") == "dev"


# --------------------------------------------------------------------------- #
# protocol / endpoint precedence in exporter kwargs
# --------------------------------------------------------------------------- #


def test_exporter_kwargs_use_config_endpoint_when_no_env():
    m = TelemetryManager()
    cfg = TelemetryConfig(enabled=True, endpoint="http://collector:4317", insecure=True)
    kwargs = m._exporter_kwargs(cfg, use_env_endpoint=False)
    assert kwargs["endpoint"] == "http://collector:4317"
    assert kwargs["insecure"] is True


def test_exporter_kwargs_defer_to_env_endpoint():
    m = TelemetryManager()
    cfg = TelemetryConfig(enabled=True, endpoint="http://collector:4317")
    kwargs = m._exporter_kwargs(cfg, use_env_endpoint=True)
    assert "endpoint" not in kwargs  # exporter reads OTEL_EXPORTER_OTLP_ENDPOINT itself


def test_effective_protocol_prefers_env(monkeypatch):
    m = TelemetryManager()
    cfg = TelemetryConfig(protocol="grpc")
    assert m._effective_protocol(cfg) == "grpc"
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_PROTOCOL", "http/protobuf")
    assert m._effective_protocol(cfg) == "http/protobuf"


# --------------------------------------------------------------------------- #
# shutdown
# --------------------------------------------------------------------------- #


def test_shutdown_flushes_and_is_idempotent():
    m = TelemetryManager()
    provider = MagicMock()
    m._tracer_provider = provider
    m.shutdown()
    m.shutdown()
    provider.force_flush.assert_called_once()
    provider.shutdown.assert_called_once()


def test_active_init_registers_atexit_flush(monkeypatch):
    """One-shot processes must flush before exit: init registers shutdown with atexit."""
    registered = []
    monkeypatch.setattr(
        "src.utils.telemetry_manager.atexit.register", lambda fn: registered.append(fn)
    )
    m = TelemetryManager()
    m.init(TelemetryConfig(enabled=True, endpoint="http://localhost:4317"))
    assert m.shutdown in registered


# --------------------------------------------------------------------------- #
# traced decorator: span tree
# --------------------------------------------------------------------------- #


def test_traced_decorator_builds_nested_spans():
    tracer, exporter = _inmemory_tracer()

    class Worker:
        def __init__(self):
            self._tracer = tracer

        @traced("spine.resource.process")
        def process(self):
            self.extract()
            self.parse()

        @traced("spine.extract")
        def extract(self):
            pass

        @traced("spine.parse")
        def parse(self):
            pass

    with tracer.start_as_current_span("spine.pipeline.run"):
        Worker().process()

    spans = {s.name: s for s in exporter.get_finished_spans()}
    assert set(spans) == {
        "spine.pipeline.run",
        "spine.resource.process",
        "spine.extract",
        "spine.parse",
    }
    # process is a child of pipeline.run; extract/parse are children of process.
    root = spans["spine.pipeline.run"]
    proc = spans["spine.resource.process"]
    assert proc.parent.span_id == root.context.span_id
    assert spans["spine.extract"].parent.span_id == proc.context.span_id
    assert spans["spine.parse"].parent.span_id == proc.context.span_id


def test_traced_decorator_records_exception_status():
    tracer, exporter = _inmemory_tracer()

    class Worker:
        _tracer = tracer

        @traced("spine.resource.process")
        def boom(self):
            raise ValueError("nope")

    try:
        Worker().boom()
    except ValueError:
        pass

    span = exporter.get_finished_spans()[0]
    assert span.status.status_code == trace.StatusCode.ERROR


def test_traced_decorator_is_noop_without_provider():
    # No global provider, no _tracer attribute -> uses API no-op tracer, no error.
    class Worker:
        @traced("spine.extract")
        def run(self):
            return 42

    assert Worker().run() == 42


# --------------------------------------------------------------------------- #
# log correlation
# --------------------------------------------------------------------------- #


def _record(msg="m"):
    import logging

    return logging.LogRecord("t", logging.INFO, __file__, 1, msg, None, None)


def test_correlation_filter_injects_ids_inside_span():
    from src.utils.telemetry_logging import TraceCorrelationFilter

    tracer, _ = _inmemory_tracer()
    set_correlation_enabled(True)
    filt = TraceCorrelationFilter()
    with tracer.start_as_current_span("demo"):
        record = _record()
        filt.filter(record)
    assert "trace_id" in record.extra_fields
    assert "span_id" in record.extra_fields


def test_correlation_filter_noop_without_span():
    from src.utils.telemetry_logging import TraceCorrelationFilter

    set_correlation_enabled(True)
    record = _record()
    TraceCorrelationFilter().filter(record)
    assert not hasattr(record, "extra_fields")


def test_correlation_filter_noop_when_disabled():
    from src.utils.telemetry_logging import TraceCorrelationFilter

    tracer, _ = _inmemory_tracer()
    set_correlation_enabled(False)
    record = _record()
    with tracer.start_as_current_span("demo"):
        TraceCorrelationFilter().filter(record)
    assert not hasattr(record, "extra_fields")


def test_record_resource_noop_without_metrics():
    m = TelemetryManager()
    m.init(TelemetryConfig(enabled=False))
    # No instruments installed; must not raise.
    m.record_resource("success", 10.0, {"source": "s", "resource": "r"})
