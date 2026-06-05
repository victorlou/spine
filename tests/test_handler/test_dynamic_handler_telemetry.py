"""Telemetry instrumentation on the handler: resource span attributes + error status.

These tests exercise the decorated ``_process_resource`` directly (the heavy extract/parse/load
path needs Spark and is covered elsewhere). They verify the span carries the resource attributes and
that a failure inside the resource sets ERROR on that span only — i.e. failure isolation is preserved.
"""

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from src.config.config_models import SourceType
from src.handler.dynamic_handler import DynamicHandler
from src.planner.execution_plan import ResourceMetadata
from src.utils.exceptions import HandlerError
from src.utils.logger import get_logger


def _handler_with_tracer():
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    h = DynamicHandler.__new__(DynamicHandler)
    h.logger = get_logger("test_dynamic_handler_telemetry")
    h._tracer = provider.get_tracer("test")
    h._telemetry = MagicMock()
    h.spark = None  # forces the early HandlerError inside _process_resource
    return h, exporter


def _resource_meta():
    cfg = SimpleNamespace(method="GET", snapshot=None)
    return ResourceMetadata(
        source_name="api",
        resource_name="users",
        dependencies=set(),
        batch_inputs={},
        config=cfg,
    )


def test_resource_span_sets_attributes_and_error_status():
    h, exporter = _handler_with_tracer()
    source_config = SimpleNamespace(type=SourceType.REST_API)

    with pytest.raises(HandlerError):
        h._process_resource(
            resource_meta=_resource_meta(), service=MagicMock(), source_config=source_config
        )

    spans = exporter.get_finished_spans()
    assert len(spans) == 1
    span = spans[0]
    assert span.name == "spine.resource.process"
    assert span.attributes["spine.source"] == "api"
    assert span.attributes["spine.resource"] == "users"
    assert span.attributes["spine.source_type"] == str(SourceType.REST_API)
    assert span.status.status_code == trace.StatusCode.ERROR
