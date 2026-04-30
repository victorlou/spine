"""Lightweight tests for streaming collectors (Spark and Redis mocked)."""

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from src.collector.base_collector import RawDataBatch
from src.collector.streaming_collector import StreamingRawDataCollector


@pytest.fixture
def streaming_deps() -> dict:
    redis_context = MagicMock()
    spark = MagicMock()
    resource_meta = SimpleNamespace(
        resource_name="r",
        config=SimpleNamespace(fields=None, transformations=[], response_key=None),
    )
    service = SimpleNamespace(source_name="src")
    execution_plan = MagicMock()
    return {
        "redis_context": redis_context,
        "spark": spark,
        "resource_meta": resource_meta,
        "service": service,
        "execution_plan": execution_plan,
    }


def test_streaming_collector_flushes_at_threshold(streaming_deps: dict, monkeypatch) -> None:
    c = StreamingRawDataCollector(
        redis_context=streaming_deps["redis_context"],
        resource_key="rk",
        flush_threshold=2,
        spark=streaming_deps["spark"],
        resource_meta=streaming_deps["resource_meta"],
        service=streaming_deps["service"],
        execution_plan=streaming_deps["execution_plan"],
    )
    df_mock = MagicMock()
    df_mock.count.return_value = 1
    c._parse_batches = MagicMock(return_value=df_mock)  # type: ignore[method-assign]

    batch = RawDataBatch(raw_data=[], request_context=None)
    c.add_batch(batch)
    assert len(c.batches) == 1
    c.add_batch(batch)
    streaming_deps["redis_context"].store.assert_called_once()
