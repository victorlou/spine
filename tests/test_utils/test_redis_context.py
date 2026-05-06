"""Tests for RedisContextManager using mocked Redis client."""

import json
import logging
from unittest.mock import MagicMock

import pytest
from pyspark.sql.types import StringType, StructField, StructType

from src.utils.redis_context import ContextError, RedisContextManager


class _FakeSparkDataFrame:
    """Stand-in for pyspark DataFrame when testing serialization branches."""

    pass


@pytest.fixture
def manager(monkeypatch: pytest.MonkeyPatch) -> RedisContextManager:
    client = MagicMock()
    monkeypatch.setattr(RedisContextManager, "_create_client", lambda self: client)
    return RedisContextManager({"host": "localhost"}, prefix="p:", default_ttl=60)


def test_store_get_metadata_exists_delete_cycle(manager: RedisContextManager) -> None:
    payload = b'{"type":"raw","data":{"k":"v"}}'
    manager.client.get.side_effect = [payload, b'{"m":1}', payload]
    manager.client.exists.return_value = 1

    manager.store("a", {"k": "v"}, metadata={"m": 1}, ttl=5)
    got = manager.get("a")
    meta = manager.get_metadata("a")

    assert got == {"k": "v"}
    assert meta == {"m": 1}
    assert manager.exists("a") is True
    manager.delete("a")
    assert manager.client.delete.call_count >= 2


def test_validate_connection_and_cleanup(manager: RedisContextManager) -> None:
    manager.client.get.return_value = b'{"type":"raw","data":{"test":"data"}}'
    manager.client.exists.return_value = 0
    manager.client.info.return_value = {"used_memory_human": "1M"}
    manager.client.scan_iter.return_value = [b"p:a", b"p:b"]

    manager.validate_connection()
    manager.clear(log_memory=True)
    manager.cleanup(flush_data=True)

    manager.client.close.assert_called_once()


def test_error_wrapping_paths(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(RedisContextManager, "_create_client", lambda self: MagicMock())
    m = RedisContextManager({"host": "localhost"})
    m.client.get.side_effect = RuntimeError("boom")
    with pytest.raises(ContextError, match="Failed to retrieve data"):
        m.get("x")


def test_clear_without_prefix_uses_flushall(manager: RedisContextManager) -> None:
    manager.prefix = ""
    manager.client.info.return_value = {"used_memory_human": "1M"}
    manager.clear(log_memory=True)
    manager.client.flushall.assert_called_once()


def test_clear_prefix_no_keys_skips_delete(manager: RedisContextManager) -> None:
    manager.client.scan_iter.return_value = []
    manager.clear(log_memory=False)
    manager.client.delete.assert_not_called()


def test_clear_prefix_with_keys_deletes_batch(manager: RedisContextManager) -> None:
    manager.client.scan_iter.return_value = [b"p:a", b"p:b"]
    manager.clear(log_memory=False)
    manager.client.delete.assert_called_once_with(b"p:a", b"p:b")


def test_validate_connection_wraps_integrity_failure(manager: RedisContextManager) -> None:
    manager.get = MagicMock(return_value={"bad": "shape"})  # type: ignore[method-assign]
    with pytest.raises(ContextError, match="Data integrity check failed"):
        manager.validate_connection()


def test_validate_connection_wraps_delete_failure(manager: RedisContextManager) -> None:
    manager.get = MagicMock(return_value={"test": "data"})  # type: ignore[method-assign]
    manager.exists = MagicMock(return_value=True)  # type: ignore[method-assign]
    with pytest.raises(ContextError, match="Delete operation failed"):
        manager.validate_connection()


def test_get_wraps_deserialize_decode_error(manager: RedisContextManager) -> None:
    manager.client.get.return_value = b"\xff\xfe"
    with pytest.raises(ContextError, match="Failed to retrieve data"):
        manager.get("x")


def test_get_metadata_wraps_decode_error(manager: RedisContextManager) -> None:
    manager.client.get.return_value = b"\xff\xfe"
    with pytest.raises(ContextError, match="Failed to retrieve metadata"):
        manager.get_metadata("x")


def test_get_metadata_returns_none_when_missing(manager: RedisContextManager) -> None:
    manager.client.get.return_value = None
    assert manager.get_metadata("absent") is None


def test_serialize_raw_non_json_encodable_raises_context_error(
    manager: RedisContextManager,
) -> None:
    class _NotJson:
        pass

    with pytest.raises(ContextError, match="Failed to serialize data"):
        manager._serialize_data(_NotJson())


def test_cleanup_without_flush_only_closes_client(manager: RedisContextManager) -> None:
    manager.clear = MagicMock()  # type: ignore[method-assign]
    manager.cleanup(flush_data=False)
    manager.client.close.assert_called_once()
    manager.clear.assert_not_called()


def test_get_returns_none_when_key_missing(manager: RedisContextManager) -> None:
    manager.client.get.return_value = None
    assert manager.get("nope") is None


def test_deserialize_empty_bytes_returns_none(manager: RedisContextManager) -> None:
    assert manager._deserialize_data(b"", None) is None


def test_deserialize_spark_dataframe_requires_spark(manager: RedisContextManager) -> None:
    schema = StructType([StructField("c", StringType(), True)])
    blob = json.dumps(
        {"type": "spark_dataframe", "schema": schema.json(), "data": ['{"c":"x"}']}
    ).encode("utf-8")
    with pytest.raises(ContextError, match="SparkSession required"):
        manager._deserialize_data(blob, None)


def test_serialize_and_deserialize_spark_dataframe_round_trip(
    manager: RedisContextManager, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr("src.utils.redis_context.DataFrame", _FakeSparkDataFrame)
    schema = StructType([StructField("c", StringType(), True)])
    df = _FakeSparkDataFrame()
    df.schema = schema
    json_row = '{"c":"hello"}'
    df.toJSON = MagicMock(return_value=MagicMock(collect=lambda: [json_row]))

    raw = manager._serialize_data(df)
    assert b"spark_dataframe" in raw

    out_df = MagicMock(name="reconstructed")
    spark = MagicMock()
    row_rdd = MagicMock()
    spark.sparkContext.parallelize.return_value.map.return_value = row_rdd
    spark.createDataFrame.return_value = out_df

    assert manager._deserialize_data(raw, spark) is out_df
    spark.createDataFrame.assert_called_once_with(row_rdd, schema)


def test_get_data_preview_dataframe_branch(
    manager: RedisContextManager, monkeypatch: pytest.MonkeyPatch
) -> None:
    class _PreviewDf:
        pass

    monkeypatch.setattr("src.utils.redis_context.DataFrame", _PreviewDf)
    row = MagicMock()
    row.asDict.return_value = {"a": 1}
    df = _PreviewDf()
    lim = MagicMock()
    lim.collect.return_value = [row]
    df.limit = MagicMock(return_value=lim)
    df.count = MagicMock(return_value=42)
    df.schema = MagicMock()
    df.schema.__str__ = lambda *_: "schema-str"

    prev_df = manager._get_data_preview(df)
    assert prev_df["data_type"] == "DataFrame"
    assert prev_df["total_rows"] == 42


def test_get_data_preview_dict_list_scalar(manager: RedisContextManager) -> None:
    long_dict = {"k": "x" * 300}
    prev_d = manager._get_data_preview(long_dict, max_length=50)
    assert prev_d["data_type"] == "dict"
    assert prev_d["preview"].endswith("...")

    prev_scalar = manager._get_data_preview(12345)
    assert prev_scalar["data_type"] == "int"
    assert "12345" in prev_scalar["preview"]


def test_get_data_preview_dataframe_inner_error(
    manager: RedisContextManager, monkeypatch: pytest.MonkeyPatch
) -> None:
    class _FailDf:
        pass

    monkeypatch.setattr("src.utils.redis_context.DataFrame", _FailDf)
    df = _FailDf()
    df.limit = MagicMock(side_effect=RuntimeError("preview boom"))
    prev_err = manager._get_data_preview(df)
    assert "error" in prev_err


def test_store_delete_exists_wrap_redis_errors(manager: RedisContextManager) -> None:
    manager.client.set.side_effect = RuntimeError("set failed")
    with pytest.raises(ContextError, match="Failed to store data"):
        manager.store("k", {"a": 1})

    manager.client.set.side_effect = None
    manager.client.delete.side_effect = RuntimeError("del failed")
    with pytest.raises(ContextError, match="Failed to delete data"):
        manager.delete("k")

    manager.client.delete.side_effect = None
    manager.client.exists.side_effect = RuntimeError("exists failed")
    with pytest.raises(ContextError, match="Failed to check key existence"):
        manager.exists("k")


def test_create_client_failure_raises_context_error(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "src.utils.redis_context.Redis",
        MagicMock(side_effect=OSError("connection refused")),
    )
    with pytest.raises(ContextError, match="Failed to create Redis client"):
        RedisContextManager({"host": "h"})


def test_clear_logs_warning_when_inner_ops_fail(
    manager: RedisContextManager, caplog: pytest.LogCaptureFixture
) -> None:
    manager.client.info.side_effect = RuntimeError("info broke")
    with caplog.at_level(logging.WARNING):
        manager.clear(log_memory=True)
    assert "Failed to clear Redis keys" in caplog.text


def test_cleanup_raises_when_close_fails(manager: RedisContextManager) -> None:
    manager.clear = MagicMock()  # type: ignore[method-assign]
    manager.client.close.side_effect = RuntimeError("close broke")
    with pytest.raises(ContextError, match="Failed to cleanup Redis connection"):
        manager.cleanup(flush_data=False)


def test_clear_returns_early_when_no_client_attribute() -> None:
    mgr = object.__new__(RedisContextManager)
    mgr.prefix = "p:"
    mgr.logger = MagicMock()
    mgr.clear(log_memory=False)
