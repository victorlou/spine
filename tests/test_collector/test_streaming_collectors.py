"""Lightweight tests for streaming collectors (Spark and Redis mocked)."""

import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from pyspark.sql.types import StringType, StructField, StructType

from src.collector.base_collector import RawDataBatch
from src.collector.disk_streaming_collector import DiskStreamingDataCollector
from src.collector.streaming_collector import StreamingRawDataCollector
from src.config.config_models import TransformationType


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


def test_streaming_collector_create_parser_failure_propagates(
    streaming_deps: dict, monkeypatch
) -> None:
    monkeypatch.setattr(
        StreamingRawDataCollector,
        "_create_parser",
        lambda self: (_ for _ in ()).throw(ValueError("parser init failed")),
    )
    with pytest.raises(ValueError, match="parser init failed"):
        StreamingRawDataCollector(
            redis_context=streaming_deps["redis_context"],
            resource_key="rk",
            flush_threshold=2,
            spark=streaming_deps["spark"],
            resource_meta=streaming_deps["resource_meta"],
            service=streaming_deps["service"],
            execution_plan=streaming_deps["execution_plan"],
        )


def test_streaming_parse_data_falls_back_to_empty_dataframe(streaming_deps: dict) -> None:
    c = StreamingRawDataCollector(
        redis_context=streaming_deps["redis_context"],
        resource_key="rk",
        flush_threshold=2,
        spark=streaming_deps["spark"],
        resource_meta=streaming_deps["resource_meta"],
        service=streaming_deps["service"],
        execution_plan=streaming_deps["execution_plan"],
    )
    schema = object()
    fallback_df = MagicMock()
    c.parser = MagicMock()
    c.parser.parse.return_value = None
    c.parser._build_target_schema.return_value = schema
    streaming_deps["spark"].createDataFrame.return_value = fallback_df

    out = c._parse_data([{"a": 1}], c.resource_meta)
    assert out is fallback_df
    streaming_deps["spark"].createDataFrame.assert_called_once_with([], schema)


def test_streaming_parse_batches_unions_multiple_batch_dataframes(streaming_deps: dict) -> None:
    c = StreamingRawDataCollector(
        redis_context=streaming_deps["redis_context"],
        resource_key="rk",
        flush_threshold=5,
        spark=streaming_deps["spark"],
        resource_meta=streaming_deps["resource_meta"],
        service=streaming_deps["service"],
        execution_plan=streaming_deps["execution_plan"],
    )
    batch = RawDataBatch(raw_data=[{"x": 1}], request_context=None)
    c.batches = [batch, batch]
    df1 = MagicMock()
    df2 = MagicMock()
    union_df = MagicMock()
    df1.unionByName.return_value = union_df
    c._parse_data = MagicMock(side_effect=[df1, df2])  # type: ignore[method-assign]

    out = c._parse_batches()
    assert out is union_df
    df1.unionByName.assert_called_once_with(df2, allowMissingColumns=True)


def test_streaming_flush_with_empty_dataframe_does_not_store(streaming_deps: dict) -> None:
    c = StreamingRawDataCollector(
        redis_context=streaming_deps["redis_context"],
        resource_key="rk",
        flush_threshold=1,
        spark=streaming_deps["spark"],
        resource_meta=streaming_deps["resource_meta"],
        service=streaming_deps["service"],
        execution_plan=streaming_deps["execution_plan"],
    )
    c.batches = [RawDataBatch(raw_data=[{"x": 1}], request_context=None)]
    empty_df = MagicMock()
    empty_df.count.return_value = 0
    c._parse_batches = MagicMock(return_value=empty_df)  # type: ignore[method-assign]

    c._flush_to_redis()
    streaming_deps["redis_context"].store.assert_not_called()
    assert c.flush_count == 1
    assert c.batches == []


def test_streaming_consolidate_falls_back_when_temp_missing(streaming_deps: dict) -> None:
    c = StreamingRawDataCollector(
        redis_context=streaming_deps["redis_context"],
        resource_key="rk",
        flush_threshold=1,
        spark=streaming_deps["spark"],
        resource_meta=streaming_deps["resource_meta"],
        service=streaming_deps["service"],
        execution_plan=streaming_deps["execution_plan"],
    )
    fallback_df = MagicMock()
    c.total_parsed_df = fallback_df
    c.flush_count = 1
    streaming_deps["redis_context"].get.return_value = None

    assert c._consolidate_temp_data() is fallback_df


def test_streaming_cleanup_temp_keys_warns_without_raising(streaming_deps: dict) -> None:
    c = StreamingRawDataCollector(
        redis_context=streaming_deps["redis_context"],
        resource_key="rk",
        flush_threshold=1,
        spark=streaming_deps["spark"],
        resource_meta=streaming_deps["resource_meta"],
        service=streaming_deps["service"],
        execution_plan=streaming_deps["execution_plan"],
    )
    c.flush_count = 2
    streaming_deps["redis_context"].delete.side_effect = RuntimeError("redis down")

    c._cleanup_temp_keys()


def test_streaming_finalize_flushes_pending_and_cleans_state(streaming_deps: dict) -> None:
    c = StreamingRawDataCollector(
        redis_context=streaming_deps["redis_context"],
        resource_key="rk",
        flush_threshold=10,
        spark=streaming_deps["spark"],
        resource_meta=streaming_deps["resource_meta"],
        service=streaming_deps["service"],
        execution_plan=streaming_deps["execution_plan"],
    )
    c.batches = [RawDataBatch(raw_data=[{"x": 1}], request_context={})]
    final_df = MagicMock()
    total_df = MagicMock()
    c._flush_to_redis = MagicMock(side_effect=lambda: setattr(c, "total_parsed_df", total_df))  # type: ignore[method-assign]
    c._consolidate_temp_data = MagicMock(return_value=final_df)  # type: ignore[method-assign]
    c._cleanup_temp_keys = MagicMock()  # type: ignore[method-assign]

    out = c.finalize()
    assert out is final_df
    c._flush_to_redis.assert_called_once()
    c._cleanup_temp_keys.assert_called_once()
    assert c.total_parsed_df is None


def test_streaming_parse_data_empty_input_returns_none(streaming_deps: dict) -> None:
    c = StreamingRawDataCollector(
        redis_context=streaming_deps["redis_context"],
        resource_key="rk",
        flush_threshold=2,
        spark=streaming_deps["spark"],
        resource_meta=streaming_deps["resource_meta"],
        service=streaming_deps["service"],
        execution_plan=streaming_deps["execution_plan"],
    )
    assert c._parse_data([], c.resource_meta) is None


def test_streaming_parse_data_parser_error_raises(streaming_deps: dict) -> None:
    c = StreamingRawDataCollector(
        redis_context=streaming_deps["redis_context"],
        resource_key="rk",
        flush_threshold=2,
        spark=streaming_deps["spark"],
        resource_meta=streaming_deps["resource_meta"],
        service=streaming_deps["service"],
        execution_plan=streaming_deps["execution_plan"],
    )
    c.parser = MagicMock()
    c.parser.parse.side_effect = RuntimeError("parse broke")
    with pytest.raises(RuntimeError, match="parse broke"):
        c._parse_data([{"x": 1}], c.resource_meta)


def test_streaming_parse_batches_returns_none_with_no_batches(streaming_deps: dict) -> None:
    c = StreamingRawDataCollector(
        redis_context=streaming_deps["redis_context"],
        resource_key="rk",
        flush_threshold=2,
        spark=streaming_deps["spark"],
        resource_meta=streaming_deps["resource_meta"],
        service=streaming_deps["service"],
        execution_plan=streaming_deps["execution_plan"],
    )
    c.batches = []
    assert c._parse_batches() is None


def test_streaming_parse_batches_returns_none_when_all_parses_none(streaming_deps: dict) -> None:
    c = StreamingRawDataCollector(
        redis_context=streaming_deps["redis_context"],
        resource_key="rk",
        flush_threshold=2,
        spark=streaming_deps["spark"],
        resource_meta=streaming_deps["resource_meta"],
        service=streaming_deps["service"],
        execution_plan=streaming_deps["execution_plan"],
    )
    c.batches = [RawDataBatch(raw_data=[{"x": 1}], request_context={})]
    c._parse_data = MagicMock(return_value=None)  # type: ignore[method-assign]
    assert c._parse_batches() is None


def test_streaming_parse_batches_returns_single_df_without_union(streaming_deps: dict) -> None:
    c = StreamingRawDataCollector(
        redis_context=streaming_deps["redis_context"],
        resource_key="rk",
        flush_threshold=2,
        spark=streaming_deps["spark"],
        resource_meta=streaming_deps["resource_meta"],
        service=streaming_deps["service"],
        execution_plan=streaming_deps["execution_plan"],
    )
    df = MagicMock()
    c.batches = [RawDataBatch(raw_data=[{"x": 1}], request_context={})]
    c._parse_data = MagicMock(return_value=df)  # type: ignore[method-assign]
    out = c._parse_batches()
    assert out is df
    df.unionByName.assert_not_called()


@pytest.fixture
def disk_deps(tmp_path) -> dict:
    redis_context = MagicMock()
    spark = MagicMock()
    resource_meta = SimpleNamespace(
        resource_name="r",
        config=SimpleNamespace(fields=None, transformations=[], response_key=None),
    )
    service = SimpleNamespace(source_name="src")
    execution_plan = MagicMock()
    return {
        "disk_path": str(tmp_path / "collector"),
        "redis_context": redis_context,
        "spark": spark,
        "resource_meta": resource_meta,
        "service": service,
        "execution_plan": execution_plan,
    }


def _make_disk_collector(disk_deps: dict, monkeypatch) -> DiskStreamingDataCollector:
    parser = MagicMock()
    monkeypatch.setattr(DiskStreamingDataCollector, "_create_parser", lambda self: parser)
    collector = DiskStreamingDataCollector(
        disk_path=disk_deps["disk_path"],
        resource_key="rk",
        file_size_threshold=10,
        spark=disk_deps["spark"],
        redis_context=disk_deps["redis_context"],
        resource_meta=disk_deps["resource_meta"],
        service=disk_deps["service"],
        execution_plan=disk_deps["execution_plan"],
    )
    collector.parser = parser
    return collector


def test_disk_collector_init_fails_when_file_creation_fails(disk_deps: dict, monkeypatch) -> None:
    monkeypatch.setattr(
        DiskStreamingDataCollector,
        "_create_new_file",
        lambda self: (_ for _ in ()).throw(RuntimeError("cannot create file")),
    )
    monkeypatch.setattr(DiskStreamingDataCollector, "_create_parser", lambda self: MagicMock())
    with pytest.raises(RuntimeError, match="cannot create file"):
        DiskStreamingDataCollector(
            disk_path=disk_deps["disk_path"],
            resource_key="rk",
            file_size_threshold=10,
            spark=disk_deps["spark"],
            redis_context=disk_deps["redis_context"],
            resource_meta=disk_deps["resource_meta"],
            service=disk_deps["service"],
            execution_plan=disk_deps["execution_plan"],
        )


def test_disk_parse_batch_guardrail_returns_empty(disk_deps: dict, monkeypatch) -> None:
    c = _make_disk_collector(disk_deps, monkeypatch)
    out = c._parse_batch_to_records(RawDataBatch(raw_data=[], request_context=None))
    assert out == {"schema": None, "records": []}


def test_disk_parse_batch_parser_exception_raises(disk_deps: dict, monkeypatch) -> None:
    c = _make_disk_collector(disk_deps, monkeypatch)
    c.parser.parse_to_records.side_effect = RuntimeError("parse failed")
    with pytest.raises(RuntimeError, match="parse failed"):
        c._parse_batch_to_records(RawDataBatch(raw_data=[{"x": 1}], request_context=None))


def test_disk_enrich_records_adds_request_columns_without_duplicate_schema(
    disk_deps: dict, monkeypatch
) -> None:
    t = SimpleNamespace(
        type=TransformationType.ADD_COLUMN_FROM_REQUEST,
        name="_advertiser_id",
        source="advertiser_id",
        location="parameters",
        data_type="string",
    )
    disk_deps["resource_meta"].config.transformations = [t]
    c = _make_disk_collector(disk_deps, monkeypatch)
    c.parser._get_request_value.return_value = 42
    schema = StructType([StructField("_advertiser_id", StringType(), False)])
    parse_result = {"schema": schema, "records": [{"x": "a"}]}

    out = c._enrich_records_with_request_columns(
        parse_result, {"parameters": {"advertiser_id": 42}}
    )
    assert out["records"][0]["_advertiser_id"] == "42"
    assert len(out["schema"].fields) == 1


def test_disk_write_parse_result_requires_current_file_path(disk_deps: dict, monkeypatch) -> None:
    c = _make_disk_collector(disk_deps, monkeypatch)
    c.current_file_path = None
    with pytest.raises(ValueError, match="Current file path is not initialized"):
        c._write_parse_result_to_disk({"records": [{"x": 1}]})


def test_disk_should_rotate_returns_false_on_filesize_errors(disk_deps: dict, monkeypatch) -> None:
    c = _make_disk_collector(disk_deps, monkeypatch)
    monkeypatch.setattr("src.collector.disk_streaming_collector.os.path.exists", lambda _p: True)
    monkeypatch.setattr(
        "src.collector.disk_streaming_collector.os.path.getsize",
        lambda _p: (_ for _ in ()).throw(RuntimeError("stat failed")),
    )
    assert c._should_rotate_file() is False


def test_disk_finalize_no_files_returns_none(disk_deps: dict, monkeypatch) -> None:
    c = _make_disk_collector(disk_deps, monkeypatch)
    monkeypatch.setattr(c, "_get_all_ndjson_files", lambda: [])
    assert c.finalize() is None


def test_disk_finalize_runs_cleanup_in_finally_on_failure(disk_deps: dict, monkeypatch) -> None:
    c = _make_disk_collector(disk_deps, monkeypatch)
    cleanup_files = MagicMock()
    cleanup_path = MagicMock()
    monkeypatch.setattr(c, "_get_all_ndjson_files", lambda: ["a.ndjson"])
    c.spark.read.json.side_effect = RuntimeError("read failed")
    monkeypatch.setattr(c, "_cleanup_ndjson_files", cleanup_files)
    monkeypatch.setattr(c, "cleanup_disk_path", cleanup_path)

    with pytest.raises(RuntimeError, match="read failed"):
        c.finalize()
    cleanup_files.assert_called_once()
    cleanup_path.assert_called_once()


def test_disk_finalize_success_reads_persists_and_cleans(disk_deps: dict, monkeypatch) -> None:
    c = _make_disk_collector(disk_deps, monkeypatch)
    ndjson = Path(disk_deps["disk_path"]) / "rk_1.ndjson"
    cleanup_files = MagicMock()
    cleanup_path = MagicMock()
    df = MagicMock()
    df.rdd.isEmpty.return_value = False
    df.persist.return_value = df
    df.count.return_value = 3
    c.spark.read.json.return_value = df
    monkeypatch.setattr(c, "_get_all_ndjson_files", lambda: [str(ndjson)])
    monkeypatch.setattr(c, "_cleanup_ndjson_files", cleanup_files)
    monkeypatch.setattr(c, "cleanup_disk_path", cleanup_path)

    out = c.finalize()
    assert out is df
    df.persist.assert_called_once()
    cleanup_files.assert_called_once()
    cleanup_path.assert_called_once()


def test_disk_add_batch_merges_schema_and_rotates(disk_deps: dict, monkeypatch) -> None:
    c = _make_disk_collector(disk_deps, monkeypatch)
    s1 = StructType([StructField("a", StringType(), True)])
    s2 = StructType([StructField("b", StringType(), True)])
    c._parse_batch_to_records = MagicMock(  # type: ignore[method-assign]
        side_effect=[
            {"schema": s1, "records": [{"a": "1"}]},
            {"schema": s2, "records": [{"b": "2"}]},
        ]
    )
    c._write_parse_result_to_disk = MagicMock()  # type: ignore[method-assign]
    c._should_rotate_file = MagicMock(side_effect=[False, True])  # type: ignore[method-assign]
    c._rotate_file = MagicMock()  # type: ignore[method-assign]

    c.add_batch(RawDataBatch(raw_data=[{"a": 1}], request_context={}))
    c.add_batch(RawDataBatch(raw_data=[{"b": 2}], request_context={}))

    assert c.schema is not None
    assert set(c.schema.fieldNames()) == {"a", "b"}
    c._rotate_file.assert_called_once()


def test_disk_parse_ndjson_file_skips_bad_line_and_infers_schema(
    disk_deps: dict, monkeypatch
) -> None:
    c = _make_disk_collector(disk_deps, monkeypatch)
    p = Path(disk_deps["disk_path"]) / "rk_file.ndjson"
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text("{not-json}\n" + '{"records":[{"x":"1"}]}\n', encoding="utf-8")
    df = MagicMock()
    df.count.return_value = 1
    c.spark.createDataFrame.return_value = df

    out = c._parse_ndjson_file(str(p))
    assert out is df
    c.spark.createDataFrame.assert_called_once()


def test_disk_parse_ndjson_file_missing_or_empty_returns_none(disk_deps: dict, monkeypatch) -> None:
    c = _make_disk_collector(disk_deps, monkeypatch)
    missing = str(Path(disk_deps["disk_path"]) / "missing.ndjson")
    assert c._parse_ndjson_file(missing) is None

    empty = Path(disk_deps["disk_path"]) / "empty.ndjson"
    empty.parent.mkdir(parents=True, exist_ok=True)
    empty.write_text("", encoding="utf-8")
    assert c._parse_ndjson_file(str(empty)) is None


def test_disk_parse_ndjson_file_uses_embedded_schema(disk_deps: dict, monkeypatch) -> None:
    c = _make_disk_collector(disk_deps, monkeypatch)
    schema = StructType([StructField("id", StringType(), True)])
    p = Path(disk_deps["disk_path"]) / "rk_schema.ndjson"
    p.parent.mkdir(parents=True, exist_ok=True)
    line = {"schema": schema.jsonValue(), "records": [{"id": "1"}]}
    p.write_text(json.dumps(line) + "\n", encoding="utf-8")
    df = MagicMock()
    df.count.return_value = 1
    c.spark.createDataFrame.return_value = df

    out = c._parse_ndjson_file(str(p))
    assert out is df
    assert c.spark.createDataFrame.call_args.kwargs["schema"] is not None
