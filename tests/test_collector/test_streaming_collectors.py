"""Lightweight tests for streaming collectors (Spark and Redis mocked)."""

import builtins
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
    df_mock.rdd.isEmpty.return_value = False
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
    empty_df.rdd.isEmpty.return_value = True
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


def test_streaming_is_empty_true_when_no_batches_and_no_accumulated_df(
    streaming_deps: dict,
) -> None:
    c = StreamingRawDataCollector(
        redis_context=streaming_deps["redis_context"],
        resource_key="rk",
        flush_threshold=3,
        spark=streaming_deps["spark"],
        resource_meta=streaming_deps["resource_meta"],
        service=streaming_deps["service"],
        execution_plan=streaming_deps["execution_plan"],
    )
    assert c.is_empty() is True


def test_streaming_is_empty_false_when_accumulated_df(streaming_deps: dict) -> None:
    c = StreamingRawDataCollector(
        redis_context=streaming_deps["redis_context"],
        resource_key="rk",
        flush_threshold=3,
        spark=streaming_deps["spark"],
        resource_meta=streaming_deps["resource_meta"],
        service=streaming_deps["service"],
        execution_plan=streaming_deps["execution_plan"],
    )
    c.total_parsed_df = MagicMock()
    assert c.is_empty() is False


def test_streaming_second_flush_merges_via_union_by_name(streaming_deps: dict) -> None:
    """After the first parsed flush, the second merges with unionByName on the accumulator."""
    c = StreamingRawDataCollector(
        redis_context=streaming_deps["redis_context"],
        resource_key="rk",
        flush_threshold=1,
        spark=streaming_deps["spark"],
        resource_meta=streaming_deps["resource_meta"],
        service=streaming_deps["service"],
        execution_plan=streaming_deps["execution_plan"],
    )
    df1 = MagicMock()
    df1.rdd.isEmpty.return_value = False
    df2 = MagicMock()
    df2.rdd.isEmpty.return_value = False
    merged = MagicMock()
    df1.unionByName.return_value = merged
    c._parse_batches = MagicMock(side_effect=[df1, df2])  # type: ignore[method-assign]

    batch = RawDataBatch(raw_data=[{"x": 1}], request_context=None)
    c.add_batch(batch)
    assert c.total_parsed_df is df1
    c.add_batch(batch)
    df1.unionByName.assert_called_once_with(df2, allowMissingColumns=True)
    assert c.total_parsed_df is merged


def test_streaming_finalize_returns_none_without_batches_or_accumulator(
    streaming_deps: dict,
) -> None:
    c = StreamingRawDataCollector(
        redis_context=streaming_deps["redis_context"],
        resource_key="rk",
        flush_threshold=10,
        spark=streaming_deps["spark"],
        resource_meta=streaming_deps["resource_meta"],
        service=streaming_deps["service"],
        execution_plan=streaming_deps["execution_plan"],
    )
    c._consolidate_temp_data = MagicMock()  # type: ignore[method-assign]
    assert c.finalize() is None
    c._consolidate_temp_data.assert_not_called()


def test_streaming_consolidate_returns_accumulator_when_flush_count_zero(
    streaming_deps: dict,
) -> None:
    c = StreamingRawDataCollector(
        redis_context=streaming_deps["redis_context"],
        resource_key="rk",
        flush_threshold=10,
        spark=streaming_deps["spark"],
        resource_meta=streaming_deps["resource_meta"],
        service=streaming_deps["service"],
        execution_plan=streaming_deps["execution_plan"],
    )
    df = MagicMock()
    c.total_parsed_df = df
    c.flush_count = 0
    assert c._consolidate_temp_data() is df
    streaming_deps["redis_context"].get.assert_not_called()


def test_streaming_request_context_taken_from_first_batch(streaming_deps: dict) -> None:
    c = StreamingRawDataCollector(
        redis_context=streaming_deps["redis_context"],
        resource_key="rk",
        flush_threshold=10,
        spark=streaming_deps["spark"],
        resource_meta=streaming_deps["resource_meta"],
        service=streaming_deps["service"],
        execution_plan=streaming_deps["execution_plan"],
    )
    first = {"run": 1}
    second = {"run": 2}
    c.add_batch(RawDataBatch(raw_data=[], request_context=first))
    c.add_batch(RawDataBatch(raw_data=[], request_context=second))
    assert c.request_context is first


def test_streaming_spark_parser_construct_failure_propagates(
    streaming_deps: dict, monkeypatch
) -> None:
    monkeypatch.setattr(
        "src.collector.streaming_collector.SparkParser",
        MagicMock(side_effect=RuntimeError("SparkParser init failed")),
    )
    with pytest.raises(RuntimeError, match="SparkParser init failed"):
        StreamingRawDataCollector(
            redis_context=streaming_deps["redis_context"],
            resource_key="rk",
            flush_threshold=2,
            spark=streaming_deps["spark"],
            resource_meta=streaming_deps["resource_meta"],
            service=streaming_deps["service"],
            execution_plan=streaming_deps["execution_plan"],
        )


def test_streaming_finalize_swallows_unpersist_failure(streaming_deps: dict) -> None:
    """finalize tolerates unpersist errors when the accumulator was never cached."""
    c = StreamingRawDataCollector(
        redis_context=streaming_deps["redis_context"],
        resource_key="rk",
        flush_threshold=10,
        spark=streaming_deps["spark"],
        resource_meta=streaming_deps["resource_meta"],
        service=streaming_deps["service"],
        execution_plan=streaming_deps["execution_plan"],
    )
    df = MagicMock()
    df.unpersist.side_effect = RuntimeError("not cached")
    c.total_parsed_df = df
    c._consolidate_temp_data = MagicMock(return_value=df)  # type: ignore[method-assign]
    c._cleanup_temp_keys = MagicMock()  # type: ignore[method-assign]
    assert c.finalize() is df


def test_streaming_parse_batches_failure_propagates_after_logging(streaming_deps: dict) -> None:
    c = StreamingRawDataCollector(
        redis_context=streaming_deps["redis_context"],
        resource_key="rk",
        flush_threshold=2,
        spark=streaming_deps["spark"],
        resource_meta=streaming_deps["resource_meta"],
        service=streaming_deps["service"],
        execution_plan=streaming_deps["execution_plan"],
    )
    c.batches = [RawDataBatch(raw_data=[{"x": 1}], request_context=None)]
    c._parse_data = MagicMock(side_effect=RuntimeError("parse batch failed"))  # type: ignore[method-assign]
    with pytest.raises(RuntimeError, match="parse batch failed"):
        c._parse_batches()


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


def test_disk_merge_schemas_merges_duplicate_field_nullability(
    disk_deps: dict, monkeypatch
) -> None:
    c = _make_disk_collector(disk_deps, monkeypatch)
    s1 = StructType([StructField("a", StringType(), nullable=False)])
    s2 = StructType([StructField("a", StringType(), nullable=True)])
    merged = c._merge_schemas(s1, s2)
    assert len(merged.fields) == 1
    assert merged.fields[0].nullable is True


def test_disk_add_batch_no_records_skips_write(disk_deps: dict, monkeypatch) -> None:
    c = _make_disk_collector(disk_deps, monkeypatch)
    c._parse_batch_to_records = MagicMock(return_value={"schema": None, "records": []})  # type: ignore[method-assign]
    c._write_parse_result_to_disk = MagicMock()  # type: ignore[method-assign]
    c.add_batch(RawDataBatch(raw_data=[{"x": 1}], request_context={}))
    c._write_parse_result_to_disk.assert_not_called()


def test_disk_is_empty_true_without_files_false_after_write(disk_deps: dict, monkeypatch) -> None:
    c = _make_disk_collector(disk_deps, monkeypatch)
    monkeypatch.setattr(c, "_get_all_ndjson_files", lambda: [])
    assert c.is_empty() is True

    c2 = _make_disk_collector(disk_deps, monkeypatch)
    c2._write_parse_result_to_disk({"records": [{"n": 1}]})
    assert c2.is_empty() is False


def test_disk_finalize_skips_persist_when_rdd_empty(disk_deps: dict, monkeypatch) -> None:
    c = _make_disk_collector(disk_deps, monkeypatch)
    ndjson = Path(disk_deps["disk_path"]) / "rk_empty.ndjson"
    monkeypatch.setattr(c, "_get_all_ndjson_files", lambda: [str(ndjson)])
    df = MagicMock()
    df.rdd.isEmpty.return_value = True
    c.spark.read.json.return_value = df
    monkeypatch.setattr(c, "_cleanup_ndjson_files", MagicMock())
    monkeypatch.setattr(c, "cleanup_disk_path", MagicMock())

    out = c.finalize()
    assert out is df
    df.persist.assert_not_called()


def test_disk_get_all_ndjson_files_returns_empty_when_glob_raises(
    disk_deps: dict, monkeypatch
) -> None:
    c = _make_disk_collector(disk_deps, monkeypatch)
    monkeypatch.setattr(
        "src.collector.disk_streaming_collector.glob.glob",
        MagicMock(side_effect=RuntimeError("glob failed")),
    )
    assert c._get_all_ndjson_files() == []


def test_disk_write_parse_result_to_disk_open_failure_propagates(
    disk_deps: dict, monkeypatch
) -> None:
    c = _make_disk_collector(disk_deps, monkeypatch)

    def boom(*_a, **_kw):
        raise OSError("cannot open")

    monkeypatch.setattr("builtins.open", boom)
    with pytest.raises(OSError, match="cannot open"):
        c._write_parse_result_to_disk({"records": [{"x": 1}]})


def test_disk_cleanup_ndjson_files_warning_when_remove_fails(disk_deps: dict, monkeypatch) -> None:
    c = _make_disk_collector(disk_deps, monkeypatch)
    p = Path(disk_deps["disk_path"]) / "orphan.ndjson"
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text("{}", encoding="utf-8")
    monkeypatch.setattr(
        "src.collector.disk_streaming_collector.os.remove", MagicMock(side_effect=OSError("rm"))
    )
    c._cleanup_ndjson_files([str(p)])


def test_disk_init_fails_when_disk_path_not_created(disk_deps: dict, monkeypatch) -> None:
    monkeypatch.setattr(
        "src.collector.disk_streaming_collector.os.makedirs",
        MagicMock(side_effect=OSError("permission denied")),
    )
    with pytest.raises(OSError, match="permission denied"):
        DiskStreamingDataCollector(
            disk_path=disk_deps["disk_path"],
            resource_key="rk",
            file_size_threshold=1024,
            spark=disk_deps["spark"],
            redis_context=disk_deps["redis_context"],
            resource_meta=disk_deps["resource_meta"],
            service=disk_deps["service"],
            execution_plan=disk_deps["execution_plan"],
        )


def test_disk_create_parser_spark_parser_failure_propagates(disk_deps: dict, monkeypatch) -> None:
    monkeypatch.setattr(
        "src.collector.disk_streaming_collector.SparkParser",
        MagicMock(side_effect=RuntimeError("parser init failed")),
    )
    with pytest.raises(RuntimeError, match="parser init failed"):
        DiskStreamingDataCollector(
            disk_path=disk_deps["disk_path"],
            resource_key="rk2",
            file_size_threshold=1024,
            spark=disk_deps["spark"],
            redis_context=disk_deps["redis_context"],
            resource_meta=disk_deps["resource_meta"],
            service=disk_deps["service"],
            execution_plan=disk_deps["execution_plan"],
        )


def test_disk_should_rotate_and_rotate_file(disk_deps: dict, monkeypatch) -> None:
    c = _make_disk_collector(disk_deps, monkeypatch)
    c.file_size_threshold = 10
    c._write_parse_result_to_disk({"records": [{"blob": "x" * 50}]})
    assert c._should_rotate_file() is True
    prev_counter = c.file_counter
    c._rotate_file()
    assert c.file_counter == prev_counter + 1


def test_disk_rotate_file_requires_current_path(disk_deps: dict, monkeypatch) -> None:
    c = _make_disk_collector(disk_deps, monkeypatch)
    c.current_file_path = None
    with pytest.raises(ValueError, match="Current file path is not initialized"):
        c._rotate_file()


def test_disk_parse_batch_parser_returns_empty_records(disk_deps: dict, monkeypatch) -> None:
    c = _make_disk_collector(disk_deps, monkeypatch)
    c.parser.parse_to_records.return_value = {"records": [], "schema": None}
    batch = RawDataBatch(raw_data=[{"x": 1}], request_context=None)
    assert c._parse_batch_to_records(batch) == {"schema": None, "records": []}


def test_disk_cleanup_ndjson_files_deletes_and_removes_tree(disk_deps: dict, monkeypatch) -> None:
    c = _make_disk_collector(disk_deps, monkeypatch)
    p = Path(c.current_file_path)
    p.write_text("{}\n", encoding="utf-8")
    disk_root = Path(disk_deps["disk_path"])
    assert disk_root.exists()
    c._cleanup_ndjson_files([str(p)])
    assert not disk_root.exists()
    assert c.cleaned_up is True


def test_disk_parse_ndjson_file_outer_error_propagates(disk_deps: dict, monkeypatch) -> None:
    c = _make_disk_collector(disk_deps, monkeypatch)
    p = Path(disk_deps["disk_path"]) / "bad.ndjson"
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text('{"records":[]}\n', encoding="utf-8")

    def broken_open(*_a, **_k):
        raise RuntimeError("open broke")

    monkeypatch.setattr(builtins, "open", broken_open)
    with pytest.raises(RuntimeError, match="open broke"):
        c._parse_ndjson_file(str(p))
