"""Behavior-focused tests for ``ObjectStoreLoader`` at Spark/store boundaries.

Table-format mechanics live under ``src.load_strategy``. These tests keep the
loader focused on DataFrame preparation, file-based writes, and delegation to
load strategies for Delta/Iceberg.
"""

from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import MagicMock, patch

import pytest
from pyspark.sql.types import StringType, StructField, StructType

import src.loader.object_store_loader as object_store_loader_module
from src.config.config_models import LoadingConfig, LoadingFormat
from src.loader.object_store_loader import ObjectStoreLoader, retry_on_transient_storage_error
from src.utils.exceptions import LoaderError


class _DummySparkDataFrame:
    """Stand-in for ``pyspark.sql.DataFrame`` when Spark cannot bind in tests."""

    def __init__(self) -> None:
        self.columns: list[str] = []


def _df_chain(columns: list[str] | None = None):
    """Return a DataFrame mock with the minimal API used by ``ObjectStoreLoader``."""
    df = MagicMock()
    writer = MagicMock()
    writer.format.return_value = writer
    writer.mode.return_value = writer
    writer.options.return_value = writer
    writer.save.return_value = None
    df.write = writer
    df.coalesce.return_value = df
    df.withColumnRenamed.return_value = df
    df.columns = columns or ["id", "Bad Col#"]
    df.count.return_value = 1
    return df, writer


def _set_store(loader: ObjectStoreLoader, store: MagicMock | None = None) -> MagicMock:
    actual = store or MagicMock(name="object_store")
    loader._object_store = actual
    return actual


def test_object_store_property_requires_spark() -> None:
    loader = ObjectStoreLoader()
    with pytest.raises(LoaderError, match="Spark session not set"):
        _ = loader.object_store


def test_set_spark_session_initializes_object_store() -> None:
    loader = ObjectStoreLoader()
    spark = MagicMock()

    loader.set_spark_session(spark)

    assert loader.spark is spark
    assert loader.object_store is loader._object_store


def test_ensure_dataframe_accepts_dataframe() -> None:
    loader = ObjectStoreLoader()
    loader.spark = MagicMock()
    with patch.object(object_store_loader_module, "DataFrame", _DummySparkDataFrame):
        df = _DummySparkDataFrame()
        df.columns = ["n"]
        out = loader._ensure_dataframe(cast(Any, df), None)
        assert out is df


def test_ensure_dataframe_list_uses_create_dataframe() -> None:
    loader = ObjectStoreLoader()
    spark = MagicMock()
    sample_schema = StructType([StructField("a", StringType(), True)])
    sample_df = MagicMock()
    sample_df.schema = sample_schema
    final_df = MagicMock()
    final_df.count.return_value = 1
    spark.createDataFrame.side_effect = [sample_df, final_df]
    loader.spark = spark

    out = loader._ensure_dataframe([{"a": 1}], None)

    assert out is final_df
    assert spark.createDataFrame.call_count == 2


def test_ensure_dataframe_invalid_input_type_raises_loader_error() -> None:
    loader = ObjectStoreLoader()
    loader.spark = MagicMock()
    loader.logger = MagicMock()
    with patch.object(object_store_loader_module, "DataFrame", _DummySparkDataFrame):
        with pytest.raises(LoaderError, match="Failed to ensure Spark DataFrame"):
            loader._ensure_dataframe(cast(Any, object()), None)


def test_create_dataframe_exception_raises_loader_error() -> None:
    loader = ObjectStoreLoader()
    spark = MagicMock()
    spark.createDataFrame.side_effect = RuntimeError("spark exploded")
    loader.set_spark_session(spark)
    loader.logger = MagicMock()

    with pytest.raises(LoaderError, match="Failed to create Spark DataFrame"):
        loader._create_dataframe([{"id": 1}], schema=None)


def test_format_prefix_and_temp_path() -> None:
    loader = ObjectStoreLoader()
    loader.spark = MagicMock()
    store = _set_store(loader)
    store.resolve_path.return_value = "s3a://b/prefix/data/_temp/spark_writes/k1"

    assert loader._format_prefix("  a/b  ") == "  a/b  /data"
    assert loader._format_prefix(None) == "data"

    out = loader._generate_temp_path("s3a://b", "a/b", "k1")

    assert out == "s3a://b/prefix/data/_temp/spark_writes/k1"
    store.resolve_path.assert_called_once_with(
        "s3a://b", "a/b/data", "_temp", "spark_writes", "k1", trailing_slash=False
    )


@patch("src.loader.object_store_loader.datetime")
@patch("src.loader.object_store_loader.uuid")
def test_generate_final_path(mock_uuid, mock_dt) -> None:
    mock_dt.now.return_value.strftime.return_value = "T"
    mock_uuid.uuid4.return_value = "u1"
    loader = ObjectStoreLoader()
    loader.spark = MagicMock()
    store = _set_store(loader)
    store.resolve_path.return_value = "s3a://b/path/T_u1.parquet"

    uri, key = loader._generate_final_path("s3a://b", "pre", "parquet")

    assert uri == "s3a://b/path/T_u1.parquet"
    assert key == "T_u1.parquet"


def test_write_dataframe_uses_path_writer() -> None:
    df, writer = _df_chain()
    loader = ObjectStoreLoader()
    loader.spark = MagicMock()

    loader._write_dataframe(
        df,
        "s3a://b/out",
        {"format": "parquet", "mode": "overwrite", "compression": "snappy"},
    )

    df.coalesce.assert_called_once_with(1)
    writer.format.assert_called_once_with("parquet")
    writer.mode.assert_called_once_with("overwrite")
    writer.options.assert_called_once_with(compression="snappy")
    writer.save.assert_called_once_with("s3a://b/out")


def test_sanitize_and_rename_columns() -> None:
    loader = ObjectStoreLoader()
    df = MagicMock()
    df.columns = ["a b#", "ok"]
    df.withColumnRenamed.return_value = df

    out = loader._sanitize_column_names(df)

    assert out is df
    df.withColumnRenamed.assert_called_once_with("a b#", "a_b")

    df2 = MagicMock()
    df2.columns = ["A", "a"]
    df2.toDF.return_value = df2

    out2 = loader._rename_duplicate_columns(df2)

    assert out2 is df2
    df2.toDF.assert_called_once_with("A", "a_1")


def test_prepare_dataframe_for_load_sanitizes_and_optionally_deduplicates() -> None:
    loader = ObjectStoreLoader()
    df, _writer = _df_chain()
    deduped = MagicMock()
    deduped.count.return_value = 1
    df.dropDuplicates.return_value = deduped
    loader_mock = cast(Any, loader)
    loader_mock._ensure_dataframe = MagicMock(return_value=df)
    loader_mock._prepare_dataframe_columns = MagicMock(return_value=df)
    cfg = LoadingConfig(
        destination="s3",
        s3_bucket="b",
        prefix="s/r",
        write_mode="merge",
        merge_keys=["id"],
        force_nondeterministic_deduplication=True,
    )

    out = loader._prepare_dataframe_for_load([{"id": 1}], None, cfg)

    assert out is deduped
    df.dropDuplicates.assert_called_once_with(["id"])


def test_load_requires_spark() -> None:
    loader = ObjectStoreLoader()
    with pytest.raises(LoaderError, match="Spark session not set"):
        loader.load([{"a": 1}], MagicMock(destination="s3", format=LoadingFormat.DELTA))


def test_load_delegates_delta_and_iceberg_to_load_strategy_factory() -> None:
    loader = ObjectStoreLoader()
    df, _writer = _df_chain()
    loader.set_spark_session(MagicMock())
    store = _set_store(loader)
    prepared_df = MagicMock()
    loader_mock = cast(Any, loader)
    loader_mock._prepare_dataframe_for_load = MagicMock(return_value=prepared_df)
    strategy = MagicMock()
    strategy.write.side_effect = ["delta-location", "iceberg-location"]

    with patch("src.loader.object_store_loader.LoadStrategyFactory") as factory:
        factory.create_load_strategy.return_value = strategy
        delta_cfg = LoadingConfig(
            destination="s3",
            s3_bucket="b",
            prefix="src/res",
            format=LoadingFormat.DELTA,
            write_mode="overwrite",
        )
        iceberg_cfg = LoadingConfig(
            destination="s3",
            s3_bucket="b",
            prefix="src/res",
            format=LoadingFormat.ICEBERG,
            write_mode="overwrite",
        )

        assert loader.load(df, delta_cfg, source_type="rest_api") == "delta-location"
        assert loader.load(df, iceberg_cfg, source_type="rest_api") == "iceberg-location"

    assert factory.create_load_strategy.call_count == 2
    first_call = factory.create_load_strategy.call_args_list[0]
    assert first_call.args == (loader.spark, store, "s3a://b", delta_cfg)
    assert first_call.kwargs == {"source_type": "rest_api"}
    strategy.write.assert_any_call(prepared_df)


def test_load_uses_file_based_path_for_non_table_format() -> None:
    loader = ObjectStoreLoader()
    df, _writer = _df_chain()
    loader.set_spark_session(MagicMock())
    loader_mock = cast(Any, loader)
    loader_mock._prepare_dataframe_for_load = MagicMock(return_value=df)
    loader_mock._load_file_based = MagicMock(return_value="file-key.parquet")
    cfg = SimpleNamespace(
        destination="s3",
        s3_bucket="b",
        bucket="b",
        gcs_bucket=None,
        azure_container=None,
        azure_account=None,
        storage_root=None,
        prefix="src/res",
        format="parquet",
        write_mode="overwrite",
        compression="snappy",
        force_nondeterministic_deduplication=False,
    )

    assert loader.load(df, cast(LoadingConfig, cfg), source_type="rest_api") == "file-key.parquet"

    loader_mock._load_file_based.assert_called_once_with(
        df, cfg, base_uri="s3a://b", source_type="rest_api"
    )


def test_load_raises_loader_error_on_invalid_base_uri() -> None:
    loader = ObjectStoreLoader()
    loader.set_spark_session(MagicMock())
    loader.logger = MagicMock()
    cfg = LoadingConfig(destination="s3", s3_bucket="b", prefix="a/r", format=LoadingFormat.DELTA)

    with patch(
        "src.loader.object_store_loader.loading_base_uri", side_effect=ValueError("bad uri")
    ):
        with pytest.raises(LoaderError, match="bad uri"):
            loader.load([{"id": 1}], cfg)


def _file_based_loader(write_side_effect=None, part_uri="s3a://b/tmp/part-0.parquet"):
    loader = ObjectStoreLoader()
    spark = MagicMock()
    loader.set_spark_session(spark)
    store = _set_store(loader)
    df, _writer = _df_chain()
    loader_mock = cast(Any, loader)
    loader_mock._write_dataframe = MagicMock(side_effect=write_side_effect)
    loader_mock._generate_final_path = MagicMock(
        return_value=("s3a://b/final.parquet", "final.parquet")
    )
    loader_mock._generate_temp_path = MagicMock(return_value="s3a://b/tmp/spark_writes/k1")
    loader_mock._move_uri = MagicMock()
    loader_mock._cleanup_temp_dir = MagicMock()
    store.glob_first_part_file.return_value = part_uri
    return loader, df


def test_load_file_based_writes_to_temp_then_moves() -> None:
    loader, df = _file_based_loader()
    cfg = LoadingConfig(destination="s3", s3_bucket="b", prefix="s/r", write_mode="overwrite")

    result = loader._load_file_based(df, cfg, base_uri="s3a://b")

    loader._write_dataframe.assert_called_once()
    loader._move_uri.assert_called_once_with(
        loader.object_store, "s3a://b/tmp/part-0.parquet", "s3a://b/final.parquet"
    )
    loader._cleanup_temp_dir.assert_called_once_with(
        loader.object_store, "s3a://b/tmp/spark_writes/k1"
    )
    assert result == "final.parquet"


def test_load_file_based_cleans_up_temp_on_write_error() -> None:
    loader, df = _file_based_loader(write_side_effect=LoaderError("disk full"))
    cfg = LoadingConfig(destination="s3", s3_bucket="b", prefix="s/r", write_mode="overwrite")

    with pytest.raises(LoaderError, match="disk full"):
        loader._load_file_based(df, cfg, base_uri="s3a://b")

    loader._cleanup_temp_dir.assert_called_once_with(
        loader.object_store, "s3a://b/tmp/spark_writes/k1"
    )


def test_load_file_based_raises_when_no_part_file_found() -> None:
    loader, df = _file_based_loader(part_uri=None)
    cfg = LoadingConfig(destination="s3", s3_bucket="b", prefix="s/r", write_mode="overwrite")

    with pytest.raises(LoaderError, match="No part file found"):
        loader._load_file_based(df, cfg, base_uri="s3a://b")


def test_move_and_cleanup_temp_dir() -> None:
    loader = ObjectStoreLoader()
    jvm = MagicMock()
    path_obj = MagicMock()
    path_obj.getParent.return_value.toString.return_value = "/x/spark_writes"
    path_obj.getParent.return_value.getParent.return_value.toString.return_value = "/x"
    jvm.org.apache.hadoop.fs.Path.return_value = path_obj
    loader.spark = MagicMock()
    loader.spark.sparkContext._jvm = jvm
    store = MagicMock()
    store.is_empty_directory.return_value = True

    loader._move_uri(store, "a", "b")
    loader._cleanup_temp_dir(store, "s3a://b/tmp/file")

    store.move.assert_called_once_with("a", "b")
    store.delete.assert_any_call("s3a://b/tmp/file", recursive=True)
    store.delete.assert_any_call("/x/spark_writes", recursive=True)
    store.delete.assert_any_call("/x", recursive=True)


def test_cleanup_temp_dir_exception_logged() -> None:
    loader = ObjectStoreLoader()
    spark = MagicMock()
    jvm = MagicMock()
    path_obj = MagicMock()
    path_obj.getParent.return_value.toString.return_value = "s3a://b/.spark-writes/"
    path_obj.getParent.return_value.getParent.return_value.toString.return_value = "s3a://b/.temp/"
    jvm.org.apache.hadoop.fs.Path.return_value = path_obj
    spark.sparkContext._jvm = jvm
    loader.set_spark_session(spark)
    loader.logger = MagicMock()
    store = MagicMock()
    store.delete.side_effect = RuntimeError("delete failed")

    loader._cleanup_temp_dir(store, "s3a://b/.temp/.spark-writes/part-0.parquet")

    loader.logger.warning.assert_called_once()


def test_destination_exists_returns_false_for_non_object_store_or_non_table_format() -> None:
    loader = ObjectStoreLoader()
    loader.set_spark_session(MagicMock())

    assert (
        loader.destination_exists(
            cast(LoadingConfig, SimpleNamespace(destination="sftp", format=LoadingFormat.DELTA))
        )
        is False
    )
    assert (
        loader.destination_exists(
            cast(LoadingConfig, SimpleNamespace(destination="s3", format="parquet"))
        )
        is False
    )


def test_destination_exists_returns_false_when_required_fields_missing_or_no_spark() -> None:
    loader = ObjectStoreLoader()
    loader.set_spark_session(MagicMock())
    assert (
        loader.destination_exists(
            LoadingConfig(destination="s3", s3_bucket="b", format=LoadingFormat.DELTA)
        )
        is False
    )

    no_spark = ObjectStoreLoader()
    cfg = LoadingConfig(destination="s3", s3_bucket="b", prefix="a/r", format=LoadingFormat.DELTA)
    assert no_spark.destination_exists(cfg) is False


def test_destination_exists_delegates_to_load_strategy_table_exists() -> None:
    loader = ObjectStoreLoader()
    loader.set_spark_session(MagicMock())
    store = _set_store(loader)
    strategy = MagicMock()
    strategy.table_exists.return_value = True
    cfg = LoadingConfig(destination="s3", s3_bucket="b", prefix="a/r", format=LoadingFormat.DELTA)

    with patch("src.loader.object_store_loader.LoadStrategyFactory") as factory:
        factory.create_load_strategy.return_value = strategy
        result = loader.destination_exists(cfg, source_type="rest_api")

    assert result is True
    factory.create_load_strategy.assert_called_once_with(
        loader.spark, store, "s3a://b", cfg, source_type="rest_api"
    )
    strategy.table_exists.assert_called_once_with()


def test_destination_exists_false_when_loading_base_uri_raises() -> None:
    loader = ObjectStoreLoader()
    loader.set_spark_session(MagicMock())
    cfg = LoadingConfig(destination="s3", s3_bucket="b", prefix="a/r", format=LoadingFormat.DELTA)

    with patch(
        "src.loader.object_store_loader.loading_base_uri", side_effect=ValueError("bad uri")
    ):
        assert loader.destination_exists(cfg) is False


def test_retry_gives_up_on_non_transient() -> None:
    @retry_on_transient_storage_error(max_retries=2, delay=0)
    def boom():
        raise RuntimeError("permanent failure")

    with pytest.raises(RuntimeError, match="permanent"):
        boom()


def test_retry_succeeds_after_one_transient_failure() -> None:
    attempts = []

    @retry_on_transient_storage_error(max_retries=3, delay=0)
    def flaky():
        if not attempts:
            attempts.append(1)
            raise RuntimeError("Connection reset by peer")
        return "ok"

    assert flaky() == "ok"
    assert len(attempts) == 1
