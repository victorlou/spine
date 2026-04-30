"""Behavior-focused tests for ``ObjectStoreLoader`` using mocks at Spark/store boundaries."""

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from pyspark.sql.types import StringType, StructField, StructType

import src.loader.object_store_loader as object_store_loader_module
from src.config.config_models import LoadingConfig, LoadingFormat
from src.loader.object_store_loader import ObjectStoreLoader, retry_on_transient_storage_error
from src.utils.exceptions import LoaderError


class _DummySparkDataFrame:
    """Stand-in for ``pyspark.sql.DataFrame`` when Spark cannot bind (CI/sandbox)."""

    def __init__(self) -> None:
        self.columns: list = []


def test_ensure_dataframe_accepts_dataframe() -> None:
    loader = ObjectStoreLoader()
    loader.spark = MagicMock()
    with patch.object(object_store_loader_module, "DataFrame", _DummySparkDataFrame):
        df = _DummySparkDataFrame()
        df.columns = ["n"]
        out = loader._ensure_dataframe(df, None)
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


def _df_chain():
    """Return a DataFrame mock with a minimal write/alias API used by the loader."""
    df = MagicMock()
    writer = MagicMock()
    format_builder = MagicMock()
    format_builder.format.return_value = format_builder
    format_builder.mode.return_value = format_builder
    format_builder.options.return_value = format_builder
    format_builder.save.return_value = None
    format_builder.saveAsTable.return_value = None
    df.write = format_builder
    df.coalesce.return_value = df
    df.withColumnRenamed.return_value = df
    df.columns = ["id", "Bad Col#"]
    df.count.return_value = 1
    merged = MagicMock()
    merged.whenMatchedUpdate.return_value = merged
    merged.whenNotMatchedInsert.return_value = merged
    merged.merge.return_value = merged
    delta_table = MagicMock()
    delta_table.toDF.return_value = df
    delta_table.alias.return_value = merged
    return df, writer, format_builder, delta_table


def test_object_store_property_requires_spark() -> None:
    loader = ObjectStoreLoader()
    with pytest.raises(LoaderError, match="Spark session not set"):
        _ = loader.object_store


def test_format_prefix_and_temp_path() -> None:
    loader = ObjectStoreLoader()
    loader.spark = MagicMock()
    store = MagicMock()
    store.resolve_path.return_value = "s3a://b/prefix/data/_temp/spark_writes/k1"
    loader._object_store = store

    # _format_prefix only strips slashes on the outside, not internal whitespace
    assert loader._format_prefix("  a/b  ") == "  a/b  /data"
    assert loader._format_prefix(None) == "data"

    out = loader._generate_temp_path("s3a://b", "a/b", "k1")
    assert "s3a://b" in out
    store.resolve_path.assert_called()


@patch("src.loader.object_store_loader.datetime")
@patch("src.loader.object_store_loader.uuid")
def test_generate_final_path(mock_uuid, mock_dt) -> None:
    mock_dt.now.return_value.strftime.return_value = "T"
    mock_uuid.uuid4.return_value.hex = "abc"
    mock_uuid.uuid4.return_value = "u1"

    loader = ObjectStoreLoader()
    loader.spark = MagicMock()
    store = MagicMock()
    store.resolve_path.return_value = "s3a://b/path/T_u1.parquet"
    loader._object_store = store

    uri, key = loader._generate_final_path("s3a://b", "pre", "parquet")
    assert key.endswith(".parquet")
    assert uri == "s3a://b/path/T_u1.parquet"


def test_get_iceberg_table_identifier() -> None:
    loader = ObjectStoreLoader()
    wh = "file:///tmp/wh"
    path = f"{wh}/ns/t1"
    ident = loader._get_iceberg_table_identifier(path, wh)
    assert ident == "iceberg.`ns`.`t1`"

    with pytest.raises(LoaderError, match="Cannot derive"):
        loader._get_iceberg_table_identifier("file:///other", wh)

    with pytest.raises(LoaderError, match="Cannot derive"):
        loader._get_iceberg_table_identifier(wh + "/", wh)


def test_write_dataframe_path_save() -> None:
    df, _, fmt, _ = _df_chain()
    loader = ObjectStoreLoader()
    loader.spark = MagicMock()
    loader._optimize_dataframe = lambda d: d  # type: ignore[assignment]

    loader._write_dataframe(
        df,
        "s3a://b/out",
        {"format": "parquet", "mode": "overwrite", "compression": "snappy"},
    )
    fmt.format.assert_called()
    fmt.save.assert_called_with("s3a://b/out")


def test_write_dataframe_delta_branch() -> None:
    df, _, fmt, _ = _df_chain()
    loader = ObjectStoreLoader()
    loader.spark = MagicMock()
    loader._optimize_dataframe = lambda d: d  # type: ignore[assignment]
    loader._write_dataframe(df, "s3a://b/d", {"format": "delta", "mode": "overwrite"})
    fmt.format.assert_called_with("delta")


@patch("src.loader.object_store_loader.DeltaTable")
def test_perform_delta_merge_minimal(mock_delta_cls) -> None:
    df, _, _, delta_table = _df_chain()
    mock_delta_cls.forPath.return_value = delta_table
    df.columns = ["Id", "name"]
    merged = delta_table.alias.return_value

    loader = ObjectStoreLoader()
    loader.spark = MagicMock()
    loader._perform_delta_merge(df, "s3a://b/t", ["id"], MagicMock())
    merged.merge.assert_called_once()


def test_perform_delta_merge_missing_key() -> None:
    df, _, _, _ = _df_chain()
    df.columns = ["x"]
    loader = ObjectStoreLoader()
    loader.spark = MagicMock()
    with pytest.raises(LoaderError, match="Merge keys not found"):
        loader._perform_delta_merge(df, "p", ["id"], MagicMock())


def test_sanitize_and_rename_columns() -> None:
    loader = ObjectStoreLoader()
    df = MagicMock()
    df.columns = ["a b#", "ok"]
    df.withColumnRenamed.return_value = df
    out = loader._sanitize_column_names(df)
    assert out is df
    df.withColumnRenamed.assert_called()

    df2 = MagicMock()
    df2.columns = ["A", "a"]
    df2.toDF.return_value = df2
    out2 = loader._rename_duplicate_columns(df2)
    assert out2 is df2
    df2.toDF.assert_called_once()


def test_table_exists_and_destination_exists() -> None:
    loader = ObjectStoreLoader()
    loader.spark = MagicMock()
    store = MagicMock()
    store.exists.return_value = True
    store.resolve_path.return_value = "s3a://b/p/_delta_log"
    loader._object_store = store

    assert loader._table_exists("s3a://b/p", LoadingFormat.DELTA) is True
    store.exists.assert_called()

    cfg = LoadingConfig(
        destination="s3",
        s3_bucket="b",
        prefix="src/res",
        format="delta",
        write_mode="overwrite",
    )
    with patch.object(loader, "_generate_table_path", return_value="s3a://b/p/"):
        assert loader.destination_exists(cfg) is True

    non_table = SimpleNamespace(destination="s3", format="not_a_table_format")
    assert loader.destination_exists(non_table) is False


def test_load_requires_spark() -> None:
    loader = ObjectStoreLoader()
    with pytest.raises(LoaderError, match="Spark session not set"):
        loader.load([{"a": 1}], MagicMock(destination="s3", format="delta"))


def test_load_branches_call_helpers() -> None:
    loader = ObjectStoreLoader()
    df, _, _, _ = _df_chain()
    loader.set_spark_session(MagicMock())
    loader._ensure_dataframe = MagicMock(return_value=df)  # type: ignore[method-assign]
    mock_delta = MagicMock(return_value="delta-path")
    mock_ice = MagicMock(return_value="ice-path")
    mock_file = MagicMock(return_value="file-path")
    loader._load_delta = mock_delta  # type: ignore[method-assign]
    loader._load_iceberg = mock_ice  # type: ignore[method-assign]
    loader._load_file_based = mock_file  # type: ignore[method-assign]

    base = {
        "destination": "s3",
        "s3_bucket": "b",
        "prefix": "src/res",
        "write_mode": "overwrite",
    }
    cfg = LoadingConfig(format="delta", **base)  # type: ignore[arg-type]
    assert loader.load(df, cfg) == "delta-path"
    mock_delta.assert_called_once()

    cfg2 = LoadingConfig(format="iceberg", **base)  # type: ignore[arg-type]
    assert loader.load(df, cfg2) == "ice-path"

    cfg3 = SimpleNamespace(
        **{
            **base,
            "format": "parquet",
            "s3_bucket": "b",
            "destination": "s3",
            "compression": "snappy",
        }
    )
    with patch("src.loader.object_store_loader.loading_base_uri", return_value="s3a://b"):
        assert loader.load(df, cfg3) == "file-path"


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
    store.move.assert_called_with("a", "b")

    loader._cleanup_temp_dir(store, "s3a://b/tmp/file")
    store.delete.assert_called()


def test_perform_iceberg_merge_builds_sql() -> None:
    loader = ObjectStoreLoader()
    src_df, _, _, _ = _df_chain()
    src_df.columns = ["id"]
    tgt_df = MagicMock()
    field = MagicMock()
    field.name = "id"
    field.dataType.simpleString.return_value = "int"
    tgt_df.schema.fields = [field]
    tgt_df.columns = ["id"]
    spark = MagicMock()
    spark.table.return_value = tgt_df
    loader.spark = spark

    loader._perform_iceberg_merge(src_df, "file:///w/ns/t", ["id"], "file:///w")
    spark.sql.assert_called_once()
    src_df.createOrReplaceTempView.assert_called_once()


def test_perform_iceberg_merge_missing_target_key() -> None:
    loader = ObjectStoreLoader()
    src_df, _, _, _ = _df_chain()
    src_df.columns = ["id"]
    tgt_df = MagicMock()
    tgt_df.columns = ["x"]
    loader.spark = MagicMock()
    loader.spark.table.return_value = tgt_df
    with pytest.raises(LoaderError, match="Merge keys not found in Iceberg table"):
        loader._perform_iceberg_merge(src_df, "file:///w/ns/t", ["id"], "file:///w")


def test_retry_gives_up_on_non_transient() -> None:
    @retry_on_transient_storage_error(max_retries=2, delay=0)
    def boom():
        raise RuntimeError("permanent failure")

    with pytest.raises(RuntimeError, match="permanent"):
        boom()
