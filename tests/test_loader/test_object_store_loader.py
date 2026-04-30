"""Behavior-focused tests for ``ObjectStoreLoader`` using mocks at Spark/store boundaries."""

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from pyspark.sql.types import StringType, StructField, StructType

import src.loader.object_store_loader as object_store_loader_module
from src.config.config_models import LoadingConfig, LoadingFormat, SourceType
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


def test_retry_succeeds_after_one_transient_failure() -> None:
    attempts = []

    @retry_on_transient_storage_error(max_retries=3, delay=0)
    def flaky():
        if not attempts:
            attempts.append(1)
            raise RuntimeError("Connection reset by peer")
        return "ok"

    assert flaky() == "ok"
    assert len(attempts) == 1  # raised once, then succeeded


# ---------------------------------------------------------------------------
# _get_source_type_prefix — all mapping branches
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "source_type, expected",
    [
        ("rest_api", "rest_api"),
        ("python_sdk", "sdk"),
        ("postgresql", "database"),
        ("hana", "database"),
        ("unknown_type", ""),
        (None, ""),
        (SourceType.REST_API, "rest_api"),
        (SourceType.PYTHON_SDK, "sdk"),
    ],
)
def test_get_source_type_prefix_all_branches(source_type, expected) -> None:
    loader = ObjectStoreLoader()
    assert loader._get_source_type_prefix(source_type) == expected


def test_prepend_source_type_prefix_strips_and_maps() -> None:
    loader = ObjectStoreLoader()
    assert loader._prepend_source_type_prefix("foo/bar", "rest_api") == "rest_api/foo/bar"
    assert loader._prepend_source_type_prefix("/foo/bar/", "python_sdk") == "sdk/foo/bar"
    assert loader._prepend_source_type_prefix("foo/bar", "unknown") == "foo/bar"


# ---------------------------------------------------------------------------
# _generate_table_path
# ---------------------------------------------------------------------------


def test_generate_table_path_constructs_path_from_prefix_and_source_type() -> None:
    loader = ObjectStoreLoader()
    loader.set_spark_session(MagicMock())
    result = loader._generate_table_path("s3a://b", "src/res", "rest_api")
    assert result == "s3a://b/rest_api/src/res/"


def test_generate_table_path_no_prefix_no_source_type() -> None:
    loader = ObjectStoreLoader()
    loader.set_spark_session(MagicMock())
    result = loader._generate_table_path("s3a://b", None, None)
    assert result == "s3a://b/"


# ---------------------------------------------------------------------------
# _load_delta — append, merge (new table), merge (existing table)
# ---------------------------------------------------------------------------


def _loader_with_mocked_internals(write_side_effect=None, table_exists_return=False):
    """Return a loader with Spark set and key internal methods mocked."""
    loader = ObjectStoreLoader()
    loader.set_spark_session(MagicMock())
    df, _, _, _ = _df_chain()
    loader._rename_duplicate_columns = MagicMock(return_value=df)
    loader._sanitize_column_names = MagicMock(return_value=df)
    loader._generate_table_path = MagicMock(return_value="s3a://b/delta-table/")
    loader._write_dataframe = MagicMock(side_effect=write_side_effect)
    loader._table_exists = MagicMock(return_value=table_exists_return)
    loader._perform_delta_merge = MagicMock()
    return loader, df


def test_load_delta_append_calls_write_dataframe() -> None:
    loader, df = _loader_with_mocked_internals()
    cfg = LoadingConfig(
        destination="s3", s3_bucket="b", prefix="s/r", write_mode="append", format="delta"
    )
    result = loader._load_delta(df, cfg, base_uri="s3a://b")
    loader._write_dataframe.assert_called_once()
    call_opts = loader._write_dataframe.call_args[0][2]
    assert call_opts["mode"] == "append"
    assert call_opts["format"] == LoadingFormat.DELTA
    assert result == "s3a://b/delta-table/"


def test_load_delta_merge_new_table_bootstraps_with_append() -> None:
    loader, df = _loader_with_mocked_internals(table_exists_return=False)
    cfg = LoadingConfig(
        destination="s3",
        s3_bucket="b",
        prefix="s/r",
        write_mode="merge",
        merge_keys=["id"],
        format="delta",
    )
    loader._load_delta(df, cfg, base_uri="s3a://b")
    loader._write_dataframe.assert_called_once()
    loader._perform_delta_merge.assert_not_called()
    call_opts = loader._write_dataframe.call_args[0][2]
    assert call_opts["mode"] == "append"


def test_load_delta_merge_existing_table_calls_perform_delta_merge() -> None:
    loader, df = _loader_with_mocked_internals(table_exists_return=True)
    cfg = LoadingConfig(
        destination="s3",
        s3_bucket="b",
        prefix="s/r",
        write_mode="merge",
        merge_keys=["id"],
        format="delta",
    )
    loader._load_delta(df, cfg, base_uri="s3a://b")
    loader._perform_delta_merge.assert_called_once()
    loader._write_dataframe.assert_not_called()


# ---------------------------------------------------------------------------
# _load_iceberg — append and finally cleanup
# ---------------------------------------------------------------------------


def _iceberg_loader(write_side_effect=None, table_exists_return=False):
    """Return a loader with Spark and internals mocked for Iceberg tests."""
    loader = ObjectStoreLoader()
    spark = MagicMock()
    loader.set_spark_session(spark)
    df, _, _, _ = _df_chain()
    loader._rename_duplicate_columns = MagicMock(return_value=df)
    loader._sanitize_column_names = MagicMock(return_value=df)
    loader._generate_table_path = MagicMock(return_value="file:///wh/ns/t/")
    loader._write_dataframe = MagicMock(side_effect=write_side_effect)
    loader._table_exists = MagicMock(return_value=table_exists_return)
    loader._perform_iceberg_merge = MagicMock()
    return loader, df, spark


def test_load_iceberg_append_calls_write_dataframe_with_iceberg_flag() -> None:
    loader, df, _spark = _iceberg_loader()
    cfg = LoadingConfig(
        destination="local",
        storage_root="/wh",
        prefix="ns/t",
        write_mode="append",
        format="iceberg",
    )
    loader._load_iceberg(df, cfg, base_uri="file:///wh")
    loader._write_dataframe.assert_called_once()
    _, _, call_kw = (
        loader._write_dataframe.call_args[0][0],
        loader._write_dataframe.call_args[0][1],
        loader._write_dataframe.call_args[1],
    )
    assert call_kw.get("iceberg") is True


def test_load_iceberg_unsets_warehouse_conf_on_write_error() -> None:
    loader, df, spark = _iceberg_loader(write_side_effect=LoaderError("write failed"))
    cfg = LoadingConfig(
        destination="local",
        storage_root="/wh",
        prefix="ns/t",
        write_mode="append",
        format="iceberg",
    )
    with pytest.raises(LoaderError):
        loader._load_iceberg(df, cfg, base_uri="file:///wh")
    spark.conf.unset.assert_called_once_with("spark.sql.catalog.iceberg.warehouse")


# ---------------------------------------------------------------------------
# _load_file_based — happy path and cleanup on error
# ---------------------------------------------------------------------------


def _file_based_loader(write_side_effect=None):
    """Return a loader with internals mocked for file-based write tests."""
    loader = ObjectStoreLoader()
    spark = MagicMock()
    loader.set_spark_session(spark)
    loader._object_store = MagicMock()
    df, _, _, _ = _df_chain()
    loader._write_dataframe = MagicMock(side_effect=write_side_effect)
    loader._generate_final_path = MagicMock(return_value=("s3a://b/final.parquet", "final.parquet"))
    loader._generate_temp_path = MagicMock(return_value="s3a://b/tmp/spark_writes/k1")
    loader._move_uri = MagicMock()
    loader._cleanup_temp_dir = MagicMock()
    loader._object_store.glob_first_part_file.return_value = "s3a://b/tmp/part-0.parquet"
    return loader, df


def test_load_file_based_writes_to_temp_then_moves() -> None:
    loader, df = _file_based_loader()
    cfg = LoadingConfig(destination="s3", s3_bucket="b", prefix="s/r", write_mode="overwrite")
    result = loader._load_file_based(df, cfg, base_uri="s3a://b")
    loader._write_dataframe.assert_called_once()
    loader._move_uri.assert_called_once()
    loader._cleanup_temp_dir.assert_called_once()
    assert result == "final.parquet"


def test_load_file_based_cleans_up_temp_on_write_error() -> None:
    loader, df = _file_based_loader(write_side_effect=LoaderError("disk full"))
    cfg = LoadingConfig(destination="s3", s3_bucket="b", prefix="s/r", write_mode="overwrite")
    with pytest.raises(LoaderError):
        loader._load_file_based(df, cfg, base_uri="s3a://b")
    loader._cleanup_temp_dir.assert_called_once()


# ---------------------------------------------------------------------------
# _load_delta — overwrite mode
# ---------------------------------------------------------------------------


def test_load_delta_overwrite_calls_write_dataframe() -> None:
    loader, df = _loader_with_mocked_internals()
    cfg = LoadingConfig(
        destination="s3", s3_bucket="b", prefix="s/r", write_mode="overwrite", format="delta"
    )
    result = loader._load_delta(df, cfg, base_uri="s3a://b")
    loader._write_dataframe.assert_called_once()
    call_opts = loader._write_dataframe.call_args[0][2]
    assert call_opts["mode"] == "overwrite"
    assert result == "s3a://b/delta-table/"


# ---------------------------------------------------------------------------
# _load_iceberg — merge on new table, merge on existing, overwrite
# ---------------------------------------------------------------------------


def test_load_iceberg_merge_new_table_bootstraps_with_append() -> None:
    loader, df, _spark = _iceberg_loader(table_exists_return=False)
    cfg = LoadingConfig(
        destination="local",
        storage_root="/wh",
        prefix="ns/t",
        write_mode="merge",
        merge_keys=["id"],
        format="iceberg",
    )
    loader._load_iceberg(df, cfg, base_uri="file:///wh")
    loader._write_dataframe.assert_called_once()
    loader._perform_iceberg_merge.assert_not_called()
    kw = loader._write_dataframe.call_args[1]
    assert kw.get("iceberg") is True
    call_opts = loader._write_dataframe.call_args[0][2]
    assert call_opts["mode"] == "append"


def test_load_iceberg_merge_existing_table_calls_perform_iceberg_merge() -> None:
    loader, df, _spark = _iceberg_loader(table_exists_return=True)
    cfg = LoadingConfig(
        destination="local",
        storage_root="/wh",
        prefix="ns/t",
        write_mode="merge",
        merge_keys=["id"],
        format="iceberg",
    )
    loader._load_iceberg(df, cfg, base_uri="file:///wh")
    loader._perform_iceberg_merge.assert_called_once()
    loader._write_dataframe.assert_not_called()


def test_load_iceberg_overwrite_calls_write_dataframe() -> None:
    loader, df, _spark = _iceberg_loader()
    cfg = LoadingConfig(
        destination="local",
        storage_root="/wh",
        prefix="ns/t",
        write_mode="overwrite",
        format="iceberg",
    )
    loader._load_iceberg(df, cfg, base_uri="file:///wh")
    loader._write_dataframe.assert_called_once()
    kw = loader._write_dataframe.call_args[1]
    assert kw.get("iceberg") is True
    call_opts = loader._write_dataframe.call_args[0][2]
    assert call_opts["mode"] == "overwrite"


# ---------------------------------------------------------------------------
# _table_exists — exception path returns False
# ---------------------------------------------------------------------------


def test_table_exists_exception_returns_false() -> None:
    loader = ObjectStoreLoader()
    loader.logger = MagicMock()
    store = MagicMock()
    store.resolve_path.side_effect = RuntimeError("storage error")
    loader._object_store = store
    result = loader._table_exists("s3a://b/path/", LoadingFormat.DELTA)
    assert result is False


# ---------------------------------------------------------------------------
# destination_exists — returns False for non-delta/iceberg format
# ---------------------------------------------------------------------------


def test_destination_exists_returns_false_for_local_destination() -> None:
    loader = ObjectStoreLoader()
    loader.set_spark_session(MagicMock())
    cfg = LoadingConfig(destination="local", storage_root="/tmp", format="delta")
    assert loader.destination_exists(cfg) is False


def test_destination_exists_returns_false_when_no_spark() -> None:
    loader = ObjectStoreLoader()
    loader._spark = None
    cfg = LoadingConfig(destination="s3", s3_bucket="b", prefix="a/r", format="delta")
    assert loader.destination_exists(cfg) is False


def test_destination_exists_delegates_to_table_exists() -> None:
    loader = ObjectStoreLoader()
    loader.set_spark_session(MagicMock())
    loader._object_store = MagicMock()
    loader._object_store.resolve_path.return_value = "s3a://b/a/r/_delta_log"
    loader._table_exists = MagicMock(return_value=True)
    loader._generate_table_path = MagicMock(return_value="s3a://b/a/r/")
    cfg = LoadingConfig(destination="s3", s3_bucket="b", prefix="a/r", format="delta")
    result = loader.destination_exists(cfg)
    loader._table_exists.assert_called_once()
    assert result is True


def test_destination_exists_s3_missing_prefix_returns_false() -> None:
    loader = ObjectStoreLoader()
    loader.set_spark_session(MagicMock())
    cfg = LoadingConfig(destination="s3", s3_bucket="b", format="delta")
    assert loader.destination_exists(cfg) is False


def test_destination_exists_gcs_missing_prefix_returns_false() -> None:
    loader = ObjectStoreLoader()
    loader.set_spark_session(MagicMock())
    cfg = LoadingConfig(destination="gcs", gcs_bucket="b", format="delta")
    assert loader.destination_exists(cfg) is False


def test_destination_exists_azure_blob_missing_prefix_returns_false() -> None:
    loader = ObjectStoreLoader()
    loader.set_spark_session(MagicMock())
    cfg = LoadingConfig(
        destination="azure_blob",
        azure_container="c",
        azure_account="acc",
        format="delta",
    )
    assert loader.destination_exists(cfg) is False


# ---------------------------------------------------------------------------
# _perform_delta_merge — missing merge keys and no spark raise LoaderError
# ---------------------------------------------------------------------------


def test_perform_delta_merge_missing_merge_key_raises() -> None:
    loader = ObjectStoreLoader()
    loader.set_spark_session(MagicMock())
    loader.logger = MagicMock()
    df = MagicMock()
    df.columns = ["name", "value"]
    cfg = LoadingConfig(
        destination="s3",
        s3_bucket="b",
        prefix="a/r",
        write_mode="merge",
        merge_keys=["id"],
        format="delta",
    )
    with pytest.raises(LoaderError, match="Merge keys not found"):
        loader._perform_delta_merge(df, "s3a://b/table/", ["id"], cfg)


def test_perform_delta_merge_no_spark_raises() -> None:
    loader = ObjectStoreLoader()
    loader.logger = MagicMock()
    loader._spark = None
    df = MagicMock()
    df.columns = ["id", "name"]
    cfg = LoadingConfig(
        destination="s3",
        s3_bucket="b",
        prefix="a/r",
        write_mode="merge",
        merge_keys=["id"],
        format="delta",
    )
    with pytest.raises(LoaderError, match="Spark session not set"):
        loader._perform_delta_merge(df, "s3a://b/table/", ["id"], cfg)


# ---------------------------------------------------------------------------
# _create_dataframe — exception path raises LoaderError (lines 329-339)
# ---------------------------------------------------------------------------


def test_create_dataframe_exception_raises_loader_error() -> None:
    loader = ObjectStoreLoader()
    spark = MagicMock()
    spark.createDataFrame.side_effect = RuntimeError("spark exploded")
    loader.set_spark_session(spark)
    loader.logger = MagicMock()
    with pytest.raises(LoaderError, match="Failed to create Spark DataFrame"):
        loader._create_dataframe([{"id": 1}], schema=None)


# ---------------------------------------------------------------------------
# _write_dataframe — iceberg without warehouse raises LoaderError (line 431)
# ---------------------------------------------------------------------------


def test_write_dataframe_iceberg_missing_warehouse_raises() -> None:
    loader = ObjectStoreLoader()
    loader.set_spark_session(MagicMock())
    loader.logger = MagicMock()
    df = MagicMock()
    df.coalesce.return_value = df
    df.write = MagicMock()
    df.write.format.return_value = df.write
    df.write.mode.return_value = df.write
    with pytest.raises(LoaderError, match="iceberg_warehouse_path"):
        loader._write_dataframe(
            df, "s3a://b/path/", {"format": "delta", "mode": "overwrite"}, iceberg=True
        )


# ---------------------------------------------------------------------------
# load — ValueError from loading_base_uri raises LoaderError (lines 520-521)
# ---------------------------------------------------------------------------


def test_load_raises_loader_error_on_invalid_base_uri() -> None:
    loader = ObjectStoreLoader()
    loader.set_spark_session(MagicMock())
    loader.logger = MagicMock()
    cfg = LoadingConfig(destination="s3", s3_bucket="b", prefix="a/r", format="delta")
    with patch(
        "src.loader.object_store_loader.loading_base_uri", side_effect=ValueError("bad uri")
    ):
        with pytest.raises(LoaderError, match="bad uri"):
            loader.load([{"id": 1}], cfg)


# ---------------------------------------------------------------------------
# _cleanup_temp_dir — exception path logs warning (lines 478-482)
# ---------------------------------------------------------------------------


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


def test_destination_exists_false_when_loading_base_uri_raises() -> None:
    loader = ObjectStoreLoader()
    loader.set_spark_session(MagicMock())
    cfg = LoadingConfig(destination="s3", s3_bucket="b", prefix="a/r", format="delta")
    with patch(
        "src.loader.object_store_loader.loading_base_uri", side_effect=ValueError("bad uri")
    ):
        assert loader.destination_exists(cfg) is False


def test_load_delta_merge_existing_table_missing_merge_keys_raises() -> None:
    loader, df = _loader_with_mocked_internals(table_exists_return=True)
    cfg = SimpleNamespace(
        write_mode="merge",
        merge_keys=None,
        compression=None,
        destination="s3",
        prefix="p/r",
        force_nondeterministic_deduplication=False,
    )
    with pytest.raises(LoaderError, match="merge_keys must be provided"):
        loader._load_delta(df, cfg, base_uri="s3a://b")


@patch("src.loader.object_store_loader.DeltaTable", None)
def test_perform_delta_merge_when_delta_table_unavailable() -> None:
    loader = ObjectStoreLoader()
    loader.set_spark_session(MagicMock())
    df = MagicMock()
    df.columns = ["id"]
    cfg = MagicMock()
    with pytest.raises(LoaderError, match="DeltaTable is not available"):
        loader._perform_delta_merge(df, "s3a://b/t", ["id"], cfg)


def test_ensure_dataframe_invalid_input_type_raises_loader_error() -> None:
    loader = ObjectStoreLoader()
    loader.spark = MagicMock()
    loader.logger = MagicMock()
    with patch.object(object_store_loader_module, "DataFrame", _DummySparkDataFrame):
        with pytest.raises(LoaderError, match="Failed to ensure Spark DataFrame"):
            loader._ensure_dataframe(object(), None)


def test_perform_iceberg_merge_sql_failure_drops_temp_view() -> None:
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
    spark.sql.side_effect = RuntimeError("merge failed")
    loader.spark = spark

    with pytest.raises(LoaderError, match="Failed to perform Iceberg MERGE"):
        loader._perform_iceberg_merge(src_df, "file:///w/ns/t", ["id"], "file:///w")

    spark.catalog.dropTempView.assert_called_once()
