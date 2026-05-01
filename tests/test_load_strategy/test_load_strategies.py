"""Focused tests for table-format load strategy routing and implementations."""

from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import MagicMock, patch

import pytest

from src.config.config_models import LoadingConfig, LoadingFormat, SourceType
from src.load_strategy.base_load_strategy import BaseLoadStrategy
from src.load_strategy.delta_strategy import DeltaStrategy
from src.load_strategy.iceberg_strategy import IcebergStrategy
from src.load_strategy.load_strategy_factory import LoadStrategyFactory
from src.utils.exceptions import LoaderError
from src.utils.path_prefix import get_source_type_prefix, prepend_source_type_prefix


class _RecordingStrategy(BaseLoadStrategy):
    """Small concrete strategy used to test base write-mode routing."""

    format_display_name = "recording"

    def __init__(self, *args, table_exists_result: bool = False, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.table_exists_result = table_exists_result
        self.simple_writes: list[tuple[object, str, str]] = []
        self.merges: list[tuple[object, str, list[str]]] = []

    def table_exists(self) -> bool:
        return self.table_exists_result

    def write_simple(self, df, table_location: str, *, mode: str, **_kwargs) -> None:
        self.simple_writes.append((df, table_location, mode))

    def perform_merge(self, df, table_location: str, merge_keys: list[str]) -> None:
        self.merges.append((df, table_location, merge_keys))


def _object_store() -> MagicMock:
    store = MagicMock()
    store.resolve_path.side_effect = lambda base, *parts, trailing_slash=False: (
        "/".join([base.rstrip("/"), *[str(p).strip("/") for p in parts if p]])
        + ("/" if trailing_slash else "")
    )
    return store


def _config(**overrides) -> LoadingConfig:
    values: dict[str, Any] = {
        "destination": "s3",
        "s3_bucket": "bucket",
        "prefix": "source/resource",
        "format": LoadingFormat.DELTA,
        "write_mode": "append",
    }
    values.update(overrides)
    return LoadingConfig(**values)


def _strategy(config: Any, *, source_type="rest_api", table_exists=False):
    return _RecordingStrategy(
        MagicMock(),
        _object_store(),
        "s3a://bucket",
        config,
        source_type,
        table_exists_result=table_exists,
    )


@pytest.mark.parametrize(
    "source_type, expected",
    [
        ("rest_api", "rest_api"),
        ("python_sdk", "sdk"),
        ("postgresql", "database"),
        ("hana", "database"),
        (SourceType.REST_API, "rest_api"),
        (None, ""),
        ("unknown", ""),
    ],
)
def test_source_type_prefix_mapping(source_type, expected) -> None:
    assert get_source_type_prefix(source_type) == expected


def test_prepend_source_type_prefix_normalizes_known_sources() -> None:
    assert prepend_source_type_prefix("source/resource", "rest_api") == "rest_api/source/resource"
    assert prepend_source_type_prefix("/source/resource/", "python_sdk") == "sdk/source/resource"
    assert prepend_source_type_prefix("source/resource", "unknown") == "source/resource"
    assert prepend_source_type_prefix(None, "hana") == "database"


def test_factory_creates_registered_table_strategies() -> None:
    spark = MagicMock()
    store = _object_store()

    delta = LoadStrategyFactory.create_load_strategy(
        spark, store, "s3a://bucket", _config(format=LoadingFormat.DELTA), "rest_api"
    )
    iceberg = LoadStrategyFactory.create_load_strategy(
        spark,
        store,
        "s3a://bucket",
        _config(format=LoadingFormat.ICEBERG),
        "rest_api",
    )

    assert isinstance(delta, DeltaStrategy)
    assert isinstance(iceberg, IcebergStrategy)


def test_factory_rejects_unregistered_format() -> None:
    with pytest.raises(LoaderError, match="Unsupported load strategy format"):
        LoadStrategyFactory.create_load_strategy(
            MagicMock(),
            _object_store(),
            "s3a://bucket",
            cast(LoadingConfig, SimpleNamespace(format="parquet")),
            "rest_api",
        )


def test_base_strategy_append_resolves_prefixed_table_location() -> None:
    cfg = _config(write_mode="append")
    strategy = _strategy(cfg, source_type="rest_api")
    df = MagicMock()

    location = strategy.write(df)

    assert location == "s3a://bucket/rest_api/source/resource/"
    assert strategy.simple_writes == [(df, location, "append")]
    assert strategy.merges == []


def test_base_strategy_merge_requires_merge_keys() -> None:
    # LoadingConfig validates this earlier in normal runtime; use a lightweight
    # config so the base strategy's own fail-fast guard is tested in isolation.
    cfg = SimpleNamespace(prefix="source/resource", write_mode="merge", merge_keys=None)
    strategy = _strategy(cfg)

    with pytest.raises(LoaderError, match="Merge keys must be specified"):
        strategy.write(MagicMock())


def test_base_strategy_merge_bootstraps_missing_table_with_append() -> None:
    cfg = _config(write_mode="merge", merge_keys=["id"])
    strategy = _strategy(cfg, table_exists=False)
    df = MagicMock()

    location = strategy.write(df)

    assert strategy.simple_writes == [(df, location, "append")]
    assert strategy.merges == []


def test_base_strategy_merge_existing_table_delegates_to_strategy_merge() -> None:
    cfg = _config(write_mode="merge", merge_keys=["id"])
    strategy = _strategy(cfg, table_exists=True)
    df = MagicMock()

    location = strategy.write(df)

    assert strategy.simple_writes == []
    assert strategy.merges == [(df, location, ["id"])]


def test_base_strategy_rejects_unsupported_write_mode() -> None:
    strategy = _strategy(_config(write_mode="ignore"))

    with pytest.raises(LoaderError, match="Unsupported write mode 'ignore'"):
        strategy.write(MagicMock())


def _df_with_writer(columns=None):
    df = MagicMock()
    df.columns = columns or ["id", "name"]
    df.coalesce.return_value = df
    writer = MagicMock()
    writer.format.return_value = writer
    writer.mode.return_value = writer
    writer.options.return_value = writer
    df.write = writer
    return df, writer


def test_delta_table_exists_checks_delta_log_directory() -> None:
    store = _object_store()
    store.exists.return_value = True
    strategy = DeltaStrategy(MagicMock(), store, "s3a://bucket", _config(), "rest_api")

    assert strategy.table_exists() is True
    store.exists.assert_called_once_with("s3a://bucket/rest_api/source/resource/_delta_log")


def test_delta_write_simple_saves_to_table_location() -> None:
    df, writer = _df_with_writer()
    strategy = DeltaStrategy(MagicMock(), _object_store(), "s3a://bucket", _config(), None)

    strategy.write_simple(df, "s3a://bucket/source/resource/", mode="overwrite")

    writer.format.assert_called_once_with(LoadingFormat.DELTA)
    writer.mode.assert_called_once_with("overwrite")
    writer.save.assert_called_once_with("s3a://bucket/source/resource/")


def test_delta_merge_validates_source_merge_keys_before_delta_lookup() -> None:
    df, _writer = _df_with_writer(columns=["other"])
    strategy = DeltaStrategy(MagicMock(), _object_store(), "s3a://bucket", _config(), None)

    with pytest.raises(LoaderError, match="Merge keys not found in DataFrame"):
        strategy.perform_merge(df, "s3a://bucket/source/resource/", ["id"])


def test_delta_merge_builds_case_insensitive_update_and_insert_maps() -> None:
    df, _writer = _df_with_writer(columns=["Id", "Name"])
    target_df = MagicMock()
    id_field = MagicMock(name="id_field")
    id_field.name = "id"
    id_field.dataType.simpleString.return_value = "int"
    name_field = MagicMock(name="name_field")
    name_field.name = "name"
    name_field.dataType.simpleString.return_value = "string"
    missing_field = MagicMock(name="missing_field")
    missing_field.name = "target_only"
    missing_field.dataType.simpleString.return_value = "string"
    target_df.schema.fields = [id_field, name_field, missing_field]

    delta_table = MagicMock()
    delta_table.toDF.return_value = target_df
    merge_builder = MagicMock()
    merge_builder.whenMatchedUpdate.return_value = merge_builder
    merge_builder.whenNotMatchedInsert.return_value = merge_builder
    delta_table.alias.return_value.merge.return_value = merge_builder

    strategy = DeltaStrategy(MagicMock(), _object_store(), "s3a://bucket", _config(), None)

    with patch("src.load_strategy.delta_strategy.DeltaTable") as delta_cls:
        delta_cls.forPath.return_value = delta_table
        strategy.perform_merge(df, "s3a://bucket/source/resource/", ["id"])

    delta_table.alias.return_value.merge.assert_called_once()
    merge_builder.whenMatchedUpdate.assert_called_once_with(set={"name": "updates.`Name`"})
    merge_builder.whenNotMatchedInsert.assert_called_once()
    insert_values = merge_builder.whenNotMatchedInsert.call_args.kwargs["values"]
    assert insert_values["id"] == "updates.`Id`"
    assert insert_values["name"] == "updates.`Name`"
    assert insert_values["target_only"] == "CAST(NULL AS string)"
    merge_builder.execute.assert_called_once()


def test_iceberg_identifier_is_derived_from_warehouse_relative_location() -> None:
    strategy = IcebergStrategy(MagicMock(), _object_store(), "file:///warehouse", _config(), None)

    assert (
        strategy._catalog_identifier_from_location("file:///warehouse/ns/table/")
        == "iceberg.`ns`.`table`"
    )

    with pytest.raises(LoaderError, match="Cannot derive Iceberg table identifier"):
        strategy._catalog_identifier_from_location("file:///other/ns/table")


def test_iceberg_table_exists_uses_catalog_and_cleans_warehouse_conf() -> None:
    spark = MagicMock()
    spark.catalog.tableExists.return_value = True
    strategy = IcebergStrategy(spark, _object_store(), "file:///warehouse", _config(), None)

    assert strategy.table_exists() is True

    spark.conf.set.assert_called_once_with("spark.sql.catalog.iceberg.warehouse", "file:///warehouse")
    spark.catalog.tableExists.assert_called_once_with("iceberg.`source`.`resource`")
    spark.conf.unset.assert_called_once_with("spark.sql.catalog.iceberg.warehouse")


def test_iceberg_write_simple_saves_as_catalog_table_and_cleans_conf() -> None:
    spark = MagicMock()
    df, writer = _df_with_writer()
    strategy = IcebergStrategy(spark, _object_store(), "file:///warehouse", _config(), None)

    strategy.write_simple(df, "file:///warehouse/source/resource/", mode="append")

    writer.format.assert_called_once_with(LoadingFormat.ICEBERG)
    writer.mode.assert_called_once_with("append")
    writer.saveAsTable.assert_called_once_with("iceberg.`source`.`resource`")
    spark.conf.unset.assert_called_once_with("spark.sql.catalog.iceberg.warehouse")


def test_iceberg_merge_builds_sql_and_drops_temp_view() -> None:
    spark = MagicMock()
    df, _writer = _df_with_writer(columns=["Id", "Name"])
    target_df = MagicMock()
    id_field = MagicMock(name="id_field")
    id_field.name = "id"
    id_field.dataType.simpleString.return_value = "int"
    name_field = MagicMock(name="name_field")
    name_field.name = "name"
    name_field.dataType.simpleString.return_value = "string"
    target_only_field = MagicMock(name="target_only_field")
    target_only_field.name = "target_only"
    target_only_field.dataType.simpleString.return_value = "string"
    target_df.columns = ["id", "name", "target_only"]
    target_df.schema.fields = [id_field, name_field, target_only_field]
    spark.table.return_value = target_df
    strategy = IcebergStrategy(spark, _object_store(), "file:///warehouse", _config(), None)

    strategy.perform_merge(df, "file:///warehouse/source/resource/", ["id"])

    df.createOrReplaceTempView.assert_called_once()
    sql = spark.sql.call_args.args[0]
    assert "MERGE INTO iceberg.`source`.`resource` AS target" in sql
    assert "target.`id` = source.`Id`" in sql
    assert "`name` = source.`Name`" in sql
    assert "CAST(NULL AS string)" in sql
    spark.catalog.dropTempView.assert_called_once()
    spark.conf.unset.assert_called_once_with("spark.sql.catalog.iceberg.warehouse")


def test_iceberg_merge_validates_source_merge_keys() -> None:
    df, _writer = _df_with_writer(columns=["other"])
    strategy = IcebergStrategy(MagicMock(), _object_store(), "file:///warehouse", _config(), None)

    with pytest.raises(LoaderError, match="Merge keys not found in DataFrame"):
        strategy.perform_merge(df, "file:///warehouse/source/resource/", ["id"])
