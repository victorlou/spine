"""Tests for optional database ``fields`` in ``DynamicHandler._build_database_dataframe``."""

import pytest
from pyspark.sql import SparkSession

from src.config.config_models import ResourceConfig, SchemaField
from src.handler.dynamic_handler import DynamicHandler
from src.planner.execution_plan import ResourceMetadata
from src.utils.logger import get_logger


@pytest.fixture(scope="module")
def spark_session() -> SparkSession:
    spark = (
        SparkSession.builder.master("local[1]")
        .appName("test_db_fields")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.ui.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    yield spark
    spark.stop()


class _FakeDbService:
    def __init__(self, spark: SparkSession) -> None:
        self._spark = spark

    def connect(self) -> None:
        return None

    def close(self) -> None:
        return None

    def extract_table(
        self, schema, table, select_query=None, spark_session=None, table_read_options=None
    ):
        return self._spark.createDataFrame([(1, "x")], schema=["id", "label"])


def test_build_database_dataframe_infers_fields_when_omitted(spark_session: SparkSession) -> None:
    handler = object.__new__(DynamicHandler)
    handler.spark = spark_session
    handler.logger = get_logger("test_dynamic_handler_db")

    rc = ResourceConfig(method="GET", database_schema="public", database_table="users")
    meta = ResourceMetadata(
        source_name="pg",
        resource_name="users",
        dependencies=set(),
        batch_inputs={},
        config=rc,
    )

    out = DynamicHandler._build_database_dataframe(
        handler, _FakeDbService(spark_session), meta, request_contexts=[{}]
    )
    assert out is not None
    assert set(out.columns) == {"id", "label"}
    for field in out.schema.fields:
        assert str(field.dataType).startswith("StringType")


def test_build_database_dataframe_respects_configured_fields(spark_session: SparkSession) -> None:
    handler = object.__new__(DynamicHandler)
    handler.spark = spark_session
    handler.logger = get_logger("test_dynamic_handler_db")

    rc = ResourceConfig(
        method="GET",
        database_schema="public",
        database_table="users",
        fields=[SchemaField(name="user_id", source="id")],
    )
    meta = ResourceMetadata(
        source_name="pg",
        resource_name="users",
        dependencies=set(),
        batch_inputs={},
        config=rc,
    )

    out = DynamicHandler._build_database_dataframe(
        handler, _FakeDbService(spark_session), meta, request_contexts=[{}]
    )
    assert out is not None
    assert out.columns == ["user_id"]
