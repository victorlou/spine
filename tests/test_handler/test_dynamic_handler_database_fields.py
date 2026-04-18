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
        self.extract_table_calls = 0

    def connect(self) -> None:
        return None

    def close(self) -> None:
        return None

    def extract_table(self, schema, table, select_query=None, spark_session=None):
        self.extract_table_calls += 1
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


def test_build_database_dataframe_single_extract_with_multiple_contexts(
    spark_session: SparkSession,
) -> None:
    """Multiple request contexts must not re-run JDBC extract or duplicate rows (issue #11)."""
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

    fake = _FakeDbService(spark_session)
    contexts = [{"batch": 1}, {"batch": 2}, {"batch": 3}]
    out = DynamicHandler._build_database_dataframe(handler, fake, meta, request_contexts=contexts)

    assert fake.extract_table_calls == 1
    assert out is not None
    assert out.count() == 1
    assert set(out.columns) == {"id", "label"}


def test_build_database_dataframe_no_contexts_no_extract(spark_session: SparkSession) -> None:
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

    fake = _FakeDbService(spark_session)
    out = DynamicHandler._build_database_dataframe(handler, fake, meta, request_contexts=[])

    assert fake.extract_table_calls == 0
    assert out is None
