"""Tests for optional database ``fields`` in ``DynamicHandler._build_database_dataframe``."""

from unittest.mock import MagicMock

import pytest

from src.config.config_models import (
    ResourceConfig,
    SchemaField,
    SourceConfig,
    SourceType,
)
from src.handler.base_handler import HandlerError
from src.handler.dynamic_handler import DynamicHandler
from src.planner.execution_plan import ResourceMetadata
from src.utils.logger import get_logger


def _pg_source_config() -> SourceConfig:
    return SourceConfig(
        type=SourceType.POSTGRESQL,
        host="localhost",
        port=5432,
        username="u",
        password="p",
        database="db",
        resources={
            "placeholder": ResourceConfig(
                method="GET",
                database_schema="public",
                database_table="users",
            )
        },
    )


class _SparkStringType:
    """Stand-in for Spark SQL string type label in ``str(field.dataType)`` assertions."""

    def __str__(self) -> str:
        return "StringType()"


@pytest.fixture
def patch_spark_col(monkeypatch: pytest.MonkeyPatch) -> None:
    """Avoid real Spark ``col()`` (requires active JVM ``SparkContext``)."""

    def _fake_col(_name: str) -> MagicMock:
        chain = MagicMock()
        chain.cast.return_value.alias.return_value = chain
        return chain

    monkeypatch.setattr("src.handler.dynamic_handler.col", _fake_col)


def _make_extract_df(*, n_select_cols: int) -> MagicMock:
    """Fake extracted DataFrame: ``select`` returns a row surface matching projection width."""
    df = MagicMock()
    df.columns = ["id", "label"]
    f1, f2 = MagicMock(), MagicMock()
    f1.dataType = _SparkStringType()
    f2.dataType = _SparkStringType()
    df.schema.fields = [f1, f2]

    projected = MagicMock()
    if n_select_cols == 1:
        projected.columns = ["user_id"]
        sf = MagicMock()
        sf.dataType = _SparkStringType()
        projected.schema.fields = [sf]
    else:
        projected.columns = ["id", "label"]
        pf1, pf2 = MagicMock(), MagicMock()
        pf1.dataType = _SparkStringType()
        pf2.dataType = _SparkStringType()
        projected.schema.fields = [pf1, pf2]
    projected.count = MagicMock(return_value=1)

    def _select(*_cols: object) -> MagicMock:
        return projected

    df.select.side_effect = _select
    return df


class _FakeDbService:
    """Spy for DB services: ``extract_invocations`` matches handler log field of the same name."""

    def __init__(self, extract_df: MagicMock) -> None:
        self._df = extract_df
        self.extract_invocations = 0

    def connect(self) -> None:
        return None

    def close(self) -> None:
        return None

    def extract_table(
        self,
        schema,
        table,
        select_query=None,
        spark_session=None,
        table_read_options=None,
        database_where_predicate=None,
    ):
        self.extract_invocations += 1
        return self._df


def test_build_database_dataframe_infers_fields_when_omitted(patch_spark_col) -> None:
    handler = object.__new__(DynamicHandler)
    handler.spark = MagicMock()
    handler.logger = get_logger("test_dynamic_handler_db")

    extract_df = _make_extract_df(n_select_cols=2)
    rc = ResourceConfig(method="GET", database_schema="public", database_table="users")
    meta = ResourceMetadata(
        source_name="pg",
        resource_name="users",
        dependencies=set(),
        batch_inputs={},
        config=rc,
    )

    out = DynamicHandler._build_database_dataframe(
        handler,
        _FakeDbService(extract_df),
        meta,
        request_contexts=[{}],
        effective_loading=None,
        source_config=_pg_source_config(),
    )
    assert out is not None
    assert set(out.columns) == {"id", "label"}
    for field in out.schema.fields:
        assert str(field.dataType).startswith("StringType")


def test_build_database_dataframe_respects_configured_fields(patch_spark_col) -> None:
    handler = object.__new__(DynamicHandler)
    handler.spark = MagicMock()
    handler.logger = get_logger("test_dynamic_handler_db")

    extract_df = _make_extract_df(n_select_cols=1)
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
        handler,
        _FakeDbService(extract_df),
        meta,
        request_contexts=[{}],
        effective_loading=None,
        source_config=_pg_source_config(),
    )
    assert out is not None
    assert out.columns == ["user_id"]


def test_build_database_dataframe_single_extract_with_multiple_contexts(patch_spark_col) -> None:
    """Multiple request contexts must not re-run JDBC extract or duplicate rows."""
    handler = object.__new__(DynamicHandler)
    handler.spark = MagicMock()
    handler.logger = get_logger("test_dynamic_handler_db")

    extract_df = _make_extract_df(n_select_cols=2)
    rc = ResourceConfig(method="GET", database_schema="public", database_table="users")
    meta = ResourceMetadata(
        source_name="pg",
        resource_name="users",
        dependencies=set(),
        batch_inputs={},
        config=rc,
    )

    fake = _FakeDbService(extract_df)
    contexts = [{"batch": 1}, {"batch": 2}, {"batch": 3}]
    out = DynamicHandler._build_database_dataframe(
        handler,
        fake,
        meta,
        request_contexts=contexts,
        effective_loading=None,
        source_config=_pg_source_config(),
    )

    assert fake.extract_invocations == 1
    assert out is not None
    assert out.count() == 1
    assert set(out.columns) == {"id", "label"}


def test_build_database_dataframe_no_contexts_no_extract() -> None:
    handler = object.__new__(DynamicHandler)
    handler.spark = MagicMock()
    handler.logger = get_logger("test_dynamic_handler_db")

    extract_df = _make_extract_df(n_select_cols=2)
    rc = ResourceConfig(method="GET", database_schema="public", database_table="users")
    meta = ResourceMetadata(
        source_name="pg",
        resource_name="users",
        dependencies=set(),
        batch_inputs={},
        config=rc,
    )

    fake = _FakeDbService(extract_df)
    out = DynamicHandler._build_database_dataframe(
        handler,
        fake,
        meta,
        request_contexts=[],
        effective_loading=None,
        source_config=_pg_source_config(),
    )

    assert fake.extract_invocations == 0
    assert out is None


def test_build_database_dataframe_raises_when_configured_source_column_missing(
    patch_spark_col,
) -> None:
    handler = object.__new__(DynamicHandler)
    handler.spark = MagicMock()
    handler.logger = get_logger("test_dynamic_handler_db")

    extract_df = _make_extract_df(n_select_cols=2)
    rc = ResourceConfig(
        method="GET",
        database_schema="public",
        database_table="users",
        fields=[SchemaField(name="user_id", source="missing_col")],
    )
    meta = ResourceMetadata(
        source_name="pg",
        resource_name="users",
        dependencies=set(),
        batch_inputs={},
        config=rc,
    )
    fake = _FakeDbService(extract_df)
    fake.close = MagicMock()

    with pytest.raises(HandlerError, match="Configured field source"):
        DynamicHandler._build_database_dataframe(
            handler,
            fake,
            meta,
            request_contexts=[{}],
            effective_loading=None,
            source_config=_pg_source_config(),
        )
    fake.close.assert_called_once()
