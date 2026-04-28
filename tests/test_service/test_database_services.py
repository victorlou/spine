"""Tests for SQL service factory and database service helpers."""

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from src.config.config_models import SourceType
from src.service.hana_service import HanaService
from src.service.postgres_service import PostgresService
from src.service.service_factory import ServiceFactory
from src.service.sql_database_service import SqlDatabaseService
from src.utils.exceptions import ServiceError


def _settings() -> SimpleNamespace:
    return SimpleNamespace(
        pipeline_config=SimpleNamespace(defaults=SimpleNamespace(log_full_row_count=False)),
        api=SimpleNamespace(TIMEOUT=5),
    )


def _source(type_value: str) -> SimpleNamespace:
    if type_value == "postgresql":
        source_type = SourceType.POSTGRESQL
    elif type_value == "hana":
        source_type = SourceType.HANA
    else:
        source_type = SimpleNamespace(value=type_value)
    return SimpleNamespace(
        type=source_type,
        host="localhost",
        port=5432,
        database="db",
        username="u",
        password="p",
        connection_params={},
        resources={},
    )


def test_service_factory_unsupported_type() -> None:
    cfg = _source("unknown")
    with pytest.raises(ServiceError, match="Unsupported service type"):
        ServiceFactory.create_service(_settings(), "s", cfg, redis_context=object())


def test_postgres_and_hana_connect_and_close() -> None:
    pg_cfg = _source("postgresql")
    pg = PostgresService(_settings(), "pg", pg_cfg, redis_context=object())
    pg.connect()
    assert pg.is_connected is True
    pg.close()
    assert pg.is_connected is False

    hana_cfg = _source("hana")
    hana = HanaService(_settings(), "hana", hana_cfg, redis_context=object())
    hana.connect()
    assert hana.is_connected is True
    hana.close()
    assert hana.is_connected is False


def test_postgres_jdbc_url_and_load_dataframe_branches() -> None:
    pg_cfg = _source("postgresql")
    pg_cfg.connection_params = {"sslmode": "require"}
    pg = PostgresService(_settings(), "pg", pg_cfg, redis_context=object())

    spark = MagicMock()
    spark.read.jdbc.return_value = "df"
    assert "sslmode=require" in pg._jdbc_url

    out = pg._load_dataframe(spark, "public", "users", None, table_read_options=None)
    assert out == "df"
    spark.read.jdbc.assert_called()


def test_sql_database_service_guard_rails() -> None:
    class _Stub(SqlDatabaseService):
        def _table_label_for_log(self, schema: str, table: str) -> str:
            return f"{schema}.{table}"

        def _load_dataframe(
            self, spark_session, schema, table, select_query, table_read_options=None
        ):
            return MagicMock(count=lambda: 1)

        def connect(self) -> None:
            return None

        def close(self) -> None:
            return None

    cfg = _source("postgresql")
    svc = _Stub(_settings(), "s", cfg, redis_context=object())
    with pytest.raises(ServiceError, match="Spark session is required"):
        svc.extract_table("public", "users", spark_session=None)
    with pytest.raises(ServiceError, match="Database sources use Spark extract_table"):
        svc.fetch_data("users")
