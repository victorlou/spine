"""Tests for SQL service factory and database service helpers."""

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from src.config.config_models import SourceType, TableReadOptions
from src.service.hana_service import HanaService
from src.service.postgres_service import PostgresService
from src.service.service_factory import ServiceFactory
from src.service.sql_database_service import SqlDatabaseService, jdbc_table_option_from_custom_sql
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


def test_hana_service_rejects_non_hana_source_type() -> None:
    bad_cfg = _source("postgresql")
    with pytest.raises(ServiceError, match="requires hana source type"):
        HanaService(_settings(), "hana", bad_cfg, redis_context=object())


def test_hana_connect_wraps_unexpected_validation_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    hana_cfg = _source("hana")
    hana = HanaService(_settings(), "hana", hana_cfg, redis_context=object())
    monkeypatch.setattr(
        hana,
        "_validate_host_and_port_for_connect",
        lambda: (_ for _ in ()).throw(RuntimeError("boom")),
    )

    with pytest.raises(ServiceError, match="Failed to validate HANA connection settings"):
        hana.connect()


def test_hana_jdbc_url_filters_reserved_and_encodes_values() -> None:
    hana_cfg = _source("hana")
    hana_cfg.database = "analytics db"
    hana_cfg.connection_params = {
        "schema": "A B",
        "user": "ignored",
        "password": "ignored",
        "driver": "ignored",
    }
    hana = HanaService(_settings(), "hana", hana_cfg, redis_context=object())

    url = hana._jdbc_url
    assert "databaseName=analytics+db" in url
    assert "schema=A+B" in url
    assert "user=" not in url
    assert "password=" not in url
    assert "driver=" not in url


def test_jdbc_table_option_from_custom_sql_wraps_bare_select() -> None:
    assert jdbc_table_option_from_custom_sql("SELECT 1") == "(SELECT 1) AS spine_jdbc_subquery"
    assert (
        jdbc_table_option_from_custom_sql("  SELECT 1; \n") == "(SELECT 1) AS spine_jdbc_subquery"
    )


def test_jdbc_table_option_from_custom_sql_passes_through_parenthesized() -> None:
    wrapped = "(SELECT 1) AS custom_alias"
    assert jdbc_table_option_from_custom_sql(wrapped) is wrapped


def test_jdbc_table_option_from_custom_sql_rejects_empty() -> None:
    with pytest.raises(ValueError, match="empty"):
        jdbc_table_option_from_custom_sql("   ;  ")


def test_hana_table_label_and_from_clause() -> None:
    hana = HanaService(_settings(), "hana", _source("hana"), redis_context=object())
    assert hana._table_label_for_log("public", "users") == '"public"."users"'
    assert hana._table_label_for_log("", "users") == '"users"'
    assert hana._quoted_from_clause("public", "users") == '"public"."users"'


@pytest.mark.parametrize(
    "select_query,table_read_options,expected_kwargs",
    [
        ("SELECT 1", None, {"table": "(SELECT 1) AS spine_jdbc_subquery"}),
        (
            None,
            TableReadOptions(predicates=["id > 10"]),
            {"predicates": ["id > 10"]},
        ),
        (
            None,
            TableReadOptions(
                partition_column="id",
                lower_bound=1,
                upper_bound=100,
                num_partitions=4,
            ),
            {"column": "id", "lowerBound": 1, "upperBound": 100, "numPartitions": 4},
        ),
        (None, None, {"table": '(SELECT * FROM "public"."users") AS data_query'}),
    ],
)
def test_hana_load_dataframe_routes_by_options(
    select_query, table_read_options, expected_kwargs
) -> None:
    hana = HanaService(_settings(), "hana", _source("hana"), redis_context=object())
    spark = MagicMock()
    spark.read.jdbc.return_value = "df"

    out = hana._load_dataframe(
        spark, "public", "users", select_query, table_read_options=table_read_options
    )
    assert out == "df"
    kwargs = spark.read.jdbc.call_args.kwargs
    for key, value in expected_kwargs.items():
        assert kwargs[key] == value


def test_sql_extract_table_ensure_prerequisites_hook_runs() -> None:
    class _Stub(SqlDatabaseService):
        ensured = False

        def _table_label_for_log(self, schema: str, table: str) -> str:
            return f"{schema}.{table}"

        def _load_dataframe(
            self, spark_session, schema, table, select_query, table_read_options=None
        ):
            return MagicMock(count=lambda: 1)

        def _ensure_extract_prerequisites(self) -> None:
            self.ensured = True

        def connect(self) -> None:
            return None

        def close(self) -> None:
            return None

    svc = _Stub(_settings(), "s", _source("postgresql"), redis_context=object())
    svc.extract_table("public", "users", spark_session=MagicMock())
    assert svc.ensured is True


def test_sql_extract_table_raises_service_error_on_loader_exception() -> None:
    class _Stub(SqlDatabaseService):
        def _table_label_for_log(self, schema: str, table: str) -> str:
            return f"{schema}.{table}"

        def _load_dataframe(
            self, spark_session, schema, table, select_query, table_read_options=None
        ):
            raise RuntimeError("jdbc failed")

        def connect(self) -> None:
            return None

        def close(self) -> None:
            return None

    svc = _Stub(_settings(), "s", _source("postgresql"), redis_context=object())
    with pytest.raises(ServiceError, match="Failed to extract data"):
        svc.extract_table("public", "users", spark_session=MagicMock())


def test_sql_extract_table_logs_without_count_when_disabled() -> None:
    class _Stub(SqlDatabaseService):
        def _table_label_for_log(self, schema: str, table: str) -> str:
            return f"{schema}.{table}"

        def _load_dataframe(
            self, spark_session, schema, table, select_query, table_read_options=None
        ):
            return MagicMock(count=MagicMock(return_value=99))

        def connect(self) -> None:
            return None

        def close(self) -> None:
            return None

    svc = _Stub(_settings(), "s", _source("postgresql"), redis_context=object())
    df = svc.extract_table("public", "users", spark_session=MagicMock())
    df.count.assert_not_called()


def test_sql_extract_table_logs_exact_count_when_requested() -> None:
    class _Stub(SqlDatabaseService):
        def _table_label_for_log(self, schema: str, table: str) -> str:
            return f"{schema}.{table}"

        def _load_dataframe(
            self, spark_session, schema, table, select_query, table_read_options=None
        ):
            return MagicMock(count=MagicMock(return_value=7))

        def connect(self) -> None:
            return None

        def close(self) -> None:
            return None

    svc = _Stub(_settings(), "s", _source("postgresql"), redis_context=object())
    read_opts = SimpleNamespace(
        fetch_size=1000,
        predicates=["id > 1"],
        partition_column="id",
        num_partitions=2,
        log_exact_row_count=True,
    )
    df = svc.extract_table(
        "public", "users", spark_session=MagicMock(), table_read_options=read_opts
    )
    df.count.assert_called_once()
