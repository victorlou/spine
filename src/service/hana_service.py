"""SAP HANA ingestion via SQLAlchemy and Spark (driver-side fetch; not distributed)."""

import warnings
from typing import Any, Optional
from urllib.parse import quote_plus

from pyspark.sql import DataFrame, Row, SparkSession
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SAWarning

from src.config.config_models import SourceConfig, SourceType, TableReadOptions
from src.config.settings import Settings
from src.service.sql_database_service import SqlDatabaseService
from src.utils.exceptions import ServiceError
from src.utils.redis_context import RedisContextManager


class HanaService(SqlDatabaseService):
    """HANA using SQLAlchemy; rows are fetched on the driver then converted to a DataFrame."""

    def __init__(
        self,
        settings: Settings,
        source_name: str,
        config: SourceConfig,
        redis_context: RedisContextManager,
    ):
        if config.type != SourceType.HANA:
            raise ServiceError(
                message=f"HanaService requires hana source type, got {config.type!r}",
                service_name="HanaService",
                operation="__init__",
            )
        super().__init__(settings, source_name, config, redis_context)
        self._engine_label = "SAP HANA"
        self._extract_object_kind = "table/view"
        self._engine: Any = None
        warnings.filterwarnings(
            "ignore",
            r"Dialect hana:hdbcli will not make use of SQL compilation caching",
            SAWarning,
        )

    def connect(self) -> None:
        try:
            self._validate_host_and_port_for_connect()
            connection_string = self._build_connection_string()
            self._engine = create_engine(connection_string)
            with self._engine.connect() as connection:
                connection.execute(text("SELECT 1 FROM DUMMY"))
            self._is_connected = True
        except ServiceError:
            raise
        except Exception as e:
            raise ServiceError(
                message=f"Failed to connect to HANA database: {e!s}",
                service_name=self.__class__.__name__,
                operation="connect",
                original_error=e,
            ) from e

    def close(self) -> None:
        if self._engine is not None:
            try:
                self._engine.dispose()
            finally:
                self._engine = None
                self._is_connected = False

    def _ensure_extract_prerequisites(self) -> None:
        if not self._is_connected or self._engine is None:
            raise ServiceError(
                message="HANA service is not connected. Call connect() first.",
                service_name=self.__class__.__name__,
                operation="extract_table",
            )

    def _table_label_for_log(self, schema: str, table: str) -> str:
        return f'"{schema}"."{table}"' if schema else f'"{table}"'

    def _load_dataframe(
        self,
        spark_session: SparkSession,
        schema: str,
        table: str,
        select_query: Optional[str],
        table_read_options: Optional[TableReadOptions] = None,
    ) -> DataFrame:
        _ = table_read_options  # HANA reads via SQLAlchemy, not Spark read.jdbc; tuning not applied here yet.
        table_ref = self._table_label_for_log(schema, table)
        if select_query:
            query = select_query
        else:
            query = f"SELECT * FROM {table_ref}"

        with self._engine.connect() as connection:
            result = connection.execute(text(query))

            if not result.returns_rows:
                raise ServiceError(
                    message="Query did not return any rows",
                    service_name=self.__class__.__name__,
                    operation="extract_table",
                )

            rows = result.fetchall()
            columns = list(result.keys())

            spark_rows = []
            for row in rows:
                try:
                    row_dict = {col: row[col] for col in columns}
                except (TypeError, KeyError):
                    row_dict = {col: row[idx] for idx, col in enumerate(columns)}
                spark_rows.append(Row(**row_dict))

            return spark_session.createDataFrame(spark_rows)

    def _build_connection_string(self) -> str:
        password = quote_plus(self.config.password or "")
        connection_string = (
            f"hana://{self.config.username}:{password}@"
            f"{self.config.host}:{int(self.config.port)}"
        )
        # hdbcli `databaseName` must match a real HANA tenant; omit URL path when unset so
        # tenant-scoped SQL ports (common on BW) can connect without a bogus placeholder.
        db = (self.config.database or "").strip()
        if db:
            connection_string = f"{connection_string}/{db}"
        if self.config.connection_params:
            param_parts = [f"{key}={value}" for key, value in self.config.connection_params.items()]
            if param_parts:
                connection_string = f"{connection_string}?{'&'.join(param_parts)}"
        return connection_string
