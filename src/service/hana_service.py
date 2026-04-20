"""SAP HANA ingestion via Spark JDBC (SAP ngdbc driver)."""

from typing import Optional
from urllib.parse import quote_plus

from pyspark.sql import DataFrame, SparkSession

from src.config.config_models import SourceConfig, SourceType, TableReadOptions
from src.config.settings import Settings
from src.service.sql_database_service import SqlDatabaseService
from src.utils.exceptions import ServiceError
from src.utils.redis_context import RedisContextManager


class HanaService(SqlDatabaseService):
    """SAP HANA using Spark JDBC (same read tuning model as PostgreSQL)."""

    HANA_JDBC_DRIVER = "com.sap.db.jdbc.Driver"

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

    def connect(self) -> None:
        try:
            self._validate_host_and_port_for_connect()
            self._is_connected = True
        except ServiceError:
            raise
        except Exception as e:
            raise ServiceError(
                message=f"Failed to validate HANA connection settings: {e!s}",
                service_name=self.__class__.__name__,
                operation="connect",
                original_error=e,
            ) from e

    def close(self) -> None:
        self._is_connected = False

    def _table_label_for_log(self, schema: str, table: str) -> str:
        return f'"{schema}"."{table}"' if schema else f'"{table}"'

    def _quoted_from_clause(self, schema: str, table: str) -> str:
        return self._table_label_for_log(schema, table)

    def _load_dataframe(
        self,
        spark_session: SparkSession,
        schema: str,
        table: str,
        select_query: Optional[str],
        table_read_options: Optional[TableReadOptions] = None,
    ) -> DataFrame:
        jdbc_url = self._build_jdbc_url()
        table_ref = self._quoted_from_clause(schema, table)

        if select_query:
            query = select_query
        else:
            query = f"(SELECT * FROM {table_ref}) AS data_query"

        connection_properties: dict[str, str] = {
            "driver": self.HANA_JDBC_DRIVER,
            "user": self.config.username or "",
            "password": self.config.password or "",
        }
        if self.config.connection_params:
            for k, v in self.config.connection_params.items():
                connection_properties[str(k)] = str(v)
        if table_read_options is not None and table_read_options.fetch_size is not None:
            connection_properties["fetchsize"] = str(table_read_options.fetch_size)

        reader = spark_session.read
        if table_read_options is not None and table_read_options.predicates:
            return reader.jdbc(
                url=jdbc_url,
                table=query,
                predicates=table_read_options.predicates,
                properties=connection_properties,
            )
        if table_read_options is not None and (table_read_options.partition_column or "").strip():
            col = table_read_options.partition_column.strip()
            return reader.jdbc(
                url=jdbc_url,
                table=query,
                column=col,
                lowerBound=table_read_options.lower_bound,
                upperBound=table_read_options.upper_bound,
                numPartitions=table_read_options.num_partitions,
                properties=connection_properties,
            )
        return reader.jdbc(
            url=jdbc_url,
            table=query,
            properties=connection_properties,
        )

    def _build_jdbc_url(self) -> str:
        port = int(self.config.port)  # type: ignore[arg-type]
        base = f"jdbc:sap://{self.config.host}:{port}/"
        parts: list[str] = []
        db = (self.config.database or "").strip()
        if db:
            parts.append(f"databaseName={quote_plus(db)}")
        if self.config.connection_params:
            for k, v in self.config.connection_params.items():
                key = str(k)
                if key.lower() in ("user", "password", "driver"):
                    continue
                parts.append(f"{key}={quote_plus(str(v))}")
        if parts:
            return f"{base}?{'&'.join(parts)}"
        return base
