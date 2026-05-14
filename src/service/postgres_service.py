"""PostgreSQL ingestion via Spark JDBC."""

from functools import cached_property
from typing import Optional

from pyspark.sql import DataFrame, SparkSession

from src.config.config_models import SourceConfig, SourceType, TableReadOptions
from src.config.settings import Settings
from src.service.sql_database_service import (
    SqlDatabaseService,
    jdbc_dbtable_from_plain_table,
    jdbc_table_option_from_custom_sql,
    normalize_database_where_predicate,
)
from src.utils.exceptions import ServiceError
from src.utils.redis_context import RedisContextManager


class PostgresService(SqlDatabaseService):
    """PostgreSQL using Spark JDBC."""

    POSTGRES_JDBC_DRIVER = "org.postgresql.Driver"

    def __init__(
        self,
        settings: Settings,
        source_name: str,
        config: SourceConfig,
        redis_context: RedisContextManager,
    ):
        if config.type != SourceType.POSTGRESQL:
            raise ServiceError(
                message=f"PostgresService requires postgresql source type, got {config.type!r}",
                service_name="PostgresService",
                operation="__init__",
            )
        super().__init__(settings, source_name, config, redis_context)
        self._engine_label = "PostgreSQL"

    def connect(self) -> None:
        try:
            self._validate_host_and_port_for_connect()
            if not self.config.database:
                raise ServiceError(
                    message="PostgreSQL database name is required",
                    service_name=self.__class__.__name__,
                    operation="connect",
                )
            self._is_connected = True
        except ServiceError:
            raise
        except Exception as e:
            raise ServiceError(
                message=f"Failed to validate PostgreSQL connection: {e!s}",
                service_name=self.__class__.__name__,
                operation="connect",
                original_error=e,
            ) from e

    def close(self) -> None:
        self._is_connected = False

    def _table_label_for_log(self, schema: str, table: str) -> str:
        return f"{schema}.{table}" if schema else table

    def _load_dataframe(
        self,
        spark_session: SparkSession,
        schema: str,
        table: str,
        select_query: Optional[str],
        table_read_options: Optional[TableReadOptions] = None,
        database_where_predicate: Optional[str] = None,
    ) -> DataFrame:
        jdbc_url = self._jdbc_url
        table_ref = self._table_label_for_log(schema, table)

        if select_query:
            if normalize_database_where_predicate(database_where_predicate):
                raise ServiceError(
                    message="database_where_predicate cannot be used together with database_select_query",
                    service_name=self.__class__.__name__,
                    operation="_load_dataframe",
                    is_retryable=False,
                )
            query = jdbc_table_option_from_custom_sql(select_query)
        else:
            query = jdbc_dbtable_from_plain_table(
                table_ref, database_where_predicate=database_where_predicate
            )

        connection_properties = self._jdbc_read_connection_properties(
            self.POSTGRES_JDBC_DRIVER, table_read_options, select_query
        )

        reader = spark_session.read
        if table_read_options is not None and table_read_options.predicates:
            return reader.jdbc(
                url=jdbc_url,
                table=query,
                predicates=table_read_options.predicates,
                properties=connection_properties,
            )
        if table_read_options is not None and table_read_options.partition_column:
            return reader.jdbc(
                url=jdbc_url,
                table=query,
                column=table_read_options.partition_column,
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

    @cached_property
    def _jdbc_url(self) -> str:
        port = int(self.config.port)  # type: ignore[arg-type]
        base_url = f"jdbc:postgresql://{self.config.host}:{port}/{self.config.database}"
        if self.config.connection_params:
            url_params = {
                k: v
                for k, v in self.config.connection_params.items()
                if k not in ("user", "password", "driver")
            }
            if url_params:
                param_str = "&".join(f"{k}={v}" for k, v in url_params.items())
                base_url = f"{base_url}?{param_str}"
        return base_url
