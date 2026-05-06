"""
Shared JDBC-style extraction helpers for relational database source services.
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, Optional

from pyspark.sql import DataFrame, SparkSession

from src.config.config_models import SourceConfig, TableReadOptions
from src.config.settings import Settings
from src.service.base_service import BaseSourceService
from src.utils.exceptions import ServiceError
from src.utils.logger import get_logger
from src.utils.redis_context import RedisContextManager

_SPARK_JDBC_SUBQUERY_ALIAS = "spine_jdbc_subquery"


def jdbc_read_mode_label(table_read_options: Optional[TableReadOptions]) -> str:
    """
    Classify how Spark JDBC will parallelize the extract for logging.

    Returns one of: ``predicates``, ``partition_range``, ``single_table``.
    """
    if table_read_options is None:
        return "single_table"
    if table_read_options.predicates:
        return "predicates"
    if table_read_options.partition_column:
        return "partition_range"
    return "single_table"


def jdbc_table_option_from_custom_sql(select_sql: str) -> str:
    """
    Format ``database_select_query`` for Spark ``DataFrameReader.jdbc(..., table=...)``.

    Spark expects either a plain table identifier or a derived table of the form
    ``( SELECT ... ) alias``. Passing a bare ``SELECT`` makes the driver emit invalid SQL
    (nested ``FROM`` / duplicate ``SELECT``) during schema resolution.

    Callers that pass the result as ``dbtable`` with a non-empty custom query should also use
    :meth:`SqlDatabaseService._jdbc_read_connection_properties`, which turns off Spark JDBC V2
    LIMIT/OFFSET pushdown so dialects such as SAP HANA accept ``LIMIT`` inside the inner SELECT.
    """
    text = select_sql.strip().rstrip(";").strip()
    if not text:
        raise ValueError("database_select_query is empty")
    if text.startswith("("):
        return text
    return f"({text}) AS {_SPARK_JDBC_SUBQUERY_ALIAS}"


class SqlDatabaseService(BaseSourceService, ABC):
    """Base for SQL sources: Spark-backed reads, shared validation and extract flow."""

    _engine_label: str = "SQL database"
    _extract_object_kind: str = "table"

    def __init__(
        self,
        settings: Settings,
        source_name: str,
        config: SourceConfig,
        redis_context: RedisContextManager,
    ):
        super().__init__(settings)
        self.source_name = source_name
        self.config = config
        self.redis_context = redis_context
        self._is_connected = False

    def get_base_url(self) -> str:
        """Unused for database sources; satisfies BaseSourceService."""
        return "http://127.0.0.1"

    def get_headers(self) -> Dict[str, str]:
        return {}

    def fetch_data(
        self,
        resource_name: str,
        parameters: Optional[Dict[str, Any]] = None,
        *,
        full_response: bool = False,
    ) -> Dict[str, Any] | list[Any]:
        raise ServiceError(
            message="Database sources use Spark extract_table, not fetch_data",
            operation="fetch_data",
            service_name=self.__class__.__name__,
            is_retryable=False,
        )

    def _require_spark(
        self,
        spark_session: Optional[Any],
        operation: str = "extract_table",
    ) -> SparkSession:
        if spark_session is None:
            raise ServiceError(
                message=f"Spark session is required for {self._engine_label} extraction",
                service_name=self.__class__.__name__,
                operation=operation,
            )
        return spark_session

    def _extraction_log_fields(self, schema: str, table: str) -> Dict[str, Any]:
        return {
            "source": self.source_name,
            "schema": schema,
            "table": table,
        }

    @abstractmethod
    def _table_label_for_log(self, schema: str, table: str) -> str:
        """Human-readable table identifier for logs."""

    def _validate_host_for_connect(self) -> None:
        if not self.config.host:
            raise ServiceError(
                message=f"{self._engine_label} host is required",
                service_name=self.__class__.__name__,
                operation="connect",
            )

    def _validate_port_for_connect(self) -> None:
        port = self.config.port
        try:
            port_int = int(port)  # type: ignore[arg-type]
        except (TypeError, ValueError) as e:
            raise ServiceError(
                message=f"Invalid {self._engine_label} port: {port!r}",
                service_name=self.__class__.__name__,
                operation="connect",
            ) from e
        if not (1 <= port_int <= 65535):
            raise ServiceError(
                message=f"Invalid {self._engine_label} port: {port}",
                service_name=self.__class__.__name__,
                operation="connect",
            )

    def _validate_host_and_port_for_connect(self) -> None:
        self._validate_host_for_connect()
        self._validate_port_for_connect()

    def _build_connection_properties(
        self,
        driver: str,
        table_read_options: Optional[TableReadOptions] = None,
    ) -> Dict[str, str]:
        props: Dict[str, str] = {
            "driver": driver,
            "user": self.config.username or "",
            "password": self.config.password or "",
        }
        if self.config.connection_params:
            for k, v in self.config.connection_params.items():
                props[str(k)] = str(v)
        if table_read_options is not None and table_read_options.fetch_size is not None:
            props["fetchsize"] = str(table_read_options.fetch_size)
        return props

    def _jdbc_read_connection_properties(
        self,
        driver: str,
        table_read_options: Optional[TableReadOptions],
        select_query: Optional[str],
    ) -> Dict[str, str]:
        """
        JDBC properties for ``DataFrameReader.jdbc`` (Spark data source options + driver props).

        When ``database_select_query`` is set, Spine disables Spark JDBC V2 LIMIT/OFFSET
        pushdown. Otherwise Spark may inject LIMIT into nested ``dbtable`` subqueries and
        break dialects (notably SAP HANA) with errors such as syntax near ``SELECT``.
        These keys are Spark-only; they are not passed to ``Driver.connect``.
        """
        props = self._build_connection_properties(driver, table_read_options)
        if select_query and select_query.strip():
            return {
                **props,
                "pushDownLimit": "false",
                "pushDownOffset": "false",
            }
        return props

    def _ensure_extract_prerequisites(self) -> None:
        """Override when extract needs an established client (e.g. SQLAlchemy engine)."""

    @abstractmethod
    def _load_dataframe(
        self,
        spark_session: SparkSession,
        schema: str,
        table: str,
        select_query: Optional[str],
        table_read_options: Optional[TableReadOptions] = None,
    ) -> DataFrame:
        """Backend-specific read into a Spark DataFrame."""

    def extract_table(
        self,
        schema: str,
        table: str,
        select_query: Optional[str] = None,
        spark_session: Optional[Any] = None,
        table_read_options: Optional[TableReadOptions] = None,
    ) -> DataFrame:
        spark_session = self._require_spark(spark_session, operation="extract_table")
        self._ensure_extract_prerequisites()

        table_label = self._table_label_for_log(schema, table)
        fields = self._extraction_log_fields(schema, table)
        logger = get_logger(self.__class__.__name__)

        try:
            read_mode = jdbc_read_mode_label(table_read_options)
            plan_fields: Dict[str, Any] = {
                **fields,
                "jdbc_read_mode": read_mode,
            }
            if table_read_options is not None:
                if table_read_options.fetch_size is not None:
                    plan_fields["fetch_size"] = table_read_options.fetch_size
                if table_read_options.predicates:
                    plan_fields["predicates_count"] = len(table_read_options.predicates)
                if table_read_options.partition_column:
                    plan_fields["partition_column"] = table_read_options.partition_column
                    plan_fields["num_partitions"] = table_read_options.num_partitions

            logger.debug(
                "JDBC extract plan",
                extra_fields=plan_fields,
            )

            if select_query:
                logger.debug(
                    f"Extracting data using custom query from '{table_label}'",
                    extra_fields=fields,
                )
            else:
                logger.debug(
                    f"Extracting data from table '{table_label}'",
                    extra_fields=fields,
                )

            df = self._load_dataframe(
                spark_session,
                schema,
                table,
                select_query,
                table_read_options=table_read_options,
            )

            spark_partitions = df.rdd.getNumPartitions()

            logger.debug(
                "JDBC DataFrame created (lazy; Spark partition count from read plan)",
                extra_fields={
                    **fields,
                    "jdbc_read_mode": read_mode,
                    "spark_partitions": spark_partitions,
                },
            )

            extra_log: Dict[str, Any] = {
                **fields,
                "jdbc_read_mode": read_mode,
                "spark_partitions": spark_partitions,
            }
            if table_read_options is not None:
                if table_read_options.fetch_size is not None:
                    extra_log["fetch_size"] = table_read_options.fetch_size
                if table_read_options.predicates:
                    extra_log["predicates_count"] = len(table_read_options.predicates)
                if table_read_options.partition_column:
                    extra_log["partition_column"] = table_read_options.partition_column
                    extra_log["num_partitions"] = table_read_options.num_partitions

            logger.info(
                f"Successfully extracted from '{table_label}'",
                extra_fields=extra_log,
            )
            return df

        except ServiceError:
            raise
        except Exception as e:
            raise ServiceError(
                message=(
                    f"Failed to extract data from {self._engine_label} "
                    f"{self._extract_object_kind} '{schema}.{table}': {e!s}"
                ),
                service_name=self.__class__.__name__,
                operation="extract_table",
                original_error=e,
            ) from e

    @property
    def is_connected(self) -> bool:
        return self._is_connected
