from abc import ABC, abstractmethod
from typing import Any, Dict, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.readwriter import DataFrameWriter

from src.config.config_models import LoadingConfig
from src.loader import ObjectStore
from src.utils.logger import get_logger


class BaseLoadStrategy(ABC):
    """Base class for table/file load strategies backed by an object store."""

    def __init__(
        self,
        spark: SparkSession,
        object_store: ObjectStore,
        base_uri: str,
        config: LoadingConfig,
        source_type: Optional[str],
    ) -> None:
        self.spark = spark
        self.object_store = object_store
        self.base_uri = base_uri
        self.config = config
        self.source_type = source_type
        self.logger = get_logger(__name__)

    def _get_source_type_prefix(self, source_type: Optional[str]) -> str:
        """
        Get the storage prefix segment for a source type.

        Known source types are grouped into stable top-level storage folders:
        - `rest_api` -> `rest_api`
        - `python_sdk` -> `sdk`
        - relational database sources such as `postgresql` and `hana` -> `database`

        Args:
            source_type: Source type string or enum value.

        Returns:
            Prefix segment for recognized source types, otherwise an empty string.
        """
        if not source_type:
            return ""

        type_key = str(source_type.value) if hasattr(source_type, "value") else str(source_type)

        source_type_mapping: Dict[str, str] = {
            "rest_api": "rest_api",
            "python_sdk": "sdk",
            "postgresql": "database",
            "hana": "database",
        }

        return source_type_mapping.get(type_key, "")

    def _prepend_source_type_prefix(self, prefix: Optional[str], source_type: Optional[str]) -> str:
        """
        Prepend the mapped source type segment to a storage prefix.

        Args:
            prefix: Original storage prefix from loading config.
            source_type: Source type string or enum value.

        Returns:
            Prefix with the source type segment prepended when the source type is
            recognized; otherwise the original prefix or an empty string.
        """
        source_type_prefix = self._get_source_type_prefix(source_type)

        if not source_type_prefix:
            return prefix or ""

        source_type_prefix = source_type_prefix.strip("/")
        clean_prefix = prefix.strip("/") if prefix else ""

        if clean_prefix:
            return f"{source_type_prefix}/{clean_prefix}"
        return source_type_prefix

    def _generate_table_path(self) -> str:
        """
        Generate the object-store URI for a directory-based table.

        The path is built from the strategy's configured `base_uri`, loading
        `config.prefix`, and optional source type prefix. The returned URI includes
        a trailing slash because table formats such as Delta and Iceberg are stored
        as directories containing data files and format-specific metadata.

        Returns:
            Fully resolved table directory URI.
        """
        full_prefix = self._prepend_source_type_prefix(self.config.prefix, self.source_type)
        clean_prefix = full_prefix.strip("/") if full_prefix else ""

        if clean_prefix:
            return self.object_store.resolve_path(self.base_uri, clean_prefix, trailing_slash=True)
        return self.object_store.resolve_path(self.base_uri, trailing_slash=True)

    def _optimize_dataframe(self, df: DataFrame) -> DataFrame:
        """
        Apply common DataFrame optimizations before writing.

        Args:
            df: Input DataFrame.

        Returns:
            DataFrame coalesced to one partition for deterministic single-file output.
        """
        return df.coalesce(1)

    def _prepare_writer(
        self,
        df: DataFrame,
        write_options: Dict[str, Any],
    ) -> DataFrameWriter:
        """
        Build a configured Spark `DataFrameWriter` for append/overwrite writes.

        This helper does not execute the write. It copies `write_options`, removes
        writer controls consumed directly by Spark (`format` and `mode`), applies
        common DataFrame optimization, and attaches any remaining entries as writer
        options such as compression or schema evolution flags.

        Args:
            df: DataFrame to write.
            write_options: Writer options. `format` defaults to `parquet`, `mode`
                defaults to `overwrite`, and all other keys are passed to
                `DataFrameWriter.options`.

        Returns:
            Configured Spark `DataFrameWriter` ready for `save` or `saveAsTable`.
        """
        options_copy = write_options.copy()
        format_type = options_copy.pop("format", "parquet")
        write_mode = options_copy.pop("mode", "overwrite")

        optimized_df = self._optimize_dataframe(df)
        writer = optimized_df.write.format(format_type).mode(write_mode)

        if options_copy:
            writer = writer.options(**options_copy)

        return writer

    def resolve_identifier(self):
        """
        Resolve Table Identifier Path for Delta Lake
         - For Delta Lake, the identifier is typically the path to the Delta table in the storage system (e.g., S3, HDFS, or local filesystem).
         - The path should point to the directory containing the Delta Lake table's metadata and data files.
         - Example: "s3://my-bucket/delta-tables/my-table" or "hdfs://namenode:8020/delta-tables/my-table" or "file:///path/to/delta-tables/my-table"
         - Ensure that the path is correctly formatted and accessible based on your storage system and permissions.
        """
        return self._generate_table_path()

    @abstractmethod
    def table_exists(self) -> bool:
        """Return whether a table or file already exists for the resolved identifier."""

    @abstractmethod
    def write(
        self,
        df: DataFrame,
        **kwargs: Any,
    ) -> str:
        """Write the given DataFrame to the destination identified by this strategy, using
        the configured write options.
        """
