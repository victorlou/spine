from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Sequence

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.readwriter import DataFrameWriter

from src.config.config_models import LoadingConfig
from src.utils.exceptions import LoaderError
from src.utils.logger import get_logger
from src.utils.path_prefix import prepend_source_type_prefix

if TYPE_CHECKING:
    from src.loader.object_store import ObjectStore


class BaseLoadStrategy(ABC):
    """Base class for table load strategies backed by an object store."""

    supported_write_modes: Sequence[str] = ("append", "overwrite", "merge")
    format_display_name = "table"

    @staticmethod
    def resolve_physical_column_name(columns: list[str], logical: str) -> str:
        """Match ``logical`` to a DataFrame column name (case-sensitive, then case-insensitive)."""
        if logical in columns:
            return logical
        want = logical.lower()
        for c in columns:
            if c.lower() == want:
                return c
        raise LoaderError(
            f"Incremental cursor column {logical!r} not found in destination columns: {columns}",
        )

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

    def _generate_table_location(self) -> str:
        """
        Generate the object-store URI for a directory-based table.

        The location is built from the strategy's configured `base_uri`, loading
        `config.prefix`, and optional source type prefix. The returned URI includes
        a trailing slash because table formats such as Delta and Iceberg are stored
        as directories containing data files and format-specific metadata.

        Returns:
            Fully resolved table directory URI.
        """
        full_prefix = prepend_source_type_prefix(self.config.prefix, self.source_type)

        if full_prefix:
            return self.object_store.resolve_path(self.base_uri, full_prefix, trailing_slash=True)
        return self.object_store.resolve_path(self.base_uri, trailing_slash=True)

    def _optimize_dataframe(self, df: DataFrame) -> DataFrame:
        """
        Apply common DataFrame optimizations before writing.

        When ``config.output_partitions`` is set the DataFrame is coalesced to that
        many partitions before writing. When unset the DataFrame is returned as-is,
        letting Spark decide the number of output files based on its own partitioning
        — which is the right default for large tables read via parallel JDBC.
        """
        if self.config.output_partitions is not None:
            return df.coalesce(self.config.output_partitions)
        return df

    def _optimize_dataframe_for_write(self, df: DataFrame) -> DataFrame:
        """
        Apply ``_optimize_dataframe`` and log Spark partition counts.

        ``coalesce`` cannot increase partition count; warn when ``output_partitions``
        is larger than the current partition count so operators do not assume extra
        write parallelism was created.
        """
        partitions_in = df.rdd.getNumPartitions()
        optimized_df = self._optimize_dataframe(df)
        partitions_out = optimized_df.rdd.getNumPartitions()
        self.logger.debug(
            "Table write partition layout",
            extra_fields={
                "partitions_before_optimize": partitions_in,
                "output_partitions": self.config.output_partitions,
                "partitions_after_optimize": partitions_out,
                "destination": self.config.destination,
                "prefix": self.config.prefix,
            },
        )
        if (
            self.config.output_partitions is not None
            and self.config.output_partitions > partitions_in
        ):
            self.logger.warning(
                "output_partitions exceeds current DataFrame partition count; coalesce cannot "
                "increase partitions. Use parallel JDBC (table_read_options) or repartition "
                "(shuffle) if more write tasks are required.",
                extra_fields={
                    "output_partitions": self.config.output_partitions,
                    "partitions_before_optimize": partitions_in,
                    "partitions_after_optimize": partitions_out,
                },
            )
        return optimized_df

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

        optimized_df = self._optimize_dataframe_for_write(df)
        writer = optimized_df.write.format(format_type).mode(write_mode)

        if options_copy:
            writer = writer.options(**options_copy)

        return writer

    def resolve_table_location(self) -> str:
        """
        Resolve the object-store location where the table data is stored.

        This is intentionally named as a storage location, not an identifier:
        catalog-backed formats such as Iceberg can derive a separate catalog
        identifier from this location, while path-backed formats such as Delta use
        the location directly for Spark writes and merge operations.
        """
        return self._generate_table_location()

    def read_max_column_as_string(self, logical_column: str) -> Optional[str]:
        """
        Return ``MAX(logical_column)`` as a string for incremental watermarking, or
        ``None`` when the table is missing, empty, or all-null for that column.

        Table formats that support JDBC ``incremental_extract`` must override this;
        the default raises :class:`LoaderError`.
        """
        raise LoaderError(
            f"Incremental MAX cursor read is not implemented for loading format {self.config.format!s}"
        )

    def write(self, df: DataFrame, **kwargs: Any) -> str:
        """
        Write a table using the common write-mode orchestration.

        Strategy subclasses own the format-specific mechanics for simple writes,
        existence checks, and merge execution. The base class owns routing and
        validation so append, overwrite, and merge modes behave consistently across
        table formats.
        """
        table_location = self.resolve_table_location()
        write_mode = str(self.config.write_mode)

        if write_mode not in self.supported_write_modes:
            supported_modes = ", ".join(f"'{mode}'" for mode in self.supported_write_modes)
            raise LoaderError(
                f"Unsupported write mode '{write_mode}' for {self.format_display_name}. "
                f"Supported modes are {supported_modes}."
            )

        if write_mode == "merge":
            merge_keys = self._validated_merge_keys()
            if not self.table_exists():
                self.logger.debug(
                    f"{self.format_display_name} table does not exist, creating it first",
                    extra_fields={"table_location": table_location},
                )
                self.write_simple(df, table_location, mode="append", **kwargs)
            else:
                self.perform_merge(df, table_location, merge_keys)
        else:
            self.write_simple(df, table_location, mode=write_mode, **kwargs)

        self.logger.info(
            f"Successfully loaded {self.format_display_name} table",
            extra_fields={
                "destination": table_location,
                "write_mode": write_mode,
                "merge_keys": self.config.merge_keys if write_mode == "merge" else None,
            },
        )
        return table_location

    def _validated_merge_keys(self) -> List[str]:
        """Return configured merge keys or fail before format-specific merge work starts."""
        merge_keys = self.config.merge_keys
        if not merge_keys:
            raise LoaderError(
                "Merge keys must be specified in the configuration for merge write mode."
            )
        return merge_keys

    @abstractmethod
    def table_exists(self) -> bool:
        """Return whether a table already exists for the resolved location."""

    @abstractmethod
    def write_simple(
        self,
        df: DataFrame,
        table_location: str,
        *,
        mode: str,
        **kwargs: Any,
    ) -> None:
        """Execute an append/overwrite-style write for this table format."""

    @abstractmethod
    def perform_merge(
        self,
        df: DataFrame,
        table_location: str,
        merge_keys: List[str],
    ) -> None:
        """Execute this table format's merge/upsert operation."""
