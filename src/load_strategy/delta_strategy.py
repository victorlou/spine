from __future__ import annotations

from typing import TYPE_CHECKING, Any, Dict, List, Optional, Union

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

from src.config.config_models import LoadingConfig, LoadingFormat
from src.utils.exceptions import LoaderError
from src.utils.transient_storage_retry import retry_on_transient_storage_error

from .base_load_strategy import BaseLoadStrategy

if TYPE_CHECKING:
    from src.loader.object_store import ObjectStore


def _get_delta_table() -> Any:
    """Load the DeltaTable API only when Delta merge support is used."""
    try:
        from delta.tables import DeltaTable
    except ImportError as exc:  # pragma: no cover - exercised only when dependency is absent
        raise LoaderError(
            "Delta Lake merge requires the delta-spark Python package. "
            "Install delta-spark and configure Spark with Delta Lake support."
        ) from exc
    return DeltaTable


class DeltaStrategy(BaseLoadStrategy):
    """Delta Lake table load strategy."""

    format_display_name = "Delta Lake"

    def __init__(
        self,
        spark: SparkSession,
        object_store: ObjectStore,
        base_uri: str,
        config: LoadingConfig,
        source_type: Optional[str],
    ):
        super().__init__(spark, object_store, base_uri, config, source_type)

    def table_exists(self) -> bool:
        """
        Check if the Delta Lake table exists by verifying the presence of the
        `_delta_log` directory.
        """
        delta_log_path = f"{self.resolve_table_location().rstrip('/')}/_delta_log"
        try:
            return self.object_store.exists(delta_log_path)
        except Exception as e:
            self.logger.debug(
                "Delta table existence check failed",
                extra_fields={"delta_log_path": delta_log_path, "error": str(e)},
            )
            return False

    def read_max_column_as_string(self, logical_column: str) -> Optional[str]:
        """Return ``MAX(logical_column)`` from the Delta table at the resolved path, or ``None``."""
        if not self.table_exists():
            return None
        path = self.resolve_table_location().rstrip("/")
        df = self.spark.read.format("delta").load(path)
        if len(df.take(1)) == 0:
            return None
        phys = self.resolve_physical_column_name(list(df.columns), logical_column)
        row = df.select(F.max(F.col(phys)).alias("_mx")).collect()[0]
        val = row["_mx"]
        if val is None:
            return None
        return str(val)

    @retry_on_transient_storage_error()
    def write_simple(
        self,
        df: DataFrame,
        table_location: str,
        *,
        mode: str,
        **kwargs: Any,
    ) -> None:
        """Write a Delta table through Spark's path-based writer."""
        write_options = {
            "format": LoadingFormat.DELTA,
            "mode": mode,
            "mergeSchema": "true",
            **kwargs.get("write_options", {}),
        }
        if self.config.compression:
            write_options["compression"] = self.config.compression

        writer = self._prepare_writer(df, write_options)
        self.spark.sparkContext.setJobDescription(
            f"spine_delta_write:{mode}:{self.config.destination}:{self.config.prefix or ''}"
        )
        try:
            writer.save(table_location)
        finally:
            self.spark.sparkContext.setJobDescription("")

    def perform_merge(self, df: DataFrame, table_location: str, merge_keys: List[str]) -> None:
        """Perform Delta Lake MERGE operation (upsert)."""
        df_columns_lower = {col.lower() for col in df.columns}
        missing_keys = [key for key in merge_keys if key.lower() not in df_columns_lower]
        if missing_keys:
            raise LoaderError(
                f"Merge keys not found in DataFrame: {missing_keys}. "
                f"Available columns: {df.columns}"
            )

        df = self._optimize_dataframe_for_write(df)

        delta_table_api = _get_delta_table()

        try:
            delta_table = delta_table_api.forPath(self.spark, table_location)
            target_df = delta_table.toDF()

            source_cols_map = {col.lower(): col for col in df.columns}

            merge_conditions = [
                f"target.`{key}` = updates.`{source_cols_map[key.lower()]}`" for key in merge_keys
            ]
            merge_condition = " AND ".join(merge_conditions)

            target_schema: Dict[str, Any] = {
                field.name: field.dataType for field in target_df.schema.fields
            }
            target_columns: List[str] = list(target_schema.keys())
            merge_keys_lower = {key.lower() for key in merge_keys}

            update_set: Dict[str, Union[str, Column]] = {}
            insert_values: Dict[str, Union[str, Column]] = {}

            for target_col in target_columns:
                target_col_lower = target_col.lower()

                if target_col_lower in source_cols_map:
                    source_col_exact = source_cols_map[target_col_lower]
                    insert_values[target_col] = f"updates.`{source_col_exact}`"

                    if target_col_lower not in merge_keys_lower:
                        update_set[target_col] = f"updates.`{source_col_exact}`"
                else:
                    data_type = target_schema[target_col].simpleString()
                    insert_values[target_col] = f"CAST(NULL AS {data_type})"

            self.logger.debug(
                "Performing Delta MERGE operation",
                extra_fields={
                    "delta_path": table_location,
                    "merge_keys": merge_keys,
                    "merge_condition": merge_condition,
                    "source_column_count": len(df.columns),
                    "shared_columns": list(update_set.keys()),
                    "insert_columns": list(insert_values.keys()),
                },
            )

            merge_builder = delta_table.alias("target").merge(
                source=df.alias("updates"), condition=merge_condition
            )

            if update_set:
                merge_builder = merge_builder.whenMatchedUpdate(set=update_set)

            merge_builder.whenNotMatchedInsert(values=insert_values).execute()

            self.logger.info(
                "Delta MERGE operation completed successfully",
                extra_fields={"delta_path": table_location, "merge_keys": merge_keys},
            )

        except Exception as e:
            error_msg = f"Failed to perform Delta MERGE operation: {e!s}"
            self.logger.error(
                error_msg,
                extra_fields={
                    "error": str(e),
                    "delta_path": table_location,
                    "merge_keys": merge_keys,
                },
            )
            raise LoaderError(error_msg) from e
