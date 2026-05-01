from __future__ import annotations

from typing import TYPE_CHECKING, Any, Dict, List, Optional, Union

try:
    import delta.tables as delta_tables
except ImportError:  # pragma: no cover - exercised only when optional dependency is absent
    DeltaTable: Any = None
else:
    DeltaTable: Any = delta_tables.DeltaTable

from pyspark.sql import Column, DataFrame, SparkSession

from src.config.config_models import LoadingConfig, LoadingFormat
from src.utils.exceptions import LoaderError
from src.utils.s3_transient_retry import retry_on_transient_storage_error

from .base_load_strategy import BaseLoadStrategy

if TYPE_CHECKING:
    from src.loader.object_store import ObjectStore


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
        writer.save(table_location)

    def perform_merge(self, df: DataFrame, table_location: str, merge_keys: List[str]) -> None:
        """Perform Delta Lake MERGE operation (upsert)."""
        if DeltaTable is None:
            raise LoaderError(
                "DeltaTable is not available. Please ensure delta-spark is installed."
            )

        df_columns_lower = {col.lower() for col in df.columns}
        missing_keys = [key for key in merge_keys if key.lower() not in df_columns_lower]
        if missing_keys:
            raise LoaderError(
                f"Merge keys not found in DataFrame: {missing_keys}. "
                f"Available columns: {df.columns}"
            )

        try:
            delta_table = DeltaTable.forPath(self.spark, table_location)
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
