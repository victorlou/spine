from typing import Any, Dict, List, Optional, Union

from delta.tables import DeltaTable
from pyspark.sql import Column, DataFrame, SparkSession

from src.config.config_models import LoadingConfig, LoadingFormat
from src.loader import ObjectStore
from src.utils.exceptions import LoaderError

from .base_load_strategy import BaseLoadStrategy


class DeltaStrategy(BaseLoadStrategy):
    def __init__(
        self,
        spark: SparkSession,
        object_store: ObjectStore,
        base_uri: str,
        config: LoadingConfig,
        source_type: Optional[str],
    ):
        super().__init__(spark, object_store, base_uri, config, source_type)

    def table_exists(self):
        """
        Check if the Delta Lake table exists by verifying the presence of the _delta_log directory.
        - The _delta_log directory is a key component of a Delta Lake table, containing the transaction log and metadata.
        - If the _delta_log directory exists at the specified path, it indicates that the Delta Lake table is present and can be loaded.
        - This method should return True if the _delta_log directory is found, and False otherwise.
        """
        delta_log_path = f"{self.resolve_identifier()}/_delta_log"
        return self.object_store.exists(delta_log_path)

    def _perform_delta_merge(self, df: DataFrame, delta_path: str, merge_keys: List[str]) -> None:
        """
        Perform Delta Lake MERGE operation (upsert).

        Matched rows: updates only columns present in both source and target (merge keys
        excluded from the update set). This avoids failures when the source schema is
        narrower than the table (e.g. upstream dropped a column that still exists in Delta).

        Unmatched rows: inserts all target columns; source columns supply values, and
        columns only on the target get typed NULL so the insert clause resolves.

        Target schema is read via DeltaTable.toDF() for column names and types. Append /
        overwrite paths still use mergeSchema for new columns from the source.

        Args:
            df: Source DataFrame with data to merge
            delta_path: Path to the Delta table
            merge_keys: List of column names to use as primary keys for matching
            config: Loading configuration

        Raises:
            LoaderError: If merge operation fails or merge keys are invalid
        """
        if DeltaTable is None:
            raise LoaderError(
                "DeltaTable is not available. Please ensure delta-spark is installed."
            )

        # Validate that all merge keys exist in the DataFrame
        df_columns_lower = {col.lower() for col in df.columns}
        missing_keys = [key for key in merge_keys if key.lower() not in df_columns_lower]
        if missing_keys:
            raise LoaderError(
                f"Merge keys not found in DataFrame: {missing_keys}. "
                f"Available columns: {df.columns}"
            )

        try:
            # Load the target Delta table
            delta_table = DeltaTable.forPath(self.spark, delta_path)
            target_df = delta_table.toDF()

            # 1. Create a mapping of lowercase source columns to their exact original casing
            source_cols_map = {col.lower(): col for col in df.columns}

            # Build merge condition using the exact casing from the source dataframe
            # Format: "target.key1 = updates.key1 AND target.key2 = updates.key2 ..."
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

            # 2. Build the sets using case-agnostic lookups
            for target_col in target_columns:
                target_col_lower = target_col.lower()

                # Case-agnostic check: Does the target column exist in the source dataframe?
                if target_col_lower in source_cols_map:
                    # Key present in both source and target. Add to update_set and insert_values.
                    # Match found! Fetch the exact source column name to build the SQL expression
                    source_col_exact = source_cols_map[target_col_lower]

                    insert_values[target_col] = f"updates.`{source_col_exact}`"

                    # Need to update this column if this merge_key already exists in table
                    if target_col_lower not in merge_keys_lower:
                        update_set[target_col] = f"updates.`{source_col_exact}`"

                else:
                    # No match found. Inject a typed NULL for inserts so new rows don't fail.
                    data_type = target_schema[target_col].simpleString()
                    insert_values[target_col] = f"CAST(NULL AS {data_type})"

            self.logger.debug(
                "Performing Delta MERGE operation",
                extra_fields={
                    "delta_path": delta_path,
                    "merge_keys": merge_keys,
                    "merge_condition": merge_condition,
                    "source_column_count": len(df.columns),
                    "shared_columns": list(update_set.keys()),
                    "insert_columns": list(insert_values.keys()),
                },
            )

            # Perform MERGE: update matched rows for shared columns only and insert all
            # target columns using typed NULLs for columns missing from the source.
            merge_builder = delta_table.alias("target").merge(
                source=df.alias("updates"), condition=merge_condition
            )

            # Shared non-key columns only (intersection); avoids referencing missing source columns.
            if update_set:
                merge_builder = merge_builder.whenMatchedUpdate(set=update_set)

            # Full target row on insert; typed NULL where the source has no column.
            merge_builder.whenNotMatchedInsert(values=insert_values).execute()

            self.logger.info(
                "Delta MERGE operation completed successfully",
                extra_fields={"delta_path": delta_path, "merge_keys": merge_keys},
            )

        except Exception as e:
            error_msg = f"Failed to perform Delta MERGE operation: {e!s}"
            self.logger.error(
                error_msg,
                extra_fields={
                    "error": str(e),
                    "delta_path": delta_path,
                    "merge_keys": merge_keys,
                },
            )
            raise LoaderError(error_msg) from e

    def write(self, df: DataFrame, **kwargs: Any) -> str:
        """
        Write the DataFrame to the Delta Lake table using the appropriate method based on the source type.
        """
        final_path = self.resolve_identifier()

        if self.config.write_mode == "merge":
            # We need to check if the table exists before attempting to merge
            if not self.table_exists():
                self.logger.debug(
                    "Delta table does not exist, creating it first",
                    extra_fields={"delta_path": final_path},
                )
                # Create table using append mode (first write)
                # This ensures schema evolution is enabled
                write_options = {
                    "format": LoadingFormat.DELTA,
                    "mode": "append",
                    "mergeSchema": "true",  # Enable schema evolution
                    **kwargs.get("write_options", {}),
                }
                if self.config.compression:
                    write_options["compression"] = self.config.compression

                writer = self._prepare_writer(df, write_options)
                writer.save(final_path)
            else:
                """
                Table does exists - we can proceed with the merge operation
                For merging, we need to specify the merge condition and the update/insert actions.
                """

                merge_keys = self.config.merge_keys
                if not merge_keys:
                    raise LoaderError(
                        "Merge keys must be specified in the configuration for merge write mode."
                    )
                self._perform_delta_merge(df, final_path, merge_keys)
        elif self.config.write_mode in ["append", "overwrite"]:
            write_options = {
                "format": LoadingFormat.DELTA,
                "mode": self.config.write_mode,
                "mergeSchema": "true",  # Enable schema evolution
                **kwargs.get("write_options", {}),
            }
            if self.config.compression:
                write_options["compression"] = self.config.compression

            writer = self._prepare_writer(df, write_options)
            writer.save(final_path)
        else:
            raise LoaderError(
                f"Unsupported write mode '{self.config.write_mode}' for Delta Lake. "
                f"Supported modes are 'append', 'overwrite', and 'merge'."
            )

        return final_path
