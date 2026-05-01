from __future__ import annotations

import uuid
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from pyspark.sql import DataFrame, SparkSession

from src.config.config_models import LoadingConfig, LoadingFormat
from src.utils.exceptions import LoaderError
from src.utils.s3_transient_retry import retry_on_transient_storage_error

from .base_load_strategy import BaseLoadStrategy

if TYPE_CHECKING:
    from src.loader.object_store import ObjectStore


class IcebergStrategy(BaseLoadStrategy):
    """Iceberg table load strategy backed by the configured Spark Iceberg catalog."""

    format_display_name = "Iceberg"
    catalog_name = "iceberg"

    def __init__(
        self,
        spark: SparkSession,
        object_store: ObjectStore,
        base_uri: str,
        config: LoadingConfig,
        source_type: Optional[str],
    ):
        super().__init__(spark, object_store, base_uri, config, source_type)

    def resolve_identifier(self) -> str:
        """Resolve the Iceberg catalog identifier for the configured table location."""
        return self._catalog_identifier_from_location(self.resolve_table_location())

    def _catalog_identifier_from_location(self, table_location: str) -> str:
        """
        Convert a warehouse-relative Iceberg table location into a quoted catalog identifier.
        """
        warehouse_root = self.base_uri.rstrip("/")
        location_prefix = f"{warehouse_root}/"

        if not table_location.startswith(location_prefix):
            raise LoaderError(
                f"Cannot derive Iceberg table identifier from location '{table_location}' "
                f"with warehouse '{self.base_uri}'"
            )

        table_only_path = table_location[len(location_prefix) :].strip("/")
        if not table_only_path:
            raise LoaderError(
                f"Cannot derive Iceberg table identifier from location '{table_location}' "
                f"with warehouse '{self.base_uri}'"
            )

        table_parts = [part for part in table_only_path.split("/") if part]
        return f"{self.catalog_name}." + ".".join(f"`{part}`" for part in table_parts)

    def _set_warehouse_conf(self) -> None:
        self.spark.conf.set(
            f"spark.sql.catalog.{self.catalog_name}.warehouse", self.base_uri.rstrip("/")
        )

    def _unset_warehouse_conf(self) -> None:
        try:
            self.spark.conf.unset(f"spark.sql.catalog.{self.catalog_name}.warehouse")
        except Exception as e:
            self.logger.trace(
                "Failed to unset Iceberg warehouse configuration after operation",
                extra_fields={"error": str(e), "warehouse": self.base_uri.rstrip("/")},
            )

    def table_exists(self) -> bool:
        """Return whether the Iceberg catalog table exists."""
        table_location = self.resolve_table_location()
        table_identifier = self._catalog_identifier_from_location(table_location)

        self._set_warehouse_conf()
        try:
            return bool(self.spark.catalog.tableExists(table_identifier))
        except Exception as e:
            self.logger.debug(
                "Iceberg table existence check failed",
                extra_fields={
                    "table_location": table_location,
                    "table_identifier": table_identifier,
                    "error": str(e),
                },
            )
            return False
        finally:
            self._unset_warehouse_conf()

    @retry_on_transient_storage_error()
    def write_simple(
        self,
        df: DataFrame,
        table_location: str,
        *,
        mode: str,
        **kwargs: Any,
    ) -> None:
        """Write an Iceberg table through the configured catalog identifier."""
        table_identifier = self._catalog_identifier_from_location(table_location)
        write_options = {
            "format": LoadingFormat.ICEBERG,
            "mode": mode,
            "mergeSchema": "true",
            **kwargs.get("write_options", {}),
        }
        if self.config.compression:
            write_options["compression"] = self.config.compression

        self._set_warehouse_conf()
        try:
            writer = self._prepare_writer(df, write_options)
            writer.saveAsTable(table_identifier)
        finally:
            self._unset_warehouse_conf()

    def perform_merge(self, df: DataFrame, table_location: str, merge_keys: List[str]) -> None:
        """Perform an Iceberg MERGE INTO operation (upsert)."""
        df_columns_lower = {col.lower() for col in df.columns}
        missing_keys = [key for key in merge_keys if key.lower() not in df_columns_lower]
        if missing_keys:
            raise LoaderError(
                f"Merge keys not found in DataFrame: {missing_keys}. "
                f"Available columns: {df.columns}"
            )

        table_identifier = self._catalog_identifier_from_location(table_location)
        source_view = f"iceberg_merge_source_{uuid.uuid4().hex}"

        self._set_warehouse_conf()
        try:
            target_df = self.spark.table(table_identifier)

            source_cols_map = {col.lower(): col for col in df.columns}
            target_cols_map = {col.lower(): col for col in target_df.columns}

            missing_target_keys = [key for key in merge_keys if key.lower() not in target_cols_map]
            if missing_target_keys:
                raise LoaderError(
                    f"Merge keys not found in Iceberg table: {missing_target_keys}. "
                    f"Available columns: {target_df.columns}"
                )

            merge_conditions = [
                f"target.`{target_cols_map[key.lower()]}` = source.`{source_cols_map[key.lower()]}`"
                for key in merge_keys
            ]

            target_schema: Dict[str, Any] = {
                field.name: field.dataType for field in target_df.schema.fields
            }
            target_columns: List[str] = list(target_schema.keys())
            merge_keys_lower = {key.lower() for key in merge_keys}

            update_assignments: List[str] = []
            insert_columns: List[str] = []
            insert_values: List[str] = []

            for target_col in target_columns:
                target_col_lower = target_col.lower()
                insert_columns.append(f"`{target_col}`")

                if target_col_lower in source_cols_map:
                    source_col_exact = source_cols_map[target_col_lower]
                    insert_values.append(f"source.`{source_col_exact}`")

                    if target_col_lower not in merge_keys_lower:
                        update_assignments.append(f"`{target_col}` = source.`{source_col_exact}`")
                else:
                    data_type = target_schema[target_col].simpleString()
                    insert_values.append(f"CAST(NULL AS {data_type})")

            df.createOrReplaceTempView(source_view)

            merge_sql_lines = [
                f"MERGE INTO {table_identifier} AS target",
                f"USING {source_view} AS source",
                f"ON {' AND '.join(merge_conditions)}",
            ]

            if update_assignments:
                merge_sql_lines.append("WHEN MATCHED THEN UPDATE SET")
                merge_sql_lines.append("  " + ", ".join(update_assignments))

            merge_sql_lines.append(f"WHEN NOT MATCHED THEN INSERT ({', '.join(insert_columns)})")
            merge_sql_lines.append(f"VALUES ({', '.join(insert_values)})")

            merge_sql = "\n".join(merge_sql_lines)

            self.logger.debug(
                "Performing Iceberg MERGE operation",
                extra_fields={
                    "table_location": table_location,
                    "table_identifier": table_identifier,
                    "merge_keys": merge_keys,
                    "merge_condition": " AND ".join(merge_conditions),
                    "source_column_count": len(df.columns),
                    "update_columns": update_assignments,
                    "insert_columns": insert_columns,
                },
            )

            self.spark.sql(merge_sql)

            self.logger.info(
                "Iceberg MERGE operation completed successfully",
                extra_fields={
                    "table_location": table_location,
                    "table_identifier": table_identifier,
                    "merge_keys": merge_keys,
                },
            )

        except Exception as e:
            error_msg = f"Failed to perform Iceberg MERGE operation: {e!s}"
            self.logger.error(
                error_msg,
                extra_fields={
                    "error": str(e),
                    "table_location": table_location,
                    "table_identifier": table_identifier,
                    "merge_keys": merge_keys,
                },
            )
            raise LoaderError(error_msg) from e
        finally:
            try:
                self.spark.catalog.dropTempView(source_view)
            except Exception:
                pass
            self._unset_warehouse_conf()
