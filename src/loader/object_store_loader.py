"""
Loader for object storage destinations (S3, GCS, Azure Blob, local) using Spark.
"""

import time
import uuid
from datetime import UTC, datetime
from typing import Any, Dict, List, Optional, Tuple, Union

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructField, StructType

from src.config.config_models import LoadingConfig, LoadingFormat
from src.config.loading_schema import OBJECT_STORE_DESTINATIONS
from src.load_strategy import LoadStrategyFactory
from src.loader.base_loader import BaseLoader
from src.loader.object_store import SparkFilesystemObjectStore, loading_base_uri
from src.utils.exceptions import LoaderError
from src.utils.s3_transient_retry import retry_on_transient_storage_error


class ObjectStoreLoader(BaseLoader):
    """Loader for object storage destinations (S3, GCS, Azure Blob, local) using Spark and Hadoop FileSystem."""

    def __init__(self):
        super().__init__()
        self.spark = None
        self._object_store: Optional[SparkFilesystemObjectStore] = None

    def set_spark_session(self, spark: SparkSession) -> None:
        """
        Set the Spark session to use for loading data.

        Args:
            spark: SparkSession to use
        """
        self.spark = spark
        self._object_store = SparkFilesystemObjectStore(spark) if spark is not None else None

    @property
    def object_store(self) -> SparkFilesystemObjectStore:
        if not self.spark or not self._object_store:
            raise LoaderError("Spark session not set. Call set_spark_session first.")
        return self._object_store

    def _format_prefix(self, prefix: Optional[str]) -> str:
        """
        Format the prefix according to the required structure.
        Ensures the prefix follows the pattern: source_name/resource_name/data

        Args:
            prefix: Raw prefix from configuration

        Returns:
            str: Formatted prefix
        """
        if not prefix:
            return "data"

        clean_prefix = prefix.strip("/")
        return f"{clean_prefix}/data"

    def _generate_temp_path(self, base_uri: str, prefix: str, key: str) -> str:
        """
        Generate temporary path for initial write.

        Args:
            base_uri: Filesystem base URI (e.g. s3a://bucket or file:///path)
            prefix: Key prefix
            key: Final key name

        Returns:
            str: Temporary path URI
        """
        clean_prefix = self._format_prefix(prefix)
        return self.object_store.resolve_path(
            base_uri, clean_prefix, "_temp", "spark_writes", key, trailing_slash=False
        )

    def _generate_final_path(
        self, base_uri: str, prefix: str, extension: str = "parquet"
    ) -> Tuple[str, str]:
        """
        Generate final path with timestamp and UUID for file-based formats (e.g., Parquet).

        Args:
            base_uri: Filesystem base URI
            prefix: Key prefix
            extension: File extension

        Returns:
            tuple[str, str]: Final path URI and key
        """
        timestamp = datetime.now(UTC).strftime("%Y-%m-%dT%H-%M-%SZ")
        unique_id = str(uuid.uuid4())
        clean_prefix = self._format_prefix(prefix)
        key = f"{timestamp}_{unique_id}.{extension}"
        final = self.object_store.resolve_path(base_uri, clean_prefix, key)
        return final, key

    def _ensure_dataframe(
        self, data: Union[DataFrame, List[Dict[str, Any]]], schema: Optional[StructType] = None
    ) -> DataFrame:
        """
        Ensure input is a Spark DataFrame, converting if necessary.

        Args:
            data: Input data (DataFrame or list of dicts)
            schema: Optional schema for DataFrame creation

        Returns:
            DataFrame: Spark DataFrame

        Raises:
            LoaderError: If conversion fails
        """
        try:
            # If already a DataFrame, validate and return
            if isinstance(data, DataFrame):
                self.logger.trace("DataFrame details", extra_fields={"columns": data.columns})
                return data

            # Convert list of dicts to DataFrame
            return self._create_dataframe(data, schema)

        except Exception as e:
            error_msg = f"Failed to ensure Spark DataFrame: {e!s}"
            self.logger.error(
                error_msg, extra_fields={"error": str(e), "input_type": type(data).__name__}
            )
            raise LoaderError(error_msg) from e

    def _create_dataframe(
        self, data: List[Dict[str, Any]], schema: Optional[StructType] = None
    ) -> DataFrame:
        """
        Create a Spark DataFrame from the input data.

        Args:
            data: List of records to convert
            schema: Optional Spark schema to use

        Returns:
            DataFrame: Spark DataFrame

        Raises:
            LoaderError: If DataFrame creation fails
        """
        try:
            # Log data structure before DataFrame creation
            self.logger.debug(
                "Creating DataFrame from input data",
                extra_fields={"record_count": len(data), "has_schema": schema is not None},
            )
            self.logger.trace(
                "Input data structure",
                extra_fields={
                    "sample_records": data[:2] if data else None,
                    "field_names": list(set().union(*(d.keys() for d in data))) if data else [],
                    "field_types": (
                        {
                            k: list(set(type(d.get(k)).__name__ for d in data[:5] if k in d))
                            for k in set().union(*(d.keys() for d in data))
                        }
                        if data
                        else {}
                    ),
                },
            )

            # Create DataFrame
            if schema:
                df = self.spark.createDataFrame(data, schema=schema)
            else:
                # First create a small sample DataFrame to validate and infer schema
                sample_size = min(len(data), 2)
                sample_df = self.spark.createDataFrame(data[:sample_size])

                # Get the inferred schema and make all fields nullable
                inferred_schema = sample_df.schema
                nullable_schema = StructType(
                    [
                        StructField(
                            field.name, field.dataType, True
                        )  # Set nullable=True for all fields
                        for field in inferred_schema.fields
                    ]
                )

                self.logger.trace(
                    "Schema inference completed",
                    extra_fields={
                        "field_names": [f.name for f in nullable_schema.fields],
                        "sample_size": sample_size,
                    },
                )

                # Create full DataFrame with nullable schema
                df = self.spark.createDataFrame(data, schema=nullable_schema)

            self.logger.debug(
                "DataFrame created successfully", extra_fields={"row_count": df.count()}
            )
            self.logger.trace("DataFrame structure", extra_fields={"columns": df.columns})

            return df

        except Exception as e:
            error_msg = f"Failed to create Spark DataFrame: {e!s}"
            self.logger.error(
                error_msg,
                extra_fields={
                    "error": str(e),
                    "error_type": type(e).__name__,
                    "record_count": len(data) if data else 0,
                },
            )
            raise LoaderError(error_msg) from e

    def _get_iceberg_table_identifier(self, path: str, iceberg_warehouse_path: str) -> str:
        """
        Convert a warehouse-relative Iceberg table path into a quoted catalog identifier.

        Args:
            path: Fully resolved table path under the Iceberg warehouse root
            iceberg_warehouse_path: Warehouse root configured for the `iceberg` catalog

        Returns:
            str: Quoted catalog table identifier

        Raises:
            LoaderError: If the table identifier cannot be derived from the path
        """
        warehouse_root = iceberg_warehouse_path.rstrip("/")
        path_prefix = f"{warehouse_root}/"

        if not path.startswith(path_prefix):
            raise LoaderError(
                f"Cannot derive Iceberg table identifier from path '{path}' with warehouse '{iceberg_warehouse_path}'"
            )

        table_only_path = path[len(path_prefix) :].strip("/")
        if not table_only_path:
            raise LoaderError(
                f"Cannot derive Iceberg table identifier from path '{path}' with warehouse '{iceberg_warehouse_path}'"
            )

        table_parts = [part for part in table_only_path.split("/") if part]
        return "iceberg." + ".".join(f"`{part}`" for part in table_parts)

    @retry_on_transient_storage_error()
    def _write_dataframe(
        self,
        df: DataFrame,
        path: str,
        write_options: Dict[str, Any],
        *,
        iceberg: bool = False,
        iceberg_warehouse_path: Optional[str] = None,
    ) -> None:
        """
        Write a DataFrame to object storage or to a catalog-backed Iceberg table.

        Args:
            df: DataFrame to write
            path: Resolved destination path
            write_options: Write options for the DataFrame (includes format, mode, compression, etc.)
            iceberg: Whether this write targets the configured Iceberg catalog
            iceberg_warehouse_path: Warehouse root used to derive the Iceberg table identifier
        """
        # Extract format and mode from write_options (create copy to avoid mutating original)
        options_copy = write_options.copy()
        format_type = options_copy.pop("format", "parquet")
        write_mode = options_copy.pop("mode", "overwrite")

        # Apply optimizations before writing
        optimized_df = self._optimize_dataframe(df)

        # Write with specified options
        writer = optimized_df.write.format(format_type).mode(write_mode)

        # Add remaining options (compression, etc.)
        if options_copy:
            writer = writer.options(**options_copy)

        is_delta = format_type == LoadingFormat.DELTA or format_type == "delta"
        if is_delta:
            t0 = time.perf_counter()
            self.logger.trace(
                "Delta write starting",
                extra_fields={"path": path, "iceberg_catalog": iceberg},
            )

        # Iceberg catalog writes must use a catalog table identifier, not a filesystem path.
        if iceberg:
            if not iceberg_warehouse_path:
                raise LoaderError("iceberg_warehouse_path must be provided for Iceberg writes")

            table_identifier = self._get_iceberg_table_identifier(path, iceberg_warehouse_path)

            # Use catalog-aware table writes so append/overwrite can create the table when missing.
            writer.saveAsTable(table_identifier)
        else:
            writer.save(path)

        if is_delta:
            elapsed = time.perf_counter() - t0
            self.logger.debug(
                "Delta write finished",
                extra_fields={
                    "path": path,
                    "elapsed_seconds": round(elapsed, 3),
                    "iceberg_catalog": iceberg,
                },
            )

    def _cleanup_temp_dir(self, store: SparkFilesystemObjectStore, temp_path: str) -> None:
        """
        Clean up temporary directory structure.
        Deletes the temp write directory and its parent directories if empty.

        Args:
            store: Object store for delete/exists checks
            temp_path: Path to temporary directory (URI string)
        """
        try:
            jvm = self.spark.sparkContext._jvm
            temp_path_obj = jvm.org.apache.hadoop.fs.Path(temp_path)

            store.delete(temp_path, recursive=True)

            spark_writes = str(temp_path_obj.getParent().toString())
            temp_dir = str(temp_path_obj.getParent().getParent().toString())

            if store.is_empty_directory(spark_writes):
                store.delete(spark_writes, recursive=True)
                if store.is_empty_directory(temp_dir):
                    store.delete(temp_dir, recursive=True)

            self.logger.trace(
                "Cleaned up temporary directories", extra_fields={"temp_path": temp_path}
            )

        except Exception as e:
            self.logger.warning(
                "Failed to clean up temporary directory",
                extra_fields={"temp_path": temp_path, "error": str(e)},
            )

    @retry_on_transient_storage_error()
    def _move_uri(self, store: SparkFilesystemObjectStore, src_uri: str, dst_uri: str) -> None:
        """Move file from temp to final location with retry logic."""
        store.move(src_uri, dst_uri)

    def load(
        self,
        data: Union[DataFrame, List[Dict[str, Any]]],
        config: LoadingConfig,
        schema: Optional[StructType] = None,
        source_type: Optional[str] = None,
        **kwargs,
    ) -> str:
        """
        Load data to object storage using Spark.
        Handles both Spark DataFrames and lists of dictionaries as input, converting to DataFrame if necessary.
        Supports both Delta (directory-based), Iceberg (directory-based) and Parquet (file-based) formats.

        Args:
            data: Input data (DataFrame or list of dicts)
            config: Loading configuration
            schema: Optional schema for DataFrame creation
            source_type: Optional source type (e.g., "rest_api") to prepend to the path
            **kwargs: Additional arguments for specific formats

        Returns:
            str: Path or key where data was loaded

        Raises:
            LoaderError: If loading fails
        """
        if not self.spark:
            raise LoaderError("Spark session not set. Call set_spark_session first.")

        try:
            base_uri = loading_base_uri(config)
        except ValueError as e:
            raise LoaderError(str(e)) from e

        self.logger.debug(
            "Starting object store data load",
            extra_fields={
                "destination": config.destination,
                "base_uri": base_uri,
                "format": config.format,
                "write_mode": config.write_mode,
                "input_type": type(data).__name__,
            },
        )
        self.logger.trace(
            "Load configuration details",
            extra_fields={
                "prefix": config.prefix,
                "compression": config.compression,
                "write_options": kwargs.get("write_options", {}),
            },
        )

        df = self._prepare_dataframe_for_load(data, schema, config)

        if config.format not in [LoadingFormat.DELTA, LoadingFormat.ICEBERG]:
            self.logger.warning(
                "Using file-based load strategy for non-table format",
                extra_fields={"format": config.format},
            )

            return self._load_file_based(
                df, config, base_uri=base_uri, source_type=source_type, **kwargs
            )

        # initialize load_strategy
        load_strategy = LoadStrategyFactory.create_load_strategy(
            self.spark,
            self.object_store,
            base_uri,
            config,
            source_type=source_type or "",
        )

        # use load_strategy to write data for table formats (Delta, Iceberg)
        load_strategy.write(df, **kwargs)

        if config.format == LoadingFormat.ICEBERG:
            # Iceberg format: write directly to final directory location
            return self._load_iceberg(
                df, config, base_uri=base_uri, source_type=source_type, **kwargs
            )

    def destination_exists(
        self,
        config: LoadingConfig,
        source_type: Optional[str] = None,
    ) -> bool:
        """
        Check if the table destination already exists (for auto-backfill detection).

        Used to decide whether to run backfill on first write: when destination does
        not exist and backfill is configured, the pipeline uses backfill date ranges.

        Args:
            config: Loading configuration (bucket, prefix, destination, format).
            source_type: Optional source type to prepend to the path (e.g. rest_api).

        Returns:
            True if the configured table exists at the resolved path, False otherwise.
            Returns False if destination is not object-store backed, format is not a
            table format, or required path fields are missing.
        """
        if config.destination not in OBJECT_STORE_DESTINATIONS or config.format not in [
            LoadingFormat.DELTA,
            LoadingFormat.ICEBERG,
        ]:
            return False
        if config.destination == "s3" and (not config.s3_bucket or not config.prefix):
            return False
        if config.destination == "local" and (not config.storage_root or not config.prefix):
            return False
        if config.destination == "gcs" and (not config.gcs_bucket or not config.prefix):
            return False
        if config.destination == "azure_blob" and (
            not config.azure_container or not config.azure_account or not config.prefix
        ):
            return False
        if not self.spark:
            return False
        try:
            base_uri = loading_base_uri(config)
        except ValueError:
            return False

        # Setup LoadStrategy and check if table exists at the generated path.
        load_strategy = LoadStrategyFactory.create_load_strategy(
            self.spark,
            self.object_store,
            base_uri,
            config,
            source_type=source_type or "",
        )

        return load_strategy.table_exists()

    def _perform_iceberg_merge(
        self,
        df: DataFrame,
        table_path: str,
        merge_keys: List[str],
        iceberg_warehouse_path: str,
    ) -> None:
        """
        Perform an Iceberg MERGE INTO operation (upsert).

        Matched rows update only columns present in both source and target, excluding
        merge keys from the update set. Unmatched rows insert all target columns, with
        typed NULLs for target-only columns.

        Args:
            df: Source DataFrame with data to merge
            table_path: Filesystem path to the Iceberg table
            merge_keys: List of column names to use as primary keys for matching
            iceberg_warehouse_path: Warehouse root configured for the `iceberg` catalog

        Raises:
            LoaderError: If merge operation fails or merge keys are invalid
        """
        if not self.spark:
            raise LoaderError("Spark session not set. Call set_spark_session first.")

        df_columns_lower = {col.lower() for col in df.columns}
        missing_keys = [key for key in merge_keys if key.lower() not in df_columns_lower]
        if missing_keys:
            raise LoaderError(
                f"Merge keys not found in DataFrame: {missing_keys}. "
                f"Available columns: {df.columns}"
            )

        table_identifier = self._get_iceberg_table_identifier(table_path, iceberg_warehouse_path)
        source_view = f"iceberg_merge_source_{uuid.uuid4().hex}"

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

                # used in both update and insert, so add to insert columns regardless of source match
                insert_columns.append(f"`{target_col}`")

                # if a column in target is present in source (case-insensitive), we can update and insert from source; otherwise insert NULL
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
                    "table_path": table_path,
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
                    "table_path": table_path,
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
                    "table_path": table_path,
                    "table_identifier": table_identifier,
                    "merge_keys": merge_keys,
                },
            )
            raise LoaderError(error_msg) from e
        finally:
            if self.spark:
                try:
                    self.spark.catalog.dropTempView(source_view)
                except Exception:
                    pass

    def _prepare_dataframe_for_load(
        self,
        data: Union[DataFrame, List[Dict[str, Any]]],
        schema: Optional[StructType],
        config: LoadingConfig,
    ) -> DataFrame:
        """
        Convert load input to a DataFrame and apply shared preprocessing before any
        format-specific load strategy runs.

        This keeps column cleanup and optional merge-key deduplication consistent for
        Delta, Iceberg, and file-based writes.
        """
        df = self._ensure_dataframe(data, schema)
        df = self._prepare_dataframe_columns(df)

        # Force deduplicate on merge keys if configured, to avoid non-deterministic
        # merge failures due to duplicate keys in source data. This is a temporary
        # workaround until merge logic can handle duplicate keys deterministically.
        if config.force_nondeterministic_deduplication and config.write_mode == "merge":
            self.logger.warning("Forcing non-deterministic deduplication...")

            df = df.dropDuplicates(config.merge_keys)

            self.logger.info(f"Source rows after deduplication: {df.count()}")

        return df

    def _prepare_dataframe_columns(self, df: DataFrame) -> DataFrame:
        """
        Sanitize, normalize, and deduplicate column names to prevent write failures
        from illegal characters or duplicate names across all load strategies.
        """
        df = self._rename_duplicate_columns(df)

        self.logger.trace(
            "DataFrame after handling duplicate columns",
            extra_fields={"columns": df.columns},
        )

        df = self._sanitize_column_names(df)

        self.logger.trace(
            "DataFrame after sanitizing column names",
            extra_fields={"columns": df.columns},
        )

        return df

    def _sanitize_column_names(self, df: DataFrame) -> DataFrame:
        """
        Sanitize column names by handling illegal characters for table formats such as
        Delta and Iceberg.

        Illegal characters are replaced/removed as follows:
        - Space ( ) -> underscore (_)
        - Other illegal characters (#, ., *, /, &) -> removed
        - Multiple consecutive underscores (2+) -> single underscore

        Only columns with illegal characters are renamed. Other columns remain unchanged.

        Args:
            df: Input DataFrame with potentially illegal column names

        Returns:
            DataFrame: DataFrame with sanitized column names

        Raises:
            LoaderError: If column renaming fails
        """
        try:
            illegal_chars = {" ", "#", ".", "*", "/", "&"}
            columns_to_rename = {}

            for col in df.columns:
                # Check if column contains any illegal characters
                if any(char in col for char in illegal_chars):
                    # Replace spaces with underscores
                    sanitized = col.strip().replace(" ", "_")
                    # Remove all other illegal characters
                    for char in illegal_chars - {" "}:
                        sanitized = sanitized.replace(char, "")
                    # Replace multiple consecutive underscores with single underscore
                    while "__" in sanitized:
                        sanitized = sanitized.replace("__", "_")
                    # Only add to rename dict if it changed
                    if sanitized != col:
                        columns_to_rename[col] = sanitized

            # Apply renames if any columns need sanitization
            if columns_to_rename:
                self.logger.debug(
                    "Sanitizing column names for table writes",
                    extra_fields={"columns_renamed": columns_to_rename},
                )
                for old_name, new_name in columns_to_rename.items():
                    df = df.withColumnRenamed(old_name, new_name)

            return df

        except Exception as e:
            error_msg = f"Failed to sanitize column names: {e!s}"
            self.logger.error(
                error_msg,
                extra_fields={"error": str(e), "columns": df.columns},
            )
            raise LoaderError(error_msg) from e

    def _rename_duplicate_columns(self, df: DataFrame) -> DataFrame:
        """
        Rename duplicate columns in the DataFrame to make them unique.

        The method renames duplicate column names while preserving the original column order.

        Args:
            df: Input DataFrame with potentially duplicate column names

        Returns:
            DataFrame: DataFrame with duplicate columns renamed

        Raises:
            LoaderError: If duplicate renaming fails
        """
        try:
            all_columns = df.columns

            # Track seen columns (case-insensitive) and build new column names
            seen = set()
            columns_array = []
            columns_to_rename = []

            for col in all_columns:
                col_lower = col.lower()

                if col_lower in seen:
                    # Duplicate found, generate unique name with counter
                    counter = 1
                    new_name = f"{col_lower}_{counter}"

                    while new_name in seen:
                        counter += 1
                        new_name = f"{col_lower}_{counter}"

                    columns_array.append(new_name)
                    columns_to_rename.append(new_name)
                    seen.add(new_name)
                else:
                    # Keep original column name if not a duplicate
                    columns_array.append(col)
                    seen.add(col_lower)

            # If there are duplicates, apply the rename
            if columns_to_rename:
                df = df.toDF(*columns_array)

            return df

        except Exception as e:
            error_msg = f"Failed to remove duplicate columns: {e!s}"
            self.logger.error(
                error_msg,
                extra_fields={"error": str(e), "columns": df.columns},
            )
            raise LoaderError(error_msg) from e

    def _load_iceberg(
        self,
        df: DataFrame,
        config: LoadingConfig,
        *,
        base_uri: str,
        source_type: Optional[str] = None,
        **kwargs,
    ) -> str:
        """
        Load data as an Iceberg table with support for append, overwrite, and merge.

        Iceberg tables are stored as directories managed by the configured `iceberg`
        catalog. Append and overwrite use catalog-aware writes. Merge uses SQL
        `MERGE INTO` against the catalog table identifier derived from the resolved
        table path.

        Args:
            df: DataFrame to write
            config: Loading configuration (must include merge_keys for merge mode)
            source_type: Optional source type to prepend to the path
            **kwargs: Additional arguments for specific formats

        Returns:
            str: Iceberg table path

        Raises:
            LoaderError: If loading fails or configuration is invalid
        """
        try:
            self.logger.trace(
                "Writing Iceberg table",
                extra_fields={
                    "table_path": final_path,
                    "write_mode": config.write_mode,
                    "has_merge_keys": config.merge_keys is not None,
                },
            )

            # Register the Iceberg catalog warehouse root for this write so the derived
            # catalog table identifier resolves to the same filesystem location as final_path.
            if self.spark:
                self.spark.conf.set("spark.sql.catalog.iceberg.warehouse", base_uri.rstrip("/"))

            if config.write_mode == "merge":
                if config.merge_keys is None:
                    raise LoaderError("merge_keys must be provided when write_mode is 'merge'")

                # First write creates the table using append semantics. Subsequent writes
                # use Iceberg MERGE INTO for upsert behavior.
                if not self._table_exists(final_path, LoadingFormat.ICEBERG):
                    self.logger.debug(
                        "Iceberg table does not exist, creating it first",
                        extra_fields={"table_path": final_path},
                    )
                    write_options = {
                        "format": LoadingFormat.ICEBERG,
                        "mode": "append",
                        "mergeSchema": "true",
                        **kwargs.get("write_options", {}),
                    }
                    if config.compression:
                        write_options["compression"] = config.compression

                    self._write_dataframe(
                        df,
                        final_path,
                        write_options,
                        iceberg=True,
                        iceberg_warehouse_path=base_uri,
                    )
                else:
                    self._perform_iceberg_merge(
                        df,
                        final_path,
                        config.merge_keys,
                        base_uri,
                    )

            elif config.write_mode == "append":
                write_options = {
                    "format": LoadingFormat.ICEBERG,
                    "mode": "append",
                    "mergeSchema": "true",
                    **kwargs.get("write_options", {}),
                }
                if config.compression:
                    write_options["compression"] = config.compression

                self._write_dataframe(
                    df,
                    final_path,
                    write_options,
                    iceberg=True,
                    iceberg_warehouse_path=base_uri,
                )

            else:
                write_options = {
                    "format": LoadingFormat.ICEBERG,
                    "mode": config.write_mode,
                    "mergeSchema": "true",
                    **kwargs.get("write_options", {}),
                }
                if config.compression:
                    write_options["compression"] = config.compression

                self._write_dataframe(
                    df,
                    final_path,
                    write_options,
                    iceberg=True,
                    iceberg_warehouse_path=base_uri,
                )

            self.logger.info(
                "Successfully loaded Iceberg table",
                extra_fields={
                    "destination": final_path,
                    "write_mode": config.write_mode,
                    "merge_keys": config.merge_keys if config.write_mode == "merge" else None,
                },
            )

            return final_path

        except Exception as e:
            error_msg = f"Failed to load Iceberg table: {e!s}"
            self.logger.error(
                error_msg,
                extra_fields={
                    "error": str(e),
                    "destination": config.destination,
                    "prefix": config.prefix,
                    "write_mode": config.write_mode,
                },
            )
            raise LoaderError(error_msg) from e
        finally:
            # Clean up the warehouse path from Spark configuration to avoid side effects on other operations
            if self.spark:
                try:
                    self.spark.conf.unset("spark.sql.catalog.iceberg.warehouse")
                except Exception as e:
                    self.logger.trace(
                        "Failed to unset Iceberg warehouse configuration after write",
                        extra_fields={"error": str(e), "warehouse": base_uri.rstrip("/")},
                    )

    def _load_file_based(
        self,
        df: DataFrame,
        config: LoadingConfig,
        *,
        base_uri: str,
        source_type: Optional[str] = None,
        **kwargs,
    ) -> str:
        """
        Load data as file-based format (e.g., Parquet) to object storage.
        Uses temporary file + move pattern for atomic writes.

        Args:
            df: DataFrame to write
            config: Loading configuration
            source_type: Optional source type to prepend to the path
            **kwargs: Additional arguments for specific formats

        Returns:
            str: Key where data was loaded
        """
        temp_path = None
        try:
            # Prepend source type prefix to the prefix
            prefixed_prefix = self._prepend_source_type_prefix(config.prefix or "data", source_type)

            # Generate paths for file-based format
            final_path, key = self._generate_final_path(
                base_uri=base_uri, prefix=prefixed_prefix, extension=config.format
            )

            temp_path = self._generate_temp_path(base_uri=base_uri, prefix=prefixed_prefix, key=key)

            # Prepare write options
            write_options = {
                "format": config.format,
                "mode": config.write_mode,
                "compression": config.compression or "snappy",
                **kwargs.get("write_options", {}),
            }

            self.logger.trace(
                "Writing data to temporary location",
                extra_fields={
                    "temp_path": temp_path,
                    "format": config.format,
                    "write_mode": config.write_mode,
                },
            )

            # Write to temporary location
            self._write_dataframe(df, temp_path, write_options)

            # Move file to final location
            try:
                store = self.object_store
                part_uri = store.glob_first_part_file(temp_path)
                if not part_uri:
                    raise LoaderError(f"No part file found in temporary location: {temp_path}")

                self.logger.debug(
                    "Moving file to final location", extra_fields={"destination": final_path}
                )

                self._move_uri(store, part_uri, final_path)

                # Clean up temp directory
                self._cleanup_temp_dir(store, temp_path)

            except Exception as e:
                raise LoaderError(f"Failed to move file to final destination: {e!s}") from e

            self.logger.info(
                "Successfully loaded data",
                extra_fields={
                    "destination": final_path,
                    "format": config.format,
                    "write_mode": config.write_mode,
                },
            )

            return key

        except Exception as e:
            # Attempt to clean up temp directory if it exists
            if temp_path:
                try:
                    self._cleanup_temp_dir(self.object_store, temp_path)
                except Exception as cleanup_error:
                    self.logger.warning(
                        "Failed to clean up temporary directory after error",
                        extra_fields={"temp_path": temp_path, "error": str(cleanup_error)},
                    )

            error_msg = f"Failed to load data: {e!s}"
            self.logger.error(
                error_msg,
                extra_fields={
                    "error": str(e),
                    "destination": config.destination,
                    "format": config.format,
                },
            )
            raise LoaderError(error_msg) from e
