"""
S3 loader for uploading data to AWS S3 using Spark.
"""

import time
import uuid
from datetime import UTC, datetime
from functools import wraps
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

from delta.tables import DeltaTable
from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql.types import StructField, StructType

from src.config.config_models import LoadingConfig
from src.loader.base_loader import BaseLoader, LoaderError
from src.loader.object_store import SparkFilesystemObjectStore, loading_base_uri
from src.utils.logger import get_logger


def retry_on_s3_error(max_retries: int = 3, delay: float = 1.0):
    """Simple retry decorator for S3 operations."""

    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    last_exception = e
                    if "Connection reset" in str(e) or "SocketException" in str(e):
                        if attempt < max_retries - 1:
                            logger = get_logger(func.__name__)
                            logger.warning(
                                f"S3 operation failed (attempt {attempt + 1}/{max_retries}). Retrying in {delay} seconds..."
                            )
                            time.sleep(delay)
                            continue
                    raise
            raise last_exception

        return wrapper

    return decorator


class S3Loader(BaseLoader):
    """Loader for object storage destinations (S3, local, …) using Spark and Hadoop FileSystem."""

    def __init__(self):
        """Initialize the S3 loader."""
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
        Format the S3 prefix according to the required structure.
        Ensures the prefix follows the pattern: source_name/resource_name/data

        Args:
            prefix: Raw prefix from configuration

        Returns:
            str: Formatted prefix
        """
        if not prefix:
            return "data"

        # Remove any leading/trailing slashes and ensure 'data' subdirectory
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

    def _get_source_type_prefix(self, source_type: Optional[str]) -> str:
        """
        Get the prefix path segment for a given source type.

        Maps source types to their storage prefixes:
        - "rest_api" -> "rest_api"
        - "python_sdk" -> "python_sdk"
        - Other types can be added as needed

        Args:
            source_type: Source type (e.g., "rest_api", "python_sdk")

        Returns:
            str: Source type prefix (e.g., "rest_api", "python_sdk") or empty string if not recognized
        """
        if not source_type:
            return ""

        type_key = source_type.value if hasattr(source_type, "value") else str(source_type)

        # Map source types to their storage prefixes
        source_type_mapping = {
            "rest_api": "rest_api",
            "python_sdk": "sdk",
            "postgresql": "database",
            "hana": "database",
        }

        return source_type_mapping.get(type_key, "")

    def _prepend_source_type_prefix(self, prefix: str, source_type: Optional[str]) -> str:
        """
        Prepend source type prefix to the given prefix.

        Args:
            prefix: Original prefix
            source_type: Source type (e.g., "rest_api", "python_sdk")

        Returns:
            str: Prefix with source type prepended (e.g., "rest_api/roundel_ads/accounts", "python_sdk/databricks/...")
        """
        source_type_prefix = self._get_source_type_prefix(source_type)

        if not source_type_prefix:
            return prefix

        # Clean both prefixes
        source_type_prefix = source_type_prefix.strip("/")
        clean_prefix = prefix.strip("/") if prefix else ""

        if clean_prefix:
            return f"{source_type_prefix}/{clean_prefix}"
        else:
            return source_type_prefix

    def _generate_delta_path(
        self, base_uri: str, prefix: str, source_type: Optional[str] = None
    ) -> str:
        """
        Generate Delta table path (directory-based, not file-based).

        Delta tables are stored as directories containing data files and metadata.
        This method returns the directory path where the Delta table will be stored.
        For Delta format, we use the prefix directly without adding the "data/" suffix.
        The source type prefix is prepended automatically.

        Args:
            base_uri: Filesystem base URI
            prefix: Key prefix
            source_type: Optional source type to prepend to the path

        Returns:
            str: Delta table directory path
        """
        # Prepend source type prefix if provided
        full_prefix = self._prepend_source_type_prefix(prefix, source_type)

        # For Delta, use prefix directly without "data/" suffix
        if not full_prefix:
            clean_prefix = ""
        else:
            clean_prefix = full_prefix.strip("/")
        # Delta tables are directories, so we return the directory path
        if clean_prefix:
            return self.object_store.resolve_path(base_uri, clean_prefix, trailing_slash=True)
        return self.object_store.resolve_path(base_uri, trailing_slash=True)

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

    def _optimize_dataframe(self, df: DataFrame) -> DataFrame:
        """
        Apply optimizations to the DataFrame before writing.

        Args:
            df: Input DataFrame

        Returns:
            DataFrame: Optimized DataFrame
        """
        # Coalesce to single file for consistent output
        return df.coalesce(1)

    @retry_on_s3_error()
    def _write_dataframe(self, df: DataFrame, path: str, write_options: Dict[str, Any]) -> None:
        """
        Write DataFrame to S3 with retry logic and optimizations.

        Args:
            df: DataFrame to write
            path: S3 path to write to
            write_options: Write options for the DataFrame (includes format, mode, compression, etc.)
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

        writer.save(path)

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

    @retry_on_s3_error()
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
        Load data to S3 using Spark.
        Handles both Spark DataFrames and lists of dictionaries.
        Supports both Delta (directory-based) and Parquet (file-based) formats.

        Args:
            data: Input data (DataFrame or list of dicts)
            config: Loading configuration
            schema: Optional schema for DataFrame creation
            source_type: Optional source type (e.g., "rest_api") to prepend to the path
            **kwargs: Additional arguments for specific formats

        Returns:
            str: S3 path or key where data was loaded

        Raises:
            LoaderError: If loading fails
        """
        if not self.spark:
            raise LoaderError("Spark session not set. Call set_spark_session first.")

        try:
            base_uri = loading_base_uri(
                destination=config.destination,
                bucket=config.bucket,
                storage_root=config.storage_root,
            )
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

        # Ensure we have a DataFrame
        df = self._ensure_dataframe(data, schema)

        # Branch based on format type
        if config.format == "delta":
            # Delta format: write directly to final directory location
            return self._load_delta(
                df, config, base_uri=base_uri, source_type=source_type, **kwargs
            )
        else:
            # Parquet or other file-based formats: use existing file-based logic
            return self._load_file_based(
                df, config, base_uri=base_uri, source_type=source_type, **kwargs
            )

    def _delta_table_exists(self, path: str) -> bool:
        """
        Check if a Delta table exists at the given path.

        Checks for the presence of the _delta_log directory, which is required
        for a valid Delta table.

        Args:
            path: URI path to the Delta table

        Returns:
            bool: True if Delta table exists, False otherwise
        """
        try:
            if DeltaTable is None:
                self.logger.warning(
                    "DeltaTable not available, cannot check if table exists",
                    extra_fields={"path": path},
                )
                return False

            store = self.object_store
            delta_log = store.resolve_path(path.rstrip("/"), "_delta_log")
            return store.exists(delta_log)

        except Exception as e:
            # If any error occurs, assume table doesn't exist
            self.logger.trace(
                "Error checking if Delta table exists, assuming it doesn't",
                extra_fields={"path": path, "error": str(e)},
            )
            return False

    def destination_exists(
        self,
        config: LoadingConfig,
        source_type: Optional[str] = None,
    ) -> bool:
        """
        Check if the Delta table destination already exists (for auto-backfill detection).

        Used to decide whether to run backfill on first write: when destination does
        not exist and backfill is configured, the pipeline uses backfill date ranges.

        Args:
            config: Loading configuration (bucket, prefix, destination, format).
            source_type: Optional source type to prepend to the path (e.g. rest_api).

        Returns:
            True if the Delta table exists at the configured path, False otherwise.
            Returns False if destination is not object-store backed, format is not delta,
            or required path fields are missing.
        """
        if config.destination not in ("s3", "local") or config.format != "delta":
            return False
        if config.destination == "s3" and (not config.bucket or not config.prefix):
            return False
        if config.destination == "local" and (not config.storage_root or not config.prefix):
            return False
        if not self.spark:
            return False
        try:
            base_uri = loading_base_uri(
                destination=config.destination,
                bucket=config.bucket,
                storage_root=config.storage_root,
            )
        except ValueError:
            return False
        path = self._generate_delta_path(
            base_uri=base_uri,
            prefix=config.prefix,
            source_type=source_type,
        )
        return self._delta_table_exists(path)

    def _perform_delta_merge(
        self, df: DataFrame, delta_path: str, merge_keys: List[str], config: LoadingConfig
    ) -> None:
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

        if not self.spark:
            raise LoaderError("Spark session not set. Call set_spark_session first.")

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

    def _sanitize_column_names(self, df: DataFrame) -> DataFrame:
        """
        Sanitize column names by handling illegal characters for Delta Lake.

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
                    "Sanitizing column names for Delta Lake",
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

    def _load_delta(
        self,
        df: DataFrame,
        config: LoadingConfig,
        *,
        base_uri: str,
        source_type: Optional[str] = None,
        **kwargs,
    ) -> str:
        """
        Load data as Delta table to S3 with support for multiple save modes.

        Delta tables are stored as directories, not single files. Supports three save modes:
        - **overwrite** (default): Replace all existing data in the table
        - **append**: Add new data without removing existing data
        - **merge**: Upsert on merge_keys; updates only columns present in both source
          and target, inserts fill missing target-only columns with typed NULL (see
          _perform_delta_merge). Append/overwrite still use mergeSchema for new source columns.

        Args:
            df: DataFrame to write
            config: Loading configuration (must include merge_keys for merge mode)
            source_type: Optional source type to prepend to the path
            **kwargs: Additional arguments for specific formats

        Returns:
            str: Delta table path

        Raises:
            LoaderError: If loading fails or configuration is invalid
        """
        try:
            # Handle duplicate column names by renaming them to unique names
            df = self._rename_duplicate_columns(df)

            self.logger.trace(
                "DataFrame after handling duplicate columns",
                extra_fields={"columns": df.columns},
            )

            # Sanitize column names to handle illegal characters for Delta Lake
            df = self._sanitize_column_names(df)

            self.logger.trace(
                "DataFrame after sanitizing column names",
                extra_fields={"columns": df.columns},
            )

            # Generate Delta table path (directory-based)
            # For Delta, we use the prefix directly without "data/" suffix
            # Source type prefix is automatically prepended
            final_path = self._generate_delta_path(
                base_uri=base_uri, prefix=config.prefix, source_type=source_type
            )

            self.logger.trace(
                "Writing Delta table",
                extra_fields={
                    "delta_path": final_path,
                    "write_mode": config.write_mode,
                    "has_merge_keys": config.merge_keys is not None,
                },
            )

            if config.force_nondeterministic_deduplication and config.write_mode == "merge":
                self.logger.warning("Forcing non-deterministic deduplication...")

                df = df.dropDuplicates(config.merge_keys)

                self.logger.info(f"Source rows after deduplication: {df.count()}")

            # Handle different write modes
            if config.write_mode == "merge":
                # Merge mode: Use Delta Lake MERGE operation for upsert
                # Check if table exists - if not, create it first using append mode
                if not self._delta_table_exists(final_path):
                    self.logger.debug(
                        "Delta table does not exist, creating it first",
                        extra_fields={"delta_path": final_path},
                    )
                    # Create table using append mode (first write)
                    # This ensures schema evolution is enabled
                    write_options = {
                        "format": "delta",
                        "mode": "append",
                        "mergeSchema": "true",  # Enable schema evolution
                    }
                    if config.compression:
                        write_options["compression"] = config.compression
                    self._write_dataframe(df, final_path, write_options)
                else:
                    # Perform merge operation
                    if config.merge_keys is None:
                        raise LoaderError("merge_keys must be provided when write_mode is 'merge'")

                    self._perform_delta_merge(df, final_path, config.merge_keys, config)

            elif config.write_mode == "append":
                # Append mode: Add new data without removing existing data
                # Schema evolution is enabled to allow new columns
                write_options = {
                    "format": "delta",
                    "mode": "append",
                    "mergeSchema": "true",  # Enable schema evolution
                    **kwargs.get("write_options", {}),
                }
                # Compression is handled differently for Delta
                # Delta uses Parquet files internally, so compression can be set
                if config.compression:
                    write_options["compression"] = config.compression

                # Write directly to final location (Delta manages its own files)
                self._write_dataframe(df, final_path, write_options)

            else:
                # Overwrite mode (default) or other modes: Use standard write
                # Schema evolution is enabled to allow new columns
                write_options = {
                    "format": "delta",
                    "mode": config.write_mode,
                    "mergeSchema": "true",  # Enable schema evolution
                    **kwargs.get("write_options", {}),
                }
                # Compression is handled differently for Delta
                # Delta uses Parquet files internally, so compression can be set
                if config.compression:
                    write_options["compression"] = config.compression

                # Write directly to final location (Delta manages its own files)
                self._write_dataframe(df, final_path, write_options)

            self.logger.info(
                "Successfully loaded Delta table",
                extra_fields={
                    "destination": final_path,
                    "write_mode": config.write_mode,
                    "merge_keys": config.merge_keys if config.write_mode == "merge" else None,
                },
            )

            return final_path

        except Exception as e:
            error_msg = f"Failed to load Delta table: {e!s}"
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
        Load data as file-based format (e.g., Parquet) to S3.
        Uses temporary file + move pattern for atomic writes.

        Args:
            df: DataFrame to write
            config: Loading configuration
            source_type: Optional source type to prepend to the path
            **kwargs: Additional arguments for specific formats

        Returns:
            str: S3 key where data was loaded
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
