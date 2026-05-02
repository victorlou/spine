"""
Loader for object storage destinations (S3, GCS, Azure Blob, local) using Spark.
"""

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
from src.utils.path_prefix import prepend_source_type_prefix
from src.utils.transient_storage_retry import retry_on_transient_storage_error


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

    @retry_on_transient_storage_error()
    def _write_dataframe(
        self,
        df: DataFrame,
        path: str,
        write_options: Dict[str, Any],
    ) -> None:
        """
        Write a DataFrame to an object-storage path for file-based formats.

        Table formats use `src.load_strategy` implementations so Delta/Iceberg
        behavior stays behind the table-format strategy boundary.

        Args:
            df: DataFrame to write
            path: Resolved destination path
            write_options: Write options for the DataFrame (includes format, mode, compression, etc.)
        """
        # Extract format and mode from write_options (create copy to avoid mutating original)
        options_copy = write_options.copy()
        format_type = options_copy.pop("format", "parquet")
        write_mode = options_copy.pop("mode", "overwrite")

        # Apply optimizations before writing
        optimized_df = df.coalesce(1)

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
        return load_strategy.write(df, **kwargs)

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
            prefixed_prefix = prepend_source_type_prefix(config.prefix or "data", source_type)

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
