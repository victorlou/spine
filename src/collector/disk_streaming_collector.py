"""
Disk-based streaming collector for memory-efficient data accumulation using NDJSON format.
"""

import gc
import glob
import json
import os
import shutil
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional

import psutil
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StringType, StructField, StructType

from src.collector.base_collector import RawDataBatch
from src.config.config_models import TransformationType
from src.parser.spark_parser import SparkParser
from src.planner.execution_plan import ResourceMetadata
from src.utils.logger import get_logger
from src.utils.redis_context import RedisContextManager


class DiskStreamingDataCollector:
    """
    Disk-based streaming collector for memory-efficient data accumulation.

    Flow:
    1. add_batch() → Parse data (extract schema + data, NO DataFrame) → Write NDJSON to disk
    2. Check file size → Rotate if needed
    3. finalize() → Read all files → Parse into DataFrames → Consolidate
    """

    def __init__(
        self,
        disk_path: str,
        resource_key: str,
        file_size_threshold: int,
        spark: SparkSession,
        redis_context: RedisContextManager,
        resource_meta: ResourceMetadata,
        service: Any,
        execution_plan: Any,
    ):
        """
        Initialize disk-based streaming data collector.

        Args:
            disk_path: Base directory for storing NDJSON files
            resource_key: Unique key for this resource (used in filenames)
            file_size_threshold: Max file size before rotation (bytes)
            spark: SparkSession for DataFrame operations
            redis_context: Redis context manager for storing intermediate data
            resource_meta: Resource metadata for parsing
            service: Service instance for parsing context
            execution_plan: Execution plan for parsing context
        """
        # Initialize core attributes
        self.disk_path = Path(disk_path).absolute().resolve()
        self.resource_key = resource_key
        self.file_size_threshold = file_size_threshold
        self.spark = spark
        self.redis_context = redis_context
        self.resource_meta = resource_meta
        self.service = service
        self.execution_plan = execution_plan
        self.logger = get_logger(self.__class__.__name__)
        self.request_context: Optional[Dict[str, Any]] = None

        # Schema tracking for this resource
        self.schema: Optional[StructType] = None

        # File management
        # Generate unique bounce ID to prevent concurrent collectors from reading each other's files
        self.bounce_id = str(uuid.uuid4())[:8]
        self.current_file_path: Optional[str] = None
        self.file_counter = 0
        self.total_records_written = 0

        # Cleanup tracking flag
        self.cleaned_up = False

        self.logger.debug(
            "Initialized DiskStreamingDataCollector",
            extra_fields={
                "resource_key": self.resource_key,
                "bounce_id": self.bounce_id,
                "disk_path": self.disk_path,
                "file_size_threshold": self.file_size_threshold,
            },
        )

        # Initialize disk path and create first file
        self._initialize_disk_path()
        self._create_new_file()

        # Initialize parser
        self.parser = self._create_parser()

    def _create_parser(self) -> SparkParser:
        """
        Create a SparkParser instance.

        Returns:
            SparkParser: Initialized parser instance

        Raises:
            ValueError: If parser creation fails
        """
        try:
            return SparkParser(
                config=self.resource_meta.config,
                spark=self.spark,
                source_name=self.service.source_name,
                resource_name=self.resource_meta.resource_name,
                execution_plan=self.execution_plan,
                redis_context=self.redis_context,
            )
        except Exception as e:
            self.logger.error(
                "Failed to create SparkParser",
                extra_fields={"error": str(e), "resource_name": self.resource_meta.resource_name},
            )
            raise

    def _initialize_disk_path(self) -> None:
        """Create disk path if it doesn't exist."""
        try:
            os.makedirs(self.disk_path, exist_ok=True)
        except Exception as e:
            self.logger.error(
                "Failed to initialize disk path",
                extra_fields={"error": str(e), "disk_path": self.disk_path},
            )
            raise

    def _create_new_file(self) -> None:
        """Create a new NDJSON file for writing."""
        filename = f"{self.resource_key}_{self.bounce_id}_{self.file_counter:04d}.ndjson"
        self.current_file_path = os.path.join(self.disk_path, filename)
        self.file_counter += 1

        self.logger.trace(
            "Created new NDJSON file",
            extra_fields={
                "file_path": self.current_file_path,
                "file_counter": self.file_counter - 1,
                "bounce_id": self.bounce_id,
            },
        )

    def _merge_schemas(self, *schemas: StructType) -> StructType:
        fields = {}

        for schema in schemas:
            for f in schema.fields:
                if f.name in fields:
                    old = fields[f.name]
                    fields[f.name] = StructField(
                        f.name,
                        f.dataType,
                        old.nullable or f.nullable,
                        f.metadata,  # safer
                    )
                else:
                    fields[f.name] = f

        return StructType(list(fields.values()))

    def add_batch(self, batch: RawDataBatch) -> None:
        """
        Add batch: Parse data and write to disk as NDJSON.

        This replaces the _flush_to_redis() logic from StreamingRawDataCollector.
        Instead of creating DataFrames and storing in Redis, we:
        1. Parse batch to extract records and schema (NO DataFrame creation)
        2. Write parse result (schema + records) to disk
        3. Check if file needs rotation

        Args:
            batch: RawDataBatch containing raw_data, parent_context, request_context
        """
        # Store request context from first batch for transformations
        if self.request_context is None:
            self.request_context = batch.request_context

        # Parse batch data to records (extract schema + data, NO DataFrame)
        parse_result = self._parse_batch_to_records(batch)

        # Enrich records with add_column_from_request columns from this batch's request context
        parse_result = self._enrich_records_with_request_columns(
            parse_result, batch.request_context
        )

        records = parse_result.get("records", [])
        if not records:
            self.logger.trace(
                "No records parsed from batch", extra_fields={"resource_name": self.resource_meta}
            )
            return

        # Set the schema if not already set and update if schema is already set
        schema = parse_result.get("schema")
        if schema and self.schema is None and isinstance(schema, StructType):
            self.schema = schema
        elif schema and self.schema is not None and isinstance(schema, StructType):
            self.schema = self._merge_schemas(self.schema, schema)

        # Write parse result (schema + records) to disk
        self._write_parse_result_to_disk(parse_result)

        # Check if file needs rotation
        if self._should_rotate_file():
            self._rotate_file()

    def _parse_batch_to_records(
        self, batch: RawDataBatch
    ) -> Dict[str, Optional[StructType] | List[Any]]:
        """
        Parse batch data into records WITHOUT creating DataFrame.

        Uses SparkParser.parse_to_records() to extract records and schema directly
        from raw data, avoiding DataFrame materialization overhead.

        Args:
            batch: RawDataBatch to parse

        Returns:
            Dict[str, Optional[StructType] | List[Any]]: Dictionary containing:
                - "schema": StructType representing the target schema
                - "records": List[Any] of parsed records
        """
        try:
            if not batch.raw_data or not self.resource_meta or not self.service:
                return {"schema": None, "records": []}

            # Parse to records directly (no DataFrame creation)
            parse_result = self.parser.parse_to_records(batch.raw_data, batch.parent_context)

            records: List[Any] = parse_result.get("records", [])

            if not records:
                self.logger.trace(
                    "No data returned from parser",
                    extra_fields={
                        "resource_name": (
                            self.resource_meta.resource_name if self.resource_meta else "unknown"
                        )
                    },
                )
                return {"schema": None, "records": []}

            self.logger.trace(
                f"Parsed batch to {len(records)} records",
                extra_fields={
                    "record_count": len(records),
                    "resource_name": (
                        self.resource_meta.resource_name if self.resource_meta else "unknown"
                    ),
                },
            )

            return parse_result

        except Exception as e:
            self.logger.error(
                "Failed to parse batch to records",
                extra_fields={
                    "error": str(e),
                    "resource_name": (
                        self.resource_meta.resource_name if self.resource_meta else "unknown"
                    ),
                },
            )
            raise

    def _enrich_records_with_request_columns(
        self,
        parse_result: Dict[str, Optional[StructType] | List[Any]],
        request_context: Optional[Dict[str, Any]],
    ) -> Dict[str, Optional[StructType] | List[Any]]:
        """
        Add add_column_from_request columns to each record and extend schema.

        Ensures each batch's rows get the correct request-derived values (e.g. _advertiser_id)
        so that after finalize the consolidated DataFrame has the right value per row.
        """
        records = parse_result.get("records", [])
        schema = parse_result.get("schema")
        if not records or not self.resource_meta.config.transformations:
            return parse_result
        request_context = request_context or {}
        new_fields: List[StructField] = []
        for transform in self.resource_meta.config.transformations:
            if transform.type != TransformationType.ADD_COLUMN_FROM_REQUEST:
                continue
            value = self.parser._get_request_value(
                source=transform.source,
                location=transform.location or "parameters",
                data_type=transform.data_type or "string",
                request_context=request_context,
            )
            str_value = str(value) if value is not None else ""
            for record in records:
                if isinstance(record, dict):
                    record[transform.name] = str_value
            if schema and not any(f.name == transform.name for f in schema.fields):
                new_fields.append(StructField(transform.name, StringType(), False))
        if new_fields and schema:
            parse_result["schema"] = StructType(list(schema.fields) + new_fields)
        parse_result["records"] = records
        return parse_result

    def _write_parse_result_to_disk(self, parse_result: Dict[str, Any]) -> None:
        """
        Write parse result (records) to disk in NDJSON format.

        Format: Each line contains a batch of records
        Multiple batches can be written to the same file (one per line).
        Schema is tracked separately in self.schema, so only records are written.

        Args:
            parse_result: Dictionary containing:
                - "records": List[Dict[str, Any]] of parsed records
        """
        try:
            if not self.current_file_path:
                raise ValueError("Current file path is not initialized")

            records = parse_result.get("records", [])

            if not records:
                return

            with open(self.current_file_path, "a", encoding="utf-8") as f:
                for record in records:
                    json.dump(record, f, default=str)
                    f.write("\n")

                # Force flush to disk (atomic write)
                f.flush()
                os.fsync(
                    f.fileno()
                )  # this function will ensure data is written to disk and not just cached in OS buffers

            self.total_records_written += len(records)

            self.logger.trace(
                "Wrote parse result to disk",
                extra_fields={
                    "record_count": len(records),
                    "file_path": self.current_file_path,
                    "total_records": self.total_records_written,
                },
            )

        except Exception as e:
            self.logger.error(
                "Failed to write parse result to disk",
                extra_fields={
                    "error": str(e),
                    "file": self.current_file_path,
                    "record_count": len(parse_result.get("records", [])),
                },
            )
            raise

    def _should_rotate_file(self) -> bool:
        """Check if current file exceeds size threshold."""
        try:
            if not self.current_file_path or not os.path.exists(self.current_file_path):
                return False

            file_size = os.path.getsize(self.current_file_path)
            should_rotate = file_size > self.file_size_threshold

            if should_rotate:
                self.logger.trace(
                    "File size threshold exceeded",
                    extra_fields={
                        "file_size": file_size,
                        "threshold": self.file_size_threshold,
                        "file_path": self.current_file_path,
                    },
                )

            return should_rotate

        except Exception as e:
            self.logger.warning(
                "Failed to check file size",
                extra_fields={"error": str(e), "file": self.current_file_path},
            )
            return False

    def _rotate_file(self) -> None:
        """
        Rotate to a new file when threshold is exceeded.

        This is called after writing records, so the current file is complete
        and ready for parsing during finalization.
        """
        try:
            if not self.current_file_path:
                raise ValueError("Current file path is not initialized")

            old_file = self.current_file_path
            file_size = os.path.getsize(old_file)  # type: ignore

            self.logger.trace(
                "Rotating NDJSON file",
                extra_fields={
                    "old_file": old_file,
                    "file_size": file_size,
                    "threshold": self.file_size_threshold,
                },
            )

            # Create new file for subsequent writes
            self._create_new_file()

        except Exception as e:
            self.logger.error("Failed to rotate file", extra_fields={"error": str(e)})
            raise

    def finalize(self) -> Optional[DataFrame]:
        """
        Finalize: Read all NDJSON files and consolidate into single DataFrame.

        This replaces the _consolidate_temp_data() logic from StreamingRawDataCollector.

        Process:
        1. Get all NDJSON files for this resource
        2. Parse each file sequentially into DataFrames
        3. Union all DataFrames
        4. Cleanup NDJSON files after successful consolidation

        Returns:
            Optional[DataFrame]: Consolidated DataFrame or None if no data
        """
        ndjson_files = []
        try:
            # Get all NDJSON files for this resource
            ndjson_files = self._get_all_ndjson_files()

            if not ndjson_files:
                self.logger.warning(
                    "No NDJSON files found for consolidation",
                    extra_fields={"resource_key": self.resource_key},
                )
                return None

            self.logger.trace(
                "Consolidating NDJSON files",
                extra_fields={
                    "file_count": len(ndjson_files),
                    "files": ndjson_files,
                    "total_records_written": self.total_records_written,
                },
            )

            pattern = os.path.join(self.disk_path, f"{self.resource_key}_{self.bounce_id}_*.ndjson")

            # Parse each file sequentially and consolidate
            all_data_df: DataFrame = self.spark.read.json(
                path=pattern, schema=self.schema if self.schema else None, multiLine=False
            )

            # Persist consolidated DataFrame, so we can delete source files
            if not all_data_df.rdd.isEmpty():
                all_data_df = all_data_df.persist()

            # Log consolidation result
            if all_data_df is not None:
                record_count = all_data_df.count()
                self.logger.debug(
                    f"Consolidated {len(ndjson_files)} files into single DataFrame",
                    extra_fields={"file_count": len(ndjson_files), "record_count": record_count},
                )

            # Log memory usage for monitoring
            try:
                process = psutil.Process(os.getpid())
                memory_mb = process.memory_info().rss / 1024 / 1024
                self.logger.debug(
                    f"Memory after finalization: {memory_mb:.2f} MB",
                    extra_fields={"memory_mb": round(memory_mb, 2)},
                )
            except ImportError:
                pass  # psutil not available

            # Force garbage collection
            gc.collect()

            return all_data_df

        except Exception as e:
            self.logger.error(
                "Failed to finalize disk collector",
                extra_fields={"error": str(e), "resource_key": self.resource_key},
            )
            raise
        finally:
            # Ensure NDJSON files and disk path are cleaned up even if an error occurs
            if ndjson_files:
                try:
                    self._cleanup_ndjson_files(ndjson_files)
                except Exception as cleanup_error:
                    self.logger.error(
                        "Failed to cleanup NDJSON files in finally block",
                        extra_fields={
                            "error": str(cleanup_error),
                            "file_count": len(ndjson_files),
                            "resource_key": self.resource_key,
                        },
                    )

            # Clean up the disk path directory
            try:
                self.cleanup_disk_path()
            except Exception as cleanup_error:
                self.logger.error(
                    "Failed to cleanup disk path in finally block",
                    extra_fields={
                        "error": str(cleanup_error),
                        "disk_path": str(self.disk_path),
                        "resource_key": self.resource_key,
                    },
                )

    def _get_all_ndjson_files(self) -> List[str]:
        """
        Get all NDJSON files for this collector instance, sorted by creation order.

        Only returns files matching this collector's bounce_id to prevent reading
        files from concurrent collectors.

        Returns:
            List[str]: Sorted list of file paths matching this collector's bounce_id
        """
        try:
            # Pattern includes bounce_id to ensure we only read our own files
            pattern = os.path.join(self.disk_path, f"{self.resource_key}_{self.bounce_id}_*.ndjson")
            files = sorted(glob.glob(pattern))

            self.logger.trace(
                "Found NDJSON files",
                extra_fields={
                    "file_count": len(files),
                    "pattern": pattern,
                    "bounce_id": self.bounce_id,
                },
            )

            return files

        except Exception as e:
            self.logger.error(
                "Failed to get NDJSON files",
                extra_fields={
                    "error": str(e),
                    "disk_path": self.disk_path,
                    "resource_key": self.resource_key,
                    "bounce_id": self.bounce_id,
                },
            )
            return []

    def _parse_ndjson_file(self, file_path: str) -> Optional[DataFrame]:
        """
        Parse a single NDJSON file into a DataFrame.

        Each line in the file contains: {schema: {...}, records: [{...}, {...}]}
        Multiple batches can exist in the same file (one per line).

        Args:
            file_path: Path to NDJSON file

        Returns:
            Optional[DataFrame]: Parsed DataFrame or None if empty
        """
        try:
            if not os.path.exists(file_path):
                self.logger.warning("NDJSON file not found", extra_fields={"file_path": file_path})
                return None

            file_size = os.path.getsize(file_path)
            if file_size == 0:
                self.logger.trace("NDJSON file is empty", extra_fields={"file_path": file_path})
                return None

            # Read NDJSON file line by line and reconstruct DataFrames
            all_records = []
            schema_obj = None
            batch_count = 0

            with open(file_path, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue

                    try:
                        batch_data = json.loads(line)
                        batch_count += 1

                        # Extract schema from first batch
                        if schema_obj is None and batch_data.get("schema"):
                            schema_dict = batch_data["schema"]
                            # Reconstruct StructType from jsonValue format
                            schema_obj = StructType.fromJson(schema_dict)

                        # Extract records from this batch
                        batch_records = batch_data.get("records", [])
                        all_records.extend(batch_records)

                    except json.JSONDecodeError as e:
                        self.logger.warning(
                            "Failed to parse batch line",
                            extra_fields={
                                "error": str(e),
                                "file": file_path,
                                "line_preview": line[:100],
                            },
                        )
                        continue

            if not all_records:
                self.logger.trace(
                    "No records found in NDJSON file",
                    extra_fields={"file_path": file_path, "batch_count": batch_count},
                )
                return None

            # Create DataFrame with reconstructed schema
            if schema_obj is None:
                self.logger.warning(
                    "No schema found in NDJSON file, inferring from data",
                    extra_fields={"file_path": file_path},
                )
                df = self.spark.createDataFrame(all_records)
            else:
                df = self.spark.createDataFrame(all_records, schema=schema_obj)

            record_count = df.count()

            self.logger.trace(
                "Parsed NDJSON file",
                extra_fields={
                    "file_path": file_path,
                    "record_count": record_count,
                    "batch_count": batch_count,
                    "file_size": file_size,
                },
            )

            return df

        except Exception as e:
            self.logger.error(
                "Failed to parse NDJSON file", extra_fields={"error": str(e), "file": file_path}
            )
            raise

    def _cleanup_ndjson_files(self, file_paths: List[str]) -> None:
        """
        Clean up NDJSON files after successful consolidation.

        Only called after successful finalization to ensure data is not lost.

        Args:
            file_paths: List of file paths to delete
        """
        try:
            deleted_count = 0
            for file_path in file_paths:
                if os.path.exists(file_path):
                    os.remove(file_path)
                    deleted_count += 1

            self.logger.debug(
                "Cleaned up NDJSON files",
                extra_fields={
                    "deleted_count": deleted_count,
                    "total_count": len(file_paths),
                    "resource_key": self.resource_key,
                },
            )

            # Remove the directory
            if os.path.exists(self.disk_path):
                shutil.rmtree(self.disk_path)
                self.logger.debug(
                    "Removed disk path directory after file cleanup",
                    extra_fields={"disk_path": str(self.disk_path)},
                )

            # Mark cleanup as successful
            self.cleaned_up = True

        except Exception as e:
            self.logger.warning(
                "Failed to cleanup NDJSON files",
                extra_fields={"error": str(e), "file_count": len(file_paths)},
            )

    def cleanup_disk_path(self) -> None:
        """
        Clean up all files and the disk path directory.

        This method removes all NDJSON files for this collector instance
        and then removes the disk path directory itself.

        Called during graceful exit or error handling to ensure no orphaned files remain.
        """
        try:
            if os.path.exists(self.disk_path):
                shutil.rmtree(self.disk_path)
                self.logger.debug(
                    "Removed disk path directory and all files",
                    extra_fields={"disk_path": str(self.disk_path)},
                )
                # Mark cleanup as successful
                self.cleaned_up = True
        except Exception as e:
            self.logger.error(
                "Failed to cleanup disk path",
                extra_fields={
                    "error": str(e),
                    "disk_path": str(self.disk_path),
                    "resource_key": self.resource_key,
                },
            )

    def is_empty(self) -> bool:
        """
        Check if collector has any data.

        Returns:
            bool: True if no NDJSON files exist, False otherwise
        """
        ndjson_files = self._get_all_ndjson_files()

        # Filter out empty files
        non_empty_files = [f for f in ndjson_files if os.path.exists(f) and os.path.getsize(f) > 0]

        return len(non_empty_files) == 0
