"""
Streaming collector for memory-efficient data accumulation.
"""

import gc
import os
from typing import Any, Dict, List, Optional, Union

import psutil
from pyspark.sql import DataFrame, SparkSession

from src.collector.base_collector import RawDataBatch
from src.parser.spark_parser import SparkParser
from src.planner.execution_plan import ResourceMetadata
from src.utils.logger import get_logger
from src.utils.redis_context import RedisContextManager


class StreamingRawDataCollector:
    """Memory-efficient collector with periodic Redis flushes."""

    def __init__(
        self,
        redis_context: RedisContextManager,
        resource_key: str,
        flush_threshold: int,
        spark: SparkSession,
        resource_meta: ResourceMetadata,
        service: Any = None,
        execution_plan: Any = None,
    ):
        """
        Initialize streaming collector.

        Args:
            redis_context: Redis context manager for storing intermediate data
            resource_key: Redis key prefix for this resource
            flush_threshold: Number of batches to accumulate before flushing
            spark: SparkSession for DataFrame operations
            resource_meta: Resource metadata for parsing
            service: Service instance for parsing context
            execution_plan: Execution plan for parsing context
        """

        self.batches: List[RawDataBatch] = []
        self.redis_context = redis_context
        self.resource_key = resource_key
        self.flush_threshold = flush_threshold
        self.flush_count = 0
        self.total_parsed_df = None
        self.spark = spark
        self.resource_meta = resource_meta
        self.service = service
        self.execution_plan = execution_plan
        self.logger = get_logger(self.__class__.__name__)
        self.request_context: Optional[Dict[str, Any]] = None
        self.parser = self._create_parser()

        self.logger.debug(
            "Initialized StreamingRawDataCollector",
            extra_fields={
                "resource_key": self.resource_key,
                "flush_threshold": self.flush_threshold,
            },
        )

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

    def add_batch(self, batch: RawDataBatch) -> None:
        """Add batch and flush if threshold reached."""
        self.batches.append(batch)

        # Store request context from first batch for transformations
        if self.request_context is None:
            self.request_context = batch.request_context

        # Flush when threshold reached
        if len(self.batches) >= self.flush_threshold:
            self._flush_to_redis()

    def _flush_to_redis(self) -> None:
        """Parse and flush current batches to Redis."""
        if not self.batches:
            return

        self.logger.debug(
            f"Flushing {len(self.batches)} batches to Redis",
            extra_fields={"flush_count": self.flush_count, "resource_key": self.resource_key},
        )

        # Parse current batches
        current_df = self._parse_batches()

        if current_df is not None and current_df.count() > 0:
            # Merge with accumulated data
            if self.total_parsed_df is None:
                self.total_parsed_df = current_df
            else:
                # Create new DataFrame and release old reference
                old_df = self.total_parsed_df
                # Unpersist old DataFrame if it was cached
                try:
                    old_df.unpersist(blocking=False)
                except Exception:
                    pass  # DataFrame may not be cached

                self.total_parsed_df = old_df.unionByName(current_df, allowMissingColumns=True)
                # Force garbage collection of old DataFrame
                del old_df

            # Unpersist current_df after merging (no longer needed)
            try:
                current_df.unpersist(blocking=False)
            except Exception:
                pass  # DataFrame may not be cached

            # Store intermediate result in Redis
            temp_key = f"{self.resource_key}:temp:{self.flush_count}"
            self.redis_context.store(temp_key, self.total_parsed_df)

            # Get count for logging (do this before potentially unpersisting)
            record_count = self.total_parsed_df.count()

            self.logger.trace(
                "Stored intermediate data in Redis",
                extra_fields={"temp_key": temp_key, "record_count": record_count},
            )

            # Force garbage collection after Redis storage
            gc.collect()

            # Log memory usage for monitoring
            try:
                process = psutil.Process(os.getpid())
                memory_mb = process.memory_info().rss / 1024 / 1024
                self.logger.debug(
                    f"Memory after flush: {memory_mb:.2f} MB",
                    extra_fields={
                        "flush_count": self.flush_count,
                        "memory_mb": round(memory_mb, 2),
                    },
                )
            except ImportError:
                pass  # psutil not available

        # Clear memory
        self.batches.clear()
        self.flush_count += 1

    def _parse_batches(self) -> Optional[DataFrame]:
        """Parse current batches into DataFrame."""
        if not self.batches:
            return None

        try:
            # Parse each batch and combine
            parsed_dfs = []
            for batch in self.batches:
                batch_df = self._parse_data(
                    raw_data=batch.raw_data,
                    resource_meta=self.resource_meta,
                    parent_context=batch.parent_context,
                    request_context=batch.request_context,
                )
                if batch_df is not None:
                    parsed_dfs.append(batch_df)

            if not parsed_dfs:
                return None

            # Combine all parsed DataFrames
            if len(parsed_dfs) == 1:
                return parsed_dfs[0]
            else:
                result_df = parsed_dfs[0]
                for df in parsed_dfs[1:]:
                    result_df = result_df.unionByName(df, allowMissingColumns=True)
                return result_df

        except Exception as e:
            self.logger.error(
                "Failed to parse batches during flush",
                extra_fields={"batch_count": len(self.batches), "error": str(e)},
            )
            raise

    def _parse_data(
        self,
        raw_data: Union[List[Dict[str, Any]], Dict[str, Any]],
        resource_meta: Any,
        parent_context: Optional[Dict[str, Any]] = None,
        request_context: Optional[Dict[str, Any]] = None,
    ) -> Optional[DataFrame]:
        """
        Parse raw data into DataFrame.

        Args:
            raw_data: Raw data to parse
            resource_meta: Resource metadata
            parent_context: Parent context for nested data
            request_context: Request context (used for add_column_from_request per batch)

        Returns:
            Optional[DataFrame]: Parsed DataFrame or None if no data

        Raises:
            Exception: If parsing fails
        """
        try:
            if not raw_data:
                return None

            data_df = self.parser.parse(raw_data, parent_context, request_context=request_context)

            # Create empty DataFrame with correct schema if no data
            if data_df is None:
                schema = self.parser._build_target_schema(parent_context)
                data_df = self.spark.createDataFrame([], schema)

            return data_df

        except Exception as e:
            self.logger.error(
                "Failed to parse data during streaming",
                extra_fields={"error": str(e), "resource_name": resource_meta.resource_name},
            )
            raise

    def finalize(self) -> Optional[DataFrame]:
        """Final flush and return complete dataset."""
        if self.batches:  # Flush remaining batches
            self._flush_to_redis()

        if self.total_parsed_df is None:
            return None

        # Clean up temp keys and return final result
        final_df = self._consolidate_temp_data()

        # Unpersist total_parsed_df if it exists and was cached
        if self.total_parsed_df is not None:
            try:
                self.total_parsed_df.unpersist(blocking=False)
            except Exception:
                pass  # DataFrame may not be cached

        # Clean up temp Redis keys
        self._cleanup_temp_keys()

        # Clear accumulated DataFrame reference
        self.total_parsed_df = None

        # Force garbage collection
        gc.collect()

        return final_df

    def _consolidate_temp_data(self) -> DataFrame | None:
        """Consolidate all temp data from Redis into final DataFrame."""
        if self.flush_count == 0:
            return self.total_parsed_df

        # Get the latest temp data (which contains all accumulated data)
        temp_key = f"{self.resource_key}:temp:{self.flush_count - 1}"
        final_df = self.redis_context.get(temp_key, spark=self.spark)

        if final_df is None:
            self.logger.warning(
                "No temp data found for final consolidation", extra_fields={"temp_key": temp_key}
            )
            return self.total_parsed_df

        return final_df

    def _cleanup_temp_keys(self) -> None:
        """Clean up temporary Redis keys."""
        try:
            for i in range(self.flush_count):
                temp_key = f"{self.resource_key}:temp:{i}"
                self.redis_context.delete(temp_key)

            self.logger.debug(
                f"Cleaned up {self.flush_count} temp Redis keys",
                extra_fields={"resource_key": self.resource_key},
            )
        except Exception as e:
            self.logger.warning(
                "Failed to cleanup temp Redis keys",
                extra_fields={"error": str(e), "flush_count": self.flush_count},
            )

    def is_empty(self) -> bool:
        """Check if collector is empty."""
        return len(self.batches) == 0 and self.total_parsed_df is None
