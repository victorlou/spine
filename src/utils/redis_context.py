"""
Redis-based context manager for storing and retrieving data.
"""

import json
from typing import Any, Dict, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType
from redis import Redis

from src.utils.logger import get_logger


class ContextError(Exception):
    """Base exception for context-related errors."""

    pass


class RedisContextManager:
    """Manages context data using Redis."""

    def __init__(self, redis_config: Dict[str, Any], prefix: str = "", default_ttl: int = 3600):
        """
        Initialize Redis context manager.

        Args:
            redis_config: Redis connection configuration
            prefix: Key prefix for Redis keys
            default_ttl: Default TTL for stored data in seconds
        """
        self.redis_config = redis_config
        self.prefix = prefix
        self.default_ttl = default_ttl
        self.client = self._create_client()
        self.logger = get_logger(self.__class__.__name__)

    def _create_client(self) -> Redis:
        """Create Redis client with retry logic."""
        try:
            client = Redis(
                host=self.redis_config.get("host", "localhost"),
                port=self.redis_config.get("port", 6379),
                db=self.redis_config.get("db", 0),
                password=self.redis_config.get("password"),
                ssl=self.redis_config.get("ssl", False),
                socket_timeout=self.redis_config.get("socket_timeout", 5),
                socket_connect_timeout=self.redis_config.get("socket_connect_timeout", 5),
                retry_on_timeout=True,
                decode_responses=False,  # We need bytes for storing raw data
            )
            return client
        except Exception as e:
            raise ContextError(f"Failed to create Redis client: {e!s}") from e

    def _format_key(self, key: str) -> str:
        """Format a key with the configured prefix."""
        return f"{self.prefix}{key}" if self.prefix else key

    def _serialize_data(self, data: Any) -> bytes:
        """
        Serialize data for storage in Redis.

        Args:
            data: Data to serialize

        Returns:
            bytes: Serialized data

        Raises:
            ContextError: If serialization fails
        """
        try:
            if isinstance(data, DataFrame):
                # Convert DataFrame to list of dicts
                raw_data = data.toJSON().collect()
                # Store as JSON string
                return json.dumps(
                    {"type": "spark_dataframe", "schema": data.schema.json(), "data": raw_data}
                ).encode("utf-8")
            else:
                return json.dumps({"type": "raw", "data": data}).encode("utf-8")
        except Exception as e:
            raise ContextError(f"Failed to serialize data: {e!s}") from e

    def _deserialize_data(self, data: bytes, spark: Optional[SparkSession] = None) -> Any:
        """
        Deserialize data from Redis.

        Args:
            data: Data to deserialize
            spark: Optional SparkSession for DataFrame reconstruction

        Returns:
            Any: Deserialized data

        Raises:
            ContextError: If deserialization fails
        """
        try:
            if not data:
                return None

            decoded = json.loads(data.decode("utf-8"))
            data_type = decoded.get("type", "raw")

            if data_type == "spark_dataframe":
                if not spark:
                    raise ContextError("SparkSession required to deserialize DataFrame")

                # Recreate DataFrame from schema and data
                schema = StructType.fromJson(json.loads(decoded["schema"]))
                raw_data = decoded["data"]

                # Create RDD from raw data
                rdd = spark.sparkContext.parallelize(raw_data)
                # Parse JSON strings back to rows
                row_rdd = rdd.map(lambda x: json.loads(x))

                # Create DataFrame
                return spark.createDataFrame(row_rdd, schema)
            else:
                return decoded["data"]

        except Exception as e:
            raise ContextError(f"Failed to deserialize data: {e!s}") from e

    def _get_data_preview(self, data: Any, max_length: int = 200) -> Dict[str, Any]:
        """
        Get a preview of data for logging.

        Args:
            data: Data to preview
            max_length: Maximum length of preview

        Returns:
            Dict[str, Any]: Preview of data with type information
        """
        try:
            if isinstance(data, DataFrame):
                # For DataFrames, show schema and first few rows
                return {
                    "data_type": "DataFrame",
                    "schema": str(data.schema)[:100],
                    "sample_rows": [row.asDict() for row in data.limit(2).collect()],
                    "total_rows": data.count(),
                }
            elif isinstance(data, (dict, list)):
                # For dict/list, use JSON representation
                preview = json.dumps(data, default=str)[:max_length]
                return {
                    "data_type": type(data).__name__,
                    "length": len(data),
                    "preview": f"{preview}..." if len(preview) >= max_length else preview,
                }
            else:
                # For other types, use string representation
                str_val = str(data)
                return {
                    "data_type": type(data).__name__,
                    "preview": (
                        f"{str_val[:max_length]}..." if len(str_val) > max_length else str_val
                    ),
                }
        except Exception as e:
            return {"data_type": type(data).__name__, "error": f"Error getting preview: {e!s}"}

    def store(
        self,
        key: str,
        data: Any,
        metadata: Optional[Dict[str, Any]] = None,
        ttl: Optional[int] = None,
    ) -> None:
        """
        Store data in Redis with optional metadata.

        Args:
            key: Key to store data under
            data: Data to store
            metadata: Optional metadata to store with the data
            ttl: Optional TTL override

        Raises:
            ContextError: If storage fails
        """
        try:
            formatted_key = self._format_key(key)
            serialized = self._serialize_data(data)

            # Log data preview before storage at TRACE level (raw data structures)
            self.logger.trace(
                f"Storing data in Redis: {formatted_key}",
                extra_fields={
                    "operation": "store",
                    "key": formatted_key,
                    "data_info": self._get_data_preview(data),
                    "metadata": metadata,
                    "ttl": ttl if ttl is not None else self.default_ttl,
                },
            )

            # Store data
            self.client.set(
                formatted_key, serialized, ex=ttl if ttl is not None else self.default_ttl
            )

            # Store metadata if provided
            if metadata:
                metadata_key = f"{formatted_key}:metadata"
                self.client.set(
                    metadata_key,
                    json.dumps(metadata).encode("utf-8"),
                    ex=ttl if ttl is not None else self.default_ttl,
                )

            # Log successful storage at DEBUG level (operation status)
            self.logger.debug(
                "Successfully stored data in Redis",
                extra_fields={"key": formatted_key, "has_metadata": bool(metadata)},
            )

        except Exception as e:
            raise ContextError(f"Failed to store data: {e!s}") from e

    def get(self, key: str, spark: Optional[SparkSession] = None) -> Optional[Any]:
        """
        Retrieve data from Redis.

        Args:
            key: Key to retrieve data for
            spark: Optional SparkSession for DataFrame reconstruction

        Returns:
            Optional[Any]: Retrieved data or None if not found

        Raises:
            ContextError: If retrieval fails
        """
        try:
            formatted_key = self._format_key(key)
            data = self.client.get(formatted_key)

            if data is None:
                self.logger.debug("No data found in Redis", extra_fields={"key": formatted_key})
                return None

            result = self._deserialize_data(data, spark)

            # Log retrieved data preview at TRACE level
            self.logger.trace(
                f"Retrieved data from Redis: {formatted_key}",
                extra_fields={
                    "operation": "get",
                    "key": formatted_key,
                    "data_info": self._get_data_preview(result),
                },
            )

            return result

        except Exception as e:
            raise ContextError(f"Failed to retrieve data: {e!s}") from e

    def get_metadata(self, key: str) -> Optional[Dict[str, Any]]:
        """
        Retrieve metadata for a key.

        Args:
            key: Key to retrieve metadata for

        Returns:
            Optional[Dict[str, Any]]: Metadata or None if not found
        """
        try:
            formatted_key = f"{self._format_key(key)}:metadata"
            data = self.client.get(formatted_key)

            if data is None:
                return None

            return json.loads(data.decode("utf-8"))

        except Exception as e:
            raise ContextError(f"Failed to retrieve metadata: {e!s}") from e

    def exists(self, key: str) -> bool:
        """
        Check if a key exists.

        Args:
            key: Key to check

        Returns:
            bool: True if key exists, False otherwise
        """
        try:
            return bool(self.client.exists(self._format_key(key)))
        except Exception as e:
            raise ContextError(f"Failed to check key existence: {e!s}") from e

    def validate_connection(self) -> None:
        """
        Validate Redis connection and permissions.

        Raises:
            ContextError: If validation fails
        """
        try:
            # Test basic operations
            test_key = self._format_key("_test")
            test_data = {"test": "data"}

            # Test write
            self.store(test_key, test_data)

            # Test read
            stored_data = self.get(test_key)
            if stored_data != test_data:
                raise ContextError("Data integrity check failed")

            # Test delete
            self.delete(test_key)

            # Verify deletion
            if self.exists(test_key):
                raise ContextError("Delete operation failed")

        except Exception as e:
            raise ContextError(f"Redis connection validation failed: {e!s}") from e

    def delete(self, key: str) -> None:
        """
        Delete data and metadata from Redis.

        Args:
            key: Key to delete

        Raises:
            ContextError: If deletion fails
        """
        try:
            formatted_key = self._format_key(key)

            # Delete both data and metadata keys
            data_result = self.client.delete(formatted_key)
            metadata_result = self.client.delete(f"{formatted_key}:metadata")

            self.logger.debug(
                f"Deleted data from Redis: {formatted_key}",
                extra_fields={
                    "operation": "delete",
                    "key": formatted_key,
                    "data_deleted": bool(data_result),
                    "metadata_deleted": bool(metadata_result),
                },
            )

        except Exception as e:
            raise ContextError(f"Failed to delete data: {e!s}") from e

    def clear(self, log_memory: bool = True) -> None:
        """Clear all keys in this context (or flush all if prefix is empty)."""
        if not hasattr(self, "client"):
            return

        try:
            if log_memory:
                info_before = self.client.info("memory")
                used_before = info_before.get("used_memory_human", "unknown")
                self.logger.debug(f"Redis Memory before clearing: {used_before}")

            if self.prefix:
                # Delete only keys with this prefix
                keys_to_delete = list(self.client.scan_iter(f"{self.prefix}*"))
                if keys_to_delete:
                    self.client.delete(*keys_to_delete)
            else:
                # Flush all keys if no prefix
                self.client.flushall()

            if log_memory:
                info_after = self.client.info("memory")
                used_after = info_after.get("used_memory_human", "unknown")
                self.logger.debug(f"Redis Memory after clearing: {used_after}")
        except Exception as e:
            self.logger.warning(f"Failed to clear Redis keys: {e}")

    def cleanup(self, flush_data: bool = True) -> None:
        """Clean up Redis connection, optionally clearing stored data first."""
        try:
            if hasattr(self, "client"):
                if flush_data:
                    self.clear(log_memory=True)

                self.client.close()
                self.logger.debug("Redis connection closed")
        except Exception as e:
            raise ContextError(f"Failed to cleanup Redis connection: {e!s}") from e
