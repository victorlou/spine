"""
Base handler class providing common orchestration functionality.
"""

import time
from abc import ABC, abstractmethod
from datetime import UTC, datetime
from typing import Any, Callable, Dict, List, Optional

import boto3
from botocore.exceptions import ClientError

from src.loader.base_loader import BaseLoader
from src.utils.exceptions import HandlerError, SparkError
from src.utils.logger import get_logger
from src.utils.spark_manager import SparkManager


class BaseHandler(ABC):
    """
    Abstract base class for handlers.
    Provides common functionality for orchestrating data flow between services, parsers, and loaders.
    """

    def __init__(
        self,
        parser: Optional[Any],
        loader: Optional[BaseLoader],
        destination: Optional[str],
        max_retries: int = 3,
        retry_delay: float = 1,  # seconds
        retry_backoff: float = 2,  # multiply delay by this factor each retry
        **kwargs,
    ):
        """
        Initialize the handler.

        Args:
            parser: Parser instance to use for data transformation (can be None if dynamic)
            loader: Loader instance to use for data loading (can be None if dynamic)
            destination: Destination for the loader (e.g., S3 bucket) (can be None if dynamic)
            max_retries: Maximum number of retry attempts for retryable operations
            retry_delay: Initial delay between retries in seconds
            retry_backoff: Multiplicative factor for retry delay
            **kwargs: Additional configuration options
        """
        self.parser = parser
        self.loader = loader
        self.destination = destination
        self.logger = get_logger(self.__class__.__name__)

        # Retry configuration
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.retry_backoff = retry_backoff

        # Store additional configuration
        self.config = kwargs

        # Initialize infrastructure components
        self.spark_manager = None
        self.spark = None

        # Initialize error tracking
        self.errors = []

    def _setup_spark(self) -> None:
        """
        Set up Spark session if needed.

        Raises:
            HandlerError: If Spark initialization fails
        """
        try:
            self.spark_manager = SparkManager()
            self.spark = self.spark_manager.init_session()
        except SparkError as e:
            raise HandlerError.from_error(e, "Failed to initialize Spark") from e

    def _test_s3_connectivity(self, bucket: str) -> None:
        """
        Test S3 bucket connectivity and permissions.

        Args:
            bucket: S3 bucket name to test

        Raises:
            HandlerError: If S3 connectivity test fails
        """
        try:
            s3_client = boto3.client("s3")

            # Test bucket existence and permissions
            try:
                s3_client.head_bucket(Bucket=bucket)
            except ClientError as e:
                error_code = e.response.get("Error", {}).get("Code", "Unknown")
                if error_code == "403":
                    raise HandlerError(f"Permission denied accessing S3 bucket: {bucket}") from e
                elif error_code == "404":
                    raise HandlerError(f"S3 bucket does not exist: {bucket}") from e
                else:
                    raise HandlerError.from_error(e, f"Error accessing S3 bucket {bucket}") from e

            # Test write permissions with a small test object
            test_key = "_test_write_permission"
            try:
                s3_client.put_object(Bucket=bucket, Key=test_key, Body="test")
                s3_client.delete_object(Bucket=bucket, Key=test_key)
            except ClientError as e:
                raise HandlerError.from_error(
                    e,
                    f"Failed to write test object to S3 bucket {bucket}",
                    is_retryable=True,  # S3 write failures are often temporary
                ) from e

            self.logger.debug(f"Successfully validated S3 bucket access: {bucket}")

        except Exception as e:
            if not isinstance(e, HandlerError):
                raise HandlerError.from_error(e, "S3 connectivity test failed") from e
            raise

    def with_retry(
        self,
        operation: Callable,
        error_message: str,
        retryable_exceptions: tuple = (Exception,),  # Default to retrying all exceptions
        **kwargs,
    ) -> Any:
        """
        Execute an operation with retry logic.

        IMPORTANT: Do not use this method for outbound requests that already have retry logic
        (like those using BaseSourceService). This retry mechanism is intended for:
        1. Infrastructure operations (S3, Redis, etc.)
        2. Data processing operations
        3. Operations that don't have their own retry mechanism

        The BaseSourceService class already implements comprehensive retry logic for HTTP calls
        using urllib3.Retry, which handles:
        - Network-level retries (timeouts, connection errors)
        - HTTP-level retries (5xx errors, rate limits)
        - Authentication retries (token refresh, signature updates)

        Args:
            operation: Function to execute
            error_message: Base error message for failures
            retryable_exceptions: Tuple of exceptions that should trigger a retry
            **kwargs: Arguments to pass to the operation

        Returns:
            Any: Result of the operation

        Raises:
            HandlerError: If operation fails after all retries
        """
        last_error = None
        delay = self.retry_delay

        for attempt in range(self.max_retries + 1):
            try:
                return operation(**kwargs)

            except retryable_exceptions as e:
                last_error = e
                if attempt < self.max_retries:
                    self.logger.warning(
                        f"{error_message} (Attempt {attempt + 1}/{self.max_retries + 1})",
                        extra_fields={
                            "error": str(e),
                            "retry_delay": delay,
                            "attempt": attempt + 1,
                        },
                    )
                    time.sleep(delay)
                    delay *= self.retry_backoff

            except Exception as e:
                # Non-retryable error
                raise HandlerError.from_error(e, error_message) from e

        # If we get here, we've exhausted all retries
        raise HandlerError(
            f"{error_message} after {self.max_retries + 1} attempts: {last_error!s}",
            original_error=last_error,
        )

    def track_error(self, error: Exception, context: Dict[str, Any]) -> None:
        """
        Track an error with context for reporting.

        Args:
            error: The exception that occurred
            context: Dictionary of contextual information about the error
        """
        error_info = {
            "timestamp": datetime.now(UTC).isoformat(),
            "error": str(error),
            "error_type": type(error).__name__,
            "is_retryable": isinstance(error, HandlerError) and error.is_retryable,
            **context,
        }
        self.errors.append(error_info)

        self.logger.error("Operation failed", extra_fields=error_info)

    def cleanup(self) -> None:
        """
        Cleanup resources used by the handler.
        Should be called when handler is no longer needed.
        """
        if self.spark_manager:
            self.spark_manager.stop_session()

    @abstractmethod
    def handle(self) -> Dict[str, Any]:
        """
        Handle the data ingestion flow.
        Must be implemented by concrete classes.

        Returns:
            Dict containing results of the operation

        Raises:
            HandlerError: If handling fails
        """
        pass

    @abstractmethod
    def validate(self) -> None:
        """
        Validate the handler configuration.
        Must be implemented by concrete classes.

        Raises:
            HandlerError: If validation fails
        """
        pass

    def _load_data(self, data: List[Dict[str, Any]], prefix: str, **kwargs) -> str:
        """
        Load data using the configured loader.

        Args:
            data: Data to load
            prefix: Prefix for the destination
            **kwargs: Additional loader arguments

        Returns:
            str: Identifier for where the data was loaded

        Raises:
            HandlerError: If loading fails
        """
        return self.with_retry(
            operation=self.loader.load,
            error_message="Failed to load data",
            data=data,
            destination=self.destination,
            prefix=prefix,
            **kwargs,
        )

    def _parse_data(self, data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Parse data using the configured parser.

        Args:
            data: Data to parse

        Returns:
            List[Dict[str, Any]]: Parsed data

        Raises:
            HandlerError: If parsing fails
        """
        try:
            return self.parser.parse(data)
        except Exception as e:
            error_msg = f"Failed to parse data: {e!s}"
            self.logger.error(error_msg)
            raise HandlerError(error_msg) from e
