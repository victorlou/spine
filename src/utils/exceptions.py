"""
Centralized exception handling for the data pipeline.
Provides a consistent pattern for error handling across components.
"""

import traceback
from datetime import UTC, datetime
from typing import Any, Dict, Optional


class PipelineError(Exception):
    """
    Base exception class for all pipeline errors.
    Provides common functionality for error tracking and context.

    Attributes:
        message: Main error message
        component: Component where the error occurred (e.g., "parser", "service")
        operation: Operation that failed
        is_retryable: Whether the operation can be retried
        details: Additional error details
        original_error: The underlying error that caused this exception
        timestamp: When the error occurred
        traceback_str: Formatted traceback string
    """

    # Default component name, should be overridden by subclasses
    component_name = None

    def __init__(
        self,
        message: str,
        component: Optional[str] = None,
        operation: Optional[str] = None,
        is_retryable: bool = False,
        details: Optional[Dict[str, Any]] = None,
        original_error: Optional[Exception] = None,
    ):
        self.component = component or self.component_name
        if not self.component:
            raise ValueError("Component must be specified either in class or constructor")

        self.operation = operation
        self.is_retryable = is_retryable
        self.details = details or {}
        self.original_error = original_error
        self.timestamp = datetime.now(UTC)

        # Capture the current stack trace
        self.traceback_str = "".join(traceback.format_stack()[:-1])  # Exclude this frame

        # If we have an original error, capture its traceback
        if original_error:
            if hasattr(original_error, "__traceback__"):
                self.original_traceback = "".join(traceback.format_tb(original_error.__traceback__))
            else:
                self.original_traceback = None
        else:
            self.original_traceback = None

        # Build detailed error message
        error_parts = [message]
        if self.component:
            error_parts.append(f"Component: {self.component}")
        if operation:
            error_parts.append(f"Operation: {operation}")
        if original_error:
            error_parts.append(f"Cause: {original_error!s}")

        # Create the base message
        self.base_message = " | ".join(error_parts)
        super().__init__(self.base_message)

    def get_detailed_message(self, include_traceback: bool = True) -> str:
        """
        Get a detailed error message including optional traceback.

        Args:
            include_traceback: Whether to include traceback information

        Returns:
            str: Detailed error message
        """
        parts = [self.base_message]

        if include_traceback:
            parts.append("\nError occurred at:")
            parts.append(self.traceback_str)

            if self.original_error and self.original_traceback:
                parts.append("\nOriginal error traceback:")
                parts.append(self.original_traceback)

        if self.details:
            parts.append("\nAdditional details:")
            for key, value in self.details.items():
                parts.append(f"  {key}: {value}")

        return "\n".join(parts)

    def format_error(self) -> Dict[str, Any]:
        """
        Format error details for logging and output.

        Returns:
            Dict[str, Any]: Structured error information
        """
        error_details = {
            "type": self.__class__.__name__,
            "message": str(self),
            "component": self.component,
            "timestamp": self.timestamp.isoformat(),
        }

        if self.operation:
            error_details["operation"] = self.operation

        if self.details:
            error_details["details"] = self.details

        if self.original_error:
            error_details["cause"] = {
                "type": self.original_error.__class__.__name__,
                "message": str(self.original_error),
            }

        return error_details

    def __str__(self) -> str:
        """
        String representation of the error.
        Includes basic message and location where the error occurred.
        """
        # Get the last frame from our traceback (where the error occurred)
        try:
            error_location = self.traceback_str.strip().split("\n")[-1]
        except (IndexError, AttributeError):
            error_location = "Unknown location"

        return f"{self.base_message}\nLocation: {error_location}"

    @classmethod
    def from_error(
        cls,
        error: Exception,
        message: str,
        operation: Optional[str] = None,
        is_retryable: bool = False,
        details: Optional[Dict[str, Any]] = None,
        component: Optional[str] = None,  # Made optional since subclasses define component_name
    ) -> "PipelineError":
        """
        Create a PipelineError from another exception.
        Preserves the original error's traceback.

        Args:
            error: The original error
            message: Context message to prepend
            operation: Operation that failed
            is_retryable: Whether the operation can be retried
            details: Additional error details
            component: Optional override for component name

        Returns:
            PipelineError: A new pipeline error wrapping the original
        """
        # Create new error instance using class's component_name if not overridden
        pipeline_error = cls(
            message=message,
            component=component,  # Will fall back to class's component_name if None
            operation=operation,
            is_retryable=is_retryable,
            details=details,
            original_error=error,
        )

        # Preserve the original traceback if possible
        if error and hasattr(error, "__traceback__"):
            pipeline_error.__cause__ = error

        return pipeline_error

    @staticmethod
    def format_unknown_error(error: Exception) -> Dict[str, Any]:
        """
        Format an unknown error that isn't a PipelineError.

        Args:
            error: The exception to format

        Returns:
            Dict[str, Any]: Structured error information
        """
        error_details = {
            "type": error.__class__.__name__,
            "message": str(error),
            "timestamp": datetime.now(UTC).isoformat(),
        }

        # Add traceback for debugging
        error_details["traceback"] = traceback.format_exc().split("\n")

        return error_details


class ConfigError(PipelineError):
    """Configuration-related errors (not retryable by default)."""

    component_name = "config"

    def __init__(
        self,
        message: str,
        operation: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None,
        original_error: Optional[Exception] = None,
        is_retryable: bool = False,
    ):
        super().__init__(
            message=message,
            operation=operation,
            is_retryable=is_retryable,
            details=details,
            original_error=original_error,
        )


class ServiceError(PipelineError):
    """Service-related errors (connectors, external services, retryable by default)."""

    component_name = "service"

    def __init__(
        self,
        message: str,
        service_name: Optional[str] = None,
        operation: Optional[str] = None,
        is_retryable: bool = True,
        details: Optional[Dict[str, Any]] = None,
        original_error: Optional[Exception] = None,
    ):
        # Add service_name to details if provided
        if service_name:
            details = details or {}
            details["service_name"] = service_name

        super().__init__(
            message=message,
            operation=operation,
            is_retryable=is_retryable,
            details=details,
            original_error=original_error,
        )


class ParserError(PipelineError):
    """Data parsing errors (not retryable by default)."""

    component_name = "parser"

    def __init__(
        self,
        message: str,
        operation: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None,
        original_error: Optional[Exception] = None,
        is_retryable: bool = False,
    ):
        super().__init__(
            message=message,
            operation=operation,
            is_retryable=is_retryable,
            details=details,
            original_error=original_error,
        )


class LoaderError(PipelineError):
    """Data loading errors (retryable by default)."""

    component_name = "loader"

    def __init__(
        self,
        message: str,
        destination: Optional[str] = None,
        operation: Optional[str] = None,
        is_retryable: bool = True,
        details: Optional[Dict[str, Any]] = None,
        original_error: Optional[Exception] = None,
    ):
        # Add destination to details if provided
        if destination:
            details = details or {}
            details["destination"] = destination

        super().__init__(
            message=message,
            operation=operation,
            is_retryable=is_retryable,
            details=details,
            original_error=original_error,
        )


class HandlerError(PipelineError):
    """Handler orchestration errors (not retryable by default)."""

    component_name = "handler"

    def __init__(
        self,
        message: str,
        operation: Optional[str] = None,
        is_retryable: bool = False,
        details: Optional[Dict[str, Any]] = None,
        original_error: Optional[Exception] = None,
    ):
        super().__init__(
            message=message,
            operation=operation,
            is_retryable=is_retryable,
            details=details,
            original_error=original_error,
        )

    @classmethod
    def from_error(
        cls,
        error: Exception,
        message: str,
        operation: Optional[str] = None,
        is_retryable: bool = False,
        details: Optional[Dict[str, Any]] = None,
        component: Optional[str] = None,  # Added but ignored
    ) -> "HandlerError":
        """
        Create a HandlerError from another exception.
        Note: component parameter is accepted but ignored since HandlerError always uses "handler".

        Args:
            error: The original error
            message: Context message to prepend
            operation: Operation that failed
            is_retryable: Whether the operation can be retried
            details: Additional error details
            component: Ignored, HandlerError always uses "handler"

        Returns:
            HandlerError: A new handler error wrapping the original
        """
        return cls(
            message=message,
            operation=operation,
            is_retryable=is_retryable,
            details=details,
            original_error=error,
        )


class AWSError(PipelineError):
    """AWS-related errors."""

    component_name = "aws"

    def __init__(
        self,
        message: str,
        service: Optional[str] = None,
        operation: Optional[str] = None,
        is_retryable: bool = True,  # Most AWS errors can be retried
        details: Optional[Dict[str, Any]] = None,
        original_error: Optional[Exception] = None,
    ):
        details = details or {}
        if service:
            details["aws_service"] = service

        super().__init__(
            message=message,
            operation=operation,
            is_retryable=is_retryable,
            details=details,
            original_error=original_error,
        )


class SparkError(PipelineError):
    """Spark processing errors."""

    component_name = "spark"

    def __init__(
        self,
        message: str,
        operation: Optional[str] = None,
        is_retryable: bool = True,  # Most Spark errors can be retried
        details: Optional[Dict[str, Any]] = None,
        original_error: Optional[Exception] = None,
    ):
        super().__init__(
            message=message,
            operation=operation,
            is_retryable=is_retryable,
            details=details,
            original_error=original_error,
        )


class ContextError(PipelineError):
    """Exception raised by context managers."""

    component_name = "context"

    def __init__(
        self,
        message: str,
        operation: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None,
        original_error: Optional[Exception] = None,
    ):
        # Format message with details if available
        if details:
            detail_str = ", ".join(f"{k}={v}" for k, v in details.items())
            message = f"{message} | Details: {detail_str}"

        super().__init__(
            message=message,
            operation=operation,
            is_retryable=False,  # Context errors are not retryable
            details=details,
            original_error=original_error,
        )


class PlanningError(Exception):
    """Exception raised for errors during execution planning."""

    def __init__(
        self,
        message: str,
        details: Optional[Dict[str, Any]] = None,
        operation: Optional[str] = None,
        original_error: Optional[Exception] = None,
    ):
        """
        Initialize planning error.

        Args:
            message: Error message
            details: Optional error details
            operation: Optional operation name where error occurred
            original_error: Optional original exception
        """
        self.message = message
        self.details = details or {}
        self.operation = operation
        self.original_error = original_error

        # Build full error message
        error_msg = message
        if operation:
            error_msg = f"[{operation}] {error_msg}"
        if details:
            error_msg = f"{error_msg} - Details: {details}"
        if original_error:
            error_msg = f"{error_msg} (Caused by: {original_error!s})"

        super().__init__(error_msg)


class GracefulShutdownError(Exception):
    """Raised when the process receives SIGTERM so that finally blocks (e.g. audit flush) can run."""

    def __init__(self, message: str = "Pipeline terminated (SIGTERM)"):
        self.message = message
        super().__init__(message)


class ResolverError(Exception):
    """Exception raised for errors during value resolution."""

    def __init__(
        self,
        message: str,
        details: Optional[Dict[str, Any]] = None,
        operation: Optional[str] = None,
        original_error: Optional[Exception] = None,
    ):
        """
        Initialize resolver error.

        Args:
            message: Error message
            details: Optional error details
            operation: Optional operation name where error occurred
            original_error: Optional original exception
        """
        self.message = message
        self.details = details or {}
        self.operation = operation
        self.original_error = original_error

        # Build full error message
        error_msg = message
        if operation:
            error_msg = f"[{operation}] {error_msg}"
        if details:
            error_msg = f"{error_msg} - Details: {details}"
        if original_error:
            error_msg = f"{error_msg} (Caused by: {original_error!s})"

        super().__init__(error_msg)
