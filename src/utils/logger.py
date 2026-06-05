"""
Centralized logging configuration.
Provides structured logging with consistent formatting.

Log Level Guidelines:
- TRACE (5): Most granular details like parameter formatting, raw data structures, and parsing details
- DEBUG (10): Important operational details needed for debugging like API status, batch progress
- INFO (20): Major milestones, final outcomes, and important state changes
- WARNING (30): Potential issues that don't affect execution but should be reviewed
- ERROR (40): Issues that affect execution but can be handled
- CRITICAL (50): Severe issues that prevent execution

Example:
    >>> from src.utils.logger import get_logger
    >>> logger = get_logger(__name__)
    >>> logger.info("Processing started", extra_fields={"batch_id": 123})
"""

import logging
import os
import re
import sys
from datetime import UTC, datetime
from functools import lru_cache
from typing import Any, Dict, Optional, Set

from src.utils.telemetry_logging import TraceCorrelationFilter

# Redaction placeholder for sensitive values
REDACTED_PLACEHOLDER = "***REDACTED***"

# Environment variables controlling redaction
ENV_REDACT_LOGS = "SPINE_REDACT_LOGS"
ENV_SENSITIVE_KEYS = "SPINE_SENSITIVE_KEYS"

# Built-in: key names that are always considered sensitive (exact match)
SENSITIVE_KEY_EXACT: frozenset = frozenset(
    {
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
        "AWS_SESSION_TOKEN",
        "DATABRICKS_TOKEN",
        "DATABRICKS_ACCESS_TOKEN",
        "DATABRICKS_OIDC_TOKEN",
        "DATABRICKS_OAUTH_CLIENT_SECRET",
        "GITLAB_OIDC_TOKEN",
        "CI_JOB_JWT_V2",
        "CI_JOB_JWT",
        "WECOM_WEBHOOK_URL",
    }
)

# Built-in: key is sensitive if it contains any of these substrings (case-insensitive)
SENSITIVE_KEY_SUBSTRINGS: tuple = (
    "PASSWORD",
    "SECRET",
    "TOKEN",
    "API_KEY",
    "CREDENTIAL",
    "PRIVATE",
    "JWT",
    "OIDC",
    "BEARER",
    "AUTH",
    "SESSION_ID",
)


def normalize_key(key: str) -> str:
    """Return lowercase key with all non-alphanumeric characters removed. E.g. API-KEY -> apikey."""
    return re.sub(r"[^a-z0-9]", "", key.lower())


NORMALIZED_EXACT: frozenset = frozenset(normalize_key(k) for k in SENSITIVE_KEY_EXACT)
NORMALIZED_SUBSTRINGS: tuple = tuple(normalize_key(s) for s in SENSITIVE_KEY_SUBSTRINGS)


def _redaction_enabled() -> bool:
    """Return True if log redaction is enabled (default). Set SPINE_REDACT_LOGS=false to disable."""
    val = os.getenv(ENV_REDACT_LOGS, "true").lower().strip()
    return val not in ("false", "0", "no")


def _get_sensitive_keys_from_env() -> Set[str]:
    """Return additional sensitive key names from SPINE_SENSITIVE_KEYS (comma-separated)."""
    raw = os.getenv(ENV_SENSITIVE_KEYS)
    if not raw:
        return set()
    return {k.strip() for k in raw.split(",") if k.strip()}


def is_sensitive_key(key: str) -> bool:
    """Return True if the key name should be treated as sensitive (value redacted)."""
    if not key:
        return False
    key_norm = normalize_key(key)
    keys_from_env = _get_sensitive_keys_from_env()
    if key_norm in NORMALIZED_EXACT or key_norm in {normalize_key(k) for k in keys_from_env}:
        return True
    return any(sub_norm in key_norm for sub_norm in NORMALIZED_SUBSTRINGS)


def redact_text(text: str) -> str:
    """
    Redact key=value and key: value substrings in text where key is sensitive.
    Used for message, exc_text, and stack_info.
    """
    if not text:
        return text

    def replace_key_value(match: re.Match) -> str:
        key_part = match.group(1)
        sep = match.group(2)
        if is_sensitive_key(key_part):
            return f"{key_part}{sep}{REDACTED_PLACEHOLDER}"
        return match.group(0)

    # key=value (value: non-whitespace, non-pipe)
    text = re.sub(
        r"([\w]+)(=)([^\s|]+)",
        replace_key_value,
        text,
    )
    # key: value (value: rest of line or until next key= or key:)
    text = re.sub(
        r"([\w]+)(:\s*)([^\n]+?)(?=\s*[\n]|\s*$|\s+[\w]+[=:]|\s*$)",
        replace_key_value,
        text,
        flags=re.DOTALL,
    )
    return text


# Define TRACE level (5 is below DEBUG which is 10)
TRACE = 5
logging.addLevelName(TRACE, "TRACE")

# Default log level if not specified in env or args
DEFAULT_LOG_LEVEL = "DEBUG"

# Valid log levels
VALID_LOG_LEVELS = {
    "TRACE": TRACE,
    "DEBUG": logging.DEBUG,
    "INFO": logging.INFO,
    "WARNING": logging.WARNING,
    "ERROR": logging.ERROR,
    "CRITICAL": logging.CRITICAL,
}


class StructuredFormatter(logging.Formatter):
    """Custom formatter for structured logging."""

    def format(self, record: logging.LogRecord) -> str:
        """
        Format the log record with additional context.

        Args:
            record: The log record to format

        Returns:
            str: The formatted log message
        """
        # Add timestamp in ISO format
        record.timestamp = datetime.now(UTC).isoformat()

        # Add extra fields if they exist (redact values for sensitive keys)
        extra_fields = ""
        if hasattr(record, "extra_fields"):
            parts = []
            for k, v in record.extra_fields.items():
                if _redaction_enabled() and is_sensitive_key(k):
                    parts.append(f"{k}={REDACTED_PLACEHOLDER}")
                else:
                    parts.append(f"{k}={v}")
            extra_fields = " ".join(parts)
            if extra_fields:
                extra_fields = " | " + extra_fields

        # Format the basic message (redact key=value / key: value in message text)
        message_text = record.getMessage()
        if _redaction_enabled():
            message_text = redact_text(message_text)
        msg = (
            f"{record.timestamp} | {record.levelname:8} | "
            f"{record.name:20} | {message_text}{extra_fields}"
        )

        # Add source location for ERROR and above
        if record.levelno >= logging.ERROR:
            location = f"{record.filename}:{record.lineno}"
            msg = f"{msg} | at {location}"

        # Add exception information if present (redact sensitive key=value in traceback)
        if record.exc_info:
            # Cache the traceback text to avoid converting it multiple times
            if not record.exc_text:
                record.exc_text = self.formatException(record.exc_info)
            if record.exc_text:
                exc_text = redact_text(record.exc_text) if _redaction_enabled() else record.exc_text
                msg = msg + "\n" + exc_text

        # Add stack information if present (redact sensitive key=value)
        if record.stack_info:
            stack_text = self.formatStack(record.stack_info)
            stack_text = redact_text(stack_text) if _redaction_enabled() else stack_text
            msg = msg + "\n" + stack_text

        return msg


class StructuredLogger:
    """
    Logger class that provides structured logging with consistent formatting.

    This class wraps Python's built-in logger to provide:
    - Structured output format with timestamps and context
    - Support for extra fields in log messages
    - Custom TRACE level for detailed debugging
    - Consistent configuration across all instances

    Args:
        name: The name of the logger
        level: Optional log level to override environment variable
    """

    def __init__(self, name: str, level: Optional[str] = None):
        self.logger = logging.getLogger(name)

        # Set log level following priority order:
        # 1. Root logger level (set via command line)
        # 2. Provided level
        # 3. Environment variable
        # 4. Default level (DEBUG)
        root_logger = logging.getLogger()
        if root_logger.level == logging.NOTSET or root_logger.level == logging.WARNING:
            log_level = get_log_level(level) if level else VALID_LOG_LEVELS[DEFAULT_LOG_LEVEL]
            self.logger.setLevel(log_level)

        # Enable propagation to root logger
        self.logger.propagate = True

        # Only add our handler if there are no handlers with our formatter
        if not any(isinstance(h.formatter, StructuredFormatter) for h in self.logger.handlers):
            console_handler = logging.StreamHandler(sys.stdout)
            console_handler.setFormatter(StructuredFormatter())
            # Inject active trace/span ids into records (no-op until telemetry enables correlation).
            console_handler.addFilter(TraceCorrelationFilter())
            self.logger.addHandler(console_handler)

    def _log(
        self, level: int, msg: str, *args, extra_fields: Optional[Dict[str, Any]] = None, **kwargs
    ):
        """
        Internal logging method with extra fields support.

        Args:
            level: The log level
            msg: The log message (supports %-style formatting)
            *args: Arguments for %-style message formatting
            extra_fields: Optional dictionary of extra fields to include
            **kwargs: Additional logging arguments (e.g., exc_info, stack_info, stacklevel)
        """
        if extra_fields:
            kwargs["extra"] = {"extra_fields": extra_fields}

        # Handle %-style formatting if args are provided
        if args:
            msg = msg % args

        # For error and above, ensure we show the caller's location
        if level >= logging.ERROR and "stacklevel" not in kwargs:
            kwargs["stacklevel"] = 3  # Skip _log, the logging method, and get to the caller

        self.logger.log(level, msg, **kwargs)

    def trace(self, msg: str, *args, extra_fields: Optional[Dict[str, Any]] = None, **kwargs):
        """Log a trace message."""
        self._log(TRACE, msg, *args, extra_fields=extra_fields, **kwargs)

    def debug(self, msg: str, *args, extra_fields: Optional[Dict[str, Any]] = None, **kwargs):
        """Log a debug message."""
        self._log(logging.DEBUG, msg, *args, extra_fields=extra_fields, **kwargs)

    def info(self, msg: str, *args, extra_fields: Optional[Dict[str, Any]] = None, **kwargs):
        """Log an info message."""
        self._log(logging.INFO, msg, *args, extra_fields=extra_fields, **kwargs)

    def warning(self, msg: str, *args, extra_fields: Optional[Dict[str, Any]] = None, **kwargs):
        """Log a warning message."""
        self._log(logging.WARNING, msg, *args, extra_fields=extra_fields, **kwargs)

    def error(self, msg: str, *args, extra_fields: Optional[Dict[str, Any]] = None, **kwargs):
        """Log an error message."""
        self._log(logging.ERROR, msg, *args, extra_fields=extra_fields, **kwargs)

    def exception(self, msg: str, *args, extra_fields: Optional[Dict[str, Any]] = None, **kwargs):
        """Log an exception with traceback."""
        self._log(logging.ERROR, msg, *args, extra_fields=extra_fields, exc_info=True, **kwargs)

    def critical(self, msg: str, *args, extra_fields: Optional[Dict[str, Any]] = None, **kwargs):
        """Log a critical message."""
        self._log(logging.CRITICAL, msg, *args, extra_fields=extra_fields, **kwargs)


@lru_cache(maxsize=32)
def get_logger(name: str, level: Optional[str] = None) -> StructuredLogger:
    """
    Get or create a logger instance.
    Uses lru_cache to ensure we only create one logger per name.

    Args:
        name: The name of the logger (typically __name__)
        level: Optional log level to override environment variable

    Returns:
        StructuredLogger: The logger instance

    Example:
        >>> logger = get_logger(__name__)
        >>> logger.info("Process started", extra_fields={"process_id": 123})
    """
    return StructuredLogger(name, level)


def get_log_level(level: Optional[str] = None) -> int:
    """
    Get the numeric log level following priority order:
    1. Provided level argument
    2. Environment variable LOG_LEVEL
    3. Default level (DEBUG)

    Args:
        level: Optional log level to override environment variable

    Returns:
        int: The numeric log level

    Raises:
        ValueError: If the log level is invalid
    """
    # Priority 1: Argument
    if level:
        level_upper = level.upper()
        if level_upper in VALID_LOG_LEVELS:
            return VALID_LOG_LEVELS[level_upper]
        raise ValueError(f"Invalid log level: {level}")

    # Priority 2: Environment variable
    env_level = os.getenv("LOG_LEVEL")
    if env_level:
        env_level_upper = env_level.upper()
        if env_level_upper in VALID_LOG_LEVELS:
            return VALID_LOG_LEVELS[env_level_upper]
        raise ValueError(f"Invalid log level in environment: {env_level}")

    # Priority 3: Default
    return VALID_LOG_LEVELS[DEFAULT_LOG_LEVEL]


def set_root_log_level(level: str) -> None:
    """
    Set the root logger's level and ensure it propagates to all loggers.

    Args:
        level: The log level to set (TRACE, DEBUG, INFO, WARNING, ERROR, CRITICAL)

    Raises:
        ValueError: If the log level is invalid

    Example:
        >>> set_root_log_level('DEBUG')  # Set debug level for all loggers
    """
    numeric_level = get_log_level(level)
    root_logger = logging.getLogger()
    root_logger.setLevel(numeric_level)

    # Ensure all existing loggers respect the root level
    for logger_name in logging.root.manager.loggerDict:
        logger = logging.getLogger(logger_name)
        logger.setLevel(numeric_level)
