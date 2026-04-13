"""Tests for src.utils.logger."""

import logging
import os
from datetime import datetime
from io import StringIO
from typing import Generator

import pytest

from src.utils.logger import (
    DEFAULT_LOG_LEVEL,
    ENV_REDACT_LOGS,
    ENV_SENSITIVE_KEYS,
    NORMALIZED_EXACT,
    NORMALIZED_SUBSTRINGS,
    REDACTED_PLACEHOLDER,
    SENSITIVE_KEY_EXACT,
    SENSITIVE_KEY_SUBSTRINGS,
    TRACE,
    VALID_LOG_LEVELS,
    StructuredFormatter,
    StructuredLogger,
    get_log_level,
    get_logger,
    is_sensitive_key,
    normalize_key,
    redact_text,
    set_root_log_level,
)


@pytest.fixture
def capture_logs() -> Generator[StringIO, None, None]:
    """Capture logs in a StringIO buffer."""
    # Save existing handlers and level
    root_logger = logging.getLogger()
    existing_handlers = root_logger.handlers[:]
    previous_level = root_logger.level

    # Create our test handler
    stream = StringIO()
    handler = logging.StreamHandler(stream)
    handler.setFormatter(StructuredFormatter())
    handler.setLevel(TRACE)  # Ensure handler captures everything

    # Configure root logger to capture everything
    root_logger.setLevel(TRACE)
    root_logger.handlers = [handler]  # Replace all handlers

    # Ensure all existing loggers propagate to root
    for name in logging.root.manager.loggerDict:
        logger = logging.getLogger(name)
        logger.handlers = []  # Remove any existing handlers
        logger.propagate = True  # Ensure propagation to root
        logger.setLevel(TRACE)  # Allow all messages through

    try:
        yield stream
    finally:
        # Restore previous state
        stream.close()
        root_logger.handlers = existing_handlers
        root_logger.setLevel(previous_level)


@pytest.fixture
def clean_env() -> Generator[None, None, None]:
    """Remove LOG_LEVEL from environment for testing."""
    log_level = os.environ.pop("LOG_LEVEL", None)
    try:
        yield
    finally:
        if log_level is not None:
            os.environ["LOG_LEVEL"] = log_level


@pytest.fixture
def clean_redact_env() -> Generator[None, None, None]:
    """Save and restore SPINE_REDACT_LOGS and SPINE_SENSITIVE_KEYS for redaction tests."""
    saved_redact = os.environ.pop(ENV_REDACT_LOGS, None)
    saved_keys = os.environ.pop(ENV_SENSITIVE_KEYS, None)
    try:
        yield
    finally:
        if saved_redact is not None:
            os.environ[ENV_REDACT_LOGS] = saved_redact
        elif ENV_REDACT_LOGS in os.environ:
            os.environ.pop(ENV_REDACT_LOGS)
        if saved_keys is not None:
            os.environ[ENV_SENSITIVE_KEYS] = saved_keys
        elif ENV_SENSITIVE_KEYS in os.environ:
            os.environ.pop(ENV_SENSITIVE_KEYS)


class TestStructuredFormatter:
    """Tests for the StructuredFormatter class."""

    def test_format_basic_message(self, capture_logs: StringIO) -> None:
        """Test basic message formatting."""
        logger = get_logger("test")
        logger.info("test message")
        log_output = capture_logs.getvalue()

        # Split the log output into its components
        parts = log_output.strip().split(" | ")

        # Verify each part
        assert len(parts) == 4  # timestamp, level, logger name, message
        assert parts[1].strip() == "INFO"
        assert parts[2].strip() == "test"
        assert parts[3] == "test message"

        # Verify timestamp format
        timestamp = parts[0]
        datetime.fromisoformat(timestamp)  # Should not raise

    def test_format_with_extra_fields(self, capture_logs: StringIO) -> None:
        """Test formatting with extra fields."""
        logger = get_logger("test")
        logger.info("test message", extra_fields={"key": "value", "number": 42})
        log_output = capture_logs.getvalue()

        assert "test message | key=value number=42" in log_output

    def test_format_without_extra_fields_attribute(self) -> None:
        """Test formatting when record has no extra_fields attribute."""
        formatter = StructuredFormatter()
        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="test.py",
            lineno=1,
            msg="test message",
            args=(),
            exc_info=None,
        )
        formatted = formatter.format(record)
        assert "test message" in formatted
        assert " | key=" not in formatted


class TestStructuredLogger:
    """Tests for the StructuredLogger class."""

    def test_all_log_levels(self, capture_logs: StringIO, clean_env: None) -> None:
        """Test all available log levels."""
        # Set logger to TRACE level to capture all messages
        logger = get_logger("test", level="TRACE")

        # Test each level
        logger.trace("trace message")
        logger.debug("debug message")
        logger.info("info message")
        logger.warning("warning message")
        logger.error("error message")
        logger.critical("critical message")

        output = capture_logs.getvalue()

        # Lines from this logger only (other libraries may emit DEBUG to the same handler).
        raw_lines = output.strip().split("\n")
        lines = []
        for line in raw_lines:
            parts = line.split(" | ")
            if len(parts) >= 4 and parts[2].strip() == "test":
                lines.append(line)
        expected_levels = {
            "TRACE": "trace message",
            "DEBUG": "debug message",
            "INFO": "info message",
            "WARNING": "warning message",
            "ERROR": "error message",
            "CRITICAL": "critical message",
        }

        for line in lines:
            parts = line.split(" | ")
            level = parts[1].strip()
            message = parts[3].strip()
            assert message == expected_levels[level], f"Unexpected message for level {level}"

        # Verify we got all expected levels
        actual_levels = {line.split(" | ")[1].strip() for line in lines}
        assert actual_levels == set(expected_levels.keys()), "Not all log levels were present"

    def test_exception_logging(self, capture_logs: StringIO) -> None:
        """Test exception logging with traceback."""
        logger = get_logger("test")
        try:
            raise ValueError("test error")
        except ValueError:
            logger.exception("caught error")

        output = capture_logs.getvalue()

        # Split the first line into components and verify
        first_line = output.strip().split("\n")[0]
        parts = first_line.split(" | ")

        # Verify log components
        assert len(parts) == 5  # timestamp, level, logger name, message, location
        assert parts[1].strip() == "ERROR"
        assert parts[2].strip() == "test"
        assert parts[3].strip() == "caught error"
        assert "test_logger.py:" in parts[4]  # Verify location

        # Verify traceback information
        assert "Traceback (most recent call last):" in output
        assert "ValueError: test error" in output
        assert "test_exception_logging" in output  # Function name in traceback

    def test_extra_fields_all_levels(self, capture_logs: StringIO) -> None:
        """Test extra fields with all log levels."""
        logger = get_logger("test")
        extra = {"request_id": "123", "user": "test"}

        logger.trace("message", extra_fields=extra)
        logger.debug("message", extra_fields=extra)
        logger.info("message", extra_fields=extra)
        logger.warning("message", extra_fields=extra)
        logger.error("message", extra_fields=extra)
        logger.critical("message", extra_fields=extra)

        output = capture_logs.getvalue()
        assert all(" | request_id=123 user=test" in line for line in output.splitlines())

    def test_handler_deduplication(self) -> None:
        """Test that handlers aren't duplicated on multiple logger instances."""
        logger1 = StructuredLogger("test")
        logger2 = StructuredLogger("test")

        assert len(logger1.logger.handlers) == 1
        assert len(logger2.logger.handlers) == 1
        assert logger1.logger.handlers == logger2.logger.handlers


class TestGetLogger:
    """Tests for the get_logger function."""

    def test_logger_caching(self) -> None:
        """Test that loggers are properly cached."""
        logger1 = get_logger("test")
        logger2 = get_logger("test")
        logger3 = get_logger("other")

        assert logger1 is logger2  # Same name should return cached instance
        assert logger1 is not logger3  # Different names should be different instances

    def test_logger_with_explicit_level(self, clean_env: None) -> None:
        """Test logger creation with explicit level."""
        logger = get_logger("test", level="ERROR")
        assert logger.logger.level == logging.ERROR

    def test_logger_respects_root_level(self) -> None:
        """Test that logger respects root logger level."""
        set_root_log_level("ERROR")
        logger = get_logger("test", level="DEBUG")  # Should be overridden by root
        assert logger.logger.level == logging.ERROR


class TestLogLevel:
    """Tests for log level management."""

    def test_get_log_level_from_argument(self, clean_env: None) -> None:
        """Test log level from direct argument."""
        assert get_log_level("DEBUG") == logging.DEBUG
        assert get_log_level("INFO") == logging.INFO
        assert get_log_level("ERROR") == logging.ERROR

    def test_get_log_level_from_env(self, clean_env: None) -> None:
        """Test log level from environment variable."""
        os.environ["LOG_LEVEL"] = "ERROR"
        assert get_log_level() == logging.ERROR

    def test_get_log_level_default(self, clean_env: None) -> None:
        """Test default log level when nothing is specified."""
        assert get_log_level() == VALID_LOG_LEVELS[DEFAULT_LOG_LEVEL]

    def test_invalid_log_level(self, clean_env: None) -> None:
        """Test handling of invalid log levels."""
        with pytest.raises(ValueError, match="Invalid log level: INVALID"):
            get_log_level("INVALID")

    def test_invalid_env_log_level(self, clean_env: None) -> None:
        """Test handling of invalid environment log level."""
        os.environ["LOG_LEVEL"] = "INVALID"
        with pytest.raises(ValueError, match="Invalid log level in environment: INVALID"):
            get_log_level()


class TestRootLogLevel:
    """Tests for root logger level management."""

    def test_set_root_log_level(self) -> None:
        """Test setting root logger level."""
        set_root_log_level("ERROR")
        assert logging.getLogger().level == logging.ERROR

        # All existing loggers should respect the new level
        test_logger = logging.getLogger("test")
        assert test_logger.level == logging.ERROR

    def test_invalid_root_log_level(self) -> None:
        """Test setting invalid root logger level."""
        with pytest.raises(ValueError):
            set_root_log_level("INVALID")


class TestEdgeCases:
    """Tests for various edge cases."""

    def test_trace_level_registration(self) -> None:
        """Test that TRACE level is properly registered."""
        assert TRACE == 5
        assert logging.getLevelName(TRACE) == "TRACE"
        assert logging.getLevelName("TRACE") == TRACE

    def test_empty_extra_fields(self, capture_logs: StringIO, clean_env: None) -> None:
        """Test logging with empty extra fields."""
        # Set explicit level to ensure messages are captured
        logger = get_logger("test", level="DEBUG")
        logger.info("message", extra_fields={})
        output = capture_logs.getvalue()

        # Verify the message exists and has the correct format
        assert "message" in output, "Log message not found in output"
        # Split on the message and verify no extra fields were added
        message_parts = output.split("message")
        assert len(message_parts) == 2, "Message not found in log output"
        assert " | " not in message_parts[1], "Extra fields separator found when none expected"

    def test_none_extra_fields(self, capture_logs: StringIO, clean_env: None) -> None:
        """Test logging with None extra fields."""
        # Set explicit level to ensure messages are captured
        logger = get_logger("test", level="DEBUG")
        logger.info("message", extra_fields=None)
        output = capture_logs.getvalue()

        # Verify the message exists and has the correct format
        assert "message" in output, "Log message not found in output"
        # Split on the message and verify no extra fields were added
        message_parts = output.split("message")
        assert len(message_parts) == 2, "Message not found in log output"
        assert " | " not in message_parts[1], "Extra fields separator found when none expected"

    def test_extra_kwargs_passthrough(self, capture_logs: StringIO, clean_env: None) -> None:
        """Test that unknown kwargs are passed to underlying logger."""
        # Use DEBUG level instead of NOTSET since NOTSET isn't in VALID_LOG_LEVELS
        logger = get_logger("test", level="DEBUG")
        logger.info("message", stack_info=True)
        output = capture_logs.getvalue()

        # Verify both the message and stack trace are present
        assert "message" in output, "Log message not found in output"
        assert "Stack (most recent call last):" in output, "Stack trace not found in output"
        assert "test_extra_kwargs_passthrough" in output, "Function name not found in stack trace"


class TestKwargsHandling:
    """Tests for handling standard logging kwargs."""

    def test_standard_kwargs_passthrough(self, capture_logs: StringIO) -> None:
        """Test that standard logging kwargs are properly handled."""
        logger = get_logger("test")

        # Test with stack_info
        logger.info("Test stack", stack_info=True)
        output = capture_logs.getvalue()
        assert "Stack (most recent call last):" in output

        # Clear buffer
        capture_logs.seek(0)
        capture_logs.truncate()

        # Test with exc_info
        try:
            raise ValueError("Test error")
        except ValueError:
            logger.error("Test exc_info", exc_info=True)
            output = capture_logs.getvalue()
            assert "Traceback (most recent call last):" in output
            assert "ValueError: Test error" in output

    def test_kwargs_with_formatting(self, capture_logs: StringIO) -> None:
        """Test kwargs work alongside string formatting."""
        logger = get_logger("test")
        logger.info("Value: %d", 42, stack_info=True)
        output = capture_logs.getvalue()
        assert "Value: 42" in output
        assert "Stack (most recent call last):" in output

    def test_kwargs_with_extra_fields(self, capture_logs: StringIO) -> None:
        """Test kwargs work alongside extra_fields."""
        logger = get_logger("test")
        logger.info("Test message", extra_fields={"user_id": 123}, stack_info=True)
        output = capture_logs.getvalue()
        assert "Test message | user_id=123" in output
        assert "Stack (most recent call last):" in output


class TestMessageFormatting:
    """Tests for message formatting features."""

    def test_percent_style_formatting(self, capture_logs: StringIO) -> None:
        """Test %-style string formatting support."""
        logger = get_logger("test")
        logger.info("Value: %d, String: %s", 42, "test")
        output = capture_logs.getvalue()

        assert "Value: 42, String: test" in output

    def test_percent_style_with_extra_fields(self, capture_logs: StringIO) -> None:
        """Test %-style formatting with extra fields."""
        logger = get_logger("test")
        logger.info("Count: %d", 42, extra_fields={"type": "test"})
        output = capture_logs.getvalue()

        assert "Count: 42 | type=test" in output


class TestErrorFormatting:
    """Tests for error-specific formatting features."""

    def test_error_includes_location(self, capture_logs: StringIO) -> None:
        """Test that error messages include source location."""
        logger = get_logger("test")
        logger.error("Test error")
        output = capture_logs.getvalue()

        # Verify location is included
        assert " | at " in output
        assert "test_logger.py:" in output  # Should include this file's name

    def test_error_location_with_formatting(self, capture_logs: StringIO) -> None:
        """Test location is included with formatted error messages."""
        logger = get_logger("test")
        logger.error("Error: %s", "test message")
        output = capture_logs.getvalue()

        assert "Error: test message" in output
        assert " | at " in output
        assert "test_logger.py:" in output


class TestRedaction:
    """Tests for secret redaction in logs."""

    def test_extra_fields_sensitive_key_redacted(
        self, capture_logs: StringIO, clean_redact_env: None
    ) -> None:
        """Sensitive keys in extra_fields have values redacted."""
        logger = get_logger("test", level="DEBUG")
        logger.info("Config", extra_fields={"API_KEY": "sk-secret-123", "region": "us-east-1"})
        output = capture_logs.getvalue()
        assert f"API_KEY={REDACTED_PLACEHOLDER}" in output
        assert "region=us-east-1" in output
        assert "sk-secret-123" not in output

    def test_extra_fields_non_sensitive_unchanged(
        self, capture_logs: StringIO, clean_redact_env: None
    ) -> None:
        """Non-sensitive keys in extra_fields are logged as-is."""
        logger = get_logger("test", level="DEBUG")
        logger.info("Info", extra_fields={"region": "eu-west-1", "batch_id": 42})
        output = capture_logs.getvalue()
        assert "region=eu-west-1" in output
        assert "batch_id=42" in output

    def test_message_scrubbing_key_value_redacted(
        self, capture_logs: StringIO, clean_redact_env: None
    ) -> None:
        """Message text containing key=value with sensitive key is scrubbed."""
        logger = get_logger("test", level="DEBUG")
        logger.info("Response API_KEY=sk-xxx and other")
        output = capture_logs.getvalue()
        assert f"API_KEY={REDACTED_PLACEHOLDER}" in output
        assert "sk-xxx" not in output

    def test_exc_text_scrubbed(self, capture_logs: StringIO, clean_redact_env: None) -> None:
        """Exception/traceback text containing sensitive key=value is redacted."""
        logger = get_logger("test", level="DEBUG")

        class CustomError(Exception):
            pass

        try:
            raise CustomError("Config had API_KEY=sk-leak and failed")
        except CustomError:
            logger.exception("Something failed")
        output = capture_logs.getvalue()
        assert f"API_KEY={REDACTED_PLACEHOLDER}" in output or REDACTED_PLACEHOLDER in output
        assert "sk-leak" not in output

    def test_redact_disabled_extra_fields_not_redacted(
        self, capture_logs: StringIO, clean_redact_env: None
    ) -> None:
        """When SPINE_REDACT_LOGS=false, extra_fields are not redacted."""
        os.environ[ENV_REDACT_LOGS] = "false"
        logger = get_logger("test", level="DEBUG")
        logger.info("Config", extra_fields={"API_KEY": "sk-visible"})
        output = capture_logs.getvalue()
        assert "API_KEY=sk-visible" in output
        assert REDACTED_PLACEHOLDER not in output

    def test_sensitive_keys_env_extends_set(
        self, capture_logs: StringIO, clean_redact_env: None
    ) -> None:
        """SPINE_SENSITIVE_KEYS adds extra key names to redact."""
        os.environ[ENV_SENSITIVE_KEYS] = "MY_CUSTOM_SECRET,CUSTOM_KEY"
        logger = get_logger("test", level="DEBUG")
        logger.info("Data", extra_fields={"MY_CUSTOM_SECRET": "hide", "CUSTOM_KEY": "also"})
        output = capture_logs.getvalue()
        assert f"MY_CUSTOM_SECRET={REDACTED_PLACEHOLDER}" in output
        assert f"CUSTOM_KEY={REDACTED_PLACEHOLDER}" in output
        assert "hide" not in output
        assert "also" not in output

    def test_empty_message_with_redaction(
        self, capture_logs: StringIO, clean_redact_env: None
    ) -> None:
        """Empty message triggers redact_text empty-path; log line still formatted."""
        logger = get_logger("test", level="DEBUG")
        logger.info("")
        output = capture_logs.getvalue()
        assert "INFO" in output
        assert "test" in output
        # Empty message produces a line with no message text before any extra_fields
        assert " | " in output

    def test_extra_fields_empty_key_treated_as_not_sensitive(
        self, capture_logs: StringIO, clean_redact_env: None
    ) -> None:
        """Empty string key in extra_fields is not considered sensitive (is_sensitive_key returns False)."""
        logger = get_logger("test", level="DEBUG")
        logger.info("Event", extra_fields={"": "empty-key-value"})
        output = capture_logs.getvalue()
        # Empty key is not redacted; we get "=empty-key-value" in the output
        assert "=empty-key-value" in output
        assert "Event" in output

    def test_redaction_api_public_and_usable(self, clean_redact_env: None) -> None:
        """Public redaction API (constants and functions) is importable and behaves as documented."""
        assert isinstance(SENSITIVE_KEY_EXACT, frozenset)
        assert "API_KEY" in SENSITIVE_KEY_SUBSTRINGS
        assert "DATABRICKS_TOKEN" in SENSITIVE_KEY_EXACT
        assert is_sensitive_key("API_KEY") is True
        assert is_sensitive_key("region") is False
        assert is_sensitive_key("") is False
        assert redact_text("") == ""
        assert redact_text("key=val") == "key=val"
        assert redact_text("API_KEY=sk-secret") == f"API_KEY={REDACTED_PLACEHOLDER}"
        assert is_sensitive_key("API-KEY") is True
        assert is_sensitive_key("API.KEY") is True
        assert normalize_key("API-KEY") == "apikey"
        assert "apikey" in NORMALIZED_SUBSTRINGS
        assert "databrickstoken" in NORMALIZED_EXACT

    def test_normalized_key_variants(self, capture_logs: StringIO, clean_redact_env: None) -> None:
        """API_KEY, API-KEY, API.KEY are all treated as sensitive via normalized substring match."""
        logger = get_logger("test", level="DEBUG")
        logger.info(
            "Config",
            extra_fields={
                "API_KEY": "secret1",
                "API-KEY": "secret2",
                "api.key": "secret3",
            },
        )
        output = capture_logs.getvalue()
        assert f"API_KEY={REDACTED_PLACEHOLDER}" in output
        assert f"API-KEY={REDACTED_PLACEHOLDER}" in output
        assert f"api.key={REDACTED_PLACEHOLDER}" in output
        assert "secret1" not in output
        assert "secret2" not in output
        assert "secret3" not in output

    def test_exact_match_normalized(self, capture_logs: StringIO, clean_redact_env: None) -> None:
        """Exact-match keys with different separators match via normalization (e.g. DATABRICKS-TOKEN)."""
        logger = get_logger("test", level="DEBUG")
        logger.info("Auth", extra_fields={"DATABRICKS-TOKEN": "pat-xxx"})
        output = capture_logs.getvalue()
        assert f"DATABRICKS-TOKEN={REDACTED_PLACEHOLDER}" in output
        assert "pat-xxx" not in output

    def test_env_key_normalized(self, capture_logs: StringIO, clean_redact_env: None) -> None:
        """SPINE_SENSITIVE_KEYS with dots; MY-API-KEY variant is redacted via normalization."""
        os.environ[ENV_SENSITIVE_KEYS] = "MY.API.KEY"
        logger = get_logger("test", level="DEBUG")
        logger.info("Data", extra_fields={"MY-API-KEY": "val"})
        output = capture_logs.getvalue()
        assert f"MY-API-KEY={REDACTED_PLACEHOLDER}" in output
        assert "val" not in output
