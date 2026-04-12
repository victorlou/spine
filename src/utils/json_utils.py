"""
Utilities for JSON parsing and handling.
Provides centralized JSON operations to avoid duplication.
"""

import json
from typing import Any, Optional

from src.utils.logger import get_logger

logger = get_logger(__name__)


def safe_json_parse(value: Any, default: Any = None) -> Any:
    """
    Safely parse a JSON string, returning the default value if parsing fails.

    Args:
        value: Value to parse (string or already parsed object)
        default: Default value to return if parsing fails

    Returns:
        Any: Parsed JSON object or default value
    """
    if not isinstance(value, str):
        return value

    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return default


def parse_json_array(value: Any) -> Any:
    """
    Parse a JSON array string, commonly used for environment variables.

    This handles the common pattern where arrays are stored as JSON strings
    like '["item1", "item2"]' in environment variables or configuration.

    Args:
        value: Value to parse (string or already parsed array)

    Returns:
        Any: Parsed array if successful, otherwise returns the original value

    Examples:
        >>> parse_json_array('["a", "b", "c"]')
        ['a', 'b', 'c']
        >>> parse_json_array('[1, 2, 3]')
        [1, 2, 3]
        >>> parse_json_array('not-json')
        'not-json'
    """
    if not isinstance(value, str):
        return value

    # Check if it looks like a JSON array
    if not (value.startswith("[") and value.endswith("]")):
        return value

    try:
        parsed = json.loads(value)
        # Only return if it's actually an array
        return parsed if isinstance(parsed, list) else value
    except json.JSONDecodeError:
        logger.trace(
            "Failed to parse JSON array string",
            extra_fields={"value": value[:100]},  # Limit logging length
        )
        return value


def parse_json_object(value: Any) -> Any:
    """
    Parse a JSON object string, commonly used for complex configuration.

    This handles the common pattern where objects are stored as JSON strings
    like '{"key": "value"}' in environment variables or configuration.

    Args:
        value: Value to parse (string or already parsed object)

    Returns:
        Any: Parsed object if successful, otherwise returns the original value

    Examples:
        >>> parse_json_object('{"name": "test"}')
        {'name': 'test'}
        >>> parse_json_object('not-json')
        'not-json'
    """
    if not isinstance(value, str):
        return value

    # Check if it looks like a JSON object
    if not (value.startswith("{") and value.endswith("}")):
        return value

    try:
        parsed = json.loads(value)
        # Only return if it's actually an object
        return parsed if isinstance(parsed, dict) else value
    except json.JSONDecodeError:
        logger.trace(
            "Failed to parse JSON object string",
            extra_fields={"value": value[:100]},  # Limit logging length
        )
        return value


def is_json_string(value: Any) -> bool:
    """
    Check if a value is a valid JSON string.

    Args:
        value: Value to check

    Returns:
        bool: True if the value is a valid JSON string
    """
    if not isinstance(value, str):
        return False

    try:
        json.loads(value)
        return True
    except json.JSONDecodeError:
        return False


def json_serialize(value: Any, default: Optional[str] = None) -> Optional[str]:
    """
    Safely serialize a value to JSON string.

    Args:
        value: Value to serialize
        default: Default value to return if serialization fails

    Returns:
        Optional[str]: JSON string or default value
    """
    try:
        return json.dumps(value)
    except (TypeError, ValueError) as e:
        logger.warning(
            "Failed to serialize value to JSON",
            extra_fields={"error": str(e), "type": type(value).__name__},
        )
        return default
