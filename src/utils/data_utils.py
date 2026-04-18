"""
Data manipulation utilities.
"""

import json
from typing import Any, Dict, List, Optional, Tuple

from src.config.config_models import ResourceConfig


def get_nested_value(data: Dict[str, Any], key_path: str, required: bool = False) -> Any:
    """
    Get a value from a nested dictionary using dot notation.

    Args:
        data: Dictionary to search in
        key_path: Dot-separated path to the value (e.g., "data.list")
        required: Whether the field is required (raises error if not found)

    Returns:
        Any: The value at the specified path, or None if not found

    Raises:
        KeyError: If field is required but not found
    """
    try:
        current = data
        for key in key_path.split("."):
            current = current[key]
        return current
    except (KeyError, TypeError):
        if required:
            raise KeyError(f"Required field '{key_path}' not found in data") from None
        return None


def dict_response_key_to_records(
    data: Dict[str, Any], response_key: str
) -> Tuple[List[Any], bool]:
    """
    Extract records from a dict API payload using ``response_key`` (dot paths via ``get_nested_value``).

    Returns:
        ``(records, missing_path)``. ``missing_path`` is True when the path is absent or ``None``.
        Otherwise ``records`` is the list at the path, or a single-element list wrapping a dict or scalar.
    """
    result_data = get_nested_value(data, response_key)
    if result_data is None:
        return [], True
    if isinstance(result_data, list):
        return result_data, False
    return [result_data], False


def set_nested_value(data: Dict[str, Any], key_path: str, value: Any) -> None:
    """
    Set a value in a nested dictionary using dot notation.

    Creates intermediate dictionaries as needed.

    Args:
        data: Dictionary to update (modified in-place)
        key_path: Dot-separated path to the value (e.g., "Paging.PageNo")
        value: Value to set at the specified path

    Example:
        >>> d = {}
        >>> set_nested_value(d, "Paging.PageNo", 2)
        >>> d
        {'Paging': {'PageNo': 2}}
    """
    keys = key_path.split(".")
    current = data

    # Navigate/create intermediate dictionaries
    for key in keys[:-1]:
        if key not in current:
            current[key] = {}
        elif not isinstance(current[key], dict):
            # If the intermediate value is not a dict, we can't set nested value
            raise TypeError(
                f"Cannot set nested value at '{key_path}': "
                f"'{key}' is {type(current[key]).__name__}, not a dict"
            )
        current = current[key]

    # Set the final value
    current[keys[-1]] = value


def build_params_json(
    resource_config: ResourceConfig, parent_context: Dict[str, Any]
) -> Optional[str]:
    """Build _params JSON string from parent context for parameter tracking."""
    params = {
        f"{sc.source}__{sc.field}": [str(parent_context[sc.field])]
        for input_config in resource_config.request_inputs.values()
        if (sc := input_config.get_source_config()) and sc.field in parent_context
    }
    return json.dumps(params) if params else None


def build_parent_context_from_parameters(
    resource_config: ResourceConfig, parameters: Dict[str, Any]
) -> Dict[str, Any]:
    """Build parent context dictionary from resolved parameters."""
    return {
        sc.field: param_value[0] if isinstance(param_value, list) and param_value else param_value
        for input_name, input_config in resource_config.request_inputs.items()
        if (sc := input_config.get_source_config()) and (param_value := parameters.get(input_name))
    }


def get_include_as_field_params(resource_config: ResourceConfig) -> Dict[str, str]:
    """
    Get mapping of field names to input names for inputs that should be included as fields.

    Returns:
        Dict mapping field_name -> input_name for inputs with include_as_field=True
    """
    result = {}
    for _input_name, input_config in resource_config.request_inputs.items():
        if input_config.include_as_field and (sc := input_config.get_source_config()):
            field_name = f"_{sc.field}" if not sc.field.startswith("_") else sc.field
            result[field_name] = sc.field
    return result


def add_iteration_context_to_record(
    record: Dict[str, Any],
    parent_context: Dict[str, Any],
    include_field_map: Optional[Dict[str, str]] = None,
    exclude_fields: Optional[set] = None,
) -> None:
    """
    Add iteration context fields from parent context to a record (modified in-place).
    Only adds fields that are explicitly configured with include_as_field=True.

    Args:
        record: Record to update in-place
        parent_context: Parent context dictionary
        include_field_map: Dict mapping output field names (with "_" prefix) to parent context field names
        exclude_fields: Optional set of field names to exclude (e.g., explicitly mapped fields)
    """
    exclude_fields = exclude_fields or set()
    include_field_map = include_field_map or {}

    # Only add fields that are configured to be included
    for output_field_name, context_field_name in include_field_map.items():
        if output_field_name not in exclude_fields and context_field_name in parent_context:
            val = parent_context[context_field_name]
            if isinstance(val, (str, int, float, bool)):
                record[output_field_name] = str(val) if val is not None else None
            elif isinstance(val, (dict, list)):
                record[output_field_name] = json.dumps(val) if val is not None else None
