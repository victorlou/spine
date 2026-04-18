"""
Shared helpers for normalizing API payloads using ``response_key``.

REST and Python SDK services and SparkParser must agree on dot-path semantics
(see ``get_nested_value`` in ``data_utils``).
"""

from typing import Any, List, Tuple

from src.utils.data_utils import get_nested_value


def dict_response_key_to_records(data: dict, response_key: str) -> Tuple[List[Any], bool]:
    """
    Extract records from a dict response using ``response_key`` (supports dot paths).

    Args:
        data: Parsed JSON object (mapping).
        response_key: Path into ``data`` (dot-separated segments).

    Returns:
        Tuple of ``(records, missing_path)``.
        ``missing_path`` is True when the path is absent or navigates to ``None``
        (same as ``get_nested_value`` returning ``None``).
        Otherwise ``records`` is a non-missing list: either the list at the path,
        or a single-element list wrapping a dict or scalar value at the path.
    """
    result_data = get_nested_value(data, response_key)
    if result_data is None:
        return [], True
    if isinstance(result_data, list):
        return result_data, False
    return [result_data], False
