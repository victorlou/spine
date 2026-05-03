from typing import Dict, Optional


def get_source_type_prefix(source_type: Optional[str]) -> str:
    """
    Get the storage prefix segment for a source type.

    Known source types are grouped into stable top-level storage folders:
    - `rest_api` -> `rest_api`
    - `python_sdk` -> `sdk`
    - relational database sources such as `postgresql` and `hana` -> `database`
    """
    if not source_type:
        return ""

    type_key = str(source_type.value) if hasattr(source_type, "value") else str(source_type)

    source_type_mapping: Dict[str, str] = {
        "rest_api": "rest_api",
        "python_sdk": "sdk",
        "postgresql": "database",
        "hana": "database",
    }

    return source_type_mapping.get(type_key, "")


def prepend_source_type_prefix(prefix: Optional[str], source_type: Optional[str]) -> str:
    """
    Prepend the mapped source type segment to a storage prefix.
    """
    source_type_prefix = get_source_type_prefix(source_type)

    if not source_type_prefix:
        return prefix or ""

    source_type_prefix = source_type_prefix.strip("/")
    clean_prefix = prefix.strip("/") if prefix else ""

    if clean_prefix:
        return f"{source_type_prefix}/{clean_prefix}"
    return source_type_prefix
