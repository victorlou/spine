"""Extra branches for query validation and ref key helper."""

from src.utils.query_utils import format_query_ref_key, validate_query_content


def test_validate_query_rejects_non_string_or_empty() -> None:
    assert validate_query_content("") is False
    assert validate_query_content("   ") is False
    assert validate_query_content(None) is False  # type: ignore[arg-type]


def test_validate_query_rejects_dangerous_keywords() -> None:
    assert validate_query_content("INSERT INTO t SELECT 1") is False
    assert validate_query_content("SELECT 1 UNION SELECT 2") is False


def test_validate_query_requires_select_or_with_prefix() -> None:
    assert validate_query_content("EXPLAIN SELECT 1") is False


def test_validate_query_rejects_comments_and_stacked_statements() -> None:
    assert validate_query_content("SELECT 1 -- no comments allowed") is False
    assert validate_query_content("SELECT 1; SELECT 2") is False
    assert validate_query_content("/**/ SELECT 1") is False


def test_format_query_ref_key() -> None:
    assert format_query_ref_key("my.q") == "databricks_query:my.q"
