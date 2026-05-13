"""Tests for ``jdbc_companion_incremental`` SQL helpers."""

from src.service.jdbc_companion_incremental import (
    build_jdbc_companion_incremental_select_sql,
    escape_sql_string_literal,
)


def test_escape_sql_string_literal_doubles_quotes() -> None:
    assert escape_sql_string_literal("a'b") == "'a''b'"


def test_build_incremental_sql_with_join_keys() -> None:
    sql = build_jdbc_companion_incremental_select_sql(
        dialect="hana",
        main_schema="S",
        main_table="M",
        companion_schema="S",
        companion_table="C",
        join_keys=["K1", "K2"],
        watermark_column="WM",
        cursor_literal_sql="'20260101000000'",
        join_predicate=None,
    )
    assert "SELECT m.* FROM" in sql
    assert '"S"."M"' in sql
    assert '"S"."C"' in sql
    assert 'm."K1" = c."K1"' in sql
    assert "c.\"WM\" > '20260101000000'" in sql


def test_build_incremental_sql_join_predicate() -> None:
    sql = build_jdbc_companion_incremental_select_sql(
        dialect="postgresql",
        main_schema="public",
        main_table="main",
        companion_schema="public",
        companion_table="cdc",
        join_keys=[],
        watermark_column="wm",
        cursor_literal_sql="'x'",
        join_predicate='m."id" = c."id"',
        main_where_predicate=None,
    )
    assert 'm."id" = c."id"' in sql
    assert "SELECT m.* FROM" in sql


def test_build_incremental_sql_with_main_where_predicate() -> None:
    sql = build_jdbc_companion_incremental_select_sql(
        dialect="hana",
        main_schema="S",
        main_table="M",
        companion_schema="S",
        companion_table="C",
        join_keys=["K"],
        watermark_column="WM",
        cursor_literal_sql="'0'",
        join_predicate=None,
        main_where_predicate="UPPER(m.\"G\") = 'X'",
    )
    assert "WHERE" in sql
    assert "EXISTS" in sql
    assert "UPPER" in sql
    assert " AND " in sql
