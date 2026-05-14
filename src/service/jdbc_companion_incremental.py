"""
JDBC companion-table incremental extract: join key discovery and bounded SELECT SQL.

Used by database handlers with ``incremental_extract.kind == jdbc_companion_cdc``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, List, Optional, Sequence

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


def quote_sql_ident(name: str) -> str:
    """Double-quote a SQL identifier and escape embedded quotes (HANA / PostgreSQL style)."""
    return '"' + str(name).replace('"', '""') + '"'


def escape_sql_string_literal(value: str) -> str:
    """Single-quoted SQL string literal with standard quote doubling."""
    return "'" + str(value).replace("'", "''") + "'"


def qualified_table_sql(dialect: str, schema: str, table: str) -> str:
    """Return ``schema.table`` fragment for use inside ``SELECT ... FROM``."""
    if not schema or not str(schema).strip():
        return f"{quote_sql_ident(table)}"
    return f"{quote_sql_ident(schema)}.{quote_sql_ident(table)}"


def jdbc_probe_column_names(
    spark: "SparkSession",
    jdbc_url: str,
    connection_properties: dict,
    dialect: str,
    schema: str,
    table: str,
) -> List[str]:
    """
    Return column names from a JDBC table using ``WHERE 1=0`` (empty result, valid schema).
    """
    from_ref = qualified_table_sql(dialect, schema, table)
    probe = f"(SELECT * FROM {from_ref} WHERE 1=0) spine_schema_probe"
    df = spark.read.jdbc(url=jdbc_url, table=probe, properties=connection_properties)
    return list(df.columns)


def discover_join_column_names(
    spark: "SparkSession",
    jdbc_url: str,
    connection_properties: dict,
    dialect: str,
    main_schema: str,
    main_table: str,
    companion_schema: str,
    companion_table: str,
    watermark_column: str,
    companion_metadata_columns: Optional[Sequence[str]],
    explicit_join_columns: Optional[Sequence[str]],
) -> List[str]:
    """
    Infer equi-join keys as the intersection of main and companion columns, excluding
    the watermark and companion-only metadata names. ``explicit_join_columns`` overrides inference.
    """
    if explicit_join_columns:
        keys = [str(c).strip() for c in explicit_join_columns if str(c).strip()]
        if not keys:
            raise ValueError(
                "incremental_extract.correlation.join_columns resolved to an empty list"
            )
        return keys

    main_cols = jdbc_probe_column_names(
        spark, jdbc_url, connection_properties, dialect, main_schema, main_table
    )
    cdc_cols = jdbc_probe_column_names(
        spark, jdbc_url, connection_properties, dialect, companion_schema, companion_table
    )
    cdc_set = set(cdc_cols)
    meta = {str(x).strip() for x in (companion_metadata_columns or []) if x and str(x).strip()}
    wm = str(watermark_column).strip()

    keys: List[str] = []
    for col in main_cols:
        if col == wm:
            continue
        if col in meta and col != wm:
            continue
        if col in cdc_set:
            keys.append(col)
    if not keys:
        raise ValueError(
            "Could not infer incremental join keys from JDBC schema; set "
            "incremental_extract.correlation.join_columns explicitly."
        )
    return keys


def _join_equality_sql(join_keys: List[str]) -> str:
    parts = [f"m.{quote_sql_ident(k)} = c.{quote_sql_ident(k)}" for k in join_keys]
    return " AND ".join(parts)


def build_jdbc_companion_incremental_select_sql(
    dialect: str,
    main_schema: str,
    main_table: str,
    companion_schema: str,
    companion_table: str,
    join_keys: List[str],
    watermark_column: str,
    cursor_literal_sql: str,
    join_predicate: Optional[str],
    main_where_predicate: Optional[str] = None,
) -> str:
    """
    Build a plain ``SELECT`` (no outer parentheses) bounded by companion watermark.

    Args:
        dialect: ``hana`` or ``postgresql`` (identifier quoting is the same here).
        cursor_literal_sql: Already escaped literal or expression for SQL (e.g. result of
            :func:`escape_sql_string_literal`).
        join_predicate: Optional SQL boolean with aliases ``m`` (main) and ``c`` (companion).
        main_where_predicate: Optional boolean on the main alias ``m`` (same fragment as
            ``database_where_predicate`` on the resource).
    """
    main_ref = qualified_table_sql(dialect, main_schema, main_table)
    cdc_ref = qualified_table_sql(dialect, companion_schema, companion_table)
    wm = quote_sql_ident(watermark_column)

    if join_predicate:
        pred = join_predicate.strip()
        exists_join = f"({pred})"
        join_sql = pred
    else:
        join_sql = _join_equality_sql(join_keys)
        exists_join = join_sql

    exists_clause = (
        f"EXISTS (SELECT 1 FROM {cdc_ref} c WHERE {exists_join} AND c.{wm} > {cursor_literal_sql})"
    )

    outer_parts = [exists_clause]
    extra = (main_where_predicate or "").strip()
    if extra:
        outer_parts.append(f"({extra})")
    return f"SELECT m.* FROM {main_ref} m WHERE " + " AND ".join(outer_parts)
