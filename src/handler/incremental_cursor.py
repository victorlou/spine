"""Read incremental high-water marks from a written Delta table."""

from __future__ import annotations

from typing import Optional

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.config.config_models import LoadingConfig, LoadingFormat, SourceType
from src.load_strategy.load_strategy_factory import LoadStrategyFactory
from src.loader.loader_factory import LoaderFactory
from src.loader.object_store import SparkFilesystemObjectStore, loading_base_uri
from src.utils.exceptions import HandlerError


def resolve_table_location_uri(
    spark: SparkSession,
    loading: LoadingConfig,
    source_type: Optional[SourceType],
) -> str:
    """Resolve the same directory URI used by the load strategy for this loading config."""
    base_uri = loading_base_uri(loading)
    store = SparkFilesystemObjectStore(spark)
    strategy = LoadStrategyFactory.create_load_strategy(
        spark,
        store,
        base_uri,
        loading,
        source_type.value if source_type is not None else None,
    )
    return strategy.resolve_table_location()


def destination_has_data(
    spark: SparkSession,
    loading: LoadingConfig,
    source_type: Optional[SourceType],
) -> bool:
    """True when the loader reports the destination exists (Delta log present, etc.)."""
    loader = LoaderFactory.create_loader(loading)
    if hasattr(loader, "set_spark_session"):
        loader.set_spark_session(spark)
    if not hasattr(loader, "destination_exists"):
        return False
    return bool(
        loader.destination_exists(
            loading, source_type=source_type.value if source_type is not None else None
        )
    )


def resolve_physical_column_name(columns: list[str], logical: str) -> str:
    """Match ``logical`` to a DataFrame column name (case-sensitive, then case-insensitive)."""
    if logical in columns:
        return logical
    want = logical.lower()
    for c in columns:
        if c.lower() == want:
            return c
    raise HandlerError(
        f"Incremental cursor column {logical!r} not found in destination columns: {columns}",
        operation="incremental_cursor",
    )


def apply_incremental_cursor_tolerance(
    max_value: Optional[str],
    *,
    tolerance_calendar_days: int,
    reference_format: str,
) -> Optional[str]:
    """
    Shift ``MAX(reference_column)`` backward by whole calendar days when configured.

    When ``tolerance_calendar_days`` is zero, returns ``max_value`` unchanged (still stripped).
    When positive, ``reference_format`` must be ``yyyymmdd``; parse fails raise ``HandlerError``.
    """
    if max_value is None:
        return None
    text = str(max_value).strip()
    if not text:
        return None
    if tolerance_calendar_days <= 0:
        return text
    if reference_format != "yyyymmdd":
        raise HandlerError(
            "incremental_extract watermark.cursor.tolerance_calendar_days > 0 requires "
            "reference_format 'yyyymmdd'.",
            operation="incremental_cursor",
        )
    if len(text) != 8 or not text.isdigit():
        raise HandlerError(
            f"incremental_extract cursor tolerance requires YYYYMMDD values; got {max_value!r}",
            operation="incremental_cursor",
        )
    from datetime import datetime, timedelta, timezone

    try:
        dt = datetime.strptime(text, "%Y%m%d").replace(tzinfo=timezone.utc)
    except ValueError as e:
        raise HandlerError(
            f"Could not parse reference_column MAX value as YYYYMMDD: {max_value!r}",
            operation="incremental_cursor",
        ) from e
    shifted = dt - timedelta(days=tolerance_calendar_days)
    return shifted.strftime("%Y%m%d")


def read_max_cursor_string_from_delta(
    spark: SparkSession,
    loading: LoadingConfig,
    source_type: Optional[SourceType],
    column: str,
) -> Optional[str]:
    """
    Return ``MAX(column)`` as a string from the Delta table at the configured location, or None
    when the table is missing or has no rows or only nulls for that column.
    """
    if loading.format != LoadingFormat.DELTA:
        raise HandlerError(
            f"Incremental MAX cursor read supports Delta only in v1; got format {loading.format!s}",
            operation="incremental_cursor",
        )
    if not destination_has_data(spark, loading, source_type):
        return None
    path = resolve_table_location_uri(spark, loading, source_type).rstrip("/")
    df = spark.read.format("delta").load(path)
    if len(df.take(1)) == 0:
        return None
    phys = resolve_physical_column_name(list(df.columns), column)
    row = df.select(F.max(F.col(phys)).alias("_mx")).collect()[0]
    val = row["_mx"]
    if val is None:
        return None
    return str(val)
