"""Read incremental high-water marks from written table destinations (Delta, Iceberg)."""

from __future__ import annotations

from typing import Optional

from pyspark.sql import SparkSession

from src.config.config_models import LoadingConfig, SourceType
from src.load_strategy.load_strategy_factory import LoadStrategyFactory
from src.loader.loader_factory import LoaderFactory
from src.loader.object_store import SparkFilesystemObjectStore, loading_base_uri
from src.utils.exceptions import HandlerError, LoaderError


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


def read_max_cursor_string_from_destination(
    spark: SparkSession,
    loading: LoadingConfig,
    source_type: Optional[SourceType],
    column: str,
) -> Optional[str]:
    """
    Return ``MAX(column)`` as a string from the written table (Delta path or Iceberg catalog),
    or ``None`` when the table is missing or has no rows or only nulls for that column.

    Delegates to the active :class:`~src.load_strategy.base_load_strategy.BaseLoadStrategy`.
    """
    base_uri = loading_base_uri(loading)
    store = SparkFilesystemObjectStore(spark)
    strategy = LoadStrategyFactory.create_load_strategy(
        spark,
        store,
        base_uri,
        loading,
        source_type.value if source_type is not None else None,
    )
    try:
        return strategy.read_max_column_as_string(column)
    except LoaderError as e:
        raise HandlerError(str(e), operation="incremental_cursor") from e
