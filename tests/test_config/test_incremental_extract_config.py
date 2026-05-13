"""Tests for incremental extract configuration validation."""

import pytest

from src.config.config_models import LoadingConfig, LoadingFormat, ResourceConfig, SchemaField
from src.config.incremental_extract import (
    IncrementalCorrelationConfig,
    IncrementalExtractConfig,
    IncrementalWatermarkCursorConfig,
    IncrementalWatermarkCursorStrategy,
)


def _base_incremental_dict() -> dict:
    return {
        "kind": "jdbc_companion_cdc",
        "companion": {"table": "cdc_t"},
        "watermark": {
            "column": "REQTSN",
            "cursor": {
                "strategy": "destination_column",
                "reference_column": "bill_dt",
            },
        },
        "correlation": {"companion_metadata_columns": ["DATAPAKID", "RECORD", "REQTSN"]},
    }


def test_incremental_rejects_overwrite_loading() -> None:
    with pytest.raises(ValueError, match="overwrite"):
        ResourceConfig(
            method="GET",
            database_schema="s",
            database_table="t",
            fields=[SchemaField(name="bill_dt", source="BILL_DATE")],
            loading=LoadingConfig(
                destination="local",
                format=LoadingFormat.DELTA,
                write_mode="overwrite",
                storage_root="/tmp",
                prefix="p/r",
            ),
            incremental_extract=IncrementalExtractConfig.model_validate(_base_incremental_dict()),
        )


def test_incremental_rejects_database_select_query() -> None:
    with pytest.raises(ValueError, match="database_select_query"):
        ResourceConfig(
            method="GET",
            database_schema="s",
            database_table="t",
            database_select_query="SELECT 1",
            fields=[SchemaField(name="bill_dt", source="BILL_DATE")],
            loading=LoadingConfig(
                destination="local",
                format=LoadingFormat.DELTA,
                write_mode="merge",
                merge_keys=["id"],
                storage_root="/tmp",
                prefix="p/r",
            ),
            incremental_extract=IncrementalExtractConfig.model_validate(_base_incremental_dict()),
        )


def test_incremental_requires_delta_for_destination_column_cursor() -> None:
    with pytest.raises(ValueError, match="delta"):
        ResourceConfig(
            method="GET",
            database_schema="s",
            database_table="t",
            fields=[SchemaField(name="bill_dt", source="BILL_DATE")],
            loading=LoadingConfig(
                destination="local",
                format=LoadingFormat.ICEBERG,
                write_mode="merge",
                merge_keys=["id"],
                storage_root="/tmp",
                prefix="p/r",
            ),
            incremental_extract=IncrementalExtractConfig.model_validate(_base_incremental_dict()),
        )


def test_incremental_reference_column_must_match_field_name() -> None:
    with pytest.raises(ValueError, match="reference_column"):
        ResourceConfig(
            method="GET",
            database_schema="s",
            database_table="t",
            fields=[SchemaField(name="bill_dt", source="BILL_DATE")],
            loading=LoadingConfig(
                destination="local",
                format=LoadingFormat.DELTA,
                write_mode="merge",
                merge_keys=["id"],
                storage_root="/tmp",
                prefix="p/r",
            ),
            incremental_extract=IncrementalExtractConfig.model_validate(
                {
                    **_base_incremental_dict(),
                    "watermark": {
                        "column": "REQTSN",
                        "cursor": {
                            "strategy": "destination_column",
                            "reference_column": "wrong_name",
                        },
                    },
                }
            ),
        )


def test_incremental_rejects_join_columns_and_predicate_together() -> None:
    with pytest.raises(ValueError, match="join_columns or join_predicate"):
        IncrementalCorrelationConfig(join_columns=["a"], join_predicate="m.a = c.a")


def test_table_metadata_cursor_strategy_rejected() -> None:
    with pytest.raises(ValueError, match="table_metadata"):
        IncrementalWatermarkCursorConfig(
            strategy=IncrementalWatermarkCursorStrategy.TABLE_METADATA,
            reference_column="x",
        )


def test_tolerance_requires_yyyymmdd_format() -> None:
    with pytest.raises(ValueError, match="tolerance_calendar_days"):
        IncrementalWatermarkCursorConfig(
            strategy=IncrementalWatermarkCursorStrategy.DESTINATION_COLUMN,
            reference_column="bill_dt",
            reference_format="none",
            tolerance_calendar_days=1,
        )


def test_database_where_rejects_with_database_select_query() -> None:
    with pytest.raises(ValueError, match="database_where_predicate"):
        ResourceConfig(
            method="GET",
            database_schema="s",
            database_table="t",
            database_select_query="SELECT 1 FROM t",
            database_where_predicate="a = 1",
        )
