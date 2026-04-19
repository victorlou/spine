"""Tests for ``TableReadOptions`` and related source validation."""

import pytest
from pydantic import ValidationError

from src.config.config_models import (
    ResourceConfig,
    SourceConfig,
    SourceType,
    TableReadOptions,
)


def test_table_read_options_range_valid() -> None:
    c = TableReadOptions(
        partition_column="id",
        lower_bound=1,
        upper_bound=1000,
        num_partitions=4,
    )
    assert c.uses_parallel_read()


def test_table_read_options_predicates_valid() -> None:
    c = TableReadOptions(predicates=["id < 500", "id >= 500"])
    assert c.uses_parallel_read()


def test_table_read_options_mutually_exclusive() -> None:
    with pytest.raises(ValidationError):
        TableReadOptions(
            partition_column="id",
            lower_bound=1,
            upper_bound=10,
            num_partitions=2,
            predicates=["x = 1"],
        )


def test_table_read_options_range_missing_bounds() -> None:
    with pytest.raises(ValidationError):
        TableReadOptions(partition_column="id", num_partitions=4)


def test_table_read_options_range_rejects_inverted_bounds() -> None:
    with pytest.raises(ValidationError) as exc:
        TableReadOptions(
            partition_column="id",
            lower_bound=100,
            upper_bound=1,
            num_partitions=4,
        )
    assert "lower_bound" in str(exc.value).lower() and "upper_bound" in str(exc.value).lower()


def test_table_read_options_empty_predicates() -> None:
    with pytest.raises(ValidationError):
        TableReadOptions(predicates=[])


def test_table_read_options_fetch_only() -> None:
    c = TableReadOptions(fetch_size=1000)
    assert not c.uses_parallel_read()


def test_postgres_allows_table_read_options() -> None:
    SourceConfig(
        type=SourceType.POSTGRESQL,
        host="localhost",
        port=5432,
        username="u",
        password="p",
        database="db",
        resources={
            "t": ResourceConfig(
                method="GET",
                database_schema="public",
                database_table="t",
                table_read_options=TableReadOptions(
                    partition_column="id",
                    lower_bound=0,
                    upper_bound=99,
                    num_partitions=2,
                ),
            )
        },
    )


def test_hana_rejects_table_read_options() -> None:
    with pytest.raises(ValidationError) as exc:
        SourceConfig(
            type=SourceType.HANA,
            host="localhost",
            port=30015,
            username="u",
            password="p",
            resources={
                "t": ResourceConfig(
                    method="GET",
                    database_schema="S",
                    database_table="T",
                    table_read_options=TableReadOptions(fetch_size=500),
                )
            },
        )
    msg = str(exc.value).lower()
    assert "table_read_options" in msg or "spark" in msg or "hana" in msg
