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
    with pytest.raises(ValidationError, match=r"(?i)predicates|partition_column"):
        TableReadOptions(
            partition_column="id",
            lower_bound=1,
            upper_bound=10,
            num_partitions=2,
            predicates=["x = 1"],
        )


def test_table_read_options_range_missing_bounds() -> None:
    with pytest.raises(ValidationError, match=r"(?i)lower_bound|upper_bound"):
        TableReadOptions(partition_column="id", num_partitions=4)


def test_table_read_options_range_rejects_inverted_bounds() -> None:
    with pytest.raises(
        ValidationError, match=r"(?i)lower_bound.*upper_bound|upper_bound.*lower_bound"
    ):
        TableReadOptions(
            partition_column="id",
            lower_bound=100,
            upper_bound=1,
            num_partitions=4,
        )


def test_table_read_options_empty_predicates() -> None:
    with pytest.raises(ValidationError, match=r"(?i)predicates"):
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


def test_hana_allows_table_read_options() -> None:
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
                table_read_options=TableReadOptions(
                    partition_column="id",
                    lower_bound=0,
                    upper_bound=99,
                    num_partitions=2,
                ),
            )
        },
    )


def test_use_on_incremental_warm_defaults_false() -> None:
    assert TableReadOptions(fetch_size=1000).use_on_incremental_warm is False


def test_effective_for_incremental_warm_jdbc_read_returns_self_when_use_on_true() -> None:
    opts = TableReadOptions(predicates=["a=1"], use_on_incremental_warm=True)
    eff = opts.effective_for_incremental_warm_jdbc_read()
    assert eff is opts
    assert eff.predicates == ["a=1"]


def test_effective_for_incremental_warm_jdbc_read_strips_predicates_by_default() -> None:
    opts = TableReadOptions(
        fetch_size=5000,
        predicates=["x=1", "x=2"],
    )
    eff = opts.effective_for_incremental_warm_jdbc_read()
    assert eff is not opts
    assert eff.fetch_size == 5000
    assert eff.predicates is None
    assert not eff.uses_parallel_read()


def test_effective_for_incremental_warm_jdbc_read_strips_range_mode_by_default() -> None:
    opts = TableReadOptions(
        fetch_size=100,
        partition_column="id",
        lower_bound=0,
        upper_bound=10,
        num_partitions=4,
    )
    eff = opts.effective_for_incremental_warm_jdbc_read()
    assert eff is not opts
    assert eff.fetch_size == 100
    assert eff.partition_column is None
    assert eff.lower_bound is None
    assert eff.upper_bound is None
    assert eff.num_partitions is None
    assert not eff.uses_parallel_read()
