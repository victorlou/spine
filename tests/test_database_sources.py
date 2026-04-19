"""Tests for relational database source configuration models."""

import pytest
from pydantic import ValidationError

from src.config.config_models import ResourceConfig, SchemaField, SourceConfig, SourceType


def _db_resource() -> ResourceConfig:
    return ResourceConfig(
        method="GET",
        database_schema="public",
        database_table="users",
        fields=[SchemaField(name="id", source="id")],
    )


def test_postgresql_source_rejects_missing_host():
    with pytest.raises(ValidationError, match="requires host"):
        SourceConfig(
            type=SourceType.POSTGRESQL,
            port=5432,
            username="u",
            password="p",
            database="d",
            resources={"r": _db_resource()},
        )


def test_postgresql_source_rejects_resource_without_database_table():
    with pytest.raises(ValidationError, match="database_schema and database_table"):
        SourceConfig(
            type=SourceType.POSTGRESQL,
            host="localhost",
            port=5432,
            username="u",
            password="p",
            database="d",
            resources={
                "r": ResourceConfig(
                    method="GET",
                    database_schema="public",
                    fields=[SchemaField(name="id", source="id")],
                )
            },
        )


def test_postgresql_source_valid():
    cfg = SourceConfig(
        type=SourceType.POSTGRESQL,
        host="localhost",
        port=5432,
        username="u",
        password="p",
        database="d",
        resources={"users": _db_resource()},
    )
    assert cfg.type == SourceType.POSTGRESQL


def test_hana_source_valid():
    cfg = SourceConfig(
        type=SourceType.HANA,
        host="hana.example",
        port="30015",
        username="u",
        password="p",
        database="HDB",
        resources={"dim": _db_resource()},
    )
    assert cfg.type == SourceType.HANA


def test_hana_source_valid_without_database():
    cfg = SourceConfig(
        type=SourceType.HANA,
        host="hana.example",
        port="30244",
        username="u",
        password="p",
        database=None,
        resources={"dim": _db_resource()},
    )
    assert cfg.type == SourceType.HANA
    assert cfg.database is None


def test_postgresql_source_rejects_empty_database():
    with pytest.raises(ValidationError, match="requires database"):
        SourceConfig(
            type=SourceType.POSTGRESQL,
            host="localhost",
            port=5432,
            username="u",
            password="p",
            database="",
            resources={"r": _db_resource()},
        )
