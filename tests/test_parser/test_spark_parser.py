"""Tests for ``SparkParser`` using mocked Spark and plan boundaries."""

import json
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from src.config.config_models import ResourceConfig, SchemaField, Transformation, TransformationType
from src.parser.spark_parser import SparkParser


def _plan(**kwargs) -> MagicMock:
    p = MagicMock()
    p.has_parent_inputs = MagicMock(return_value=kwargs.get("has_parent", False))
    return p


def test_extract_all_fields_coerces_types() -> None:
    parser = SparkParser(
        config=ResourceConfig(),
        spark=MagicMock(),
        source_name="api",
        resource_name="r",
        execution_plan=_plan(),
        redis_context=MagicMock(),
    )
    out = parser._extract_all_fields(
        {
            "n": 1,
            "d": {"x": 1},
            "l": [1, 2],
        }
    )
    assert out["n"] == "1"
    assert out["d"] == json.dumps({"x": 1})
    assert out["l"] == json.dumps([1, 2])


def test_build_target_schema_with_fields_and_transforms() -> None:
    config = ResourceConfig(
        fields=[SchemaField(name="id", source="id")],
        transformations=[
            Transformation(type=TransformationType.ADD_COLUMN, name="ts", value="{{ now_iso() }}")
        ],
    )
    plan = _plan()
    plan.has_parent_inputs = MagicMock(return_value=True)
    parser = SparkParser(
        config=config,
        spark=MagicMock(),
        source_name="s",
        resource_name="r",
        execution_plan=plan,
        redis_context=MagicMock(),
    )
    schema = parser._build_target_schema()
    names = [f.name for f in schema.fields]
    assert "_params" in names
    assert "id" in names
    assert "ts" in names


def test_apply_transformations_add_column(monkeypatch) -> None:
    config = ResourceConfig(
        transformations=[
            Transformation(type=TransformationType.ADD_COLUMN, name="c1", value="static")
        ],
    )
    parser = SparkParser(
        config=config,
        spark=MagicMock(),
        source_name="s",
        resource_name="r",
        execution_plan=_plan(),
        redis_context=MagicMock(),
    )
    monkeypatch.setattr(
        "src.parser.spark_parser.get_resolver", lambda _r: SimpleNamespace(resolve=lambda v: v)
    )
    monkeypatch.setattr("src.parser.spark_parser.lit", lambda _x: MagicMock())
    df = MagicMock()
    df.columns = ["a"]
    df.withColumn = MagicMock(return_value=df)
    out = parser._apply_transformations(df, None)
    assert out is df
    df.withColumn.assert_called_once()


def test_apply_transformations_add_column_from_request(monkeypatch: pytest.MonkeyPatch) -> None:
    config = ResourceConfig(
        transformations=[
            Transformation(
                type=TransformationType.ADD_COLUMN_FROM_REQUEST,
                name="ctx",
                source="k",
                location="parameters",
                data_type="string",
            )
        ],
    )
    parser = SparkParser(
        config=config,
        spark=MagicMock(),
        source_name="s",
        resource_name="r",
        execution_plan=_plan(),
        redis_context=MagicMock(),
    )
    parser._get_request_value = MagicMock(return_value="v")  # type: ignore[method-assign]
    df = MagicMock()
    df.columns = ["a"]
    df.withColumn = MagicMock(return_value=df)
    monkeypatch.setattr("src.parser.spark_parser.lit", lambda _x: MagicMock())
    out = parser._apply_transformations(df, {"k": "v"})
    assert out is df


def test_extract_records_empty_data() -> None:
    parser = SparkParser(
        config=ResourceConfig(),
        spark=MagicMock(),
        source_name="s",
        resource_name="r",
        execution_plan=_plan(),
        redis_context=MagicMock(),
    )
    res = parser._extract_records_and_schema([], None)
    assert res["records"] == []


def test_log_dataframe_info_swallows_errors() -> None:
    parser = SparkParser(
        config=ResourceConfig(),
        spark=MagicMock(),
        source_name="s",
        resource_name="r",
        execution_plan=_plan(),
        redis_context=MagicMock(),
    )
    bad_df = MagicMock()
    bad_df.limit.side_effect = RuntimeError("boom")
    parser._log_dataframe_info(bad_df, "x")
