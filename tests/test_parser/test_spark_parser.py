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


def test_get_request_value_returns_none_on_missing_or_invalid_paths() -> None:
    parser = SparkParser(
        config=ResourceConfig(),
        spark=MagicMock(),
        source_name="s",
        resource_name="r",
        execution_plan=_plan(),
        redis_context=MagicMock(),
    )
    assert parser._get_request_value("x.y", "parameters", request_context={}) is None
    assert (
        parser._get_request_value(
            "x.y",
            "parameters",
            request_context={"parameters": {"x": "not-a-dict"}},
        )
        is None
    )
    assert (
        parser._get_request_value(
            "x",
            "parameters",
            data_type="integer",
            request_context={"parameters": {"x": "bad-int"}},
        )
        is None
    )


def test_apply_transformations_skips_existing_request_column(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
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
    df = MagicMock()
    df.columns = ["ctx", "a"]
    df.withColumn = MagicMock(return_value=df)
    monkeypatch.setattr("src.parser.spark_parser.lit", lambda _x: MagicMock())

    out = parser._apply_transformations(df, {"parameters": {"k": "v"}})
    assert out is df
    df.withColumn.assert_not_called()


def test_extract_records_and_schema_response_key_non_dict_returns_empty() -> None:
    parser = SparkParser(
        config=ResourceConfig(response_key="data"),
        spark=MagicMock(),
        source_name="s",
        resource_name="r",
        execution_plan=_plan(),
        redis_context=MagicMock(),
    )
    out = parser._extract_records_and_schema({"data": [1, 2, 3]}, None)
    assert out["records"] == []


def test_extract_records_single_item_list_not_re_extracted() -> None:
    """A list with one item must not have response_key applied again.

    RestService already extracts the response_key before passing data to the
    parser.  When an account has exactly one campaign the parser receives
    [{"sub_request_status": "SUCCESS", "campaign": {...}}].  The old code
    hit the ``len(records) == 1`` branch and tried to find "campaigns" inside
    that wrapper dict, found nothing, and returned 0 records.
    """
    fields = [
        SchemaField(name="id", source="campaign.id"),
        SchemaField(name="name", source="campaign.name"),
    ]
    parser = SparkParser(
        config=ResourceConfig(response_key="campaigns", fields=fields),
        spark=MagicMock(),
        source_name="snapchat_ads",
        resource_name="campaigns",
        execution_plan=_plan(),
        redis_context=MagicMock(),
    )
    # Already-extracted list with exactly one item (the campaign wrapper dict)
    data = [{"sub_request_status": "SUCCESS", "campaign": {"id": "abc-123", "name": "Test"}}]
    out = parser._extract_records_and_schema(data, None)
    assert len(out["records"]) == 1
    assert out["records"][0]["id"] == "abc-123"
    assert out["records"][0]["name"] == "Test"


def test_extract_records_raw_dict_response_key_unwrapped() -> None:
    """When the parser receives a raw response dict it must still unwrap response_key."""
    fields = [SchemaField(name="id", source="campaign.id")]
    parser = SparkParser(
        config=ResourceConfig(response_key="campaigns", fields=fields),
        spark=MagicMock(),
        source_name="snapchat_ads",
        resource_name="campaigns",
        execution_plan=_plan(),
        redis_context=MagicMock(),
    )
    # Raw full response dict — needs response_key unwrapping
    data = {
        "request_status": "SUCCESS",
        "campaigns": [{"campaign": {"id": "abc-123"}}],
    }
    out = parser._extract_records_and_schema(data, None)
    assert len(out["records"]) == 1
    assert out["records"][0]["id"] == "abc-123"


def test_parse_ensure_param_values_skips_when_output_field_missing() -> None:
    ensure_cfg = {"enabled": True, "param_name": "id", "output_field": "missing_col"}
    parser = SparkParser(
        config=ResourceConfig(ensure_param_values_in_output=ensure_cfg),
        spark=MagicMock(),
        source_name="s",
        resource_name="r",
        execution_plan=_plan(),
        redis_context=MagicMock(),
    )
    base_df = MagicMock()
    base_df.columns = ["id"]
    parser.spark.createDataFrame.side_effect = [base_df, MagicMock()]
    parser._get_request_value = MagicMock(return_value='["1","2"]')  # type: ignore[method-assign]

    out = parser.parse([{"id": "1"}])
    assert out is base_df


def test_parse_ensure_param_values_inner_error_is_swallowed() -> None:
    ensure_cfg = {"enabled": True, "param_name": "id", "output_field": "id"}
    parser = SparkParser(
        config=ResourceConfig(ensure_param_values_in_output=ensure_cfg),
        spark=MagicMock(),
        source_name="s",
        resource_name="r",
        execution_plan=_plan(),
        redis_context=MagicMock(),
    )
    base_df = MagicMock()
    base_df.columns = ["id"]
    base_df.alias.return_value = base_df
    params_df = MagicMock()
    params_df.alias.return_value = params_df
    parser.spark.createDataFrame.side_effect = [base_df, params_df]
    parser._get_request_value = MagicMock(return_value='["1"]')  # type: ignore[method-assign]
    params_df.join.side_effect = RuntimeError("join failed")

    out = parser.parse([{"id": "1"}])
    assert out is base_df


def test_parse_ensure_param_values_success_join_branch() -> None:
    ensure_cfg = {"enabled": True, "param_name": "id", "output_field": "id"}
    parser = SparkParser(
        config=ResourceConfig(ensure_param_values_in_output=ensure_cfg),
        spark=MagicMock(),
        source_name="s",
        resource_name="r",
        execution_plan=_plan(),
        redis_context=MagicMock(),
    )
    base_df = MagicMock()
    base_df.columns = ["id", "name"]
    base_df.__getitem__ = MagicMock(return_value=MagicMock())
    base_df.alias.return_value = base_df
    params_df = MagicMock()
    params_df.alias.return_value = params_df
    params_df.__getitem__ = MagicMock(return_value=MagicMock())
    joined = MagicMock()
    result_df = MagicMock()
    joined.select.return_value = result_df
    params_df.alias.return_value.join.return_value = joined
    parser.spark.createDataFrame.side_effect = [base_df, params_df]
    parser._get_request_value = MagicMock(return_value='["1","2"]')  # type: ignore[method-assign]

    out = parser.parse([{"id": "1", "name": "A"}])
    assert out is result_df
    joined.select.assert_called_once()


def test_parse_ensure_param_values_skips_when_no_values_found() -> None:
    ensure_cfg = {"enabled": True, "param_name": "id", "output_field": "id"}
    parser = SparkParser(
        config=ResourceConfig(ensure_param_values_in_output=ensure_cfg),
        spark=MagicMock(),
        source_name="s",
        resource_name="r",
        execution_plan=_plan(),
        redis_context=MagicMock(),
    )
    base_df = MagicMock()
    base_df.columns = ["id"]
    parser.spark.createDataFrame.return_value = base_df
    parser._get_request_value = MagicMock(return_value=None)  # type: ignore[method-assign]

    out = parser.parse([{"id": "1"}])
    assert out is base_df
    assert parser.spark.createDataFrame.call_count == 1


def test_apply_transformations_add_column_from_request_missing_value_skips(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
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
    parser._get_request_value = MagicMock(return_value=None)  # type: ignore[method-assign]
    df = MagicMock()
    df.columns = ["a"]
    df.withColumn = MagicMock(return_value=df)
    monkeypatch.setattr("src.parser.spark_parser.lit", lambda _x: MagicMock())
    out = parser._apply_transformations(df, {"parameters": {}})
    assert out is df
    df.withColumn.assert_not_called()


@pytest.mark.parametrize(
    "data_type,ctx,expected",
    [
        ("integer", {"parameters": {"k": [1]}}, "1"),
        ("float", {"parameters": {"k": [1.5]}}, "1.5"),
        ("array", {"parameters": {"k": "x"}}, '["x"]'),
    ],
)
def test_get_request_value_conversion_branches(data_type: str, ctx: dict, expected: str) -> None:
    parser = SparkParser(
        config=ResourceConfig(),
        spark=MagicMock(),
        source_name="s",
        resource_name="r",
        execution_plan=_plan(),
        redis_context=MagicMock(),
    )
    out = parser._get_request_value("k", "parameters", data_type=data_type, request_context=ctx)
    assert out == expected
