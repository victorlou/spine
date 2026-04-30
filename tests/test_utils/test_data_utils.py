"""Tests for ``src.utils.data_utils`` nested helpers and parent-context builders."""

import json

import pytest

from src.config.config_models import RequestInputConfig, ResourceConfig
from src.utils.data_utils import (
    add_iteration_context_to_record,
    build_params_json,
    build_parent_context_from_parameters,
    dict_response_key_to_records,
    get_include_as_field_params,
    get_nested_value,
    set_nested_value,
)
from src.utils.dynamic_values import ComplexDynamicValue, DynamicSourceReference, DynamicValueType


def test_get_nested_value_paths_and_required() -> None:
    data = {"a": {"b": {"c": 1}}}
    assert get_nested_value(data, "a.b.c") == 1
    assert get_nested_value(data, "a.missing") is None
    assert get_nested_value({"x": []}, "x.y") is None
    with pytest.raises(KeyError, match="Required"):
        get_nested_value({}, "a.b", required=True)


def test_dict_response_key_to_records() -> None:
    assert dict_response_key_to_records({}, "data") == ([], True)
    assert dict_response_key_to_records({"data": [1, 2]}, "data") == ([1, 2], False)
    assert dict_response_key_to_records({"data": {"k": 1}}, "data") == ([{"k": 1}], False)


def test_set_nested_value_creates_intermediate_and_errors() -> None:
    d: dict = {}
    set_nested_value(d, "a.b.c", 3)
    assert d == {"a": {"b": {"c": 3}}}
    set_nested_value(d, "a.x", 9)
    assert d["a"]["x"] == 9
    d["bad"] = "scalar"
    with pytest.raises(TypeError, match="not a dict"):
        set_nested_value(d, "bad.inner", 1)


def test_build_params_json_and_parent_context_from_parameters() -> None:
    rc = ResourceConfig(
        method="GET",
        path="/x",
        response_type="json",
        request_inputs={
            "pid": RequestInputConfig(
                value=ComplexDynamicValue(
                    type=DynamicValueType.SOURCE,
                    source_config=DynamicSourceReference(source="parent", field="id"),
                ),
                location="query",
                batch_size=1,
            ),
        },
    )
    params_json = build_params_json(rc, {"id": "42"})
    assert params_json is not None
    loaded = json.loads(params_json)
    assert "parent__id" in loaded

    ctx = build_parent_context_from_parameters(rc, {"pid": ["99"]})
    assert ctx == {"id": "99"}

    ctx_scalar = build_parent_context_from_parameters(rc, {"pid": "flat"})
    assert ctx_scalar == {"id": "flat"}


def test_get_include_as_field_params_mapping() -> None:
    rc = ResourceConfig(
        method="GET",
        path="/x",
        response_type="json",
        request_inputs={
            "inp": RequestInputConfig(
                value=ComplexDynamicValue(
                    type=DynamicValueType.SOURCE,
                    source_config=DynamicSourceReference(source="p", field="tid"),
                ),
                location="query",
                batch_size=1,
                include_as_field=True,
            ),
        },
    )
    m = get_include_as_field_params(rc)
    assert m["_tid"] == "tid"


def test_add_iteration_context_to_record_types() -> None:
    rec: dict = {}
    add_iteration_context_to_record(
        rec,
        {"tid": "a", "obj": {"x": 1}, "lst": [1]},
        include_field_map={"_tid": "tid", "_obj": "obj", "_lst": "lst"},
    )
    assert rec["_tid"] == "a"
    assert rec["_obj"] == json.dumps({"x": 1})
    assert rec["_lst"] == "[1]"


def test_add_iteration_context_respects_exclude_fields() -> None:
    rec: dict = {}
    add_iteration_context_to_record(
        rec,
        {"tid": "z"},
        include_field_map={"_tid": "tid"},
        exclude_fields={"_tid"},
    )
    assert "_tid" not in rec
