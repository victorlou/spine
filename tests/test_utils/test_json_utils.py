"""Tests for JSON helper utilities."""

from src.utils import json_utils


def test_safe_json_parse_and_default() -> None:
    assert json_utils.safe_json_parse('{"a": 1}') == {"a": 1}
    assert json_utils.safe_json_parse("not-json", default={"x": 1}) == {"x": 1}
    assert json_utils.safe_json_parse({"already": "parsed"}) == {"already": "parsed"}


def test_parse_json_array_variants() -> None:
    assert json_utils.parse_json_array('["a", "b"]') == ["a", "b"]
    assert json_utils.parse_json_array("[1,2]") == [1, 2]
    assert json_utils.parse_json_array('{"not":"array"}') == '{"not":"array"}'
    assert json_utils.parse_json_array("plain") == "plain"


def test_parse_json_object_variants() -> None:
    assert json_utils.parse_json_object('{"a":1}') == {"a": 1}
    assert json_utils.parse_json_object("[1,2]") == "[1,2]"
    assert json_utils.parse_json_object("plain") == "plain"


def test_parse_json_array_and_object_logs_on_decode_error(caplog) -> None:
    import logging

    caplog.set_level(logging.DEBUG)
    assert json_utils.parse_json_object('{"x":}') == '{"x":}'
    assert json_utils.parse_json_array("[not valid json]") == "[not valid json]"


def test_is_json_string_and_serialize_default() -> None:
    assert json_utils.is_json_string('{"a":1}') is True
    assert json_utils.is_json_string("bad json") is False
    assert json_utils.is_json_string(123) is False

    class _Nope:
        pass

    assert json_utils.json_serialize({"x": 1}) == '{"x": 1}'
    assert json_utils.json_serialize(_Nope(), default="fallback") == "fallback"
