"""Tests for shared API response_key normalization."""

import pytest
from pydantic import ValidationError

from src.config.config_models import ResourceConfig
from src.utils.data_utils import dict_response_key_to_records


class TestDictResponseKeyToRecords:
    def test_dot_path_returns_list_of_rows(self):
        data = {"a": {"b": [{"id": 1}, {"id": 2}]}}
        records, missing = dict_response_key_to_records(data, "a.b")
        assert missing is False
        assert records == [{"id": 1}, {"id": 2}]

    def test_top_level_key_list(self):
        data = {"data": [{"x": 1}]}
        records, missing = dict_response_key_to_records(data, "data")
        assert missing is False
        assert records == [{"x": 1}]

    def test_top_level_key_single_dict_wrapped(self):
        data = {"payload": {"nested": 1}}
        records, missing = dict_response_key_to_records(data, "payload")
        assert missing is False
        assert records == [{"nested": 1}]

    def test_missing_path(self):
        data = {"id": 1}
        records, missing = dict_response_key_to_records(data, "data")
        assert missing is True
        assert records == []

    def test_none_value_at_path(self):
        data = {"data": None}
        records, missing = dict_response_key_to_records(data, "data")
        assert missing is True
        assert records == []

    def test_empty_list_at_path(self):
        data = {"items": []}
        records, missing = dict_response_key_to_records(data, "items")
        assert missing is False
        assert records == []

    def test_scalar_at_path(self):
        data = {"count": 42}
        records, missing = dict_response_key_to_records(data, "count")
        assert missing is False
        assert records == [42]


class TestResourceConfigResponseKey:
    def test_strips_whitespace(self):
        r = ResourceConfig(response_key="  a.b  ")
        assert r.response_key == "a.b"

    def test_none_allowed(self):
        r = ResourceConfig(response_key=None)
        assert r.response_key is None

    def test_empty_string_rejected(self):
        with pytest.raises(ValidationError):
            ResourceConfig(response_key="")

    def test_whitespace_only_rejected(self):
        with pytest.raises(ValidationError):
            ResourceConfig(response_key="   \t  ")
