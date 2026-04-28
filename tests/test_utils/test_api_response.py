"""Tests for shared API response_key normalization."""

from src.utils.data_utils import dict_response_key_to_records


class TestDictResponseKeyToRecords:
    def test_dot_path_returns_list_of_rows(self) -> None:
        data = {"a": {"b": [{"id": 1}, {"id": 2}]}}
        records, missing = dict_response_key_to_records(data, "a.b")
        assert missing is False
        assert records == [{"id": 1}, {"id": 2}]

    def test_top_level_key_list(self) -> None:
        data = {"data": [{"x": 1}]}
        records, missing = dict_response_key_to_records(data, "data")
        assert missing is False
        assert records == [{"x": 1}]

    def test_top_level_key_single_dict_wrapped(self) -> None:
        data = {"payload": {"nested": 1}}
        records, missing = dict_response_key_to_records(data, "payload")
        assert missing is False
        assert records == [{"nested": 1}]

    def test_missing_path(self) -> None:
        data = {"id": 1}
        records, missing = dict_response_key_to_records(data, "data")
        assert missing is True
        assert records == []

    def test_none_value_at_path(self) -> None:
        data = {"data": None}
        records, missing = dict_response_key_to_records(data, "data")
        assert missing is True
        assert records == []

    def test_empty_list_at_path(self) -> None:
        data = {"items": []}
        records, missing = dict_response_key_to_records(data, "items")
        assert missing is False
        assert records == []

    def test_scalar_at_path(self) -> None:
        data = {"count": 42}
        records, missing = dict_response_key_to_records(data, "count")
        assert missing is False
        assert records == [42]
