"""Tests for DatabricksUtils query resolution behavior."""

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from databricks.sdk.service.sql import StatementState

from src.utils.databricks_utils import DatabricksUtils


def _status(state: str, data_array=None, next_chunk_index=None, columns=None):
    state_obj = (
        StatementState.SUCCEEDED
        if state == "SUCCEEDED"
        else StatementState.FAILED if state == "FAILED" else SimpleNamespace(value=state)
    )
    response = SimpleNamespace(
        status=SimpleNamespace(state=state_obj),
        result=SimpleNamespace(data_array=data_array, next_chunk_index=next_chunk_index),
    )
    if columns is not None:
        response.manifest = SimpleNamespace(
            schema=SimpleNamespace(columns=[SimpleNamespace(name=c) for c in columns])
        )
    return response


def test_resolve_databricks_query_flattens_single_column(monkeypatch: pytest.MonkeyPatch) -> None:
    utils = DatabricksUtils.__new__(DatabricksUtils)
    client = MagicMock()
    utils._databricks_client = client
    utils._warehouse_id = "wh"

    client.statement_execution.execute_statement.return_value = SimpleNamespace(
        statement_id="stmt1"
    )
    client.statement_execution.get_statement.side_effect = [
        _status("PENDING", [["a"]]),
        _status("SUCCEEDED", [["a"], ["b"]], next_chunk_index=1),
    ]
    client.statement_execution.get_statement_result_chunk_n.return_value = SimpleNamespace(
        data_array=[["c"]], next_chunk_index=None
    )
    monkeypatch.setattr("src.utils.databricks_utils.time.sleep", lambda _: None)

    out = utils.resolve_databricks_query("select x")

    assert out == ["a", "b", "c"]


def test_resolve_databricks_query_multi_column_returns_row_dicts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    utils = DatabricksUtils.__new__(DatabricksUtils)
    client = MagicMock()
    utils._databricks_client = client
    utils._warehouse_id = "wh"

    client.statement_execution.execute_statement.return_value = SimpleNamespace(
        statement_id="stmt_multi"
    )
    client.statement_execution.get_statement.side_effect = [
        _status("PENDING", [["a", 1]], columns=["store", "gtin"]),
        _status("SUCCEEDED", [["a", 1], ["b", 2]], next_chunk_index=1, columns=["store", "gtin"]),
    ]
    client.statement_execution.get_statement_result_chunk_n.return_value = SimpleNamespace(
        data_array=[["c", 3]], next_chunk_index=None
    )
    monkeypatch.setattr("src.utils.databricks_utils.time.sleep", lambda _: None)

    out = utils.resolve_databricks_query("select store, gtin")

    assert out == [
        {"store": "a", "gtin": 1},
        {"store": "b", "gtin": 2},
        {"store": "c", "gtin": 3},
    ]


def test_resolve_databricks_query_multi_column_without_manifest_returns_row_lists(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    utils = DatabricksUtils.__new__(DatabricksUtils)
    client = MagicMock()
    utils._databricks_client = client
    utils._warehouse_id = "wh"

    client.statement_execution.execute_statement.return_value = SimpleNamespace(
        statement_id="stmt_nomanifest"
    )
    client.statement_execution.get_statement.return_value = _status(
        "SUCCEEDED", [[1, 2], [3, 4]], next_chunk_index=None
    )
    monkeypatch.setattr("src.utils.databricks_utils.time.sleep", lambda _: None)

    out = utils.resolve_databricks_query("select a, b")

    assert out == [[1, 2], [3, 4]]


def test_resolve_databricks_query_multi_column_and_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    utils = DatabricksUtils.__new__(DatabricksUtils)
    client = MagicMock()
    utils._databricks_client = client
    utils._warehouse_id = "wh"
    client.statement_execution.execute_statement.return_value = SimpleNamespace(
        statement_id="stmt2"
    )
    client.statement_execution.get_statement.return_value = _status("FAILED", [[1, 2]])
    monkeypatch.setattr("src.utils.databricks_utils.time.sleep", lambda _: None)

    with pytest.raises(ValueError, match="Failed to execute Databricks query"):
        utils.resolve_databricks_query("select a,b")

    utils._databricks_client = None
    with pytest.raises(ValueError, match="Databricks client is not initialized"):
        utils.resolve_databricks_query("select 1")
