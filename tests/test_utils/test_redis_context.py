"""Tests for RedisContextManager using mocked Redis client."""

from unittest.mock import MagicMock

import pytest

from src.utils.redis_context import ContextError, RedisContextManager


@pytest.fixture
def manager(monkeypatch: pytest.MonkeyPatch) -> RedisContextManager:
    client = MagicMock()
    monkeypatch.setattr(RedisContextManager, "_create_client", lambda self: client)
    return RedisContextManager({"host": "localhost"}, prefix="p:", default_ttl=60)


def test_store_get_metadata_exists_delete_cycle(manager: RedisContextManager) -> None:
    payload = b'{"type":"raw","data":{"k":"v"}}'
    manager.client.get.side_effect = [payload, b'{"m":1}', payload]
    manager.client.exists.return_value = 1

    manager.store("a", {"k": "v"}, metadata={"m": 1}, ttl=5)
    got = manager.get("a")
    meta = manager.get_metadata("a")

    assert got == {"k": "v"}
    assert meta == {"m": 1}
    assert manager.exists("a") is True
    manager.delete("a")
    assert manager.client.delete.call_count >= 2


def test_validate_connection_and_cleanup(manager: RedisContextManager) -> None:
    manager.client.get.return_value = b'{"type":"raw","data":{"test":"data"}}'
    manager.client.exists.return_value = 0
    manager.client.info.return_value = {"used_memory_human": "1M"}
    manager.client.scan_iter.return_value = [b"p:a", b"p:b"]

    manager.validate_connection()
    manager.clear(log_memory=True)
    manager.cleanup(flush_data=True)

    manager.client.close.assert_called_once()


def test_error_wrapping_paths(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(RedisContextManager, "_create_client", lambda self: MagicMock())
    m = RedisContextManager({"host": "localhost"})
    m.client.get.side_effect = RuntimeError("boom")
    with pytest.raises(ContextError, match="Failed to retrieve data"):
        m.get("x")
