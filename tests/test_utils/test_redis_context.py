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


def test_clear_without_prefix_uses_flushall(manager: RedisContextManager) -> None:
    manager.prefix = ""
    manager.client.info.return_value = {"used_memory_human": "1M"}
    manager.clear(log_memory=True)
    manager.client.flushall.assert_called_once()


def test_clear_prefix_no_keys_skips_delete(manager: RedisContextManager) -> None:
    manager.client.scan_iter.return_value = []
    manager.clear(log_memory=False)
    manager.client.delete.assert_not_called()


def test_clear_prefix_with_keys_deletes_batch(manager: RedisContextManager) -> None:
    manager.client.scan_iter.return_value = [b"p:a", b"p:b"]
    manager.clear(log_memory=False)
    manager.client.delete.assert_called_once_with(b"p:a", b"p:b")


def test_validate_connection_wraps_integrity_failure(manager: RedisContextManager) -> None:
    manager.get = MagicMock(return_value={"bad": "shape"})  # type: ignore[method-assign]
    with pytest.raises(ContextError, match="Data integrity check failed"):
        manager.validate_connection()


def test_validate_connection_wraps_delete_failure(manager: RedisContextManager) -> None:
    manager.get = MagicMock(return_value={"test": "data"})  # type: ignore[method-assign]
    manager.exists = MagicMock(return_value=True)  # type: ignore[method-assign]
    with pytest.raises(ContextError, match="Delete operation failed"):
        manager.validate_connection()


def test_get_wraps_deserialize_decode_error(manager: RedisContextManager) -> None:
    manager.client.get.return_value = b"\xff\xfe"
    with pytest.raises(ContextError, match="Failed to retrieve data"):
        manager.get("x")


def test_get_metadata_wraps_decode_error(manager: RedisContextManager) -> None:
    manager.client.get.return_value = b"\xff\xfe"
    with pytest.raises(ContextError, match="Failed to retrieve metadata"):
        manager.get_metadata("x")


def test_cleanup_without_flush_only_closes_client(manager: RedisContextManager) -> None:
    manager.clear = MagicMock()  # type: ignore[method-assign]
    manager.cleanup(flush_data=False)
    manager.client.close.assert_called_once()
    manager.clear.assert_not_called()
