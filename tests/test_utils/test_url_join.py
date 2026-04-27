"""Tests for HTTP base + path joining (composition, not config canonicalization)."""

from src.utils.url_join import join_http_base_and_path


def test_join_http_base_and_path() -> None:
    assert join_http_base_and_path("https://a.com", "b") == "https://a.com/b"
    # Mirrors prior rstrip(base)/lstrip(endpoint) semantics; trailing slashes on path are preserved.
    assert join_http_base_and_path("https://a.com/", "/b/") == "https://a.com/b/"
    assert join_http_base_and_path("https://a.com/v1", "") == "https://a.com/v1"
