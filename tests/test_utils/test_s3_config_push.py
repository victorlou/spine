"""Tests for S3 operator config push (promotion helper)."""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from src.utils.s3_config_push import (
    iter_operator_config_files,
    parse_s3_uri,
    push_config_to_s3,
)


def test_parse_s3_uri_bucket_only() -> None:
    assert parse_s3_uri("s3://my-bucket") == ("my-bucket", "")


def test_parse_s3_uri_with_prefix() -> None:
    assert parse_s3_uri("s3://my-bucket/spine/prod") == ("my-bucket", "spine/prod")


def test_parse_s3_uri_strips_whitespace() -> None:
    assert parse_s3_uri("  s3://b/p  ") == ("b", "p")


@pytest.mark.parametrize(
    "bad",
    [
        "http://bucket/key",
        "bucket/key",
        "s3://",
        "",
    ],
)
def test_parse_s3_uri_invalid(bad: str) -> None:
    with pytest.raises(ValueError):
        parse_s3_uri(bad)


def test_iter_operator_config_files_includes_and_excludes(tmp_path: Path) -> None:
    (tmp_path / "defaults.yml").write_text("d", encoding="utf-8")
    (tmp_path / "defaults.example.yml").write_text("x", encoding="utf-8")
    (tmp_path / "README.md").write_text("r", encoding="utf-8")
    (tmp_path / "sources").mkdir()
    (tmp_path / "sources" / "a.yml").write_text("a", encoding="utf-8")
    (tmp_path / "sources" / "skip.txt").write_text("t", encoding="utf-8")
    (tmp_path / "queries").mkdir()
    (tmp_path / "queries" / "q.sql").write_text("q", encoding="utf-8")
    examples = tmp_path / "examples"
    examples.mkdir()
    (examples / "e.yml").write_text("e", encoding="utf-8")
    (tmp_path / "sources" / ".gitkeep").write_text("", encoding="utf-8")

    rels = {rel for _, rel in iter_operator_config_files(tmp_path)}
    assert rels == {"defaults.yml", "sources/a.yml", "queries/q.sql"}


def test_push_config_to_s3_uploads(tmp_path: Path) -> None:
    (tmp_path / "defaults.yml").write_text("x", encoding="utf-8")
    mock_client = MagicMock()

    with patch("src.utils.s3_config_push.boto3.client", return_value=mock_client):
        n = push_config_to_s3("s3://my-bucket/prefix", tmp_path)

    assert n == 1
    mock_client.upload_file.assert_called_once()
    call = mock_client.upload_file.call_args
    assert call[0][0] == str(tmp_path / "defaults.yml")
    assert call[0][1] == "my-bucket"
    assert call[0][2] == "prefix/defaults.yml"


def test_push_config_to_s3_bucket_only_prefix(tmp_path: Path) -> None:
    (tmp_path / "defaults.yml").write_text("x", encoding="utf-8")
    mock_client = MagicMock()

    with patch("src.utils.s3_config_push.boto3.client", return_value=mock_client):
        push_config_to_s3("s3://onlybucket", tmp_path)

    assert mock_client.upload_file.call_args[0][2] == "defaults.yml"


def test_push_config_to_s3_empty_raises(tmp_path: Path) -> None:
    (tmp_path / "examples").mkdir()
    (tmp_path / "examples" / "x.yml").write_text("x", encoding="utf-8")

    with pytest.raises(RuntimeError, match="No operator config files"):
        push_config_to_s3("s3://b/p", tmp_path)
