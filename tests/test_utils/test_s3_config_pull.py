"""Tests for optional S3 config pull (ECS/Fargate, boto3)."""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from src.utils.s3_config_pull import pull_s3_prefix_to_directory


def test_pull_s3_prefix_to_directory_writes_files(tmp_path: Path) -> None:
    mock_client = MagicMock()

    def paginate(Bucket, Prefix):
        assert Bucket == "b"
        assert Prefix == "p/"
        yield {
            "Contents": [
                {"Key": "p/"},
                {"Key": "p/defaults.yml"},
                {"Key": "p/sources/x.yml"},
            ]
        }

    mock_client.get_paginator.return_value.paginate = paginate

    def download_file(bucket, key, filename):
        Path(filename).write_text(f"body-{key}", encoding="utf-8")

    mock_client.download_file.side_effect = download_file

    with patch("src.utils.s3_config_pull.boto3.client", return_value=mock_client):
        pull_s3_prefix_to_directory("s3://b/p", tmp_path)

    assert (tmp_path / "defaults.yml").read_text() == "body-p/defaults.yml"
    assert (tmp_path / "sources" / "x.yml").read_text() == "body-p/sources/x.yml"


def test_pull_s3_prefix_folder_marker_only_raises(tmp_path: Path) -> None:
    mock_client = MagicMock()
    mock_client.get_paginator.return_value.paginate.return_value = iter(
        [{"Contents": [{"Key": "prefix/"}]}]
    )

    with patch("src.utils.s3_config_pull.boto3.client", return_value=mock_client):
        with pytest.raises(RuntimeError, match="No objects downloaded"):
            pull_s3_prefix_to_directory("s3://b/prefix/", tmp_path)


def test_pull_s3_prefix_empty_raises(tmp_path: Path) -> None:
    mock_client = MagicMock()
    mock_client.get_paginator.return_value.paginate.return_value = iter([{}])

    with patch("src.utils.s3_config_pull.boto3.client", return_value=mock_client):
        with pytest.raises(RuntimeError, match="No objects downloaded"):
            pull_s3_prefix_to_directory("s3://b/prefix/", tmp_path)
