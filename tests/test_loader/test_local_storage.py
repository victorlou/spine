"""Tests for local storage path checks."""

import os
import sys
from pathlib import Path

import pytest

from src.loader.local_storage import check_local_storage_root
from src.utils.exceptions import HandlerError


def test_check_local_storage_root_ok(tmp_path: Path) -> None:
    d = tmp_path / "w"
    d.mkdir()
    check_local_storage_root(d)


def test_check_local_storage_root_not_dir(tmp_path: Path) -> None:
    f = tmp_path / "file"
    f.write_text("x")
    with pytest.raises(HandlerError, match="not a directory"):
        check_local_storage_root(f)


@pytest.mark.skipif(sys.platform == "win32", reason="directory mode bits differ on Windows")
@pytest.mark.skipif(getattr(os, "getuid", lambda: -1)() == 0, reason="root may write to any path")
def test_check_local_storage_root_not_writable(tmp_path: Path) -> None:
    d = tmp_path / "ro"
    d.mkdir()
    os.chmod(d, 0o555)
    try:
        with pytest.raises(HandlerError, match="not writable"):
            check_local_storage_root(d)
    finally:
        os.chmod(d, 0o755)
