"""
Upload operator pipeline config from disk to S3 (promotion / deployment helper).

Skips templates and examples under ``config/``. Intended for one-off or CI use.

At runtime on AWS, either set ``SPINE_CONFIG_S3_URI`` so ``docker/startup.sh``
pulls via ``s3_config_pull`` (boto3), or populate ``CONFIG_PATH`` some other way
(``aws s3 sync``, EFS, init container, etc.).
"""

from __future__ import annotations

import os
import sys
from fnmatch import fnmatch
from pathlib import Path
from typing import Iterable, Iterator, Tuple

import boto3
from botocore.exceptions import ClientError

__all__ = ["iter_operator_config_files", "parse_s3_uri", "push_config_to_s3"]


def _repository_root() -> Path:
    return Path(__file__).resolve().parents[2]


def resolve_local_config_root(config_path: str | None = None) -> Path:
    """
    Match runtime ``CONFIG_PATH`` resolution: absolute paths as-is; otherwise
    ``<repo>/config/<segment>`` (default segment ``.`` → ``config/``).
    """
    raw = (config_path if config_path is not None else os.environ.get("CONFIG_PATH", ".")).strip()
    p = Path(raw)
    if p.is_absolute():
        return p.resolve()
    return (_repository_root() / "config" / p).resolve()


def parse_s3_uri(uri: str) -> Tuple[str, str]:
    """Parse ``s3://bucket`` or ``s3://bucket/prefix`` into bucket and key prefix."""
    u = uri.strip()
    if not u.startswith("s3://"):
        raise ValueError(f"S3 URI must start with s3://, got: {uri!r}")
    rest = u[5:]
    if not rest:
        raise ValueError("S3 URI is missing bucket name")
    if "/" in rest:
        bucket, prefix = rest.split("/", 1)
    else:
        bucket, prefix = rest, ""
    if not bucket:
        raise ValueError("S3 URI has an empty bucket name")
    return bucket, prefix


def _normalize_key_prefix(prefix: str) -> str:
    p = prefix.strip().strip("/")
    return f"{p}/" if p else ""


def iter_operator_config_files(config_root: Path) -> Iterator[Tuple[Path, str]]:
    """
    Yield ``(absolute_path, relative_posix_path)`` for files to upload.

    Includes: ``defaults.yml``, ``sources/**/*.yml``, ``queries/**/*.sql``.
    Excludes: ``examples/``, ``*.example.yml``, ``README.md``, ``.gitkeep``.
    """
    root = config_root.resolve()
    if not root.is_dir():
        raise ValueError(f"Config root is not a directory: {root}")

    for path in sorted(root.rglob("*")):
        if not path.is_file():
            continue
        try:
            rel = path.relative_to(root).as_posix()
        except ValueError:
            continue
        if rel.startswith("examples/") or "/examples/" in rel:
            continue
        if path.name == "README.md":
            continue
        if path.name == ".gitkeep":
            continue
        if fnmatch(path.name, "*.example.yml"):
            continue

        if rel == "defaults.yml":
            yield path, rel
        elif rel.startswith("sources/") and rel.endswith(".yml"):
            yield path, rel
        elif rel.startswith("queries/") and rel.endswith(".sql"):
            yield path, rel


def push_config_to_s3(s3_uri: str, config_root: Path | None = None) -> int:
    """
    Upload operator files under ``config_root`` to ``s3_uri``.

    Returns the number of objects uploaded. Raises ``RuntimeError`` if nothing
    would be uploaded (empty tree or no matching files).
    """
    bucket, prefix = parse_s3_uri(s3_uri)
    key_prefix = _normalize_key_prefix(prefix)
    root = resolve_local_config_root() if config_root is None else Path(config_root).resolve()

    pairs: list[Tuple[Path, str]] = list(iter_operator_config_files(root))
    if not pairs:
        raise RuntimeError(
            f"No operator config files to upload under {root} "
            f"(expected defaults.yml and/or sources/**/*.yml and/or queries/**/*.sql; "
            f"excludes examples/, *.example.yml, README.md, .gitkeep)."
        )

    client = boto3.client("s3")
    count = 0
    try:
        for local_path, rel in pairs:
            key = f"{key_prefix}{rel}"
            client.upload_file(str(local_path), bucket, key)
            count += 1
    except ClientError as e:
        raise RuntimeError(f"Failed to upload config to {s3_uri!r}: {e}") from e

    return count


def main(argv: Iterable[str] | None = None) -> None:
    args = list(argv if argv is not None else sys.argv[1:])
    if len(args) < 1 or args[0] in ("-h", "--help"):
        print(
            "Usage: python -m src.utils.s3_config_push <s3_uri> [local_config_dir]\n\n"
            "Upload defaults.yml, sources/**/*.yml, and queries/**/*.sql from the\n"
            "given directory (default: CONFIG_PATH resolved like runtime, usually repo config/).",
            file=sys.stderr if args and args[0] not in ("-h", "--help") else sys.stdout,
        )
        sys.exit(0 if args and args[0] in ("-h", "--help") else 1)

    s3_uri = args[0]
    local_override = Path(args[1]).resolve() if len(args) > 1 else None
    n = push_config_to_s3(s3_uri, local_override)
    print(f"Uploaded {n} file(s) to {s3_uri}")


if __name__ == "__main__":
    main()
