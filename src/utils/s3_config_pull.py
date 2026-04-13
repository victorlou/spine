"""
Download operator pipeline config from S3 into a local directory (runtime helper).

Uses boto3 only (no AWS CLI). Intended for ECS/Fargate when
``SPINE_CONFIG_S3_URI`` is set; ``docker/startup.sh`` invokes this before
``python -m src.main``. When the variable is unset, startup skips this module.

Unlike ``aws s3 sync --delete``, extra files already present under the target
directory are not removed—only keys under the S3 prefix are written/overwritten.
On Fargate ephemeral storage this is usually fine; if the image ships template
files under ``config/`` that collide with your layout, sync into a dedicated
empty path and set ``CONFIG_PATH`` to that path.
"""

from __future__ import annotations

import sys
from pathlib import Path

import boto3
from botocore.exceptions import ClientError

from src.utils.s3_config_push import parse_s3_uri

__all__ = ["pull_s3_prefix_to_directory"]


def pull_s3_prefix_to_directory(uri: str, target: Path) -> None:
    """
    List all objects under the URI prefix and write them under ``target``,
    preserving relative paths (same layout as a local ``CONFIG_PATH`` directory).
    """
    bucket, prefix = parse_s3_uri(uri)
    if prefix and not prefix.endswith("/"):
        prefix = prefix + "/"

    client = boto3.client("s3")
    paginator = client.get_paginator("list_objects_v2")
    target = target.resolve()
    target.mkdir(parents=True, exist_ok=True)

    downloaded_any = False
    try:
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if key.endswith("/"):
                    continue
                rel = key[len(prefix) :] if prefix else key
                if not rel or rel.endswith("/"):
                    continue
                dest = target / rel
                dest.parent.mkdir(parents=True, exist_ok=True)
                client.download_file(bucket, key, str(dest))
                downloaded_any = True
    except ClientError as e:
        raise RuntimeError(f"Failed to pull config from {uri!r}: {e}") from e

    if not downloaded_any:
        raise RuntimeError(
            f"No objects downloaded from {uri!r} (check bucket, prefix, and IAM). "
            f"Expected files such as defaults.yml under the prefix."
        )


def main() -> None:
    if len(sys.argv) != 3:
        print(
            "Usage: python -m src.utils.s3_config_pull <s3_uri> <target_dir>",
            file=sys.stderr,
        )
        sys.exit(1)
    pull_s3_prefix_to_directory(sys.argv[1], Path(sys.argv[2]))


if __name__ == "__main__":
    main()
