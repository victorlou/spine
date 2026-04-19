"""Spark-backed Hadoop FileSystem operations and loading base URI resolution."""

from pathlib import Path
from typing import Optional, Protocol, runtime_checkable

from pyspark.sql import SparkSession

from src.utils.exceptions import LoaderError


def loading_base_uri(
    *,
    destination: str,
    bucket: Optional[str] = None,
    storage_root: Optional[str] = None,
) -> str:
    """
    Return the authority + path prefix URI for Spark writes (no trailing slash).

    - ``s3`` → ``s3a://{bucket}``
    - ``local`` → ``file:///...`` from resolved ``storage_root``. After ``ConfigLoader``
      runs, ``storage_root`` is absolute (relative paths are anchored to the repository
      root: the directory containing ``src/``). Constructing ``LoadingConfig`` without the
      loader resolves relative paths against the current working directory instead.
    """
    if destination == "s3":
        if not bucket:
            raise ValueError("bucket is required for S3 loading destination")
        return f"s3a://{bucket.strip().strip('/')}"
    if destination == "local":
        if not storage_root:
            raise ValueError("storage_root is required for local loading destination")
        root = Path(storage_root).expanduser().resolve()
        return root.as_uri().rstrip("/")
    raise ValueError(f"Unsupported loading destination: {destination!r}")


@runtime_checkable
class ObjectStore(Protocol):
    """Narrow seam for URI-based paths used by Spark loaders (s3a, file, gs, abfs, …)."""

    def resolve_path(self, base_uri: str, *segments: str, trailing_slash: bool = False) -> str:
        """Join base URI with path segments; optional trailing slash for directory roots."""
        ...

    def exists(self, uri: str) -> bool: ...

    def delete(self, uri: str, *, recursive: bool = True) -> None: ...

    def move(self, src_uri: str, dst_uri: str) -> None: ...

    def glob_first_part_file(self, dir_uri: str) -> Optional[str]:
        """Return the Hadoop path string of the first ``part-*`` under ``dir_uri``, or None."""
        ...

    def is_empty_directory(self, uri: str) -> bool:
        """True if path is missing, or exists as a directory with no children."""
        ...


class SparkFilesystemObjectStore:
    """Object store backed by Spark's JVM Hadoop FileSystem (per-URI scheme)."""

    def __init__(self, spark: SparkSession) -> None:
        self._spark = spark

    def resolve_path(self, base_uri: str, *segments: str, trailing_slash: bool = False) -> str:
        base = base_uri.rstrip("/")
        parts = [s.strip("/") for s in segments if s and s.strip("/")]
        if parts:
            out = base + "/" + "/".join(parts)
        else:
            out = base
        if trailing_slash and not out.endswith("/"):
            out += "/"
        return out

    def exists(self, uri: str) -> bool:
        jvm = self._spark.sparkContext._jvm
        hadoop_conf = self._spark.sparkContext._jsc.hadoopConfiguration()
        path = jvm.org.apache.hadoop.fs.Path(uri)
        fs = jvm.org.apache.hadoop.fs.FileSystem.get(path.toUri(), hadoop_conf)
        return bool(fs.exists(path))

    def delete(self, uri: str, *, recursive: bool = True) -> None:
        jvm = self._spark.sparkContext._jvm
        hadoop_conf = self._spark.sparkContext._jsc.hadoopConfiguration()
        path = jvm.org.apache.hadoop.fs.Path(uri)
        fs = jvm.org.apache.hadoop.fs.FileSystem.get(path.toUri(), hadoop_conf)
        if fs.exists(path):
            fs.delete(path, recursive)

    def move(self, src_uri: str, dst_uri: str) -> None:
        jvm = self._spark.sparkContext._jvm
        hadoop_conf = self._spark.sparkContext._jsc.hadoopConfiguration()
        src = jvm.org.apache.hadoop.fs.Path(src_uri)
        dst = jvm.org.apache.hadoop.fs.Path(dst_uri)
        fs = jvm.org.apache.hadoop.fs.FileSystem.get(src.toUri(), hadoop_conf)
        if not fs.rename(src, dst):
            raise LoaderError(f"Failed to move file from {src_uri} to {dst_uri}")

    def glob_first_part_file(self, dir_uri: str) -> Optional[str]:
        jvm = self._spark.sparkContext._jvm
        hadoop_conf = self._spark.sparkContext._jsc.hadoopConfiguration()
        glob_path = jvm.org.apache.hadoop.fs.Path(f"{dir_uri.rstrip('/')}/part-*")
        fs = jvm.org.apache.hadoop.fs.FileSystem.get(glob_path.toUri(), hadoop_conf)
        statuses = fs.globStatus(glob_path)
        if not statuses or len(statuses) == 0:
            return None
        return str(statuses[0].getPath().toString())

    def is_empty_directory(self, uri: str) -> bool:
        jvm = self._spark.sparkContext._jvm
        hadoop_conf = self._spark.sparkContext._jsc.hadoopConfiguration()
        path = jvm.org.apache.hadoop.fs.Path(uri)
        fs = jvm.org.apache.hadoop.fs.FileSystem.get(path.toUri(), hadoop_conf)
        if not fs.exists(path):
            return True
        if not fs.isDirectory(path):
            return False
        return len(fs.listStatus(path)) == 0
