"""
Cloud-agnostic destination preflight.

Probes each unique loading destination URI through Spark's Hadoop ``FileSystem``
layer so credential or reachability problems surface before any ingestion runs.
The same seam covers ``s3``, ``gcs``, ``azure_blob``, and ``local``: whichever
Hadoop connector and credential chain the operator wired up is exercised here.

Two probe modes:

- read probe (default): ``FileSystem.listStatus`` on the destination root URI
  (``s3a://…``, ``gs://…``, ``abfs://…``). Non-mutating; intended for the regular
  ``handle()`` path so failures stop the pipeline before service creation.
  We list unconditionally (not gated on ``exists``) so S3A empty buckets still
  exercise ``ListBucket`` / credentials.
- write probe (``write_probe=True``): adds a temporary marker write/delete
  under the destination, mirroring the historic boto3 ``head_bucket + put +
  delete`` check. Intended for explicit ``--validate-only`` runs.

Errors are translated into ``HandlerError`` with the destination scheme and
bucket/container/account in ``details`` so operators can see which destination
failed without reading JVM stack traces.

Before any Hadoop call, preflight validates that bucket/container names look
like real DNS-style identifiers (not URIs pasted into the bucket field). For
``gcs`` on a developer machine it also checks that Google credential files exist,
are non-empty, and parse as the expected JSON credential shapes so misconfiguration
fails in Python instead of blocking inside ``FileSystem.get``.
"""

from __future__ import annotations

import json
import os
import re
import uuid
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import TimeoutError as FuturesTimeoutError
from pathlib import Path
from time import monotonic
from typing import Any, Dict, Iterable, List, Optional, Tuple

from pyspark.sql import SparkSession

from src.config.config_models import LoadingConfig
from src.config.loading_schema import OBJECT_STORE_DESTINATIONS
from src.config.spark_runtime import detect_managed_spark_platform
from src.loader.local_storage import check_local_storage_root
from src.loader.object_store import SparkFilesystemObjectStore, loading_base_uri
from src.utils.exceptions import HandlerError
from src.utils.logger import get_logger

_logger = get_logger(__name__)

_PREFLIGHT_MARKER_PREFIX = "_spine_preflight"
_DEFAULT_FILESYSTEM_TIMEOUT_SECONDS = 45.0

# GCS / Azure container: DNS-style labels (reject ``gs://`` embedded in the bucket field, etc.).
_GCS_BUCKET_RE = re.compile(r"^[a-z0-9][a-z0-9_.-]{1,61}[a-z0-9]$")
_AZURE_ACCOUNT_RE = re.compile(r"^[a-z0-9]{3,24}$")
_AZURE_CONTAINER_RE = re.compile(r"^[a-z0-9][a-z0-9-]{1,61}[a-z0-9]$")


def _destination_dedup_key(config: LoadingConfig) -> Tuple[str, ...]:
    """Stable identity for a destination so the same bucket/root is probed once."""
    if config.destination == "s3":
        return ("s3", config.s3_bucket or "")
    if config.destination == "gcs":
        return ("gcs", config.gcs_bucket or "")
    if config.destination == "azure_blob":
        return (
            "azure_blob",
            config.azure_container or "",
            config.azure_account or "",
        )
    if config.destination == "local":
        # Resolve to absolute so two different relative spellings of the same root
        # are recognized as one destination.
        root = config.storage_root or ""
        if not root:
            return ("local", "")
        return ("local", str(Path(root).expanduser().resolve()))
    return (config.destination,)


def _destination_details(config: LoadingConfig) -> Dict[str, Any]:
    """Operator-readable destination context attached to errors and logs."""
    if config.destination == "s3":
        return {"destination": "s3", "s3_bucket": config.s3_bucket}
    if config.destination == "gcs":
        return {"destination": "gcs", "gcs_bucket": config.gcs_bucket}
    if config.destination == "azure_blob":
        return {
            "destination": "azure_blob",
            "azure_container": config.azure_container,
            "azure_account": config.azure_account,
        }
    if config.destination == "local":
        return {"destination": "local", "storage_root": config.storage_root}
    return {"destination": config.destination}


def _s3_bucket_name_issue(name: str) -> Optional[str]:
    """Return a human-readable issue or ``None`` if the name is plausibly valid."""
    if len(name) < 3 or len(name) > 63:
        return f"S3 bucket name must be 3-63 characters ({len(name)} given)."
    if "://" in name or any(c.isspace() for c in name):
        return "Use the bare bucket name, not a URI (no scheme or whitespace)."
    if not name[0].isalnum() or not name[-1].isalnum():
        return "S3 bucket name must start and end with a letter or number."
    for c in name:
        if not (c.isalnum() or c in ".-_"):
            return f"S3 bucket name contains invalid character {c!r}."
    return None


def _validate_destination_identity(config: LoadingConfig, details: Dict[str, Any]) -> None:
    """
    Reject obviously broken bucket/container values before any JVM I/O.

    Pydantic already requires non-empty fields; this catches common misconfigurations
    (URI pasted into the bucket field, illegal characters, length violations) so
    operators get a Python-side error instead of an obscure Hadoop failure or hang.
    """
    step_details = {**details, "step": "destination_identity_precheck"}

    def _fail(message: str) -> None:
        raise HandlerError(
            message,
            operation="destination_preflight",
            details=step_details,
        )

    if config.destination == "s3":
        name = config.s3_bucket or ""
        issue = _s3_bucket_name_issue(name)
        if issue:
            _fail(f"Invalid S3 bucket name {name!r}: {issue}")
    elif config.destination == "gcs":
        name = config.gcs_bucket or ""
        if not name:
            _fail("GCS bucket name is empty.")
        if "://" in name or any(c.isspace() for c in name):
            _fail(
                f"GCS bucket name {name!r} looks invalid: use the bare bucket label "
                "(no ``gs://`` prefix or whitespace)."
            )
        if len(name) < 3 or len(name) > 63:
            _fail(f"GCS bucket name must be 3-63 characters ({len(name)} given): {name!r}.")
        if not _GCS_BUCKET_RE.match(name):
            _fail(
                f"GCS bucket name {name!r} does not match a typical DNS bucket pattern "
                "(lowercase letters, digits, hyphen, underscore, dot; start/end alphanumeric)."
            )
    elif config.destination == "azure_blob":
        account = config.azure_account or ""
        container = config.azure_container or ""
        if not _AZURE_ACCOUNT_RE.match(account):
            _fail(
                f"Azure storage account {account!r} must be 3-24 lowercase letters or digits "
                "(DNS storage account name)."
            )
        if not container:
            _fail("Azure container name is empty.")
        if "://" in container or any(c.isspace() for c in container):
            _fail(
                f"Azure container name {container!r} looks invalid: use the bare container name, "
                "not a URI."
            )
        if len(container) < 3 or len(container) > 63:
            _fail(
                f"Azure container name must be 3-63 characters ({len(container)} given): {container!r}."
            )
        if not _AZURE_CONTAINER_RE.match(container):
            _fail(
                f"Azure container name {container!r} does not match a typical container pattern "
                "(lowercase letters, digits, hyphen; start/end alphanumeric)."
            )


def _validate_google_credentials_json_file(path: Path, details: Dict[str, Any]) -> None:
    """Ensure a credential file is readable and parseable JSON."""
    step_details = {**details, "step": "gcs_credential_json_precheck"}
    try:
        raw = path.read_text(encoding="utf-8")
    except OSError as exc:
        raise HandlerError(
            f"Cannot read Google credential file {path}: {exc!s}",
            operation="destination_preflight",
            details=step_details,
            original_error=exc,
        ) from exc
    if not raw.strip():
        raise HandlerError(
            f"Google credential file is empty: {path}",
            operation="destination_preflight",
            details=step_details,
        )
    try:
        data = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise HandlerError(
            f"Google credential file is not valid JSON ({path}): {exc!s}",
            operation="destination_preflight",
            details=step_details,
            original_error=exc,
        ) from exc
    if not isinstance(data, dict):
        raise HandlerError(
            f"Google credential JSON must be an object ({path}).",
            operation="destination_preflight",
            details=step_details,
        )


def _is_gcp_managed_identity_environment() -> bool:
    """Best-effort signal for runtimes where metadata/workload identity is expected."""
    # Cloud Run
    if os.getenv("K_SERVICE"):
        return True
    # Cloud Functions (Gen1/Gen2)
    if os.getenv("FUNCTION_TARGET") or os.getenv("FUNCTION_NAME"):
        return True
    # App Engine
    if os.getenv("GAE_ENV"):
        return True
    # GKE Workload Identity and generic GCP workload hints
    if os.getenv("GKE_METADATA_HOST"):
        return True
    return False


def _validate_gcs_credentials_for_preflight(details: Dict[str, Any]) -> None:
    """
    Fail fast before ``FileSystem.get`` for ``gs://``.

    On a workstation without credentials, the Java client often blocks for a long
    time probing GCE metadata. When a path *is* present, validate JSON so corrupt
    or empty files fail in Python instead of inside the JVM.
    """
    _, detected_source = detect_managed_spark_platform()
    auth_type = (os.getenv("SPINE_GCS_AUTH_TYPE") or "APPLICATION_DEFAULT").strip().upper()
    gac = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "").strip()
    if gac:
        path = Path(gac).expanduser()
        if not path.is_file():
            raise HandlerError(
                f"GOOGLE_APPLICATION_CREDENTIALS is set ({gac!r}) but is not a readable file.",
                operation="destination_preflight",
                details={**details, "step": "gcs_adc_precheck"},
            )
        _validate_google_credentials_json_file(path, details)
        return

    adc = Path.home() / ".config" / "gcloud" / "application_default_credentials.json"
    if adc.is_file():
        _validate_google_credentials_json_file(adc, details)
        return

    if _is_gcp_managed_identity_environment():
        return

    # Fail fast for local/non-GCP runtimes so Spark does not block probing metadata.
    if auth_type == "COMPUTE_ENGINE":
        raise HandlerError(
            "SPINE_GCS_AUTH_TYPE=COMPUTE_ENGINE without a GCP managed-identity environment usually "
            "blocks inside FileSystem.get(gs://...) while probing metadata. Use "
            "SPINE_GCS_AUTH_TYPE=APPLICATION_DEFAULT with ADC (`gcloud auth application-default login`) "
            "or set GOOGLE_APPLICATION_CREDENTIALS to a service-account JSON key path.",
            operation="destination_preflight",
            details={
                **details,
                "step": "gcs_adc_precheck",
                "detected_platform_source": detected_source,
            },
        )

    raise HandlerError(
        "Spark's GCS connector needs credentials before preflight can probe gs://. Provide "
        "GOOGLE_APPLICATION_CREDENTIALS or run `gcloud auth application-default login`. "
        "Without ADC, FileSystem.get(gs://...) often blocks while probing metadata.",
        operation="destination_preflight",
        details={
            **details,
            "step": "gcs_adc_precheck",
            "expected_adc_path": str(adc),
            "detected_platform_source": detected_source,
        },
    )


def _filesystem_timeout_seconds() -> float:
    raw = (os.getenv("SPINE_DESTINATION_PREFLIGHT_FILESYSTEM_TIMEOUT_SECONDS") or "").strip()
    if not raw:
        return _DEFAULT_FILESYSTEM_TIMEOUT_SECONDS
    try:
        value = float(raw)
        if value <= 0:
            raise ValueError("must be > 0")
        return value
    except Exception:
        _logger.warning(
            "Invalid SPINE_DESTINATION_PREFLIGHT_FILESYSTEM_TIMEOUT_SECONDS; using default",
            extra_fields={
                "raw_value": raw,
                "default_seconds": _DEFAULT_FILESYSTEM_TIMEOUT_SECONDS,
            },
        )
        return _DEFAULT_FILESYSTEM_TIMEOUT_SECONDS


def _run_with_timeout(
    *,
    timeout_seconds: float,
    call_name: str,
    details: Dict[str, Any],
    fn,
):
    """Run a potentially-blocking JVM bridge call with a hard timeout."""
    started = monotonic()
    with ThreadPoolExecutor(max_workers=1, thread_name_prefix="spine-preflight-fs") as executor:
        future = executor.submit(fn)
        try:
            result = future.result(timeout=timeout_seconds)
        except FuturesTimeoutError as exc:
            elapsed_ms = int((monotonic() - started) * 1000)
            raise HandlerError(
                f"Preflight {call_name} timed out after {timeout_seconds:.1f}s for "
                f"destination {details.get('destination')!r} ({details.get('base_uri')}).",
                operation="destination_preflight",
                details={
                    **details,
                    "step": call_name,
                    "timeout_seconds": timeout_seconds,
                    "elapsed_ms": elapsed_ms,
                },
                original_error=exc,
            ) from exc
    elapsed_ms = int((monotonic() - started) * 1000)
    if elapsed_ms >= int(timeout_seconds * 1000 * 0.8):
        _logger.warning(
            "Destination preflight call approached timeout threshold",
            extra_fields={
                **details,
                "step": call_name,
                "elapsed_ms": elapsed_ms,
                "timeout_seconds": timeout_seconds,
            },
        )
    return result


def _probe_local(config: LoadingConfig) -> None:
    """Local destinations do not need Spark; reuse the filesystem check."""
    if not config.storage_root:
        raise HandlerError(
            "storage_root is required for local destination preflight",
            details=_destination_details(config),
        )
    check_local_storage_root(Path(config.storage_root))


def _probe_object_store(
    spark: SparkSession,
    config: LoadingConfig,
    base_uri: str,
    *,
    write_probe: bool,
) -> None:
    """
    Read (and optionally write) probe for s3/gcs/azure_blob using Spark's
    Hadoop ``FileSystem``. Any JVM exception is re-raised as ``HandlerError``
    with the destination scheme and bucket/container in ``details``.
    """
    store = SparkFilesystemObjectStore(spark)
    details = _destination_details(config)
    details["base_uri"] = base_uri
    fs_timeout_seconds = _filesystem_timeout_seconds()

    _validate_destination_identity(config, details)

    if config.destination == "gcs":
        _validate_gcs_credentials_for_preflight(details)

    jvm = spark.sparkContext._jvm
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    path = jvm.org.apache.hadoop.fs.Path(base_uri)

    _logger.trace(
        "Destination preflight calling Hadoop FileSystem.get",
        extra_fields={
            **details,
            "step": "filesystem_get_begin",
            "timeout_seconds": fs_timeout_seconds,
        },
    )

    get_start = monotonic()
    try:
        fs = _run_with_timeout(
            timeout_seconds=fs_timeout_seconds,
            call_name="filesystem_get",
            details=details,
            fn=lambda: jvm.org.apache.hadoop.fs.FileSystem.get(path.toUri(), hadoop_conf),
        )
    except Exception as e:
        if isinstance(e, HandlerError):
            raise
        raise HandlerError(
            f"Cannot initialize filesystem client for destination {config.destination!r} ({base_uri}): {e!s}",
            operation="destination_preflight",
            details={**details, "step": "filesystem_get"},
            original_error=e,
        ) from e
    get_elapsed_ms = int((monotonic() - get_start) * 1000)
    _logger.trace(
        "Destination preflight filesystem client initialised",
        extra_fields={**details, "step": "filesystem_get", "elapsed_ms": get_elapsed_ms},
    )

    list_start = monotonic()
    try:
        # Always list at the destination root. On S3A, ``exists(bucketRoot)`` can be
        # false for an empty bucket while ``listStatus`` still succeeds and is what
        # actually exercises ``s3:ListBucket`` / credentials; gating list behind exists
        # skipped that check and made preflight flaky or a false negative.
        _run_with_timeout(
            timeout_seconds=fs_timeout_seconds,
            call_name="list_status",
            details={**details, "filesystem_get_elapsed_ms": get_elapsed_ms},
            fn=lambda: fs.listStatus(path),
        )
    except Exception as e:
        if isinstance(e, HandlerError):
            raise
        raise HandlerError(
            f"Cannot list destination {config.destination!r} ({base_uri}): {e!s}",
            operation="destination_preflight",
            details={**details, "step": "list_status", "filesystem_get_elapsed_ms": get_elapsed_ms},
            original_error=e,
        ) from e
    list_elapsed_ms = int((monotonic() - list_start) * 1000)
    _logger.debug(
        "Destination preflight destination root listed",
        extra_fields={**details, "step": "list_status", "elapsed_ms": list_elapsed_ms},
    )

    if not write_probe:
        return

    marker_uri = store.resolve_path(base_uri, _PREFLIGHT_MARKER_PREFIX, f"check-{uuid.uuid4().hex}")
    try:
        jvm = spark.sparkContext._jvm
        hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
        marker_path = jvm.org.apache.hadoop.fs.Path(marker_uri)
        fs = jvm.org.apache.hadoop.fs.FileSystem.get(marker_path.toUri(), hadoop_conf)
        out = fs.create(marker_path, True)
        try:
            out.write(b"spine-preflight")
        finally:
            out.close()
        try:
            fs.delete(marker_path, False)
        except Exception:
            # Best-effort cleanup; preflight has already proven write capability.
            _logger.debug(
                "Preflight marker cleanup failed; continuing",
                extra_fields={"marker_uri": marker_uri, **details},
            )
    except HandlerError:
        raise
    except Exception as e:
        raise HandlerError(
            f"Cannot write to loading destination {config.destination!r} ({base_uri}): {e!s}",
            operation="destination_preflight",
            details={**details, "marker_uri": marker_uri},
            original_error=e,
        ) from e


def preflight_destinations(
    spark: Optional[SparkSession],
    configs: Iterable[LoadingConfig],
    *,
    write_probe: bool = False,
) -> None:
    """
    Probe every unique destination behind the supplied ``LoadingConfig``s.

    Args:
        spark: Spark session used for object-store probes. May be ``None`` only
            when the config set is exclusively ``local``; otherwise raises.
        configs: Effective ``LoadingConfig`` objects (already merged with
            defaults). ``None`` and disabled configs should be filtered by the
            caller.
        write_probe: When ``True`` also writes (and deletes) a temporary marker
            object to confirm write permissions. Skipped on ``local`` because
            ``check_local_storage_root`` already verifies write access.

    Raises:
        HandlerError: If any destination is unreachable, missing required
            fields, or rejects the probe.
    """
    seen: set[Tuple[str, ...]] = set()
    deduped: List[LoadingConfig] = []
    for config in configs:
        if config is None:
            continue
        if config.destination not in OBJECT_STORE_DESTINATIONS:
            continue
        key = _destination_dedup_key(config)
        if key in seen:
            continue
        seen.add(key)
        deduped.append(config)

    if not deduped:
        return

    for config in deduped:
        details = _destination_details(config)
        _logger.debug(
            "Running destination preflight",
            extra_fields={**details, "write_probe": write_probe},
        )

        if config.destination == "local":
            _probe_local(config)
            continue

        if spark is None:
            raise HandlerError(
                f"Spark session is required to preflight destination {config.destination!r}",
                operation="destination_preflight",
                details=details,
            )

        try:
            base_uri = loading_base_uri(config)
        except ValueError as e:
            raise HandlerError(
                f"Invalid loading destination configuration: {e!s}",
                operation="destination_preflight",
                details=details,
                original_error=e,
            ) from e

        _probe_object_store(spark, config, base_uri, write_probe=write_probe)

        _logger.debug(
            "Destination preflight succeeded",
            extra_fields={**details, "write_probe": write_probe},
        )
