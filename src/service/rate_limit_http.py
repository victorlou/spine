"""
Retry-After and rate-limit header helpers for HTTP client observability.

Uses urllib3's ``Retry.parse_retry_after`` so caps match transport behavior.
"""

from typing import Any, Dict, Mapping, Optional

import requests
from urllib3.exceptions import InvalidHeader
from urllib3.util.retry import Retry

from src.audit import mask_headers


def _retry_with_cap(retry_after_max: int) -> Retry:
    return Retry(retry_after_max=retry_after_max)


def parse_retry_after_seconds(
    retry_after_header: str,
    *,
    retry_after_max: int,
) -> Optional[float]:
    """
    Parse a Retry-After header value using urllib3 rules and the same cap as transport retries.

    Returns None if the header is missing, invalid, or unparsable.
    """
    try:
        return float(_retry_with_cap(retry_after_max).parse_retry_after(retry_after_header.strip()))
    except (InvalidHeader, ValueError):
        return None


def _is_rate_limit_header_name(name: str) -> bool:
    lower = name.lower()
    if lower == "retry-after":
        return True
    return lower.startswith("x-ratelimit-") or lower.startswith("ratelimit-")


def extract_rate_limit_headers(
    headers: Mapping[str, str],
    *,
    mask: bool = True,
) -> Dict[str, str]:
    """Return whitelisted rate-limit-related headers for logs and ServiceError.details."""
    raw = {k: v for k, v in headers.items() if _is_rate_limit_header_name(k)}
    if not raw:
        return {}
    return dict(mask_headers(raw)) if mask else dict(raw)


def rate_limit_context_from_response(
    response: requests.Response,
    *,
    retry_after_max: int,
) -> Dict[str, Any]:
    """Build structured fields for ServiceError.details and structured logging."""
    out: Dict[str, Any] = {}
    hdrs = extract_rate_limit_headers(dict(response.headers), mask=True)
    if hdrs:
        out["rate_limit_headers"] = hdrs
    ra = response.headers.get("Retry-After")
    if ra:
        parsed = parse_retry_after_seconds(ra, retry_after_max=retry_after_max)
        if parsed is not None:
            out["retry_after_seconds"] = parsed
    return out
