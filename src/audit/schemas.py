"""
Audit trail record schemas for API request/response persistence.
"""

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Optional

from pyspark.sql.types import (
    IntegerType,
    MapType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# Truncation limit for request_preview and response_preview (bytes / chars)
PREVIEW_TRUNCATE_LENGTH = 2048


@dataclass
class ApiRequestRecord:
    """Record for a single API request (stored in api_requests Delta table)."""

    request_id: str
    timestamp: datetime
    method: str
    url: str
    endpoint: str
    headers: Dict[str, str]
    request_preview: str
    source: str
    attempt: int
    http_version: Optional[str] = None
    latency_ms: Optional[int] = None

    def to_row(self) -> Dict[str, Any]:
        """Convert to a dict suitable for Spark DataFrame / Delta write."""
        return {
            "request_id": self.request_id,
            "timestamp": self.timestamp,
            "method": self.method,
            "url": self.url,
            "endpoint": self.endpoint,
            "headers": self.headers,
            "request_preview": self.request_preview,
            "source": self.source,
            "attempt": self.attempt,
            "http_version": self.http_version,
            "latency_ms": self.latency_ms,
        }


@dataclass
class ApiResponseRecord:
    """Record for a single API response (stored in api_responses Delta table)."""

    request_id: str
    timestamp: datetime
    status_code: int
    content_length: int
    response_headers: Optional[Dict[str, str]] = None
    response_preview: Optional[str] = None
    content_type: Optional[str] = None
    upstream_time_ms: Optional[int] = None
    server_timing: Optional[str] = None

    def to_row(self) -> Dict[str, Any]:
        """Convert to a dict suitable for Spark DataFrame / Delta write."""
        return {
            "request_id": self.request_id,
            "timestamp": self.timestamp,
            "status_code": self.status_code,
            "response_headers": self.response_headers,
            "response_preview": self.response_preview,
            "content_length": self.content_length,
            "content_type": self.content_type,
            "upstream_time_ms": self.upstream_time_ms,
            "server_timing": self.server_timing,
        }


def request_preview_from_payload(payload: Any) -> str:
    """
    Build a request preview from field names only (no values).
    Adds context without exposing request data.

    - For dict: top-level keys, comma-separated.
    - For list of dicts: keys from first item.
    - Otherwise: empty string.
    """
    if payload is None:
        return ""
    if isinstance(payload, dict):
        return ", ".join(sorted(str(k) for k in payload.keys()))
    if isinstance(payload, (list, tuple)) and payload:
        first = payload[0]
        if isinstance(first, dict):
            return ", ".join(sorted(str(k) for k in first.keys()))
    return ""


def truncate_preview(value: str, max_length: int = PREVIEW_TRUNCATE_LENGTH) -> str:
    """Truncate a string for request/response preview storage."""
    if not value or max_length <= 0:
        return value or ""
    if len(value) <= max_length:
        return value
    return value[:max_length] + "... [truncated]"


# Spark / Delta table schemas (single source of truth for api_requests and api_responses)
API_REQUESTS_SCHEMA = StructType(
    [
        StructField("request_id", StringType(), False),
        StructField("timestamp", TimestampType(), False),
        StructField("method", StringType(), False),
        StructField("url", StringType(), False),
        StructField("endpoint", StringType(), False),
        StructField("headers", MapType(StringType(), StringType(), True), True),
        StructField("request_preview", StringType(), True),
        StructField("source", StringType(), False),
        StructField("attempt", IntegerType(), False),
        StructField("http_version", StringType(), True),
        StructField("latency_ms", IntegerType(), True),
    ]
)

API_RESPONSES_SCHEMA = StructType(
    [
        StructField("request_id", StringType(), False),
        StructField("timestamp", TimestampType(), False),
        StructField("status_code", IntegerType(), False),
        StructField("response_headers", MapType(StringType(), StringType(), True), True),
        StructField("response_preview", StringType(), True),
        StructField("content_length", IntegerType(), False),
        StructField("content_type", StringType(), True),
        StructField("upstream_time_ms", IntegerType(), True),
        StructField("server_timing", StringType(), True),
    ]
)
