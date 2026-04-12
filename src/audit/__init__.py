"""
Audit trail for API request/response observability.

Records request and response metadata in memory and flushes to Delta tables
(api_requests, api_responses) in S3 control bucket at end of run.
"""

from src.audit.recorder import AuditRecorder

mask_headers = AuditRecorder.mask_headers
from src.audit.schemas import (
    API_REQUESTS_SCHEMA,
    API_RESPONSES_SCHEMA,
    PREVIEW_TRUNCATE_LENGTH,
    ApiRequestRecord,
    ApiResponseRecord,
    request_preview_from_payload,
    truncate_preview,
)

__all__ = [
    "API_REQUESTS_SCHEMA",
    "API_RESPONSES_SCHEMA",
    "PREVIEW_TRUNCATE_LENGTH",
    "ApiRequestRecord",
    "ApiResponseRecord",
    "AuditRecorder",
    "mask_headers",
    "request_preview_from_payload",
    "truncate_preview",
]
