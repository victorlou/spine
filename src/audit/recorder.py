"""
In-memory audit recorder for API requests/responses with optional Delta flush.
"""

from typing import Any, Dict, List

from pyspark.sql import SparkSession

from src.audit.schemas import (
    API_REQUESTS_SCHEMA,
    API_RESPONSES_SCHEMA,
    ApiRequestRecord,
    ApiResponseRecord,
)
from src.utils.logger import REDACTED_PLACEHOLDER, get_logger, is_sensitive_key


class AuditRecorder:
    """
    In-memory recorder for API request/response audit records.
    Records are appended to lists and flushed to Delta at end of run (non-blocking on hot path).
    """

    @staticmethod
    def mask_headers(headers: Dict[str, str]) -> Dict[str, str]:
        """
        Return a copy of headers with sensitive values replaced by REDACTED_PLACEHOLDER.
        Uses ``is_sensitive_key`` from ``src.utils.logger`` (header names matched case-insensitively).

        Args:
            headers: Original headers dict

        Returns:
            New dict with sensitive header values masked
        """
        if not headers:
            return {}
        return {
            k: REDACTED_PLACEHOLDER if is_sensitive_key(k.lower()) else v
            for k, v in headers.items()
        }

    def __init__(self) -> None:
        self._requests: List[ApiRequestRecord] = []
        self._responses: List[ApiResponseRecord] = []
        self.logger = get_logger(self.__class__.__name__)

    def record_request(self, record: ApiRequestRecord) -> None:
        """Append a request record (caller must pass already-masked headers)."""
        self._requests.append(record)

    def record_response(self, record: ApiResponseRecord) -> None:
        """Append a response record."""
        self._responses.append(record)

    def _flush_to_delta(
        self,
        spark: SparkSession,
        bucket: str,
        filesystem_scheme: str,
        request_rows: List[Dict[str, Any]],
        response_rows: List[Dict[str, Any]],
    ) -> None:
        """
        Append audit records to Delta tables under ``{scheme}://{bucket}/logs/``.
        Does not re-raise on failure; logs and continues.
        """
        base_path = f"{filesystem_scheme}://{bucket}/logs"
        requests_path = f"{base_path}/api_requests"
        responses_path = f"{base_path}/api_responses"

        try:
            if request_rows:
                df_requests = spark.createDataFrame(request_rows, schema=API_REQUESTS_SCHEMA)
                df_requests.write.format("delta").mode("append").option("mergeSchema", "true").save(
                    requests_path
                )
                self.logger.debug(
                    "Appended api_requests",
                    extra_fields={"path": requests_path, "count": len(request_rows)},
                )

            if response_rows:
                df_responses = spark.createDataFrame(response_rows, schema=API_RESPONSES_SCHEMA)
                df_responses.write.format("delta").mode("append").option(
                    "mergeSchema", "true"
                ).save(responses_path)
                self.logger.debug(
                    "Appended api_responses",
                    extra_fields={"path": responses_path, "count": len(response_rows)},
                )
        except Exception as e:
            self.logger.error(
                "Failed to flush audit trail to Delta",
                extra_fields={
                    "requests_path": requests_path,
                    "responses_path": responses_path,
                    "request_count": len(request_rows),
                    "response_count": len(response_rows),
                    "error": str(e),
                    "error_type": type(e).__name__,
                },
            )

    def flush(
        self,
        spark: Any,
        bucket: str,
        *,
        filesystem_scheme: str = "s3a",
    ) -> None:
        """
        Write buffered records to Delta tables and clear buffers.

        Does not re-raise on failure; logs and continues so the pipeline is not failed.

        Args:
            spark: SparkSession (from SparkManager)
            bucket: Storage authority used by ``filesystem_scheme`` for audit tables
                (e.g. ``my-bucket`` for ``s3a``/``gs``, or
                ``container@account.dfs.core.windows.net`` for ``abfs``)
            filesystem_scheme: ``s3a`` for AWS S3, ``gs`` for Google Cloud Storage,
                or ``abfs`` for Azure Blob/ADLS Gen2
        """
        if not bucket or not spark:
            if self._requests or self._responses:
                self.logger.debug(
                    "Audit flush skipped (no bucket or Spark)",
                    extra_fields={
                        "request_count": len(self._requests),
                        "response_count": len(self._responses),
                    },
                )
            self._requests.clear()
            self._responses.clear()
            return

        request_count = len(self._requests)
        response_count = len(self._responses)

        if request_count == 0 and response_count == 0:
            return

        request_rows = [r.to_row() for r in self._requests]
        response_rows = [r.to_row() for r in self._responses]

        self._flush_to_delta(
            spark=spark,
            bucket=bucket,
            filesystem_scheme=filesystem_scheme,
            request_rows=request_rows,
            response_rows=response_rows,
        )

        self.logger.info(
            "Audit trail flushed",
            extra_fields={
                "request_count": request_count,
                "response_count": response_count,
            },
        )

        self._requests.clear()
        self._responses.clear()
