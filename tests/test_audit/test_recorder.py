"""Tests for audit recorder behaviors."""

from datetime import UTC, datetime
from unittest.mock import MagicMock

from src.audit.recorder import AuditRecorder
from src.audit.schemas import ApiRequestRecord, ApiResponseRecord


def test_mask_headers_and_record_append() -> None:
    masked = AuditRecorder.mask_headers({"Authorization": "abc", "X-Trace": "1"})
    assert masked["Authorization"] != "abc"
    assert masked["X-Trace"] == "1"

    rec = AuditRecorder()
    req = ApiRequestRecord(
        request_id="1",
        timestamp=datetime.now(UTC),
        method="GET",
        url="https://x",
        endpoint="/a",
        headers={},
        request_preview="k",
        source="s",
        attempt=1,
    )
    resp = ApiResponseRecord(
        request_id="1",
        timestamp=datetime.now(UTC),
        status_code=200,
        content_length=2,
    )
    rec.record_request(req)
    rec.record_response(resp)
    assert len(rec._requests) == 1
    assert len(rec._responses) == 1


def test_flush_skips_and_clears_without_bucket_or_spark() -> None:
    rec = AuditRecorder()
    rec.record_request(
        ApiRequestRecord(
            request_id="1",
            timestamp=datetime.now(UTC),
            method="GET",
            url="https://x",
            endpoint="/a",
            headers={},
            request_preview="k",
            source="s",
            attempt=1,
        )
    )
    rec.flush(spark=None, bucket="")
    assert rec._requests == []
    assert rec._responses == []


def test_flush_to_delta_invokes_dataframe_writes() -> None:
    rec = AuditRecorder()
    spark = MagicMock()
    writer = MagicMock()
    writer.format.return_value = writer
    writer.mode.return_value = writer
    writer.option.return_value = writer
    df = MagicMock()
    df.write = writer
    spark.createDataFrame.return_value = df

    rec._flush_to_delta(
        spark=spark,
        bucket="b",
        filesystem_scheme="s3a",
        request_rows=[{"request_id": "1"}],
        response_rows=[{"request_id": "1"}],
    )

    assert spark.createDataFrame.call_count == 2
    assert writer.save.call_count == 2
