"""
Trace/log correlation for the structured logger.

When telemetry is active and ``log_correlation`` is enabled, this filter injects the active span's
``trace_id`` / ``span_id`` into each log record's ``extra_fields`` so logs correlate to spans. It is a
no-op when correlation is disabled or when there is no active span, and it never replaces or reshapes
the existing logging mechanism — it only adds two fields the formatter already knows how to render.

Correlation is gated by a module-level flag rather than config so the widely-cached ``get_logger``
factory does not need access to pipeline config; ``TelemetryManager`` flips the flag on at init.
"""

import logging

from opentelemetry import trace

_correlation_enabled = False


def set_correlation_enabled(enabled: bool) -> None:
    """Enable or disable trace/log correlation process-wide. Called by ``TelemetryManager``."""
    global _correlation_enabled
    _correlation_enabled = enabled


class TraceCorrelationFilter(logging.Filter):
    """Adds ``trace_id`` / ``span_id`` to ``record.extra_fields`` when a span is active."""

    def filter(self, record: logging.LogRecord) -> bool:
        if not _correlation_enabled:
            return True
        span_context = trace.get_current_span().get_span_context()
        if not span_context.is_valid:
            return True
        existing = getattr(record, "extra_fields", None)
        merged = dict(existing) if isinstance(existing, dict) else {}
        merged.setdefault("trace_id", trace.format_trace_id(span_context.trace_id))
        merged.setdefault("span_id", trace.format_span_id(span_context.span_id))
        record.extra_fields = merged
        return True
