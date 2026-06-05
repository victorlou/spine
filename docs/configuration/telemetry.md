# Telemetry (OpenTelemetry)

Spine can emit OpenTelemetry **traces** and **metrics** over OTLP. It is a backend-agnostic **producer**: it speaks standard OTLP and uses standard OTEL semantic conventions, and it knows nothing about any specific collector, storage format, or vendor attribute namespace. Where the telemetry goes — a local collector, a managed observability backend, an in-cluster OTEL Collector that lands data in a warehouse — is entirely the deployment's concern and out of scope here.

Telemetry is **disabled by default**. When disabled, all instrumentation is a zero-cost no-op.

## Quick start

Point Spine at any OTLP collector and turn it on in `defaults.yml`:

```yaml
defaults:
  telemetry:
    enabled: true
    service_name: "spine"
    endpoint: "${OTEL_EXPORTER_OTLP_ENDPOINT:-http://localhost:4317}"
    protocol: "grpc"          # grpc (4317) or http/protobuf (4318)
```

Then run the pipeline normally (`python -m src.main`). A run produces this span tree:

```
spine.pipeline.run
└─ spine.resource.process        (attrs: spine.source, spine.resource, spine.source_type)
   ├─ spine.service.create
   ├─ spine.extract              (per HTTP request, or once per database read)
   ├─ spine.parse                (per parsed batch)
   └─ spine.load                 (attrs: spine.destination, spine.format)
```

and two metrics:

| Metric | Type | Attributes | Meaning |
|--------|------|------------|---------|
| `spine.resource.processed` | counter | `source`, `resource`, `stage`, `status` | Resources processed, by outcome (`success` / `partial_failure` / `failed`). |
| `spine.resource.duration` | histogram (ms) | `source`, `resource`, `stage` | Wall-clock time to process one resource. |

A resource that fails is isolated (the pipeline continues), and its `spine.resource.process` span is marked with `ERROR` status while sibling resources' spans stay unaffected.

## Config-first, with `OTEL_*` env vars taking precedence

Every setting lives under `defaults.telemetry`, but the **standard `OTEL_*` environment variables override the YAML at init**, so the same pipeline config deploys anywhere without edits. This mirrors how `spark_runtime` lets `SPARK_*` env vars override YAML.

| Config field | Standard env var that overrides it |
|--------------|------------------------------------|
| (`enabled`) | `OTEL_SDK_DISABLED=true` forces no-op regardless of config |
| `service_name` | `OTEL_SERVICE_NAME` |
| `service_version`, `deployment_environment`, `resource_attributes` | `OTEL_RESOURCE_ATTRIBUTES` (wins on key collision) |
| `endpoint` | `OTEL_EXPORTER_OTLP_ENDPOINT` (or signal-specific `..._TRACES_ENDPOINT` / `..._METRICS_ENDPOINT`) |
| `protocol` | `OTEL_EXPORTER_OTLP_PROTOCOL` (or `..._TRACES_PROTOCOL`) |
| `headers` | `OTEL_EXPORTER_OTLP_HEADERS` |

When an `OTEL_EXPORTER_OTLP_*` endpoint env var is set, Spine passes nothing from `endpoint` /
`insecure` / `headers` and lets the OTLP exporter read its own standard env configuration.

## Resource attributes are operator-supplied

Spine only sets the standard `service.name`, `service.version`, and `deployment.environment` from config. Anything else — team, cost center, product/workflow/business-event attribution, or any vendor-specific key — is **operator-supplied** through the free-form `resource_attributes` map or the standard `OTEL_RESOURCE_ATTRIBUTES` env var. Spine never names or hardcodes these.

```yaml
defaults:
  telemetry:
    enabled: true
    resource_attributes:
      team: "data-platform"
      deployment.region: "ap-southeast-2"
```

## Log correlation

When telemetry is active and `log_correlation` is true (default), the active span's `trace_id` and `span_id` are injected into Spine's structured logs so log lines correlate to spans. This adds two fields to existing log records and changes nothing else; it is a no-op when no span is active.

## Telemetry never breaks ingestion

If telemetry is enabled but no endpoint can be resolved (neither `endpoint` nor an `OTEL_*` endpoint env var), or the OTEL SDK is unavailable, Spine logs a warning and continues with telemetry as a no-op rather than failing the run. This is a deliberate choice — an unreachable collector must not stop a data pipeline. (It is the one place Spine relaxes its usual fail-early validation stance.)

## One-shot flush

Spine runs as a one-shot process. The exporter buffers spans, so Spine registers an `atexit` flush (and flushes explicitly on `SIGTERM`) to export buffered telemetry before the process exits. A hard `SIGKILL` / `os._exit` cannot be protected and may drop the final buffer.

## Spark / executor traces

Spine's spans cover the orchestration and I/O timeline in the **driver** process. Executor-level traces are not produced by Spine. Operators who want JVM/executor traces can attach the OpenTelemetry Java agent through the standard Spark options Spine already supports (`spark.driver.extraJavaOptions` / `spark.executor.extraJavaOptions`); Spine also supports Spark event logs (`spark_event_log_*`, see [Loading](loading.md)) for the executor view.
