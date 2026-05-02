# Loading Configuration

## Table of Contents

- [Destinations](#destinations)
  - [Amazon S3](#amazon-s3)
  - [Google Cloud Storage](#google-cloud-storage)
  - [Azure Blob Storage (ABFS)](#azure-blob-storage-abfs)
  - [Local filesystem](#local-filesystem)
  - [Other object stores](#other-object-stores)
- [Table formats and write modes](#table-formats-and-write-modes)
  - [Delta Lake](#delta-lake)
  - [Overwrite](#overwrite)
  - [Append](#append)
  - [Merge (Upsert)](#merge-upsert)
  - [Iceberg](#iceberg)
  - [Append](#append-1)
  - [Overwrite](#overwrite-1)
  - [Merge](#merge)
  - [Current Iceberg limitations](#current-iceberg-limitations)
- [Quick reference](#quick-reference)
- [Spark Runtime Readiness](#spark-runtime-readiness)

## Destinations

Loading uses Spark with Hadoop `FileSystem` URIs. Use **`destination: local`** with **`storage_root`** and **`prefix`** to write under a local directory as `file://` URIs, or cloud destinations (`s3`, `gcs`, `blob`) with destination-specific bucket/container fields and **`prefix`**. If your `defaults.yml` omits a `defaults.loading` block entirely, Spine applies a built-in default of **`destination: local`** with relative **`storage_root: ".spine/local-output"`** (resolved under the **repository root**—the directory that contains `src/`—so in a normal checkout it lands next to `config/`) and a placeholder **`prefix`**; copy [`config/defaults.example.yml`](../../config/defaults.example.yml) for an explicit starting point.

Credentials and connectors follow your Spark deployment (for example IAM on AWS, or Hadoop `fs.*` settings for other schemes).

### Amazon S3

Use `destination: "s3"` with `s3_bucket` (canonical) or `bucket` (alias), and `prefix`.
If both `s3_bucket` and `bucket` are set with different values, configuration validation fails.

### Google Cloud Storage

Use `destination: "gcs"` with `gcs_bucket` (canonical) or `bucket` (alias), and `prefix`.
If both `gcs_bucket` and `bucket` are set with different values, configuration validation fails.

```yaml
loading:
  destination: "gcs"
  format: "delta"
  write_mode: "overwrite"
  gcs_bucket: "my-gcs-bucket"
  prefix: "source/resource"
```

### Azure Blob Storage (ABFS)

Use `destination: "blob"` with `azure_container` (canonical) or `bucket` (alias), plus `azure_account`.
If both `azure_container` and `bucket` are set with different values, configuration validation fails.
Compatibility aliases `destination: "azure"` and `destination: "azure_blob"` are also accepted and normalized internally to `azure_blob`.

```yaml
loading:
  destination: "blob"
  format: "delta"
  write_mode: "overwrite"
  azure_container: "my-container"
  azure_account: "my-storage-account"
  prefix: "source/resource"
```

### Local filesystem

**`storage_root`** may be **absolute** or **relative**. Relative values are resolved when the operator config is loaded: they are joined to the **repository root** (the directory that contains `src/`), not to `CONFIG_PATH` or the process current working directory. For a normal checkout `.../myapp/config/`, output goes under `.../myapp/` (for example `.../myapp/.spine/local-output`), not under `config/`. If `CONFIG_PATH` points at YAML outside that tree (for example an absolute mount), relative `storage_root` still resolves under the Spine install root—use an absolute `storage_root` when output should live with that mount.

The same `prefix`, `format`, `write_mode`, and `merge_keys` rules apply across all object-store destinations.

**Recommended for dev/CI:** use a path under the repo that is gitignored, for example:

```yaml
loading:
  destination: "local"
  format: "delta"
  write_mode: "overwrite"
  storage_root: ".spine/local-output"
  prefix: "source/resource"
```

For bind-mounted directories in containers, use an absolute path:

```yaml
loading:
  destination: "local"
  format: "delta"
  write_mode: "overwrite"
  storage_root: "/var/lib/spine/out"
  prefix: "source/resource"
```

At startup validation, the **resolved** directory must already exist and be writable by the process. The check lives in `src/loader/local_storage.py` and runs from the handler during configuration validation.

### Other object stores

Spark can write to **`gs://`** or **`abfs://`** (and other schemes) when the correct Hadoop filesystem implementation and credentials are on the classpath and configured. Spine does not ship those connectors; operators add jars and Spark config as usual. Path and filesystem operations go through the Hadoop `FileSystem` layer in `src/loader/object_store.py`.

## Table formats and write modes

Table formats are handled by load strategies under `src/load_strategy/`. `ObjectStoreLoader` prepares the Spark DataFrame and resolves the object-store base URI, then delegates Delta and Iceberg table behavior to the matching strategy. The shared strategy layer owns write-mode routing, so table formats behave consistently for `append`, `overwrite`, and `merge`.

**Available table formats**: `delta` (default), `iceberg`

**Available table write modes**: `overwrite` (default), `append`, `merge`

For `merge`, `merge_keys` is required and supports composite keys. If the target table does not exist yet, Spine creates it with an append-style write before later runs perform format-specific merge operations.

### Delta Lake

Delta is path-backed in Spine. The resolved table location is the Spark write and merge target, and table existence is checked by looking for the `_delta_log` directory under that location. Delta append/overwrite writes use Spark path writes with `format: "delta"` and schema merge enabled.

Delta merge uses the Delta Lake `DeltaTable.forPath(...)` API. That requires the `delta-spark` Python package and Spark Delta Lake runtime support. Spine imports the Python Delta API lazily when merge is used, so non-Delta and non-merge imports do not fail just because the optional Python API is unavailable.

### Overwrite

Replace all existing data in the Delta table.

```yaml
loading:
  destination: "s3"
  format: "delta"
  write_mode: "overwrite"
  bucket: "my-bucket"
  prefix: "source/resource"
```

### Append

Add new data without removing existing data.

```yaml
loading:
  destination: "s3"
  format: "delta"
  write_mode: "append"
  bucket: "my-bucket"
  prefix: "source/resource"
```

### Merge (Upsert)

Update existing rows and insert new ones based on the configured merge keys.

```yaml
loading:
  destination: "s3"
  format: "delta"
  write_mode: "merge"
  merge_keys: ["id", "timestamp"]
  bucket: "my-bucket"
  prefix: "source/resource"
```

## Iceberg

Iceberg is catalog-backed in Spine. The resolved table location still determines where table data and metadata live, but Spark writes and merges go through the configured `iceberg` Spark catalog. The warehouse root depends on the destination: for S3 writes it is rooted at `s3a://<bucket-name>`, for GCS at `gs://<bucket-name>`, for Azure Blob at `abfs://<container>@<account>.dfs.core.windows.net`, and for local writes at the resolved `storage_root` `file://` URI.

Spine resolves the final table location from the warehouse root plus the configured `prefix`, removes the warehouse root from that location, and converts the remaining path into a quoted catalog table identifier. For example, if the warehouse root is `s3a://my-bucket` and the resolved table location is `s3a://my-bucket/source/resource/`, Spine derives the catalog table identifier ``iceberg.`source`.`resource```.

Iceberg table existence is checked through the Spark catalog using that derived table identifier, not by looking directly for metadata files in object storage.

### Append

Append writes add rows to the existing Iceberg table, creating the table on first write when it does not already exist.

```yaml
loading:
  destination: "local"
  format: "iceberg"
  write_mode: "append"
  storage_root: ".spine/local-iceberg-warehouse"
  prefix: "source/resource"
```

### Overwrite

Overwrite replaces the contents of the target Iceberg table.

```yaml
loading:
  destination: "s3"
  format: "iceberg"
  write_mode: "overwrite"
  bucket: "my-bucket"
  prefix: "source/resource"
```

### Merge

Merge mode performs an Iceberg `MERGE INTO` using the configured `merge_keys`. If the table does not exist yet, Spine creates it first and later runs merges against the catalog table.

```yaml
loading:
  destination: "s3"
  format: "iceberg"
  write_mode: "merge"
  merge_keys: ["id"]
  bucket: "my-bucket"
  prefix: "source/resource"
```

### Current Iceberg limitations

Plan around the following current Iceberg behavior:

- `merge_keys` is required for Iceberg merge mode
- Merge updates only columns present in both the source data and the existing target table
- Merge inserts are shaped to the current target schema; target-only columns are filled with typed `NULL`
- The current merge path does not auto-evolve the target schema before `MERGE INTO`; if the source introduces new columns, use append/overwrite first or evolve the table separately

## Quick reference

| `destination` | Required fields | Notes |
|---------------|-----------------|--------|
| `s3` | `s3_bucket` (or `bucket` alias), `prefix` | `prefix` uses the `source/resource` shape described above. Set **`destination: "s3"`** on the resource (or in defaults) whenever you set `s3_bucket`/`bucket`; shallow merge with **`destination: local`** defaults would otherwise keep `local`. |
| `gcs` | `gcs_bucket` (or `bucket` alias), `prefix` | Requires GCS Hadoop connector and credentials in Spark. |
| `blob` (`azure`, `azure_blob` aliases) | `azure_container` (or `bucket` alias), `azure_account`, `prefix` | Uses ABFS URI form `abfs://container@account.dfs.core.windows.net`. |
| `local` | `storage_root`, `prefix` | `storage_root` may be absolute, or **relative to the repository root** (directory containing `src/`). Relative values are resolved when the config is loaded. |
| *(omitted `defaults.loading`)* | *(built-in default)* | Same as **`local`** with **`storage_root: ".spine/local-output"`** (resolved under the repository root) and **`prefix: "default/output"`**; override per resource or in **`defaults.yml`**. |

For table formats, `format: "delta"` and `format: "iceberg"` both resolve the table location from the destination base URI plus the `source/resource`-style prefix. Delta uses that location directly for path-backed writes and merges. Iceberg uses that location for storage, then derives a catalog table identifier by removing the destination-specific warehouse root (`s3a://<bucket-name>` for S3, `gs://<bucket-name>` for GCS, `abfs://...` for Azure Blob, or the resolved local `file://` URI) and quoting the remaining path parts under the `iceberg` catalog.

Filesystem helpers for loaders live under **`src/loader/`** (for example `object_store.py`, `local_storage.py`).

## Spark Runtime Readiness

Spine composes Spark connector packages and Hadoop filesystem settings from the effective destination set in your selected pipeline config and from **`defaults.spark_runtime`** (see [`config/defaults.example.yml`](../../config/defaults.example.yml)).

| Destination | Required loading fields | Spark connector expectation | Auth expectation |
|-------------|-------------------------|-----------------------------|------------------|
| `local` | `storage_root`, `prefix` | none | local filesystem permissions |
| `s3` | `s3_bucket` (or `bucket` alias), `prefix` | `fs.s3a.*` settings; `hadoop-aws` via Ivy when `defaults.spark_runtime.s3_connector_mode` resolves to `packages` | IAM role, profile, or environment credentials (or explicit keys in local dev paths); S3A region from AWS chain / env, not `spark_runtime` |
| `gcs` | `gcs_bucket` (or `bucket` alias), `prefix` | Shaded GCS Hadoop connector JAR on `spark.jars` (default Maven Central URL) + `fs.gs.*` implementation | runtime-provided Google auth (ADC/service account/workload identity) |
| `azure_blob` (`blob`/`azure`) | `azure_container` (or `bucket` alias), `azure_account`, `prefix` | ABFS connector + `fs.abfs.*` implementation | runtime-provided Azure storage auth |

**Configuration-first:** set `defaults.spark_runtime.profile` (`auto`, `local_dev`, or `cluster_managed`) and `s3_connector_mode` / `gcs_connector_mode` / `azure_connector_mode` (`auto`, `packages`, or `external`). With `auto`, Spine inspects the process environment: on Databricks and EMR, all three default to `external` (connectors expected on the cluster); elsewhere they default to `packages` (Ivy for Delta/S3/Azure connectors; GCS uses the shaded connector JAR on `spark.jars`). S3A endpoint **region** is not part of `spark_runtime`; it follows the AWS credential chain and standard AWS environment variables.

**Optional environment overrides** (same semantics as YAML when set): `SPARK_S3_CONNECTOR_MODE`, `SPARK_GCS_CONNECTOR_MODE`, `SPARK_GCS_CONNECTOR_JAR_URL` (defaults to the official shaded `gcs-connector` JAR on Maven Central when mode is `packages`), `SPARK_AZURE_CONNECTOR_MODE`, `SPINE_GCS_AUTH_TYPE` (defaults to `APPLICATION_DEFAULT`; set `COMPUTE_ENGINE` only when you intentionally rely on GCE metadata). Use these when CI or a container image cannot carry pipeline YAML changes.

### Destination preflight

Spine probes every effective loading destination immediately after the Spark session is initialised, before any source/resource ingestion runs. The probe is **cloud-agnostic and read-only** by default: for each unique destination URI (`s3a://…`, `gs://…`, `abfs://…`, or resolved `file://…`) Spine calls `FileSystem.listStatus` on that root through Spark's Hadoop layer (always, not gated on `exists`, so empty S3 buckets still exercise `ListBucket` / credentials). Failures stop the run with `HandlerError(operation="destination_preflight")` and the destination's scheme/bucket/container in `details`, so missing credentials never reach data write time.

`--validate-only` runs the same code path with `write_probe=True`, additionally writing and deleting a temporary marker object under each destination to confirm write permissions. Local destinations rely on `src/loader/local_storage.py` (existence + `os.W_OK`) which already covers writability without a marker file.

The preflight is the single source of truth for *can we reach this destination*. Spine does not ship per-cloud Python SDK validators; do not add `google-cloud-storage`, `azure-storage-blob`, or similar runtime checks. Extend [`src/loader/destination_preflight.py`](../../src/loader/destination_preflight.py) (or its caller in [`src/handler/dynamic_handler.py`](../../src/handler/dynamic_handler.py)) when symmetric coverage is needed for new destinations.
