# Loading Configuration

## Table of Contents

- [Destinations](#destinations)
  - [Amazon S3](#amazon-s3)
  - [Local filesystem](#local-filesystem)
  - [Other object stores (preview)](#other-object-stores-preview)
- [Delta Save Modes](#delta-save-modes)
  - [Overwrite](#overwrite)
  - [Append](#append)
  - [Merge (Upsert)](#merge-upsert)
- [Iceberg](#iceberg)
  - [Append](#append-1)
  - [Overwrite](#overwrite-1)
  - [Merge (preview)](#merge-preview)
  - [Current limitations](#current-limitations)
- [Quick reference](#quick-reference)

## Destinations

Loading uses Spark with Hadoop `FileSystem` URIs. Use **`destination: local`** with **`storage_root`** and **`prefix`** to write under a local directory as `file://` URIs, or **`destination: s3`** with **`bucket`** and **`prefix`** for Amazon S3. If your `defaults.yml` omits a `defaults.loading` block entirely, Spine applies a built-in default of **`destination: local`** with relative **`storage_root: ".spine/local-output"`** (resolved under the **repository root**—the directory that contains `src/`—so in a normal checkout it lands next to `config/`) and a placeholder **`prefix`**; copy [`config/defaults.example.yml`](../../config/defaults.example.yml) for an explicit starting point.

Credentials and connectors follow your Spark deployment (for example IAM on AWS, or Hadoop `fs.*` settings for other schemes).

### Amazon S3

Use `destination: "s3"`, `bucket`, and `prefix` as in the examples below. `prefix` must look like `source_name/resource_name` (not a single segment).

### Local filesystem

**`storage_root`** may be **absolute** or **relative**. Relative values are resolved when the operator config is loaded: they are joined to the **repository root** (the directory that contains `src/`), not to `CONFIG_PATH` or the process current working directory. For a normal checkout `.../myapp/config/`, output goes under `.../myapp/` (for example `.../myapp/.spine/local-output`), not under `config/`. If `CONFIG_PATH` points at YAML outside that tree (for example an absolute mount), relative `storage_root` still resolves under the Spine install root—use an absolute `storage_root` when output should live with that mount.

The same `prefix`, `format`, `write_mode`, and `merge_keys` rules apply as for S3.

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

### Other object stores (preview)

Spark can write to **`gs://`** or **`abfs://`** (and other schemes) when the correct Hadoop filesystem implementation and credentials are on the classpath and configured. Spine does not ship those connectors; operators add jars and Spark config as usual. Path and filesystem operations go through the Hadoop `FileSystem` layer in `src/loader/object_store.py`.

## Delta Save Modes

When using Delta format, control how data is written with the `write_mode` option. Schema evolution is automatically enabled for all modes.

**Available modes**: `overwrite` (default), `append`, `merge`

### Overwrite

Replace all existing data in the table.

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

Update existing rows and insert new ones based on primary keys.

```yaml
loading:
  destination: "s3"
  format: "delta"
  write_mode: "merge"
  merge_keys: ["id", "timestamp"]
  bucket: "my-bucket"
  prefix: "source/resource"
```

**Notes**

- `merge_keys` is required for merge mode (list of column names)
- Supports composite keys (multiple columns)
- Tables are created automatically if they don't exist

## Iceberg

When using Iceberg format, Spine writes through the configured `iceberg` Spark catalog. The warehouse root depends on the destination: for S3 writes it is rooted at `s3a://<bucket-name>`, and for local writes it is rooted at the configured `storage_root`. Spine first resolves the final table path from that warehouse root plus the configured `prefix`, then removes the warehouse root from the resolved path and converts the remaining path into a catalog table identifier by replacing `/` with `.` and prefixing it with `iceberg.`. For example, if the warehouse root is `s3a://my-bucket` and the resolved table path is `a://my-bucket/source/resource`, Spine derives the catalog table identifier `iceberg.source.resource`.

**Available modes**: `overwrite`, `append`, `merge`

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

### Merge (preview)

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

### Current limitations

Iceberg support is still in an early merge-support phase. Plan around the following current behavior:

- `merge_keys` is required for Iceberg merge mode
- Merge updates only columns present in both the source data and the existing target table
- Merge inserts are shaped to the current target schema; target-only columns are filled with typed `NULL`
- The current merge path does not auto-evolve the target schema before `MERGE INTO`; if the source introduces new columns, use append/overwrite first or evolve the table separately
- Iceberg table existence is currently detected from filesystem metadata under the resolved table path

## Quick reference

| `destination` | Required fields | Notes |
|---------------|-----------------|--------|
| `s3` | `bucket`, `prefix` | `prefix` uses the `source/resource` shape described above. Set **`destination: "s3"`** on the resource (or in defaults) whenever you set `bucket`; shallow merge with **`destination: local`** defaults would otherwise keep `local`. |
| `local` | `storage_root`, `prefix` | `storage_root` may be absolute, or **relative to the repository root** (directory containing `src/`). Relative values are resolved when the config is loaded. |
| *(omitted `defaults.loading`)* | *(built-in default)* | Same as **`local`** with **`storage_root: ".spine/local-output"`** (resolved under the repository root) and **`prefix: "default/output"`**; override per resource or in **`defaults.yml`**. |

For table formats, `format: "delta"` and `format: "iceberg"` both use the `source/resource`-style prefix directly as the table location. For Iceberg, Spine resolves the final table path under the destination-specific warehouse root (`s3://<bucket-name>` for S3, `storage_root` for local), removes that warehouse root, and converts the remaining path into the catalog table name by replacing `/` with `.` and prefixing it with `iceberg.`.

Filesystem helpers for loaders live under **`src/loader/`** (for example `object_store.py`, `local_storage.py`).
