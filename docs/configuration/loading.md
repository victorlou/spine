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
- [Quick reference](#quick-reference)

## Destinations

Loading uses Spark with Hadoop `FileSystem` URIs. Use **`destination: local`** with **`storage_root`** and **`prefix`** to write under a local directory as `file://` URIs, or **`destination: s3`** with **`bucket`** and **`prefix`** for Amazon S3. If your `defaults.yml` omits a `defaults.loading` block entirely, Spine applies a built-in default of **`destination: local`** with a relative **`storage_root`** under `.spine/local-output` and a placeholder **`prefix`**; copy [`config/defaults.example.yml`](../../config/defaults.example.yml) for an explicit starting point.

Credentials and connectors follow your Spark deployment (for example IAM on AWS, or Hadoop `fs.*` settings for other schemes).

### Amazon S3

Use `destination: "s3"`, `bucket`, and `prefix` as in the examples below. `prefix` must look like `source_name/resource_name` (not a single segment).

### Local filesystem

**`storage_root`** may be **absolute** or **relative**. Relative values are resolved when the operator config is loaded: they are joined to the **operator config directory** (the directory you pass as `CONFIG_PATH` / the folder that contains `defaults.yml`), not the process current working directory. That keeps paths portable across macOS, Linux, and Windows (paths are normalized with `pathlib` before Spark sees them).

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

## Quick reference

| `destination` | Required fields | Notes |
|---------------|-----------------|--------|
| `s3` | `bucket`, `prefix` | `prefix` uses the `source/resource` shape described above. Set **`destination: "s3"`** on the resource (or in defaults) whenever you set `bucket`; shallow merge with **`destination: local`** defaults would otherwise keep `local`. |
| `local` | `storage_root`, `prefix` | `storage_root` may be absolute, or relative to the operator config directory (`CONFIG_PATH`, the folder that contains `defaults.yml`). Relative values are resolved when the config is loaded. |
| *(omitted `defaults.loading`)* | *(built-in default)* | Same as **`local`** with relative **`storage_root: ".spine/local-output"`** and placeholder **`prefix: "default/output"`**; override per resource or in **`defaults.yml`**. |

Filesystem helpers for loaders live under **`src/loader/`** (for example `object_store.py`, `local_storage.py`).
