# Configuration Overview

## Table of Contents

- [Configuration Layout](#configuration-layout)
- [Main Structure](#main-structure)
- [Configuration Topics](#configuration-topics)
- [Spark JDBC read tuning (database resources)](#spark-jdbc-read-tuning-database-resources)
- [Database resources and request contexts](#database-resources-and-request-contexts)

## Configuration Layout

Execution configuration lives in the repository `config/` directory (next to `src/`):

- `defaults.yml` — Version, global retry, loading, context defaults, and named queries (operator-local; copy from `defaults.example.yml` in the public repo)
- `sources/` — One YAML file per data source (filename stem = source name); templates live under `config/examples/`
- `queries/` — SQL files referenced from `defaults.yml` (operator-local)

Python modules that load and validate this data live under `src/config/` (for example `config_loader.py`, `config_models.py`).

### `CONFIG_PATH`

Environment variable `CONFIG_PATH` (via pydantic-settings) selects which directory under `<repo_root>/config/` to load:

- Default `.` → `<repo_root>/config/` (the `config/` folder at the repository root).
- Relative values → resolved under `<repo_root>/config/` (e.g. `staging` → `config/staging/`).
- Absolute path → used as the pipeline config directory directly (for custom layouts or mounted volumes).

## Main Structure

`defaults.yml` and each `sources/*.yml` use this structure:

```yaml
version: "1.0"

defaults:
  retry:
    max_attempts: 3
    initial_delay: 1
    backoff_factor: 2
    # HTTP client (urllib3): Retry-After on 429/503/413, capped by max_retry_after_seconds.
    honor_retry_after_header: true
    max_retry_after_seconds: 21600
    max_backoff_seconds: 120
    backoff_jitter_seconds: 0
  loading:
    destination: "s3"
    format: "delta"
    write_mode: "overwrite"
    compression: "snappy"
    bucket: "your-bucket-name"
  context:
    type: "redis"
    ttl: 3600

sources:
  your_source_name:
    enabled: true
    type: "rest_api"
    base_url: "https://api.example.com"
    auth:
      type: "oauth_jwt"  # or "bearer_token", "api_key", "basic"
    resources:
      resource_name:
        enabled: true
        path: "/api/endpoint"
        method: "GET"
        # ... resource configuration
```

## Configuration Topics

| Topic | Description |
|-------|-------------|
| [Request inputs](parameters.md) | Request inputs (path/query/body), static/dynamic, SOURCE, DATABRICKS, DATE, batching, formats, shorthand, headers, POST body |
| [Backfill](backfill.md) | Date-range backfill for path, query, or body request inputs |
| [Loading](loading.md) | Delta save modes (overwrite, append, merge), destination options (`local`, `s3`, `gcs`, `azure_blob`) and field aliases |
| [Auth](auth.md) | OAuth JWT, bearer token, API key |
| [Transformations](transformations.md) | add_column, add_column_from_request, ensure_param_values_in_output |
| **PostgreSQL / HANA** | `type: postgresql` or `type: hana` with JDBC-style connection fields. PostgreSQL requires `database`. For HANA, `database` is the optional **tenant database name** sent as the JDBC `databaseName` parameter (must match a real tenant when your SQL port is shared). Omit it when `host:port` already targets a single tenant. Runtime images need the SAP **ngdbc** JAR on the Spark classpath (Spine adds `com.sap.cloud.db.jdbc:ngdbc` via `spark.jars.packages`). See [config/examples/postgres.example.yml](../../config/examples/postgres.example.yml). |

### Spark JDBC read tuning (database resources)

Optional **`table_read_options`** describes **Spark `DataFrameReader.jdbc`** options: parallel range reads, predicate lists, JDBC `fetchSize`, and whether to run an exact `count()` after the read for logs. **PostgreSQL** and **HANA** both read through Spark JDBC in this repository, so the same block is honored for those `type` values. Future relational source types may reject the block until they use the same Spark read path.

- **`fetch_size`**: optional JDBC `fetchSize` hint (positive integer) in Spark connection properties when the backend uses Spark JDBC.
- **Range partitioning** (mutually exclusive with `predicates`): **`partition_column`**, **`lower_bound`**, **`upper_bound`**, **`num_partitions`**. Bounds are **operator-supplied** (Spine does not infer min/max). The column must suit Spark’s JDBC partitioner (typically an integer key).
- **`predicates`**: non-empty list of `WHERE` fragments for predicate-based JDBC reads. Do not combine with range mode fields.
- **`log_exact_row_count`**: when `true`, run `df.count()` after the JDBC read for exact logging (extra scan). When `false` (default), that count is skipped unless **`defaults.log_full_row_count`** is `true` in `defaults.yml`.

Future JDBC-backed sources (for example MySQL or Redshift) can reuse this block once they use the same Spark read path. See commented examples in [config/examples/postgres.example.yml](../../config/examples/postgres.example.yml).

### Default loading and row counts

- **`defaults.loading`** is merged into every resource that does not define its own `loading` block (and `loading: null` in YAML is treated the same as omitted: inherit defaults). For object-store destinations (**`local`**, **`s3`**, **`gcs`**, **`azure_blob`**), if **`prefix`** is omitted, the handler sets **`{source_name}/{resource_name}`** before writing.
- **`loading.enabled`**: after merging defaults, set **`enabled: false`** on a resource to skip loader writes for that resource only.
- **`defaults.log_full_row_count`**: when **`true`**, the handler runs a full Spark **`df.count()`** for result summaries and enables the same global behavior for database extracts unless a resource opts in with **`table_read_options.log_exact_row_count`**. When **`false`** (default), the handler uses a lightweight non-empty check instead of a full count, and database extracts skip **`df.count()`** unless **`table_read_options.log_exact_row_count`** is **`true`** for that resource.

### Retry budget and HTTP transport (REST)

- **`max_attempts`**, **`initial_delay`**, and **`backoff_factor`** still describe the overall retry budget surfaced to **`APISettings`** (including **`RestService`** auth backoff).
- **`honor_retry_after_header`**, **`max_retry_after_seconds`**, **`max_backoff_seconds`**, and **`backoff_jitter_seconds`** configure urllib3 `Retry` on the shared **`requests.Session`**: when **`honor_retry_after_header`** is true, the client sleeps per **`Retry-After`** on responses urllib3 retries (including **429**, **503**, and **413**), with **`Retry-After`** capped by **`max_retry_after_seconds`**. Exponential backoff between attempts is capped by **`max_backoff_seconds`**; **`backoff_jitter_seconds`** adds random delay on that backoff path only (urllib3 behavior).
- Turning on **`Retry-After`** can increase wall-clock time per call versus ignoring it; **`max_attempts`** defaults are unchanged.

### Database resources and request contexts

For **database-backed** resources (relational sources configured with `database_schema` / `database_table` or `database_select_query`), Spine reads the configured table or query **once per resource run**. [Request contexts](parameters.md) from `batch_inputs` and related expansion drive REST/SDK calls on other source types; on database resources they do not cause repeated `SELECT`-style extracts of the same static query.

- **Rejection:** If expansion would produce **more than one** request context for a database-backed resource, Spine **fails before ingest** when that is provable from static batch configuration (execution plan build). If batch values are resolved only at run time (for example from other resources), the handler **still raises** after expansion (after any `record_limit` on contexts) and before the extract. Only the first context would influence transformations otherwise. Fix the pipeline by removing batch expansion for that resource, using a single context (for example via `record_limit`), or splitting work into separate resources.
- **Why:** The handler does not substitute per-context values into `database_schema`, `database_table`, or `database_select_query` today. A single extract avoids duplicate database load and avoids duplicating identical rows in Spark.
- **Transformations:** When transformations run on database-sourced DataFrames, the request context passed in is taken from **`request_contexts[0]`** (the sole context after a successful run). Design transforms for that single context.
- **Scoping data:** To limit which rows are read, set **`database_select_query`** to the SQL you need (static query in YAML). Per-context or templated SQL is not supported yet; if you need different extracts per context, split into separate resources or follow future docs for SQL templating.
