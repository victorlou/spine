# Configuration Overview

## Table of Contents

- [Configuration Layout](#configuration-layout)
- [Main Structure](#main-structure)
- [Configuration Topics](#configuration-topics)
- [Spark JDBC read tuning (database resources)](#spark-jdbc-read-tuning-database-resources)
- [Database resources and request contexts](#database-resources-and-request-contexts)
- [Database incremental extract (JDBC)](database-incremental.md)

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
| [Loading](loading.md) | Object-store destinations (`local`, `s3`, `gcs`, `azure_blob`), field aliases, table formats (`delta`, `iceberg`), and write modes (`overwrite`, `append`, `merge`) |
| [Auth](auth.md) | OAuth JWT, bearer token, API key |
| [Transformations](transformations.md) | add_column, add_column_from_request, ensure_param_values_in_output |
| [Database incremental (JDBC)](database-incremental.md) | Companion CDC table, watermark/cursor, **`correlation`** (`join_columns` vs `join_predicate` vs inference, **`companion_metadata_columns`**) |
| **PostgreSQL / HANA** | `type: postgresql` or `type: hana` with JDBC-style connection fields. PostgreSQL requires `database`. For HANA, `database` is the optional **tenant database name** sent as the JDBC `databaseName` parameter (must match a real tenant when your SQL port is shared). Omit it when `host:port` already targets a single tenant. Runtime images need the SAP **ngdbc** JAR on the Spark classpath (Spine adds `com.sap.cloud.db.jdbc:ngdbc` via `spark.jars.packages`). See [config/examples/postgres.example.yml](../../config/examples/postgres.example.yml). |

### Spark JDBC read tuning (database resources)

Optional **`table_read_options`** describes **Spark `DataFrameReader.jdbc`** options: parallel range reads, predicate lists, and JDBC `fetchSize`. **PostgreSQL** and **HANA** both read through Spark JDBC in this repository, so the same block is honored for those `type` values. Future relational source types may reject the block until they use the same Spark read path.

**`fetch_size` vs parallel reads**

- **`fetch_size`**: JDBC **`fetchSize`** hint passed through Spark connection properties (`fetchsize`). It controls **how many rows the driver requests per fetch** for each Spark task’s cursor: fewer round-trips to the database and often smoother **per-task** read behavior. It does **not** add Spark partitions, cap total rows read, or guarantee freedom from out-of-memory failures. Large extracts still need adequate Spark executor memory and storage, avoiding unnecessary full scans for logging, and (when needed) parallel JDBC reads below.
- **Range partitioning** (mutually exclusive with `predicates`): **`partition_column`**, **`lower_bound`**, **`upper_bound`**, **`num_partitions`**. This splits the read into **multiple JDBC queries** (one per partition range), so Spark can run **parallel tasks** across executors. Bounds are **operator-supplied** (Spine does not infer min/max). The column must suit Spark’s JDBC partitioner (typically an integer key). Without range mode or predicates, the extract uses a **single** JDBC partition regardless of `fetch_size`; write parallelism later follows that upstream partition count unless you set **`loading.output_partitions`** (see [Loading](loading.md)).
- **`predicates`**: non-empty list of `WHERE` fragments for predicate-based JDBC reads (parallel tasks per predicate). Do not combine with range mode fields.

At extract time the pipeline logs **`JDBC extract plan`** (read mode, predicate or range parameters, optional **`fetch_size`**) and **`spark_partitions`** from the lazy Spark plan so you can confirm the JDBC call shape without opening the Spark UI. **`take(1)`** for non-empty checks uses only one task and does not validate parallel reads; bulk write parallelism follows upstream partitions unless **`loading.output_partitions`** coalesces downward—see [Loading — Writer partitioning](loading.md#writer-partitioning-output_partitions).

Future JDBC-backed sources (for example MySQL or Redshift) can reuse this block once they use the same Spark read path. See commented examples in [config/examples/postgres.example.yml](../../config/examples/postgres.example.yml).

### Default loading

- **`defaults.loading`** is merged into every resource that does not define its own `loading` block (and `loading: null` in YAML is treated the same as omitted: inherit defaults). For object-store destinations (**`local`**, **`s3`**, **`gcs`**, **`azure_blob`**), if **`prefix`** is omitted, the handler sets **`{source_name}/{resource_name}`** before writing. Table formats (**`delta`**, **`iceberg`**) use that resolved prefix as the table location; Iceberg additionally derives a Spark catalog table identifier from the location.
- **`loading.output_partitions`** (optional): for **Delta and Iceberg**, narrows the **incoming** DataFrame to this many Spark partitions with **`coalesce`** before **append**, **overwrite**, or **merge** (merge: source batch only). **Omit** for large extracts so partition count follows the JDBC read (for example parallel **`table_read_options`**). Non-table Parquet writes through the object-store loader still use a single output partition. Details: [Loading — Writer partitioning](loading.md#writer-partitioning-output_partitions).
- **`loading.enabled`**: after merging defaults, set **`enabled: false`** on a resource to skip loader writes for that resource only.

### Database resources and request contexts

For **database-backed** resources (relational sources configured with `database_schema` / `database_table` or `database_select_query`), Spine reads the configured table or query **once per resource run**. [Request contexts](parameters.md) from `batch_inputs` and related expansion drive REST/SDK calls on other source types; on database resources they do not cause repeated `SELECT`-style extracts of the same static query.

- **Rejection:** If expansion would produce **more than one** request context for a database-backed resource, Spine **fails before ingest** when that is provable from static batch configuration (execution plan build). If batch values are resolved only at run time (for example from other resources), the handler **still raises** after expansion (after any `record_limit` on contexts) and before the extract. Only the first context would influence transformations otherwise. Fix the pipeline by removing batch expansion for that resource, using a single context (for example via `record_limit`), or splitting work into separate resources.
- **Why:** The handler does not substitute per-context values into `database_schema`, `database_table`, or `database_select_query` today. A single extract avoids duplicate database load and avoids duplicating identical rows in Spark.
- **Transformations:** When transformations run on database-sourced DataFrames, the request context passed in is taken from **`request_contexts[0]`** (the sole context after a successful run). Design transforms for that single context.
- **Scoping data (schema/table):** Optional **`database_where_predicate`** is a boolean SQL fragment on the main table when **`database_select_query`** is not set (a leading `WHERE` is stripped; use alias **`m.`** for main columns). It applies with or without **`incremental_extract`** and composes with **`table_read_options.predicates`** as `AND` fragments, not a second top-level `WHERE`.
- **Scoping data (custom SQL):** Set **`database_select_query`** for a static custom `SELECT` (cannot be combined with **`database_where_predicate`**; put filters in the SQL). Spine turns off Spark JDBC V2 LIMIT/OFFSET pushdown for those reads so a `LIMIT` inside your `SELECT` is not merged into nested subqueries in a way that breaks some databases (for example SAP HANA). Per-context or templated SQL is not supported yet; if you need different extracts per context, split into separate resources or follow future docs for SQL templating.
- **Incremental extract (optional):** Resource-level **`incremental_extract`** (`jdbc_companion_cdc`) needs **`loading.format`** **`delta`** or **`iceberg`**, **`append`** or **`merge`**, and cannot be used with **`database_select_query`**. Operator guide (**`correlation`**, cold vs warm runs, examples): [Database incremental extract (JDBC)](database-incremental.md). Schema definitions: **`src/config/incremental_extract.py`**; contributor summary: [`AGENTS.md`](../../AGENTS.md).
