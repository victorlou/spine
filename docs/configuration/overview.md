# Configuration Overview

## Table of Contents

- [Configuration Layout](#configuration-layout)
- [Main Structure](#main-structure)
- [Configuration Topics](#configuration-topics)
- [Database resources and request contexts](#database-resources-and-request-contexts)

## Configuration Layout

Execution configuration lives in the repository `config/` directory (next to `src/`):

- `defaults.yml` — Version, global retry, loading, context defaults, and named queries (operator-local; copy from `defaults.example.yml` in the public repo)
- `sources/` — One YAML file per data source (filename stem = source name); templates live under `config/examples/`
- `queries/` — SQL files referenced from `defaults.yml` (operator-local)

Python modules that load and validate this data live under `src/config/` (for example `config_loader.py`, `config_models.py`).

### `CONFIG_PATH`

Environment variable `CONFIG_PATH` (via pydantic-settings) selects which directory under `<repo_root>/config/` to load:

- Default `.` → `<repo_root>/config/` (the `config/` folder at the project root).
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
| [Backfill](backfill.md) | Date-range backfill for body request inputs |
| [Loading](loading.md) | Delta save modes (overwrite, append, merge), S3 |
| [Auth](auth.md) | OAuth JWT, bearer token, API key |
| [Transformations](transformations.md) | add_column, add_column_from_request, ensure_param_values_in_output |
| **PostgreSQL / HANA** | `type: postgresql` or `type: hana` with JDBC-style connection fields. PostgreSQL requires `database`. For HANA, `database` is the **tenant database name** passed to hdbcli as `databaseName` (must match a real tenant; errors such as `database '…' not connected` usually mean the wrong name). It can be omitted when your `host:port` already targets a single tenant. See [config/examples/postgres.example.yml](../../config/examples/postgres.example.yml). |

### Database resources and request contexts

For PostgreSQL and HANA resources, Spine reads the configured table (or `database_select_query`) **once per resource run**, regardless of how many [request contexts](parameters.md) batching or backfill produces. Request contexts still drive REST/SDK calls; they do **not** cause repeated `SELECT`-style extracts of the same static query.

- **Why:** The handler does not substitute per-context values into `database_schema`, `database_table`, or `database_select_query` today. Running one extract avoids duplicate database load and avoids duplicating identical rows in Spark when `request_contexts` has length greater than one.
- **Transformations:** When transformations run on database-sourced DataFrames, the request context passed in is taken from **`request_contexts[0]`** (first context after expansion). Design batch inputs and transforms accordingly.
- **Scoping data:** To limit which rows are read, set **`database_select_query`** to the SQL you need (static query in YAML). Per-context or templated SQL is not supported yet; if you need different extracts per context, split into separate resources or follow future docs for SQL templating.
