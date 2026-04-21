# Value Resolution

Value resolution in the pipeline uses two distinct syntaxes, resolved at different stages:

## Delimiter Convention

| Syntax | When | Purpose |
|--------|------|---------|
| `${VAR}` / `${VAR:-default}` | **Load time** | Environment variables (secrets, config) |
| `{{ expr }}` | **Runtime** | Dynamic values (dates, timestamps, cross-source refs) |

## Load-time Resolution (`${...}`)

Resolved during configuration loading, before Pydantic validation.

**Supported patterns:**
- `${VAR_NAME}` – simple reference (required; raises if unset)
- `${VAR_NAME:-default}` – with default if unset

**Example:**
```yaml
headers:
  "api_key": "${API_KEY}"
  "Content-Type": "application/json"

# Path with env var
path: "/workspaces/${WORKSPACE_ID}/projects"

# With default
redis:
  host: "${REDIS_HOST:-localhost}"
  port: "${REDIS_PORT:-6379}"
```

## Runtime Resolution (`{{ ... }}`)

Resolved when building requests, headers, parameters, and transformations using Jinja2 with custom functions.

**Available functions:**

| Function | Example | Description |
|----------|---------|-------------|
| `now_iso()` | `{{ now_iso() }}` | Current time as ISO 8601 string |
| `now_ms()` | `{{ now_ms() }}` | Millisecond timestamp |
| `now_unix()` | `{{ now_unix() }}` | Unix timestamp |
| `uuid()` | `{{ uuid() }}` | UUID v4 |
| `date(op, days?, format?)` | `{{ date('DAYS_AGO', days=1) }}` | Date by operation |
| `databricks(query_ref)` | `{{ databricks('uk_aus_nz_store_locations') }}` | Query result from Redis/Databricks |
| `rsa_sign(key, inputs, algorithm?)` | `{{ rsa_sign(key=env.KEY, inputs=[env.ID, now_ms(), '1']) }}` | RSA signature |
| `env` | `{{ env.VAR_NAME }}` | Environment variable at runtime |

**Date operations:** `TODAY`, `DAYS_AGO`, `DAYS_FUTURE`, `PREVIOUS_SUNDAY`, `PREVIOUS_SATURDAY`, `LINKEDIN_PREVIOUS_MONTH_RANGE`, `MONTH_START`, `MONTH_END`.

**Examples:**
```yaml
# Headers
headers:
  "X-Timestamp": "{{ now_ms() }}"
  "X-Request-ID": "{{ uuid() }}"

# Inline Jinja (embed expressions in strings)
request_inputs:
  query:
    value: >-
      SELECT ... WHERE date BETWEEN '{{ date('PREVIOUS_SUNDAY') }}' AND '{{ date('PREVIOUS_SATURDAY') }}'
  start_date:
    value: "{{ date('DAYS_AGO', days=7) }}"
  query:
    value: "{{ databricks('store_locations') }}"

# Transformations
transformations:
  - type: "add_column"
    name: "_ingested_at"
    value: "{{ now_iso() }}"

# RSA signature (requires env vars)
  "WM_SEC.AUTH_SIGNATURE": "{{ rsa_sign(key=env.PRIVATE_KEY_BASE64, inputs=[env.CONSUMER_ID, now_ms(), '1'], algorithm='SHA256') }}"
```

## SOURCE Type (Structured Config)

Request inputs that pull values from other resources use a structured format (not Jinja) because they need filter and context handling:

```yaml
request_inputs:
  projectId:
    value:
      type: SOURCE
      source_config:
        source: "projects"
        field: "project_id"
    input_format: "single"
    request_format: "string"
    batch_size: 1
```

## Backfill Config

Backfill for path, query, or body inputs uses a `value` dict with a `backfill` key. Jinja can be used for dynamic `end` and `limit`:

```yaml
request_inputs:
  startDate:
    value:
      value: "{{ date('DAYS_AGO', days=17) }}"
      backfill:
        type: STATIC_DATE
        start: '2024-01-01'
        end: "{{ date('TODAY') }}"
        increment: '60 DAY'
  endDate:
    value:
      value: "{{ date('DAYS_AGO', days=2) }}"
      backfill:
        type: REFERENCE
        field: startDate
        increment: '60 DAY'
        limit: "{{ date('DAYS_AGO', days=2) }}"
```
