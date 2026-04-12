# Request Inputs Configuration

All resource inputs (path, query, and body) are configured under **request_inputs**. Each input can specify **location** (`path`, `query`, or `body`). If omitted, the default is **query** for GET and **body** for POST.

## Table of Contents

- [Overview](#overview)
  - [Path inputs](#path-inputs)
  - [Static inputs and shorthand](#static-inputs-and-shorthand)
- [Dynamic inputs](#dynamic-inputs)
  - [SOURCE](#source)
  - [DATE](#date)
  - [DATABRICKS](#databricks)
- [Iteration and batch size](#iteration-and-batch-size)
- [Nested value support](#nested-value-support)
- [Request and input formats](#request-and-input-formats)
- [Preprocessing](#preprocessing)
- [Dynamic values in headers](#dynamic-values-in-headers)
- [POST body inputs and exclude_from_request_body](#post-body-inputs-and-exclude_from_request_body)
- [Workflow examples](#workflow-examples)

## Overview

Request inputs can be:
- **Query inputs**: Added to the URL (e.g. `?field_group=ACCOUNT`) — default for GET
- **Path inputs**: Substituted into the URL path (e.g. `/store/{storeNbr}/gtin/{gtin}/available`) — use `location: "path"`
- **Body inputs**: Sent in the POST body — default for POST

Types: **static** (fixed values) or **dynamic** (from sources, Databricks, date calculations).

### Path inputs

Use `{paramName}` placeholders in the path. Path inputs support the same features as query/body inputs but must resolve to single values. Set **location: "path"** explicitly.

```yaml
resources:
  store_inventory:
    path: "store/{storeNbr}/gtin/{gtin}/available"
    method: "GET"
    request_inputs:
      storeNbr:
        location: "path"
        value: ["1", "2", "3"]
        batch_size: 1
      gtin:
        location: "path"
        value: ["00840191614491", "00840191614477"]
        batch_size: 1
```

### Static inputs and shorthand

You can use full form (`value: ...`) or shorthand (scalar or list as the key value; the pipeline normalizes to `{ value: ... }`).

```yaml
request_inputs:
  # Full form
  field_group:
    value: "CAMPAIGN"
  advertiser_id:
    value: "${ADVERTISER_ID}"

  # Shorthand (same result after normalization)
  username: "${UNIS_LOGISTICS_USERNAME}"
  dimensions: ["EVENT_DAY", "FLIGHT_NAME"]
```

Use full form when you need `batch_size`, `source_config`, `location`, `request_format`, etc.

## Dynamic inputs

### SOURCE

Reference data from other resources, creating dependencies:

```yaml
request_inputs:
  account_ids:
    value:
      type: SOURCE
      source_config:
        source: "accounts"
        field: "account_id"
    input_format: "single"
    request_format: "array"
    batch_size: 1
```

**With filtering** (filter source data by current request context):

```yaml
request_inputs:
  campaign_ids:
    value:
      type: SOURCE
      source_config:
        source: "campaigns"
        field: "campaign_id"
        filter:
          field: "account_id"
          type: "column"
          operator: "eq"
          value_source:
            source: "accounts"
            field: "account_id"
          value_type: "parameter"
    input_format: "single"
    request_format: "array"
    batch_size: 1
```

### DATE

```yaml
request_inputs:
  historical_date_start:
    value: "{{ date('DAYS_AGO', days=10) }}"
  startDate:
    value: "{{ date('DAYS_AGO', days=9) }}"
  endDate:
    value: "{{ date('DAYS_AGO', days=2) }}"
```

(On a POST resource, omitting `location` makes these body inputs by default.)

**Supported date operations**
- `TODAY` — Current date (YYYY-MM-DD)
- `DAYS_AGO` — Date N days in the past (specify `days`)
- `DAYS_FUTURE` — Date N days in the future
- `LINKEDIN_PREVIOUS_MONTH_RANGE` — Previous month in LinkedIn format: `(start:(day:D,month:M,year:Y),end:(day:D,month:M,year:Y))`

### DATABRICKS

Query Databricks tables for input values (query results are cached in Redis by `query_ref`):

```yaml
request_inputs:
  query:
    value: "{{ databricks('uk_aus_nz_store_locations') }}"
    batch_size: 50
    request_format:
      type: "string"
      preprocess:
        - type: "concat"
          separator: ";"
```

**Multiple fields**: The result is a list of lists; downstream logic must handle parsing.

## Iteration and batch size

Static list inputs create a cartesian product (each combination = separate request). Use `batch_size` to group values:

```yaml
request_inputs:
  genre:
    value: ["POP", "ROCK", "HIP_HOP"]
    request_format: "string"
  country_code:
    value: ["AU", "BE", "NL", "CA"]
    request_format: "string"
# Creates 3 × 4 = 12 request combinations
```

**Batch size options**: `1` (one per request), `50`, `"all"` (all in one request).

## Nested value support

Nested `value` fields are resolved independently. Useful for pagination inside a larger structure:

```yaml
request_inputs:
  paging:
    value:
      Limit: 1000
      PageNo:
        value:
          type: PAGINATION
          pagination_config:
            type: "page_number"
            page_info_path: "data.page_info"
            response_total_pages_field: "total_page"
```

## Request and input formats

| Request format | Result |
|----------------|--------|
| `"string"` | Single string |
| `"array"` | JSON array `["v1","v2"]` |
| `"json_string"` | Stringified JSON array |

| Input format | Interpretation |
|--------------|----------------|
| `"single"` | Individual values |
| `"array"` | Array of values |

```yaml
request_inputs:
  advertiser_ids:
    value: "${ADVERTISER_IDS}"
    input_format: "array"
    request_format: "string"
    batch_size: 1
```

## Preprocessing

Apply transformations before sending. Example: concat with separator:

```yaml
request_format:
  type: "string"
  preprocess:
    - type: "concat"
      separator: ";"
```

## Dynamic values in headers

```yaml
headers:
  "WM_QOS.CORRELATION_ID": "{{ uuid() }}"
  "WM_CONSUMER.INTIMESTAMP": "{{ now_ms() }}"
  "WM_SEC.AUTH_SIGNATURE": "{{ rsa_sign(key=env.PRIVATE_KEY_BASE64, inputs=[env.CONSUMER_ID, now_ms(), '1'], algorithm='SHA256') }}"
```

See [value-resolution.md](value-resolution.md) for all runtime functions (`now_iso`, `now_ms`, `uuid`, `date`, `databricks`, `rsa_sign`, etc.).

## POST body inputs and exclude_from_request_body

On POST resources, inputs without a location (or with `location: "body"`) are sent in the request body. The pipeline builds the body from these **body inputs** and applies **exclude_from_request_body** to strip keys that should not be sent (e.g. backfill driver/reference fields used only for date generation).

```yaml
resources:
  campaigns_daily:
    path: "/aggregate"
    method: "POST"
    request_inputs:
      reporting_type: "PACING_PERFORMANCE"
      dimensions: ["EVENT_DAY", "FLIGHT_NAME"]
      metrics: ["IMPRESSIONS", "CLICKS"]
      account_ids:
        value:
          type: SOURCE
          source_config:
            source: "accounts"
            field: "account_id"
        input_format: "single"
        request_format: "array"
        batch_size: 1
    exclude_from_request_body:
      - startDate
      - endDate
```

## Workflow examples

### Multi-level dependency with filtering

```yaml
resources:
  accounts:
    path: "/accounts"
    request_inputs:
      field_group: "ACCOUNT"
    fields:
      - name: "account_id"
        source: "id"

  campaigns:
    path: "/campaigns"
    request_inputs:
      account_ids:
        value:
          type: SOURCE
          source_config:
            source: "accounts"
            field: "account_id"
        input_format: "single"
        request_format: "array"
        batch_size: 1
    fields:
      - name: "campaign_id"
        source: "id"
      - name: "account_id"
        source: "account_id"

  campaign_details:
    path: "/campaign/details"
    request_inputs:
      account_ids:
        value:
          type: SOURCE
          source_config:
            source: "accounts"
            field: "account_id"
        input_format: "single"
        request_format: "array"
        batch_size: 1
      campaign_ids:
        value:
          type: SOURCE
          source_config:
            source: "campaigns"
            field: "campaign_id"
            filter:
              field: "account_id"
              type: "column"
              operator: "eq"
              value_source:
                source: "accounts"
                field: "account_id"
              value_type: "parameter"
        input_format: "single"
        request_format: "array"
        batch_size: 50
```

Creates: accounts → campaigns → campaign_details (filtered by account).

### Iteration with multiple sources

```yaml
resources:
  trending_list:
    path: "/trending"
    request_inputs:
      country_code:
        value: ["US", "GB", "AU"]
        request_format: "string"
      date_range:
        value: ["7DAY", "30DAY"]
        request_format: "string"
    fields:
      - name: "hashtag_id"
        source: "id"

  detail:
    path: "/hashtag/detail"
    request_inputs:
      hashtag_id:
        value:
          type: SOURCE
          source_config:
            source: "trending_list"
            field: "hashtag_id"
        input_format: "single"
        request_format: "string"
        batch_size: 1
      country_code:
        value: ["US", "GB", "AU"]
        request_format: "string"
    transformations:
      - type: "add_column_from_request"
        name: "country_code"
        source: "country_code"
        location: "parameters"
        data_type: "string"
```

(3 countries × 2 date ranges) for trending_list; then each hashtag × 3 countries for detail.
