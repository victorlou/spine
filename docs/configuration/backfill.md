# Backfill Configuration

Date-range backfill splits historical data fetching into multiple API requests with fixed-size date windows. Configure backfill under **request_inputs** on **path**, **query**, or **body** inputs (same rules as normal inputs: GET defaults to query, POST defaults to body unless you set `location`). Use a `value` that is a dict containing a **backfill** key (e.g. `request_inputs.startDate.value.backfill`).

## Table of Contents

- [When Backfill Runs](#when-backfill-runs)
- [Configuration Shape](#configuration-shape)
- [Date Pair Generation](#date-pair-generation)
- [The `inclusive` Flag](#the-inclusive-flag)
- [API Data Lag](#api-data-lag)
- [Example](#example)

## When Backfill Runs

Backfill can run in two ways:

1. **Auto-backfill**: When a dependent resource's object-store destination is empty, the pipeline automatically uses backfill date ranges instead of the default date range for the upstream snapshot resource.

2. **Manual backfill**: Run with `--backfill` or `-b` to force backfill date ranges regardless of destination state.

## Configuration Shape

Backfill requires exactly two request inputs (in `request_inputs`, any of path / query / body) whose `value` contains a **backfill** key:

- **Driver (STATIC_DATE)**: Defines the sequence of window starts. Requires `start`, `end`, and `increment`. Supports static YYYY-MM-DD strings or dynamic values (e.g. `type: DATE`, `operation: TODAY`).

- **Reference (REFERENCE)**: Defines the window end for each start. References the driver field, adds `increment`, and caps at `limit`. Requires `field`, `increment`, and `limit`.

Only **one** such pair is allowed per resource. You may place the driver on one location (e.g. query) and the reference on another (e.g. body); that still drives **one** sequence of windows per request batch, not two independent backfills or a Cartesian product. If the merged `request_inputs` contain more than one valid `STATIC_DATE` driver or more than one valid `REFERENCE`, plan build fails with a clear error.

## Date Pair Generation

Each window spans at most `ref_increment` days. `endDate` is always the **last day included** in the window (inclusive semantics). The last window may be shorter when capped at `limit`.

- **For each window**: `endDate = min(window_start + (ref_increment - 1), limit)`.
- **Next window start** depends on the `inclusive` flag (see below).

## The `inclusive` Flag

The driver backfill config supports an `inclusive` flag that controls how windows abut:

- **`inclusive: false`** (default): Windows are contiguous with no overlap. The next window starts the day after `endDate`.  
  Example with `limit=2026-02-20`: `(2026-01-01, 2026-01-15)`, `(2026-01-16, 2026-01-30)`, `(2026-01-31, 2026-02-14)`, `(2026-02-15, 2026-02-20)`.

- **`inclusive: true`**: The boundary day is included in both windows (1-day overlap). The next window starts on `endDate`.  
  Example with `limit=2026-02-20`: `(2026-01-01, 2026-01-15)`, `(2026-01-15, 2026-01-29)`, `(2026-01-29, 2026-02-13)`, `(2026-02-13, 2026-02-20)`.

In both cases, each window contains at most `ref_increment` days (e.g. 15 days for `15 DAY`).

## API Data Lag

Many reporting APIs expose data only up to a few days in the past (e.g. "reports available through 2 days ago"). If you use `limit: { type: DATE, operation: TODAY }`, the last backfill window may use an `endDate` that the API rejects.

**Fix**: Set the reference `limit` to match the API's data availability lag. For example, for an API that lags by 2 days:

```yaml
limit:
  type: DATE
  operation: DAYS_AGO
  days: 2
```

This caps the last window's `endDate` at the latest date the API supports.

## Example

Minimal backfill config (POST body example; `value` contains `backfill`):

```yaml
request_inputs:
  startDate:
    value:
      value: "{{ date('DAYS_AGO', days=9) }}"
      backfill:
        type: STATIC_DATE
        start: '2026-01-01'
        end: "{{ date('TODAY') }}"
        inclusive: false
        increment: '15 DAY'
  endDate:
    value:
      value: "{{ date('DAYS_AGO', days=2) }}"
      backfill:
        type: REFERENCE
        field: startDate
        increment: '15 DAY'
        limit: "{{ date('DAYS_AGO', days=2) }}"
exclude_from_request_body:
  - startDate
  - endDate
```

This produces 15-day windows from 2026-01-01 up to 2 days ago. For **body** inputs, list backfill driver/reference keys in **exclude_from_request_body** when those keys must not appear in the JSON body (they still drive date windows and request context). Query and path parameters are sent as usual for each backfill window.
