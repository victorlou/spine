# Database incremental extract (JDBC companion CDC)

For **PostgreSQL** and **HANA** resources that use `database_schema` / `database_table` (not `database_select_query`), optional **`incremental_extract`** bounds the **main** table read using a **companion** changelog/CDC table and a **watermark** column on that companion. Only **`kind: jdbc_companion_cdc`** is supported today.

This page focuses on **`correlation`** (how main rows match companion rows) and how runs behave. Field-level rules and Pydantic descriptions also live in **`src/config/incremental_extract.py`**.

## Prerequisites (short)

- **`loading`**: enabled, **`format`**: **`delta`** or **`iceberg`**, **`write_mode`**: **`append`** or **`merge`** (not **`overwrite`**).
- **`watermark.cursor.strategy`**: only **`destination_column`** is implemented; it reads **`MAX(reference_column)`** from the **written** table (Delta path or Iceberg catalog) to set the lower bound for the next companion watermark comparison.
- **`watermark.ordering`**: only **`lexical`** is implemented for JDBC SQL generation.
- Cannot combine with **`database_select_query`**. Optional **`database_where_predicate`** still applies on the main table (see [Configuration overview — Database resources](overview.md#database-resources-and-request-contexts)).

## Cold run vs warm run

| Phase | Extract | Meaning |
|--------|---------|--------|
| **Cold** (no prior bound, or empty `MAX`) | Full main table (respects **`database_where_predicate`** and **`table_read_options`**) | First load or empty destination. |
| **Warm** | `SELECT m.* FROM main m WHERE EXISTS (SELECT 1 FROM companion c WHERE <join> AND c.<watermark> > <bound>)` plus optional `AND (<database_where_predicate>)` on **`m`** | Only main rows tied to companion rows newer than the bound. |

The bound comes from **`watermark.cursor`** (tolerance / **`reference_format`** apply after the `MAX` read). **`reference_column`** must match a **`fields`** entry **`name`** when **`fields`** is configured.

### `table_read_options` on warm incremental runs

Optional **`use_on_incremental_warm`** (default **`false`**) lives on **`table_read_options`**. Unless set to **`true`**, **warm** JDBC reads (after a cursor bound exists) omit **`predicates`** and **range partitioning** so the extract uses one Spark JDBC partition; **`fetch_size`** is still applied. **Cold** runs (no bound yet) always keep predicates/range as configured. Set **`true`** only when you intentionally want parallel JDBC on small warm batches.

## `incremental_extract.correlation`

Correlation answers: *which companion rows “explain” a main row for the EXISTS filter?* You can rely on **automatic key inference**, set **explicit equi-join columns**, or supply a **custom join predicate**. **`join_columns`** and **`join_predicate`** are **mutually exclusive**.

### Option A — Omit both `join_columns` and `join_predicate` (default)

Spine issues lightweight JDBC probes (`WHERE 1=0`) on the main and companion tables and builds join keys as:

- column names that appear on **both** tables, **and**
- not equal to **`watermark.column`**, **and**
- not listed in **`companion_metadata_columns`** (unless that name is the watermark column).

If this intersection is empty, **extract fails** (including a **cold** full load) with an error asking you to set **`join_columns`** explicitly—Spine still probes schemas up front for incremental resources. Use **`companion_metadata_columns`** when the companion has extra technical columns (SAP-style **`DATAPAKID`**, **`RECORD`**, **`REQTSN`**, etc.) that should **not** be treated as natural join keys even if the names exist on both sides.

**`fields` validation:** If you list **`companion_metadata_columns`**, you must not map a **`fields`** entry with **`source`** equal to one of those names **unless** it is the watermark column (the watermark is allowed where the model permits it).

### Option B — `join_columns` (explicit natural keys)

```yaml
correlation:
  join_columns: ["order_id", "line_no"]
```

Spine generates:

`m."order_id" = c."order_id" AND m."line_no" = c."line_no"`

inside the **`EXISTS`** subquery, plus **`AND c.<watermark> > <bound>`**.

When **`fields`** is configured, every name in **`join_columns`** must appear as some field’s **`source`** (so the extract contract includes the join keys). If **`fields`** is omitted, that check is skipped.

### Option C — `join_predicate` (custom SQL)

```yaml
correlation:
  join_predicate: "m.\"doc_id\" = c.\"doc_id\" AND m.\"item\" = c.\"item\""
```

- Use aliases **`m`** (main table) and **`c`** (companion table). Quote identifiers however your database requires (HANA/Postgres often need doubled quotes for mixed-case or special names).
- The predicate is inserted into:  
  `EXISTS (SELECT 1 FROM companion c WHERE (<join_predicate>) AND c.<watermark> > <bound>)`
- Do **not** set **`join_columns`** at the same time.
- Schema probes for join-key inference are **skipped**; you do not need a non-empty column intersection between the two tables for configuration to be usable.
- With **`join_predicate`**, **`companion_metadata_columns`** still affects **validation** vs **`fields`** as in Option A.

## `companion` and `watermark` (reminder)

```yaml
incremental_extract:
  kind: jdbc_companion_cdc
  companion:
    schema: optional   # defaults to resource database_schema
    table: "YOUR_CDC_TABLE"
  watermark:
    column: REQTSN     # column on the **companion** compared to the bound
    ordering: lexical  # only lexical implemented
    cursor:
      strategy: destination_column
      reference_column: YOUR_WRITTEN_COLUMN_NAME  # MAX from written table; must match fields[].name if fields set
      reference_format: none          # or yyyymmdd if using calendar tolerance
      tolerance_calendar_days: 0      # >0 only with reference_format: yyyymmdd
  correlation:
    # Pick one style: default inference, join_columns, or join_predicate
    companion_metadata_columns: [DATAPAKID, RECORD]  # optional; see Option A
```

## Example (pattern only)

Shape matches a main billing table with a parallel CDC table and REQTSN watermark; adjust identifiers for your catalog. See a concrete HANA resource under **`config/sources/sap_bw_prod.yml`** (`vbrp`) for a real **`companion_metadata_columns`** + default inference setup.

```yaml
resources:
  main_with_cdc:
    enabled: false
    method: GET
    database_schema: MYSCHEMA
    database_table: MAIN_FACT
    database_where_predicate: "m.status = 'ACTIVE'"   # optional; use m. prefix
    fields:
      - { name: id, source: id }
      - { name: ts, source: ts }   # reference_column "ts" must be a field name here
    incremental_extract:
      kind: jdbc_companion_cdc
      companion:
        table: MAIN_CDC
      watermark:
        column: cdc_seq
        ordering: lexical
        cursor:
          strategy: destination_column
          reference_column: ts
          reference_format: none
          tolerance_calendar_days: 0
      correlation:
        join_columns: ["id"]        # or omit and use inference / or use join_predicate instead
    loading:
      format: delta
      write_mode: merge
      merge_keys: ["id"]
```

## See also

- [Configuration overview — Database resources](overview.md#database-resources-and-request-contexts) — request contexts, **`database_where_predicate`**, **`database_select_query`**.
- [`AGENTS.md`](https://github.com/victorlou/spine/blob/main/AGENTS.md) — contributor map for handler and JDBC SQL helpers.
