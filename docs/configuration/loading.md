# Loading Configuration

## Table of Contents

- [Delta Save Modes](#delta-save-modes)
  - [Overwrite](#overwrite)
  - [Append](#append)
  - [Merge (Upsert)](#merge-upsert)

## Delta Save Modes

When using Delta format, control how data is written to S3 with the `write_mode` option. Schema evolution is automatically enabled for all modes.

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
