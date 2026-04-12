# Transformations

## Table of Contents

- [Add Column](#add-column)
- [Add Column from Request](#add-column-from-request)
- [Ensure Param Values in Output](#ensure-param-values-in-output)

## Add Column

Add a static or dynamic column to each record.

```yaml
transformations:
  - type: "add_column"
    name: "_ingested_at"
    value: "{{ now_iso() }}"  # or static value
```

## Add Column from Request

Add a value from the resolved request to each record. **location** is the key in the request context to read from: `"parameters"` (full resolved payload: path + query + body) or `"request_body"` (body-only).

```yaml
transformations:
  - type: "add_column_from_request"
    name: "_request_date"
    source: "start_date"
    location: "parameters"  # or "request_body"
    data_type: "string"
```

## Ensure Param Values in Output

Ensure all values for a given request input appear in the output, even when the API returns no data for some.

```yaml
ensure_param_values_in_output:
  enabled: true
  param_name: "barcode"   # Request input name (from request_inputs)
  output_field: "barcode_number"
```

**How it works**
1. Extracts all values from the specified request input
2. LEFT JOINs those values with response data
3. Every requested value appears in output; missing response fields are NULL

**Use cases**
- Barcode/UPC lookups where some codes may not exist
- Product ID queries where some products may be unavailable
- Complete request-to-response mapping
