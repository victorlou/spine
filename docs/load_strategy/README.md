# Load Strategy Architecture

Spine uses load strategies to isolate table-format behavior from destination loading. `ObjectStoreLoader` owns the object-store loading flow, while `src/load_strategy/` owns Delta and Iceberg table semantics such as table existence checks, write-mode routing, catalog/path resolution, and merge execution.

This keeps the loader focused on preparing data and choosing the right strategy instead of carrying format-specific branches for every table format.

## Main components

### `ObjectStoreLoader`

Location: `src/loader/object_store_loader.py`

`ObjectStoreLoader` is the entry point for object-store-backed destinations such as S3, local storage, GCS, and Azure Blob.

It owns:

- validating that a Spark session is available
- resolving the destination base URI through `loading_base_uri(config)`
- converting input records into a Spark DataFrame when needed
- normalizing DataFrame columns before writes
- optionally deduplicating merge input when configured
- choosing between file-based loading and table-format loading
- delegating table formats to `LoadStrategyFactory`

It should not own Delta- or Iceberg-specific table behavior. Table-format-specific logic belongs in concrete load strategies.

### `LoadStrategyFactory`

Location: `src/load_strategy/load_strategy_factory.py`

`LoadStrategyFactory` maps `LoadingConfig.format` to a concrete strategy class.

Current mappings:

- `LoadingFormat.DELTA` → `DeltaStrategy`
- `LoadingFormat.ICEBERG` → `IcebergStrategy`

The factory receives:

- Spark session
- object store helper
- resolved base URI
- loading config
- optional source type

It returns a `BaseLoadStrategy` instance that can answer table existence checks and execute writes.

### `BaseLoadStrategy`

Location: `src/load_strategy/base_load_strategy.py`

`BaseLoadStrategy` owns the shared table-format workflow.

It owns:

- generating the object-store table location
- resolving the table storage location through `resolve_table_location()`
- validating supported write modes
- validating merge keys before merge writes
- routing write modes:
  - `append`
  - `overwrite`
  - `merge`
- bootstrapping a missing merge target with an append-style write
- calling strategy-specific hooks for table existence, simple writes, and merge execution

It does not own:

- catalog identifier derivation
- Delta-specific path writes
- Iceberg-specific catalog writes
- table-format-specific merge SQL/API calls
- metadata existence checks beyond calling `table_exists()`

Concrete strategies must implement:

- `table_exists() -> bool`
- `write_simple(df, table_location, *, mode, **kwargs) -> None`
- `perform_merge(df, table_location, merge_keys) -> None`

### `DeltaStrategy`

Location: `src/load_strategy/delta_strategy.py`

`DeltaStrategy` is path-backed. For Delta, the table location is the operative write and merge target.

It owns:

- checking table existence by looking for the `_delta_log` directory
- writing append/overwrite data with Spark path writes using `writer.save(table_location)`
- executing Delta merge through `DeltaTable.forPath(...)`
- handling Delta writer options such as `mergeSchema`

Delta does not need a catalog identifier for the current implementation. The table location is the canonical destination.

### `IcebergStrategy`

Location: `src/load_strategy/iceberg_strategy.py`

`IcebergStrategy` is catalog-backed. It distinguishes between:

- table location: the object-store location where data/metadata is stored
- table identifier: the Spark Iceberg catalog identifier used by Spark SQL and `saveAsTable`

It owns:

- resolving the object-store table location through `resolve_table_location()`
- deriving the Iceberg catalog identifier through `resolve_identifier()`
- configuring the Spark Iceberg warehouse setting for operations
- checking table existence through the Spark catalog
- writing append/overwrite data through catalog-aware `saveAsTable(...)`
- executing Iceberg merge through SQL `MERGE INTO`
- cleaning up temporary Spark catalog configuration after operations

For Iceberg, `resolve_identifier()` intentionally means catalog identifier.

## End-to-end load flow

### 1. Handler calls the loader

The pipeline handler resolves loading config and calls the configured loader. For object-store destinations, that loader is `ObjectStoreLoader`.

### 2. `ObjectStoreLoader.load(...)` prepares the write

`ObjectStoreLoader.load(...)` performs common work before format-specific behavior starts:

1. Requires a Spark session.
2. Resolves the base URI using validated loading config.
3. Converts input records to a DataFrame when needed.
4. Renames duplicate columns.
5. Sanitizes illegal column characters.
6. Applies optional merge-key deduplication when configured.

### 3. Loader chooses file-based vs table-format path

If `config.format` is not a table format, the loader uses the file-based path.

File-based writes remain in `ObjectStoreLoader` because they use temporary directories, part-file discovery, file movement, and cleanup.

If `config.format` is Delta or Iceberg, the loader delegates to `LoadStrategyFactory`.

### 4. Factory creates the strategy

`LoadStrategyFactory.create_load_strategy(...)` looks at `config.format` and returns the matching strategy.

The loader then calls:

- `load_strategy.write(df, **kwargs)` during normal loads
- `load_strategy.table_exists()` during destination existence checks

### 5. `BaseLoadStrategy.write(...)` routes the write mode

The base strategy resolves the table location and routes based on `config.write_mode`.

For `append` and `overwrite`:

1. Validate the write mode is supported.
2. Call `write_simple(df, table_location, mode=...)` on the concrete strategy.

For `merge`:

1. Validate merge keys exist in config.
2. Call `table_exists()` on the concrete strategy.
3. If the table does not exist, call `write_simple(..., mode="append")` to create it first.
4. If the table exists, call `perform_merge(...)` on the concrete strategy.

This keeps merge bootstrap behavior consistent between Delta and Iceberg.

## Destination existence flow

`ObjectStoreLoader.destination_exists(...)` is used for decisions such as auto-backfill detection.

The flow is:

1. Return `False` for non-object-store destinations or non-table formats.
2. Return `False` if required destination fields are missing.
3. Resolve the base URI.
4. Create the format strategy using `LoadStrategyFactory`.
5. Return `load_strategy.table_exists()`.

The concrete strategy decides how existence is checked:

- Delta checks `_delta_log` under the table location.
- Iceberg checks the catalog table identifier through Spark catalog APIs.

## Naming conventions

Use precise names for storage locations and catalog identifiers.

### `table_location`

Use `table_location` for object-store paths/URIs where table data and metadata live.

Examples:

- `s3a://bucket/rest_api/source/resource/`
- `file:///tmp/spine/source/resource/`

Shared strategy orchestration should pass `table_location` into concrete hooks.

### `resolve_table_location()`

Use `resolve_table_location()` when resolving the object-store destination location.

This method lives on `BaseLoadStrategy` and is safe for both path-backed and catalog-backed table formats.

### `resolve_identifier()`

Reserve `resolve_identifier()` for catalog identifiers.

Currently, `IcebergStrategy.resolve_identifier()` returns the Spark Iceberg catalog identifier derived from the table location.

Delta should not use `resolve_identifier()` for path-backed operations. Delta should use `resolve_table_location()`.

### `table_identifier`

Use `table_identifier` only for catalog names used by Spark SQL/catalog APIs.

Example:

- `iceberg.<namespace>.<table>`

Do not use `identifier` as a generic synonym for path or destination. It becomes ambiguous between Delta path-backed writes and Iceberg catalog-backed writes.

## Adding a new table format

To add another table format:

1. Create a new strategy under `src/load_strategy/`.
2. Subclass `BaseLoadStrategy`.
3. Implement:
   - `table_exists()`
   - `write_simple(...)`
   - `perform_merge(...)`
4. If the format is catalog-backed, implement a format-specific `resolve_identifier()` that returns the catalog identifier.
5. Register the format in `LoadStrategyFactory._load_strategies`.
6. Keep write-mode routing in `BaseLoadStrategy` unless the common contract itself changes.
7. Keep `ObjectStoreLoader` free from table-format-specific branches.

If the new format requires Spark runtime or connector configuration, update the Spark configuration models and defaults in the same change set so behavior remains configuration-first.

## Design rules

- Keep config shape stable unless the issue explicitly changes the external contract.
- Keep `ObjectStoreLoader` focused on loader concerns and DataFrame preparation.
- Keep table-format semantics inside concrete strategies.
- Prefer `table_location` for storage paths and `table_identifier` for catalog identifiers.
- Do not duplicate write-mode routing in child strategies.
- Do not add Delta/Iceberg branches to `ObjectStoreLoader` for table writes.
- Make existence checks fail closed where reasonable: a strategy should return `False` when it cannot prove the table exists.
