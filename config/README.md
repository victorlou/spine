# Pipeline configuration

This directory holds **operator-local** YAML and SQL: `defaults.yml`, `sources/*.yml`, and `queries/*.sql`. Those paths are ignored by git so credentials and internal definitions stay out of the public repository.

## First-time setup

1. Copy the template defaults file:

   ```bash
   cp config/defaults.example.yml config/defaults.yml
   ```

2. Add at least one source under `config/sources/` (copy from [`examples/`](examples/)):

   ```bash
   cp config/examples/jsonplaceholder.yml config/sources/jsonplaceholder.yml
   ```

3. Edit `config/defaults.yml` and the repo-root `.env` for your environment. See [Getting started](../docs/getting-started.md) and [Configuration overview](../docs/configuration/overview.md).

## `CONFIG_PATH`

The environment variable `CONFIG_PATH` selects a subdirectory under `config/` (or an absolute path). The default `.` means this folder. See [`src/config/settings.py`](../src/config/settings.py) for resolution rules.

## Default loading

If `defaults.yml` has no `defaults.loading` section, Spine uses a built-in default: **`destination: local`** with **`storage_root: ".spine/local-output"`** (resolved relative to the **repository root**: the directory that contains `src/`, so `.spine/` sits next to `config/` in a normal checkout) and a placeholder **`prefix`**. [`defaults.example.yml`](defaults.example.yml) shows the same layout explicitly; switch that block to S3/GCS/Blob as needed (for example `s3_bucket`, `gcs_bucket`, or `azure_container` + `azure_account`, with optional `bucket` alias support) for cloud object storage.
