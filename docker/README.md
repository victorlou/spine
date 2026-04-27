# Docker Configuration

Docker-related configuration for Spine. The container includes Python 3.12, Java 21, and Redis.

## Prerequisites

- Docker and Docker Compose
- **`.env` at the repository root** (copy from `.env.example`) — keeps secrets out of `src/` and matches how the app loads dotenv files inside the container (`/.env`)
- Pipeline config under `config/` (`defaults.yml` and `sources/`; copy from `config/defaults.example.yml` and `config/examples/`)

## Local Development

### Option 1: Docker Compose (Recommended)

1. **Ensure `.env` exists** at the repo root (`cp .env.example .env`) and configure it.

2. **Ensure pipeline YAML exists** (not committed in the public layout):
   ```bash
   cp config/defaults.example.yml config/defaults.yml
   cp config/examples/jsonplaceholder.yml config/sources/jsonplaceholder.yml
   ```

3. **Build and run**
   ```bash
   docker-compose up --build
   ```

Docker Compose builds the image, mounts `.env` at `/.env`, mounts `config/`, and runs the pipeline.

### Option 2: Docker CLI

For more control (for example passing CLI args):

**Windows note:** run these commands in **PowerShell**. If you use Git Bash (MSYS), prefix with `MSYS_NO_PATHCONV=1 MSYS2_ARG_CONV_EXCL='*'` to avoid path conversion issues with mounts and `--entrypoint`.

1. **Build**
   ```bash
   docker build \
     --platform linux/amd64 \
     -t spine \
     -f docker/Dockerfile .
   ```

2. **Run** (mount repo-root `.env` so `load_pipeline_dotenv()` can read `/.env`; mount `config/` the same way as Compose)
   ```bash
   docker run --rm \
    -v "$(pwd)/.env:/.env:ro" \
    -v "$(pwd)/config:/config:ro" \
     spine
   ```

   **Alternative:** omit the `.env` mount and pass variables in with `docker run --env-file .env` (or `-e` / `--env`). Injected variables are not overwritten when the app loads `.env` files.

### Passing CLI Arguments

`docker-compose up` runs the default pipeline. For `--show-plan`, `--validate-only`, `--select`, etc., use `docker run` and append the args:

```bash
docker run --rm \
  -v "$(pwd)/.env:/.env:ro" \
  -v "$(pwd)/config:/config:ro" \
  spine --show-plan

docker run --rm \
  -v "$(pwd)/.env:/.env:ro" \
  -v "$(pwd)/config:/config:ro" \
  spine --validate-only

docker run --rm \
  -v "$(pwd)/.env:/.env:ro" \
  -v "$(pwd)/config:/config:ro" \
  spine --select jsonplaceholder --limit 5
```

## CI-built images

On pushes to **`main`** or **`v*` version tags**, GitHub Actions publishes a multi-arch image (manifest list for `linux/amd64` and `linux/arm64`) to `ghcr.io` (see [docs/deployment.md](../docs/deployment.md)). Pull with your organization or user name and the repository name in lowercase, for example `docker pull ghcr.io/victorlou/spine:latest` or `docker pull ghcr.io/victorlou/spine:v1.0.0`.

Verify published manifest platforms with:

```bash
docker buildx imagetools inspect ghcr.io/victorlou/spine:latest
```

Runtime configuration in production should come from your orchestrator (secrets, task env), not from baking `.env` into the image.

## External Operator Repo (published image)

If you keep pipeline config in a separate operator repository, you can run the published image directly without cloning Spine source.

### Recommended path mapping

Spine resolves config from `/config` by default (`CONFIG_PATH=.`). So the simplest mapping is:

```bash
docker run --rm \
  -v "$(pwd)/config:/config:ro" \
  --env-file .env \
  ghcr.io/victorlou/spine:vX.Y.Z \
  --select your_source
```


### Spark runtime and object-store connectors

Spine builds Spark Hadoop filesystem and Ivy package lists from your **selected pipeline config** (effective loading destinations) and from **`defaults.spark_runtime`** in `defaults.yml`: `profile` (`auto` / `local_dev` / `cluster_managed`) plus **`s3_connector_mode`**, **`gcs_connector_mode`**, and **`azure_connector_mode`** (`auto` / `packages` / `external`). Prefer editing YAML there; use environment variables only when the image or CI cannot carry config changes.

| Concern | Primary (config) | Optional env override |
|--------|-------------------|------------------------|
| S3 / S3A (`hadoop-aws`, Ivy) | `defaults.spark_runtime.s3_connector_mode` | `SPARK_S3_CONNECTOR_MODE` |
| GCS | `defaults.spark_runtime.gcs_connector_mode` | `SPARK_GCS_CONNECTOR_MODE`, `SPARK_GCS_CONNECTOR_JAR_URL` (when mode is `packages`; defaults to shaded connector JAR on Maven Central) |
| Azure Blob / ABFS | `defaults.spark_runtime.azure_connector_mode` | `SPARK_AZURE_CONNECTOR_MODE` |

With `auto`, Databricks and EMR default to **`external`** for all three (cluster-provided jars); other environments default to **`packages`**. Use **`external`** when your Spark image or platform already ships connectors and `fs.*` auth wiring.

### AWS credentials options (only needed for S3 destinations)

Profile-based (`AWS_PROFILE` in `.env`):

```bash
docker run --rm \
  -v "$(pwd)/config:/config:ro" \
  -v "$HOME/.aws:/root/.aws:ro" \
  --env-file .env \
  ghcr.io/victorlou/spine:vX.Y.Z \
  --select your_source
```

Use `:ro` for static key-based profiles. For SSO profiles, use a writable mount so token cache can refresh:

```bash
-v "$HOME/.aws:/root/.aws"
```

Before running with SSO profiles, authenticate on the host:

```bash
aws sso login --profile your_profile
```

**Windows + Git Bash:** use:

```bash
MSYS_NO_PATHCONV=1 MSYS2_ARG_CONV_EXCL='*' docker run ...
```

Without this, Git Bash may rewrite `/root/.aws` and break profile detection in the container.

Environment-key-based (no shared profile mount):

```bash
docker run --rm \
  -v "$(pwd)/config:/config:ro" \
  -e AWS_ACCESS_KEY_ID=... \
  -e AWS_SECRET_ACCESS_KEY=... \
  -e AWS_SESSION_TOKEN=... \
  -e AWS_REGION=us-east-1 \
  ghcr.io/victorlou/spine:vX.Y.Z \
  --select your_source
```

### Troubleshooting profile errors

If you see `The config profile (...) could not be found`:

1. Confirm the profile exists on host: `aws configure list-profiles`.
2. Confirm host auth works: `aws sts get-caller-identity --profile your_profile`.
3. Ensure `.aws` is mounted at `/root/.aws` when using `AWS_PROFILE`.
4. If using SSO profile, mount `.aws` writable (`-v "$HOME/.aws:/root/.aws"`), not `:ro`.
5. Re-run `aws sso login --profile your_profile` if using SSO.
6. Confirm region is set (`AWS_REGION` or profile region).

### GCP credentials options (only needed for GCS destinations)

Spark's GCS connector reads Google Application Default Credentials (ADC) at the JVM level — `spark.hadoop.fs.gs.auth.type` defaults to `APPLICATION_DEFAULT` and the account from `gcloud auth login` is **not** used.

Service-account JSON via `GOOGLE_APPLICATION_CREDENTIALS`:

```bash
docker run --rm \
  -v "$(pwd)/config:/config:ro" \
  -v "$(pwd)/secrets/gcp-sa.json:/secrets/gcp-sa.json:ro" \
  -e GOOGLE_APPLICATION_CREDENTIALS=/secrets/gcp-sa.json \
  --env-file .env \
  ghcr.io/victorlou/spine:vX.Y.Z \
  --select your_source
```

ADC mount for local development (uses `gcloud auth application-default login` cache):

```bash
gcloud auth application-default login

docker run --rm \
  -v "$(pwd)/config:/config:ro" \
  -v "$HOME/.config/gcloud:/root/.config/gcloud:ro" \
  --env-file .env \
  ghcr.io/victorlou/spine:vX.Y.Z \
  --select your_source
```

On Dataproc / GKE Workload Identity / GCE, omit both — the connector picks up the platform identity automatically. The destination preflight will surface a clear `HandlerError(operation="destination_preflight")` if the JVM cannot authenticate against your `gcs_bucket`.

### Azure credentials options (only needed for Azure Blob / ABFS destinations)

Spark's ABFS connector reads Azure auth from Hadoop config / environment variables. Spine does not run an Azure preflight at settings import time; the unified destination preflight probes the configured container at handler startup.

Storage account key (development convenience):

```bash
docker run --rm \
  -v "$(pwd)/config:/config:ro" \
  -e AZURE_STORAGE_ACCOUNT=mystorageaccount \
  -e AZURE_STORAGE_KEY=... \
  --env-file .env \
  ghcr.io/victorlou/spine:vX.Y.Z \
  --select your_source
```

Connection string:

```bash
docker run --rm \
  -v "$(pwd)/config:/config:ro" \
  -e AZURE_STORAGE_CONNECTION_STRING="DefaultEndpointsProtocol=https;AccountName=...;AccountKey=...;EndpointSuffix=core.windows.net" \
  --env-file .env \
  ghcr.io/victorlou/spine:vX.Y.Z \
  --select your_source
```

Service principal / managed identity / OAuth: configure `fs.azure.account.auth.type.<account>.dfs.core.windows.net` and the `fs.azure.account.oauth2.*` keys in your Spark runtime (Hadoop config) — typically through your platform image (Databricks instance profile, AKS workload identity, etc.). On managed Spark platforms, omit AZURE_* env vars and rely on the platform identity.

For local development against a real account, mount `~/.azure` (CLI cache) writable so token refresh works:

```bash
az login

docker run --rm \
  -v "$(pwd)/config:/config:ro" \
  -v "$HOME/.azure:/root/.azure" \
  --env-file .env \
  ghcr.io/victorlou/spine:vX.Y.Z \
  --select your_source
```

## Private package index (optional)

Adding private wheels or an internal index later require the use of a BuildKit secret and configuring uv (extra indexes or credentials via environment variables or `pyproject.toml`; see [uv’s authentication docs](https://docs.astral.sh/uv/configuration/authentication/)) during `docker build`. The default `Dockerfile` runs **`uv sync --frozen`** against the committed [`uv.lock`](../uv.lock) and public PyPI only.

## Configuration Files

- `Dockerfile` — Python 3.12, Java 21, Redis, application
- `docker-compose.yml` — Local development with volume mounts
- `startup.sh` — Starts Redis, optional S3 config pull, then runs the pipeline

## Apple Silicon (M1/M2)

Published GHCR images are multi-arch, so Apple Silicon hosts can pull `ghcr.io/victorlou/spine:*` directly without setting `--platform`.

Use `--platform linux/amd64` only when you explicitly need to emulate x86_64 (for compatibility testing against amd64-only environments):

```bash
docker build --platform linux/amd64 ...
```

## Notes

- The container uses Python 3.12 and OpenJDK 21 for Spark
- Redis runs in-memory inside the container
- Mount `.env` as read-only (`:ro`) for security
- Spark startup resolves several JARs at launch (Delta, Iceberg, ngdbc, and destination-specific connectors); first-run dependency resolution adds time
