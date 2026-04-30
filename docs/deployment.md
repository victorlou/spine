# Deployment

## Table of Contents

- [Docker Local Development](#docker-local-development)
- [CI and container images (GitHub Actions)](#ci-and-container-images-github-actions)
- [Runtime configuration](#runtime-configuration)
- [ECS Fargate and external config (reference)](deployment/ecs-s3-reference.md)
- [Error Handling and Monitoring](#error-handling-and-monitoring)

## Docker Local Development

Copy pipeline config from the templates under `config/`, set up a repo-root `.env`, then run:

```bash
docker-compose up --build
```

For full details (CLI args, Apple Silicon), see [docker/README.md](../docker/README.md).

## CI and container images (GitHub Actions)

Linting and tests run on pushes and pull requests to **`dev`** and **`main`**, and on **version tag** pushes (`v*`), via [.github/workflows/ci.yml](../.github/workflows/ci.yml).

The workflow **builds and pushes** a multi-arch container image (manifest list for `linux/amd64` and `linux/arm64`) to the [GitHub Container Registry](https://docs.github.com/en/packages/working-with-a-github-packages-registry/working-with-the-container-registry) (`ghcr.io`) on pushes to **`main`** and on **`v*` tags** (for example `ghcr.io/victorlou/spine:latest` plus a SHA tag on `main`, and `ghcr.io/victorlou/spine:v1.2.3` when you push tag `v1.2.3`). Pushes to `dev` only run lint and tests.

The package may show multiple digests for a single publish. That is expected for multi-arch images: one top-level manifest list plus child manifests for each architecture.

Requirements for the image job:

- **Packages** permission for `GITHUB_TOKEN` (the workflow grants `packages: write` on the publish job only).

Images are public or private according to your GitHub package visibility settings for the container package.

After publish, you can verify manifest platforms with:

```bash
docker buildx imagetools inspect ghcr.io/victorlou/spine:latest
```

A separate weekly cleanup workflow keeps the registry tidy while preserving multi-arch integrity:

- keep `latest`
- keep all `v*` tags
- keep the 10 most recent SHA tags
- delete untagged versions

## Runtime configuration

- Set environment variables via your orchestrator (for example Kubernetes secrets, AWS Secrets Manager/SSM, or plain `.env` in development).
- If you run without the default repo layout (for example a minimal image without operator config baked in), set `CONFIG_PATH` to an **absolute** path to a directory that contains `defaults.yml`, `sources/`, and optionally `queries/` (see [Configuration overview](configuration/overview.md)).
- Configure loading destinations and credentials in pipeline YAML (`defaults.loading` and per-resource overrides); see [Loading configuration](configuration/loading.md). Cloud auth follows your platform (IAM, workload identity, Azure managed identity, and so on).
- **Pipeline aborts before ingestion** when the configured destination (`s3`, `gcs`, `azure_blob`, or `local`) cannot be reached. Spine probes each destination through Spark's Hadoop `FileSystem` after session init and stops with `HandlerError(operation="destination_preflight")` if credentials or permissions are missing. Configure cloud auth for your platform (IAM role / profile, ADC, managed identity) before deploying. See [Destination preflight](configuration/loading.md#destination-preflight).
- Prefer **`defaults.spark_runtime`** in `defaults.yml` for Spark host profile and symmetric S3/GCS/Azure connector provisioning (`packages` vs `external`). Spine detects common managed environments (Databricks, EMR, ECS, Kubernetes) when `profile` is `auto`.
- Optional environment overrides (for CI or images that cannot edit YAML): `SPARK_S3_CONNECTOR_MODE`, `SPARK_GCS_CONNECTOR_MODE`, `SPARK_GCS_CONNECTOR_JAR_URL` (shaded GCS connector URL for `spark.jars` when mode is `packages`), `SPARK_AZURE_CONNECTOR_MODE`, `SPINE_GCS_AUTH_TYPE` (defaults to `APPLICATION_DEFAULT`; set `COMPUTE_ENGINE` only when intentionally relying on metadata auth). When unset, YAML and auto-detection drive behavior.
- Destination preflight filesystem timeout can be tuned with `SPINE_DESTINATION_PREFLIGHT_FILESYSTEM_TIMEOUT_SECONDS` (default `45`). This applies to Hadoop `FileSystem.get(...)` and root listing probes across S3/GCS/Azure so hangs fail fast with actionable logs.
- For **ECS Fargate**, promoting config to **S3**, optional **`SPINE_CONFIG_S3_URI`** pull at container start (boto3), GHCR image pinning, and task-definition patterns (all placeholders), see the [ECS + S3 reference](deployment/ecs-s3-reference.md), including `python -m scripts.s3_config_push` for one-off or CI uploads.

## Error Handling and Monitoring

- Structured logging (TRACE level for debugging)
- Automatic retries with exponential backoff
- Error isolation per source/resource
- Detailed execution results with status and errors
- Configuration validation before execution
