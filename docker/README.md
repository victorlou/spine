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

On pushes to **`main`** or **`v*` version tags**, GitHub Actions publishes an image to `ghcr.io` (see [docs/deployment.md](../docs/deployment.md)). Pull with your organization or user name and the repository name in lowercase, for example `docker pull ghcr.io/victorlou/spine:latest` or `docker pull ghcr.io/victorlou/spine:v1.0.0`.

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


### AWS credentials options

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
  -e AWS_REGION=ap-southeast-2 \
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

## Private PyPI (optional)

If you add private wheels later, you can use a BuildKit secret and `pip.conf` during `docker build`; the default `Dockerfile` installs only from public `requirements.txt`.

## Configuration Files

- `Dockerfile` — Python 3.12, Java 21, Redis, application
- `docker-compose.yml` — Local development with volume mounts
- `startup.sh` — Starts Redis, optional S3 config pull, then runs the pipeline

## Apple Silicon (M1/M2)

Use `--platform linux/amd64` when building to match common Linux/x86 deploy targets and avoid Java/Spark issues:

```bash
docker build --platform linux/amd64 ...
```

## Notes

- The container uses Python 3.12 and OpenJDK 21 for Spark
- Redis runs in-memory inside the container
- Mount `.env` as read-only (`:ro`) for security
