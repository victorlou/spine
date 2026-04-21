# Spine

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

**Spine is a configuration-first ingestion framework for teams that are tired of rebuilding the same ingestion logic for every new source, table, and destination.**

If your team keeps rewriting the same auth, retry, pagination, backfill, and loading logic for every new API or table, Spine is the part where that repetition stops getting a free pass.

It is not a no-code product, a connector marketplace, or a black box. It is a modular runtime with one execution model, one config shape, and clear extension points for services, parsers, collectors, and loaders.

---

## Why Spine?

The hard part is usually not getting data out of one API once. The hard part is doing it the same way across many sources, keeping it reliable in production, and not creating a new snowflake every time somebody asks for one more table.

Ingestion usually drifts into:
- One-off scripts that only one person trusts
- Different auth, retry, and pagination patterns per source
- Config scattered across code, notebooks, and environment-specific glue
- Pipelines that work until backfills, partial failures, or new destinations show up

Spine gives you a shared modular shape for that work:

> Define pipelines in YAML. Validate them up front. Build an execution plan. Run them with the same service, parser, and loader patterns every time.

---

## Features

- **Configuration-driven** — YAML-based sources, resources, defaults, and queries under `config/`
- **Modular architecture** — Clear seams for services, handlers, parsers, collectors, and loaders
- **Execution planning** — Dependency resolution and optimized execution order
- **Resilient execution** — Validation up front, retries, backfills, and failure isolation per source/resource
- **Extensible design** — Add new sources, auth methods, loaders, and transformations without inventing a new flow
- **Flexible inputs** — Path, query, body, dynamic parameters (dates, upstream data, etc.)

---

## Quick Start

Copy the template pipeline config (working files under `config/` are not committed; see [config/README.md](config/README.md)):

```bash
cp config/defaults.example.yml config/defaults.yml
cp config/examples/jsonplaceholder.yml config/sources/jsonplaceholder.yml
```

### Try with Docker

```bash
git clone https://github.com/victorlou/spine.git
cd spine
cp .env.example .env
docker-compose up --build
```

---

### Local Development

Requires [uv](https://docs.astral.sh/uv/getting-started/installation/) (use a current release; the Docker image pins a specific uv version).

```bash
uv sync --all-groups
source .venv/bin/activate   # Windows: .venv\Scripts\activate
python -m src.main --show-plan
```

Use `--select jsonplaceholder` (or the stem of the YAML file you placed under `config/sources/`) once your source files are in place.

If you prefer not to activate the venv, `uv run python -m src.main …` runs against the same `.venv` uv created during sync.

---

## Basic Usage

With `.venv` activated (see [Local development](#local-development) above):

```bash
python -m src.main --select my_source_name
python -m src.main --select my_source_name:resource_name
python -m src.main --limit 10
python -m src.main --backfill
```

Replace `my_source_name` with the filename stem of a file under `config/sources/` (for example `jsonplaceholder` when using `config/sources/jsonplaceholder.yml`).

---

## Configuration

The public repository includes **templates** only (`config/defaults.example.yml`, `config/examples/`). Your `config/defaults.yml`, `config/sources/*.yml`, and `config/queries/*.sql` stay local and are listed in `.gitignore`. Use the environment variable `CONFIG_PATH` to point at a different directory if you keep configs outside the repo (see [Configuration overview](docs/configuration/overview.md)).

---

## Project Structure

```
config/              # defaults.yml, sources/, queries/ (operator-local; templates in examples/)
src/
├── handler/         # Pipeline orchestration
├── planner/         # Execution planning & dependency resolution
├── service/         # Source integrations (REST, databases, SDKs, …)
├── collector/       # Collection strategy during extraction
├── loader/          # Data destinations (local filesystem, S3, …)
├── parser/          # Lightweight transformations
├── config/          # Config parsing and models
├── auth/            # JWT providers for oauth_jwt
├── audit/           # Optional audit trail recording
├── utils/
└── main.py
```

---

## Documentation

| Topic | Link |
|-------|------|
| **Getting Started** | [Prerequisites, install, usage, CLI](docs/getting-started.md) |
| **Configuration** | [Overview, layout, defaults](docs/configuration/overview.md) |
| **Request inputs** | [Path/query/body, static, dynamic, SOURCE, DATABRICKS, DATE, formats, shorthand](docs/configuration/parameters.md) |
| **Backfill** | [Date-range backfill, inclusive flag, API lag](docs/configuration/backfill.md) |
| **Loading** | [Delta modes: overwrite, append, merge](docs/configuration/loading.md) |
| **Auth** | [OAuth JWT, bearer token, API key](docs/configuration/auth.md) |
| **Transformations** | [add_column, add_column_from_request, ensure_param_values_in_output](docs/configuration/transformations.md) |
| **Deployment** | [Docker, GitHub Actions, GHCR](docs/deployment.md) |
| **ECS / Fargate + S3 (reference)** | [S3 push/pull boto3, IAM, ECS task def](docs/deployment/ecs-s3-reference.md) |
| **Development** | [Linting, testing, debugging, extending](docs/development.md) |

---

## When Spine Fits

Spine is a good fit when you want ingestion work to stop fragmenting into source-specific scripts and conventions.

It is especially useful when:
- You have multiple APIs, databases, or resources that should follow the same operational model
- You want new tables or sources to be mostly a config and schema exercise, not a brand new code path
- You need production concerns like retries, validation, backfills, and failure isolation to be built in from the start
- You want a framework your team can extend deliberately, instead of a connector platform you eventually have to work around
