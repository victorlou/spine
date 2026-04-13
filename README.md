# Spine

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

**Spine is a developer-first ingestion layer that standardizes how data enters your platform.**

It is not a no-code tool, nor a collection of prebuilt connectors.  
Spine is an opinionated foundation for building, scaling, and maintaining ingestion pipelines through configuration, structure, and shared patterns.

---

## Why Spine?

Most data teams don’t struggle to *ingest data* — they struggle to do it **consistently, reliably, and at scale**.

Ingestion often becomes:
- Dozens of one-off scripts
- Inconsistent patterns across sources
- Hard-to-maintain pipelines
- No shared standards across teams

Spine solves this by introducing a **unified ingestion layer**:

> One way to define ingestion. One way to run it. One way to scale it.

---

## What Spine Is (and isn’t)

**Spine is:**
- A **configuration-first ingestion system**
- A **standardized entry point** into your data platform
- A **developer-focused tool** designed for extensibility and control
- A **foundation layer** that fits naturally into modern architectures (e.g. medallion)

**Spine is not:**
- A no-code ingestion tool
- A managed connector platform
- A black-box abstraction over your pipelines

You build and extend it. Spine ensures everything follows the same structure.

---

## Core Principles

### 1. Standardization over ad hoc pipelines
All ingestion follows the same structure — regardless of source.

### 2. Configuration over code
Pipelines are defined through YAML, not scattered scripts.

### 3. Extensibility by design
New sources, auth methods, and loaders can be added without breaking the system.

### 4. Clear separation of concerns
Authentication, extraction, transformation, and loading are modular and composable.

### 5. Platform-first thinking
Spine is not just a tool — it’s a **layer in your data platform**.

---

## Features

- **Modular architecture** — Authentication, fetching, transformation, and loading
- **Configuration-driven** — YAML-based sources and endpoints
- **Execution planning** — Dependency resolution and optimized execution order
- **Resilient execution** — Retries and failure isolation per source/endpoint
- **Extensible design** — Plug in new sources, loaders, and services
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

```bash
pip install -r requirements.txt
python -m src.main --show-plan
```

Use `--select jsonplaceholder` (or the stem of the YAML file you placed under `config/sources/`) once your source files are in place.

---

## Basic Usage

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
├── service/         # API/auth implementations
├── loader/          # Data destinations (S3, etc.)
├── parser/          # Lightweight transformations
├── config/          # Config parsing and models
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
| **Transformations** | [add_column, add_column_from_request, ensure_param_values](docs/configuration/transformations.md) |
| **Deployment** | [Docker, GitHub Actions, GHCR](docs/deployment.md) |
| **ECS / Fargate + S3 (reference)** | [S3 push/pull boto3, IAM, ECS task def](docs/deployment/ecs-s3-reference.md) |
| **Development** | [Linting, testing, debugging, extending](docs/development.md) |

---

## Vision

Spine aims to become the **standard foundation layer for ingestion in modern data platforms**.

As data ecosystems grow, ingestion should not be reinvented for every source.
It should be structured, consistent, and scalable by design.

Spine is that foundation.
