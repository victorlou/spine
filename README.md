# Spine

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