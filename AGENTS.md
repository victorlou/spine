# AGENTS.md

## Purpose

Spine is a configuration-first ingestion framework. Treat it like production pipeline infrastructure, not a generic CRUD app or a place for source-specific one-off scripts.

## Architecture Map

- `src/config/`: config loading, validation, settings, and schema models
- `src/handler/`: pipeline orchestration and resource execution flow
- `src/planner/`: execution planning, dependency resolution, and staged runs
- `src/service/`: source integrations such as REST, PostgreSQL, HANA, and Python SDK services
- `src/parser/`: lightweight transformations over collected data
- `src/collector/`: collection/storage strategy during extraction
- `src/loader/`: destination loading such as S3
- `config/`: operator-local YAML and SQL; committed files here are templates and examples

## Working Rules

- Prefer extending existing abstractions over adding source-specific branching in the main flow.
- Keep concerns separated: auth, extraction, parsing, collection, loading, and planning should stay modular.
- Preserve config-first behavior. New sources, resources, and tables should primarily be enabled through YAML under `config/`.
- Fail early. If you change config shape or runtime assumptions, update validation in `src/config/` instead of relying on runtime errors.
- Be explicit about production concerns: retries, logging, failure isolation, and backfill behavior should remain visible in code and docs.
- Be critical of requested changes. Do not just implement the first workable idea if it adds avoidable coupling, weakens production behavior, or bloats the codebase.
- Help operators get to the best solution, not just the fastest patch. Challenge assumptions when a request conflicts with maintainability, observability, or engineering best practices.

## Configuration Discipline

- Do not commit operator-local config, secrets, or internal-only endpoints. Public templates live in `config/defaults.example.yml` and `config/examples/`.
- If you change config schema, also update the relevant examples and docs in `config/README.md` and `docs/configuration/`.
- Respect `CONFIG_PATH` behavior in `src/config/settings.py` when changing config resolution.

## Extending Spine

- New auth behavior: extend the service/auth configuration model and validation together.
- New destination: add a loader under `src/loader/`, register it in `src/loader/loader_factory.py`, and document any new config.
- New source/service type: implement it under `src/service/`, wire it through the factory/config models, and avoid bypassing planner/handler flow.
- New transformation or collection behavior should fit the existing parser/collector split rather than being embedded ad hoc in services.

## Quality Bar

- Run or preserve compatibility with `black`, `ruff`, `yamllint`, and `pytest`.
- Keep logs useful for operations and debugging. Prefer structured, actionable messages over vague summaries.
- Avoid hidden coupling across modules. If a change affects planning, config, and runtime behavior, update all three deliberately.
- Be skeptical of convenience shortcuts that weaken validation, observability, or repeatability.
- Prefer the smallest change that keeps the design clean. Avoid introducing new abstractions, flags, or special cases unless they clearly earn their keep.

## Docs Expectations

- Keep the `README.md` practical and honest. Personality is fine, but claims should match the implementation.
- Use `docs/getting-started.md`, `docs/deployment.md`, and `docs/configuration/` as the source of truth for setup and behavior details.

## GitHub issues

When work is tracked in a GitHub issue, treat the issue body as the contract.

- **Before coding** — Read **Problem statement**, **Scope** / **Out of scope**, and **Done when**. Stay inside that boundary. If the issue is wrong, incomplete, or you need a wider change, **suggest** comment text for the operator to post on the issue (or propose splitting a follow-up issue) instead of folding extra requirements into one PR.
- **Traceability** — Assistants should **not** open or edit pull requests or issues on GitHub via MCP/API unless the operator explicitly asks for that. Default: propose a **Related** line (and the rest of the PR description) the operator can paste when they open the PR, following [`.github/pull_request_template.md`](.github/pull_request_template.md): e.g. `Fixes #N` if merge should close the issue, or `Refs #N` for partial or exploratory work.
- **Operator-visible changes** — If the issue changes YAML shape, defaults, or runtime behavior operators rely on, remind them to update `docs/configuration/` (and examples under `config/` where applicable) in the same change set, not as a silent follow-up.

For filing issues, use [`.github/ISSUE_DRAFT_STANDARD.md`](.github/ISSUE_DRAFT_STANDARD.md) and the **Engineering task** template under [`.github/ISSUE_TEMPLATE/engineering_task.md`](.github/ISSUE_TEMPLATE/engineering_task.md) so scope and acceptance criteria stay consistent with how implementers work.
