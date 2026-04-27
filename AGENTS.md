# AGENTS.md

## Purpose

Spine is a configuration-first ingestion framework. Treat it like production pipeline infrastructure, not a generic CRUD app or a place for source-specific one-off scripts.

## Architecture Map

- `src/config/`: config loading, validation, settings, and schema models
- `src/handler/`: pipeline orchestration and resource execution flow
- `src/planner/`: execution planning, dependency resolution, and staged runs
- `src/service/`: source integrations such as REST, relational database connectors, and Python SDK services
- `src/parser/`: lightweight transformations over collected data
- `src/collector/`: collection/storage strategy during extraction
- `src/loader/`: destination loading (S3, local, …), Spark writes, and `object_store` / `local_storage` helpers
- `config/`: operator-local YAML and SQL; committed files here are templates and examples

## Working Rules

- **Refresh this file when patterns change** — Whenever you establish or clarify a convention that should hold across the repo (where validation lives, how errors are phrased, how new source types register, duplication policy above), update `AGENTS.md` in the same change set so later contributors and assistants inherit it without repeated reminders.
- **Single source of truth** — If two symbols express the same rule (for example a module-level predicate and a class staticmethod that only forwards to it), consolidate to one public entry point and update call sites. Do not preserve duplicate APIs for habit or “convenience.”
- Prefer extending existing abstractions over adding source-specific branching in the main flow.
- Keep concerns separated: auth, extraction, parsing, collection, loading, and planning should stay modular.
- Preserve config-first behavior. New sources, resources, and tables should primarily be enabled through YAML under `config/`.
- Fail early. If you change config shape or runtime assumptions, update validation in `src/config/` (and the planner when the check depends on the execution graph) instead of relying on runtime errors.
- Be explicit about production concerns: retries, logging, failure isolation, and backfill behavior should remain visible in code and docs.
- Be critical of requested changes. Do not just implement the first workable idea if it adds avoidable coupling, weakens production behavior, or bloats the codebase.
- Help operators get to the best solution, not just the fastest patch. Challenge assumptions when a request conflicts with maintainability, observability, or engineering best practices.

## Configuration Discipline

- Do not commit operator-local config, secrets, or internal-only endpoints. Public templates live in `config/defaults.example.yml` and `config/examples/`.
- If you change config schema, also update the relevant examples and docs in `config/README.md` and `docs/configuration/`.
- Respect `CONFIG_PATH` behavior in `src/config/settings.py` when changing config resolution.

## Extending Spine

- New auth behavior: extend the service/auth configuration model and validation together.
- New destination: add a loader under `src/loader/`, register it in `src/loader/loader_factory.py` under the **canonical** destination id (see `src/config/loading_destinations.py` for object-store destinations, aliases, and `normalize_loading_destination`), and document any new config. If Spark needs filesystem or connector wiring, extend `src/config/config_spark.py` (`SparkSessionConf`), `src/config/spark_runtime.py`, and `defaults.spark_runtime` in the same change set so behavior stays configuration-first.
- New source/service type: implement it under `src/service/`, wire it through the factory/config models, and avoid bypassing planner/handler flow. For a new **relational database** kind that shares the same table/query extract and request-context rules, add its `SourceType` to `is_database_source_type()` in `src/config/config_models.py` (that function is the only database-kind predicate; call it from planner, handler, and validators rather than re-listing types).
- New transformation or collection behavior should fit the existing parser/collector split rather than being embedded ad hoc in services.

## Quality Bar

- Run or preserve compatibility with `black`, `ruff`, `yamllint`, and `pytest`.
- Keep logs useful for operations and debugging. Prefer structured, actionable messages over vague summaries.
- Avoid hidden coupling across modules. If a change affects planning, config, and runtime behavior, update all three deliberately.
- Be skeptical of convenience shortcuts that weaken validation, observability, or repeatability.
- Prefer the smallest change that keeps the design clean. Avoid introducing new abstractions, flags, or special cases unless they clearly earn their keep.
- **No issue or PR references in implementation code** — Do not put GitHub issue numbers, pull request numbers, or links in source comments, docstrings, or user-facing error messages. Traceability belongs in commit messages and PR descriptions (see **GitHub issues** below). Describe behavior objectively so docs and errors stay accurate as new source types are added (avoid hardcoding today's short list of database or API names unless the text is truly specific to one integration).

## Docs Expectations

- Keep the `README.md` practical and honest. Personality is fine, but claims should match the implementation.
- Use `docs/getting-started.md`, `docs/deployment.md`, and `docs/configuration/` as the source of truth for setup and behavior details.
- Write for someone discovering the project for the first time. Prefer direct, timeless descriptions of behavior and configuration. Avoid framing docs around past releases or repo history (for example “unchanged from earlier versions”, “previously”, “now you can”) unless you are explicitly documenting a breaking migration or upgrade path.

## GitHub issues

When work is tracked in a GitHub issue, treat the issue body as the contract.

- **Before coding** — Read **Problem statement**, **Scope** / **Out of scope**, and **Done when**. Stay inside that boundary. If the issue is wrong, incomplete, or you need a wider change, **suggest** comment text for the operator to post on the issue (or propose splitting a follow-up issue) instead of folding extra requirements into one PR.
- **Traceability** — Assistants should **not** open or edit pull requests or issues on GitHub via MCP/API unless the operator explicitly asks for that. Default: propose a **Related** line (and the rest of the PR description) the operator can paste when they open the PR, following [`.github/pull_request_template.md`](.github/pull_request_template.md): e.g. `Fixes #N` if merge should close the issue, or `Refs #N` for partial or exploratory work.
- **Comments and other mutating actions** — Do **not** post issue or pull request **comments**, labels, assignments, or other GitHub mutations via MCP/API unless the operator **explicitly confirms** that you should do so in that conversation. Default to **drafting** comment text in the chat for them to copy.
- **Operator-visible changes** — If the issue changes YAML shape, defaults, or runtime behavior operators rely on, remind them to update `docs/configuration/` (and examples under `config/` where applicable) in the same change set, not as a silent follow-up.

### Issue-related questions (triage and overlap)

When the task is to check existing issues, relate work to an issue, or avoid duplicating tickets:

- **Prefer the GitHub MCP** when it is enabled for the workspace: use read-only tools (`list_issues`, `search_issues`, `issue_read`, and so on) rather than guessing from memory. Discover the correct MCP server identifier and tool parameters from the project’s MCP descriptor files.
- **Gather full context** — The issue body alone is not enough. Also pull **comments**, **labels**, and **sub-issues** when the API exposes them, and scan for cross-links in the description (`Depends on`, `Refs`, duplicate pointers). Summarize overlap with the current task using that complete picture.
- **Read-only vs write** — Using MCP to **read** issues and comments is encouraged for accuracy; creating, editing, or commenting still requires explicit operator consent per **Traceability** and **Comments and other mutating actions** above.

For filing issues, use [`.github/ISSUE_DRAFT_STANDARD.md`](.github/ISSUE_DRAFT_STANDARD.md) and the **Engineering task** template under [`.github/ISSUE_TEMPLATE/engineering_task.md`](.github/ISSUE_TEMPLATE/engineering_task.md) so scope and acceptance criteria stay consistent with how implementers work.
