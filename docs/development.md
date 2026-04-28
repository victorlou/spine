# Development

## Table of Contents

- [Code Quality](#code-quality)
- [Pre-commit (git hooks)](#pre-commit-git-hooks)
- [Testing](#testing)
- [Debugging](#debugging)
- [Extending the Framework](#extending-the-framework)

## Code Quality

| Tool | Purpose | Config |
|------|---------|--------|
| **Black** | Python formatter | `pyproject.toml` |
| **Ruff** | Python linter | `pyproject.toml` |
| **yamllint** | YAML linter | `.yamllint` |

**Run locally**

```bash
uv sync --all-groups
source .venv/bin/activate   # Windows: .venv\Scripts\activate
black --check --diff src/ tests/
ruff check src/ tests/
yamllint config/defaults.example.yml config/examples/
```

Without activating the venv, the same tools work as `uv run black …`, `uv run ruff …`, `uv run yamllint …` (CI uses `uv run` so each step does not rely on shell activation).

### CI

GitHub Actions runs the same lint commands as above, installs dependencies with **`uv sync --frozen --all-groups`** (from [`uv.lock`](../uv.lock)), and runs **`uv run pytest`** on **pull requests and pushes to `dev` and `main`**, and on **pushes of version tags** (`v*`). A container image is **built and pushed to GHCR on pushes to `main`** (including `latest` and a SHA tag) **and on `v*` tag pushes** (image tagged with the release version). No private package index or repository secrets are required for the default pipeline.

See [.github/workflows/ci.yml](../.github/workflows/ci.yml).

### Pre-commit (git hooks)

Pre-commit is optional locally, but useful so **`git commit`** runs the same checks as CI (Black, Ruff, yamllint) before your commit is created.

**Important:** Hooks run only **when you run `git commit`**, not when you save files, stage files, or push. Nothing runs until you install the hook script into `.git/hooks/` (step 2 below).

1. **Install dependencies** (includes the `pre-commit` package):

   ```bash
   uv sync --all-groups
   ```

2. **Install the Git hook once per clone** (writes `.git/hooks/pre-commit`). Run from the repository root:

   ```bash
   uv run pre-commit install
   ```

   If you prefer a classic venv session:

   ```bash
   source .venv/bin/activate   # Windows: .venv\Scripts\activate
   pre-commit install
   ```

3. **Sanity check** (runs all hooks on the whole repo; optional but good after install):

   ```bash
   uv run pre-commit run --all-files
   ```

4. **Make a commit as usual.** If a hook fails, Git leaves your changes staged and does not create the commit until you fix the issues (or skip with `SKIP=hook-id git commit`, which is discouraged for routine use).

**Auto-fix**
```bash
black src/ tests/
ruff check --fix src/ tests/
```

## Testing

```bash
uv run pytest
```

Coverage (same baseline gate used in CI):

```bash
uv run pytest --cov=src --cov-report=term-missing --cov-report=xml --cov-fail-under=38
```

The team target remains 85% line coverage on `src/`; the current gate is a ratchet floor so
coverage regressions are blocked while tests are expanded intentionally.

## Debugging

With `.venv` activated after `uv sync --all-groups` (see **Run locally** under Code quality), or use `uv run python -m src.main …` without activating.

1. **TRACE logging**
   ```bash
   python -m src.main --log-level TRACE
   ```

2. **Check execution plan**
   ```bash
   python -m src.main --show-plan
   ```
   Note: Plan build requires Redis to be available. If any resource uses Databricks-sourced
   parameters (via `databricks` query_ref), Databricks must also be reachable during plan
   build. Queries are only loaded for resources included in the plan (selection + dependencies).

3. **Validate configuration**
   ```bash
   python -m src.main --validate-only
   ```

4. **Run single source**
   ```bash
   python -m src.main --select problematic_source --log-level TRACE
   ```

## Extending the Framework

### Add a new authentication type

1. Extend `src/service/base_service.py` with auth logic
2. Update `AuthConfig` in `src/config/config_models.py`
3. Add validation for required fields

### Add a new destination

1. Create loader class in `src/loader/` (inherit from `BaseLoader`)
2. Register in `src/loader/loader_factory.py`
3. Update configuration models for the new destination type
