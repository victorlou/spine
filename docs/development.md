# Development

## Table of Contents

- [Code Quality](#code-quality)
- [Pre-commit (git hooks)](#pre-commit-git-hooks)
- [Testing](#testing)
- [Debugging](#debugging)
- [Documentation site](#documentation-site)
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

GitHub Actions runs the same lint commands as above, installs dependencies with **`uv sync --frozen --all-groups`** (from [`uv.lock`](https://github.com/victorlou/spine/blob/main/uv.lock)), and runs **`uv run pytest`** with **`--cov-fail-under=85`** on **pull requests and pushes to `dev` and `main`**, and on **pushes of version tags** (`v*`).

Container publishing:

- **`main`**: push **`latest`** plus SHA tags to GHCR.
- **`dev`**: push **`dev`** (rolling, mutable) plus **`dev-<short_sha>`** to GHCR; cleanup keeps the **three** newest trace tags besides **`dev`** (see [deployment.md](deployment.md#ci-and-container-images-github-actions)).
- **`v*` tags**: push version-tagged images.

Pull request Docker checks (see [.github/workflows/ci.yml](https://github.com/victorlou/spine/blob/main/.github/workflows/ci.yml)):

- **Path filter:** a Docker **build** (no push) and **`docker-smoke`** run only when the PR changes **`docker/**`**, **`requirements*.txt`**, or **`src/**`**. Other edits (docs-only, config templates only, and so on) skip those jobs to save CI time; `pyproject.toml` / **`uv.lock`**-only changes do **not** trigger the Docker job unless they also touch those paths—run a local **`docker build -f docker/Dockerfile .`** before merging if you rely on lockfile-only Dockerfile behavior.
- **Promotion smoke:** PRs with **`base` = `main`** and **`head` = `dev`** run **`dev-image-smoke`**, which pulls the published **`ghcr.io/.../spine:dev`** image and runs **`--show-plan`** with example config. Repository maintainers should treat **`dev-image-smoke`** as a **required check** for **`main`** if merges should be blocked when that step fails.

Workflow triggers include **`ready_for_review`** so marking a draft PR as ready re-runs CI without requiring an empty commit.

No private package index or repository secrets are required for the default pipeline (beyond `GITHUB_TOKEN` permissions declared in the workflow).

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
uv run pytest --cov=src --cov-report=term-missing --cov-report=xml --cov-fail-under=85
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

## Documentation site

The public documentation site at **[victorlou.github.io/spine](https://victorlou.github.io/spine/)** is built from this `docs/` tree with [MkDocs](https://www.mkdocs.org/) and the [Material](https://squidfunk.github.io/mkdocs-material/) theme. The landing page is a custom template; everything else is the Markdown under `docs/` rendered as-is.

**Where things live** — `docs/` is **pure Markdown**; all site chrome (templates, CSS, JS, fonts, brand assets) lives under `theme/`, so writing docs never means touching site code.

| Path | Purpose |
|------|---------|
| `mkdocs.yml` | Site config: navigation, theme, palette, and strict-link validation. |
| `docs/` | All content pages. Add a Markdown page here and list it under `nav:` in `mkdocs.yml`. |
| `theme/` | Everything visual: `home.html` landing page, `main.html` head, `assets/` (CSS, JS, fonts, marks, favicons). See [`theme/README.md`](https://github.com/victorlou/spine/blob/main/theme/README.md). |
| `docs/index.md` | The landing page's content stub (renders via `theme/home.html`). |
| `.github/workflows/pages.yml` | Builds and deploys the site to GitHub Pages on push to `main`. |

**Preview locally**

```bash
uv run --group docs mkdocs serve
```

Then open <http://127.0.0.1:8000/spine/>. Edits to `docs/` reload live.

**Validate before merging**

```bash
uv run --group docs mkdocs build --strict
```

Strict mode fails on broken internal links or missing heading anchors. CI runs the same command on pull requests that touch `docs/**`, `mkdocs.yml`, or `theme/**`, so a PR cannot merge a change that would break the published site. Links to files **outside** `docs/` (for example `AGENTS.md` or `config/`) must use absolute `https://github.com/...` URLs, since relative paths that escape the docs tree do not resolve in strict mode.

**Publishing** happens automatically: merging to `main` triggers `pages.yml`, which rebuilds and deploys within a few minutes. Enable GitHub Pages once in **repository Settings → Pages → Source: GitHub Actions**. No manual `gh-pages` branch or `mkdocs gh-deploy` is used.

## Extending the Framework

### Add a new authentication type

1. Extend `src/service/base_service.py` with auth logic
2. Update `AuthConfig` in `src/config/config_models.py`
3. Add validation for required fields

### Add a new destination

1. Create loader class in `src/loader/` (inherit from `BaseLoader`)
2. Add the destination to `OBJECT_STORE_DESTINATIONS` in `src/config/loading_schema.py`.
3. Update configuration models for the new destination type
