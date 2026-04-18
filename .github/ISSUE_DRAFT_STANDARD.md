# Issue draft standard (Spine)

Use this structure for engineering issues so they stay actionable, reviewable, and easy to sequence.

## Required sections

### Title

- Imperative, specific: `Add PR-time Docker build to CI` not `Improve CI`.

### Labels

Use a **small** set so filing issues stays quick (create these labels once in the repo if missing):

| Label | Use for |
|-------|---------|
| `area:runtime` | Services, handler, planner, parsers, Spark execution |
| `area:config` | Pydantic models, YAML schema, settings, validation |
| `area:loader` | Destinations, writes, storage paths |
| `area:ci` | GitHub Actions, GHCR, build/publish gates |
| `area:docs` | User docs, GitHub Pages, README |
| `enhancement` | New capability or meaningful improvement |
| `chore` | Refactor, hygiene, internal-only change |
| `bug` | Incorrect behavior / regression |

Pick **one "type" and one or two `area:*`** per issue.

### Body template

```markdown
## Problem statement
<!-- What is broken, missing, or suboptimal today? -->

## Why this matters
<!-- Impact on operators, production risk, maintenance cost. -->

## Scope
**In scope:**
- …

**Out of scope:**
- …

## Proposed approach
<!-- High-level design, key files, migration strategy. -->

## Done when
<!-- Merged acceptance + wrap-up: behavior, docs, quality bar. Implementer owns test approach. -->
- [ ] …
- [ ] …

## Dependencies / sequencing
<!-- Links to other issues; must ship before/after. -->
```

## Conventions

- **Link code** with paths like `src/service/base_service.py` (no line numbers required in issues).
- **One primary outcome** per issue; split if the PR would exceed ~400 lines or mix unrelated concerns.
- **Dependencies**: reference other issues by title or GitHub number once filed.

## GitHub UI template

Contributors can use **Engineering task** in the “New issue” chooser ([`.github/ISSUE_TEMPLATE/engineering_task.md`](ISSUE_TEMPLATE/engineering_task.md)).
