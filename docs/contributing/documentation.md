# How To Update Favn Documentation

Reader: Favn contributors and AI agents changing documentation.

Documentation type: how-to guide.

Use this guide when adding or changing Favn docs. Read
`docs/DOCUMENTATION_GUIDE.md` first for the full writing standard and
`docs/DOCUMENTATION_PLAN.md` for the current documentation work.

## Before You Start

- Know who the reader is: new user, asset author, operator, adapter author,
  contributor, AI agent, or maintainer.
- Know what the reader is trying to do.
- Inspect only the paths needed for the doc you are changing.
- Prefer the smallest accurate update.
- Do not turn internal modules into public APIs by how you describe them.

## Pick The Right Doc Shape

| Reader need | Use |
| --- | --- |
| First guided success | Tutorial |
| One concrete task | How-to guide |
| Exact options, fields, states, return values, or failures | Reference |
| Why Favn works this way | Explanation |

If one page starts doing all four jobs, split it or make the scope smaller.

## Where To Put Docs

| Doc kind | Preferred location |
| --- | --- |
| Public package overview and user guides | `apps/favn/README.md` and `apps/favn/guides/` |
| App ownership maps | `docs/structure/` |
| Architecture decisions | `docs/architecture/` |
| Operator procedures | `docs/operators/` or `docs/production/` |
| Adapter contributor docs | `docs/adapters/` or stable module docs |
| Historical plans and issue work | Existing plan files, `docs/refactor/`, or `docs/archive/` |
| Public API details | Module docs, typespecs, typedocs, and ExDoc extras |

Do not create new documentation directories unless the existing tree cannot serve
the reader.

## Who Owns What

Use plain ownership language. Readers should be able to tell what each part does.

| Part | What it does |
| --- | --- |
| `favn` | Public dependency, DSL facade, public guides, and public Mix task entrypoints. |
| `favn_core` | Shared compiler, manifest, and domain shapes used across Favn. |
| `favn_orchestrator` | Persisted runtime truth: manifests, runs, schedules, backfills, admission, auth/session/audit, diagnostics, and control-plane storage. |
| `favn_runner` | Executes pinned work. It does not own schedules or persisted control-plane truth. |
| `favn_view` | Renders operator state and sends commands through orchestrator facades. It does not call storage, scheduler, or runner internals directly. |
| SQL execution adapters | Open backend sessions, bootstrap SQL systems, inspect relations, and run SQL for assets or `Favn.SQLClient`. |
| Storage adapters | Save orchestrator state. They do not own user asset data, DuckDB tables, SQL sessions, scheduling policy, or UI state. |

## Storage Adapter Or SQL Execution Adapter

When writing adapter docs, keep this distinction clear:

- A storage adapter saves Favn control-plane state for the orchestrator.
- A SQL execution adapter connects to a SQL backend and runs SQL work.
- DuckDB and DuckDB ADBC are SQL execution adapters.
- SQLite and Postgres storage adapters are orchestrator persistence
  implementations.
- Public users should not call storage adapters directly.
- Public users should normally see SQL execution through `Favn.SQLClient`, SQL
  assets, connection modules, and adapter configuration guides.

This keeps adapter docs useful without exposing storage adapters or the SQL
runtime as ordinary user-facing APIs.

## Writing Checklist

Before you finish a documentation change, check:

- Is the reader named or obvious?
- Is the task or purpose clear in the first few lines?
- Is the doc shape clear: tutorial, how-to guide, reference, or explanation?
- Does the doc say who owns the behavior when that matters?
- Does it separate compile-time authoring from runtime behavior?
- Does it avoid telling `favn_view` to call storage, scheduler, or runner
  internals?
- Does it avoid telling the runner to own schedules or persisted control-plane
  truth?
- Does it avoid exposing storage adapters or SQL runtime internals as public user
  APIs?
- Are inputs, outputs, options, and failure modes concrete enough to act on?
- Are examples small, realistic, and copyable?
- Are links helpful rather than a replacement for explanation?

## Failure Modes

Do not document only the success path.

Use plain language for failures:

| Instead of only saying | Say what it means |
| --- | --- |
| Validation failed. | Which value is invalid and what the reader should change. |
| Persistence failed. | Whether the database is missing, not migrated, unreachable, locked, or in conflict. |
| Timeout. | Which operation timed out and whether the result may be unknown. |
| Conflict. | Which guarded write lost and who should decide retry or rejection. |
| Adapter error. | Which external system failed and which config or credential is likely involved. |

Keep exact error shapes when they matter for callers, but add a short explanation
so the table is useful to humans.

## Verification

For Markdown-only changes, review the rendered text, links, headings, examples,
and ownership language. Run `git diff --check` to catch whitespace problems.

Do not run the full umbrella test suite for documentation-only work.

Use focused checks only when needed:

- Run app-scoped doctests when executable public examples changed.
- Run `mix format` when Elixir source, Mix files, or doctest-bearing code changed.
- Run `cd apps/favn && mix docs --warnings-as-errors` only after ExDoc is
  configured and public package docs or public module docs changed.
- Run Storybook or focused Playwright checks only when documented UI flows or
  stories changed.

## Completion Report

When a documentation subagent finishes, report:

- Files read.
- Files changed.
- Assumptions made.
- Boundaries preserved.
- Verification run.
- Verification intentionally skipped.
- Remaining risks or open questions.
