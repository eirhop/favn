# Favn Documentation Guide

Use this guide when changing documentation. Its goal is accurate, findable docs
with one canonical explanation per contract. Start from [`README.md`](README.md)
and inspect only the owning code and relevant pages.

## Choose the reader and page type

Name the reader and their task before writing.

| Need | Page type |
| --- | --- |
| First successful outcome | Tutorial |
| Complete one task | How-to |
| Look up exact fields, options, states, or failures | Reference |
| Understand a decision and its tradeoffs | Explanation |

Split a page that tries to serve all four needs. Put prerequisites and the
expected result near the beginning. Include failure behavior when readers must
operate or call the documented feature.

## Put information in one canonical place

| Information | Canonical location |
| --- | --- |
| Public package overview and user workflows | `apps/favn/README.md`, `apps/favn/guides/` |
| Public API contract | Moduledocs, function docs, typespecs, and typedocs |
| App ownership and code map | `docs/structure/` |
| System decisions and invariants | `docs/architecture/` |
| PostgreSQL implementation | `docs/storage/postgresql/` |
| Deployment and operations | `docs/production/`, `docs/operators/` |
| Implemented product capability | `docs/FEATURES.md` |
| Forward work | `docs/ROADMAP.md` and linked GitHub issues |
| Point-in-time findings or completed plans | `docs/report/`, `docs/refactor/`, `docs/archive/` |

Overview pages summarize and link; they do not copy reference material. The root
`README.md` is only a public introduction. Historical plans and reports must be
marked as such and must not masquerade as current requirements.

When sources disagree, executable contracts and migrations win. Correct the
canonical document in the same change.

## Preserve product boundaries

Follow [`../AGENTS.md`](../AGENTS.md) and the relevant page in
[`structure/`](structure/). In particular:

- Public consumers depend on `:favn` and supported plugins, not runtime apps.
- `favn_view` calls only public orchestrator facades.
- The orchestrator owns schedules and durable control-plane truth; runners execute
  pinned work.
- PostgreSQL capability stores persist control-plane state. SQL execution adapters
  connect to user data systems. Do not call both concepts “storage adapters.”
- Compile-time authoring, manifest publication, orchestration, and execution are
  separate phases; name the owner and phase when behavior could be ambiguous.

Documentation does not make an internal module public. Describe the supported
boundary unless the page is explicitly an internal contributor reference.

## Write durable pages

- Lead with purpose, prerequisites, and outcome.
- Prefer concrete nouns and exact ownership over “manager,” “helper,” or “service.”
- State inputs, outputs, defaults, limits, side effects, and failure/unknown-outcome
  behavior where they matter.
- Use small copyable examples. Do not invent APIs or omit required setup.
- Keep examples consistent with formatter output and test important examples.
- Link directly to the canonical page with descriptive link text.
- Use repository-relative links for repository Markdown and ExDoc references for
  published module documentation.
- Avoid version-sensitive external links unless the external system is the source
  of truth.
- Do not copy long status inventories into README files, guides, or plans.

Public module docs should explain what the module owns, when to use it, and its
main contract. Public functions need a useful `@doc` and typespec; document
meaningful options, return shapes, errors, and examples. Internal modules may use
`@moduledoc false`, but stable boundary behaviors still need contributor-facing
documentation.

## ExDoc and HexDocs

The `:favn` package owns the public landing page and guide extras. Keep the guide
list in `apps/favn/mix.exs` explicit and ordered. Do not publish internal
orchestrator, runner, persistence, or UI documentation as consumer API.

`Favn.AI` is the compiled-doc router for agents using Favn. When a public DSL or
workflow changes, update its moduledocs, canonical guide, and `Favn.AI` pointer
together.

## Workflow

1. Identify the reader, task, owner, and page type.
2. Find the current canonical page before creating a new one.
3. Inspect the owning code, tests, and relevant structure page.
4. Make the smallest complete update; replace duplicated detail with links.
5. Check that public/internal and compile-time/runtime boundaries remain clear.
6. Review headings, examples, links, and failure behavior.
7. Run focused verification and report anything intentionally skipped.

For Markdown-only changes, render or read the affected pages and run:

```bash
git diff --check
```

Run `mix format` when Elixir source or doctest-bearing code changes. Run focused
doctests or `mix docs --warnings-as-errors` when public executable examples or
ExDoc configuration changes. Use Storybook or Playwright only when the documented
browser behavior changed. Documentation-only edits do not require the umbrella
test suite.

## Review checklist

- Is the intended reader and outcome obvious?
- Is this the canonical place for the information?
- Did the change remove or avoid duplicate explanations?
- Are ownership and lifecycle boundaries accurate?
- Are examples, links, options, and failure modes actionable?
- Is current behavior separated from plans and historical evidence?
- Can another contributor update this page without searching the whole repository?
