# Favn Documentation Plan

> Historical coordination note: this file records the first documentation pass.
> Current product and persistence direction lives in `docs/FEATURES.md`,
> `docs/ROADMAP.md`, and `docs/architecture/postgresql-control-plane-storage-v2.md`.

Reader: documentation contributors and maintainers reviewing that first pass.

Documentation type: explanation and reference checklist.

Purpose: define the documentation initiative, preserve Favn's manifest-first app boundaries, and split the work into scoped packages that do not require reading the whole repository.

Every subagent must read `docs/DOCUMENTATION_GUIDE.md` and this document before editing documentation.

## Documentation Goals

This initiative should make Favn understandable without turning unstable internals into public commitments.

User-facing public docs should:

- Introduce Favn as the public `:favn` dependency and DSL surface.
- Give new users a safe first path from authoring modules to a manifest.
- Explain asset, pipeline, manifest, local development, adapter, and runtime concepts at the supported public boundary.
- Keep runtime, orchestrator, runner, storage, and UI internals out of ordinary user dependency guidance.

Internal contributor docs should:

- State which app owns each concept.
- Help contributors and AI agents preserve boundaries when changing code.
- Point to app-specific structure docs and focused verification commands.
- Mark migration plans, issue plans, and reviews as planning/history rather than public product docs.

Architecture docs should:

- Explain why Favn is manifest-first.
- Show compile-time authoring flow into manifest data and runtime flow through persisted manifest versions.
- Document app ownership: `favn`, `favn_core`, `favn_runner`, `favn_orchestrator`, `favn_view`, and adapters/plugins.
- Reject unsafe shortcuts, especially UI-to-storage coupling, runner-owned schedules, and public docs for internal helper modules.

API/reference docs should:

- Live primarily in moduledocs, typedocs, specs, and ExDoc extras for stable public APIs.
- Focus public reference on `Favn`, authoring modules, `Favn.SQLClient`, manifest shapes exposed through public docs, and supported local Mix tasks.
- Keep unstable runtime helper functions and internal modules hidden or explicitly internal.

Runtime/operator docs should:

- Explain manifest registration, active manifests, runs, schedules, backfills, diagnostics, SSE, idempotency, repair, and single-node operation through orchestrator-owned semantics.
- Document operator commands and UI behavior without making `favn_view` or storage adapters appear to own lifecycle state.
- Include failure modes: validation, persistence, duplicate command keys, cancellation, runner timeout, projection repair, and restore constraints.

Adapter/plugin docs should:

- Explain optional plugin dependency shape and adapter responsibilities.
- Document DuckDB, ADBC, SQL runtime, and PostgreSQL control-plane storage at the right boundary.
- Distinguish data-plane execution adapters from control-plane storage adapters.
- Avoid putting scheduling, UI state, or product lifecycle semantics in adapter docs.

UI docs should:

- Explain the operator UI as a thin Phoenix/LiveView boundary.
- Name the orchestrator facade or DTO each page uses.
- Cover loading, empty, success, error, stale, and disabled states when documented.
- Use Storybook or Playwright only for focused UI behavior that is actually documented.

HexDocs/package docs should:

- Publish `apps/favn` first.
- Include `apps/favn/README.md` and a small set of guides under `apps/favn/guides/`.
- Avoid configuring public HexDocs for internal apps until their contracts are explicitly stable and approved.

## Audience Map

| Audience | Needs to understand | Docs that should serve them | Relevant repo areas | Explicitly not relevant |
| --- | --- | --- | --- | --- |
| New users trying Favn | What Favn is, how to add `:favn`, how to define one asset, how to generate a manifest, how to run local tooling safely | `README.md`, `apps/favn/README.md`, `apps/favn/guides/getting-started.md`, `apps/favn/guides/local-development.md` | `apps/favn`, selected public authoring modules, `examples/basic-workflow-tutorial`, `docs/FEATURES.md` | Orchestrator internals, runner server internals, storage adapter implementation, Phoenix internals |
| Developers authoring assets and pipelines | DSL modules, relation namespaces, runtime config refs, SQL assets, pipelines, schedules, manifest generation, planning | `apps/favn/guides/authoring-assets.md`, `apps/favn/guides/manifest-first.md`, public moduledocs for `Favn.Asset`, `Favn.SQLAsset`, `Favn.MultiAsset`, `Favn.Pipeline`, `Favn.Connection`, `Favn.SQLClient` | `apps/favn`, `apps/favn_authoring/lib`, stable shared manifest structs in `apps/favn_core/lib/favn` | `apps/favn_view`, scheduler internals, storage schemas, runner process implementation |
| Operators using runtime/operator tooling | Local dev stack, single-node runtime assumptions, manifests, runs, schedules, backfills, diagnostics, logs, restore, idempotent commands | `apps/favn/guides/local-development.md`, `apps/favn/guides/runtime-model.md`, `docs/operators/runs-and-schedules.md`, `docs/production/*.md` | `apps/favn/lib/mix/tasks`, `apps/favn_local`, `apps/favn_orchestrator` public facade/API contracts, production docs | Public DSL internals except manifest inputs, direct storage table internals unless restoring/diagnosing, UI component implementation |
| Adapter/plugin authors | Adapter ownership, SQL runtime contracts, persistence capability expectations, external system assumptions, failure modes | `apps/favn/guides/adapters.md`, `docs/adapters/storage-adapters.md`, `docs/adapters/duckdb.md`, module docs for stable behaviours | `apps/favn_sql_runtime`, `apps/favn_duckdb`, `apps/favn_duckdb_adbc`, `apps/favn_storage_postgres`, relevant structure docs | `apps/favn_view`, public DSL implementation details unrelated to adapter inputs, scheduling policy internals |
| Contributors and AI agents | App ownership, where to inspect, where to verify, which docs are public, how not to blur boundaries | `docs/DOCUMENTATION_GUIDE.md`, this plan, `docs/structure/*.md`, `docs/architecture/*.md`, `docs/contributing/documentation.md` | `docs/structure`, root `mix.exs`, affected app paths only | Whole-repo scans by default, accidental public HexDocs for internals |
| Future maintainers | Why manifest-first matters, stable boundary decisions, public/internal package policy, deferred work | `docs/architecture/manifest-first.md`, `docs/architecture/runtime-model.md`, `docs/production/public_api_boundary.md`, this plan | `docs/architecture`, `docs/production`, selected public facades | Historical issue plans unless debugging a specific migration |

## Proposed Docs Tree

Keep the target tree small. Create only the files needed for the first coherent pass; leave deeper references as follow-up work.

```text
README.md
docs/
  DOCUMENTATION_GUIDE.md
  DOCUMENTATION_PLAN.md
  FEATURES.md
  ROADMAP.md
  archive/
    README.md
    ai-planning/
      README.md
      *.md
  architecture/
    manifest-first.md
    runtime-model.md
    runner-boundary.md
  adapters/
    storage-adapters.md
    duckdb.md
  contributing/
    documentation.md
  operators/
    runs-and-schedules.md
    ui-overview.md
  production/
    public_api_boundary.md
    single_node_contract.md
    single_node_operator_runbook.md
  structure/
    *.md
apps/favn/
  README.md
  guides/
    getting-started.md
    manifest-first.md
    authoring-assets.md
    local-development.md
    adapters.md
    runtime-model.md
    cheatsheet.cheatmd
```

Do not create all proposed files in one pass. The minimum coherent first set is:

- `docs/DOCUMENTATION_PLAN.md`
- `apps/favn/README.md`
- `apps/favn/guides/getting-started.md`
- `apps/favn/guides/manifest-first.md`
- `apps/favn/guides/authoring-assets.md`
- `apps/favn/guides/local-development.md`
- `apps/favn/guides/runtime-model.md`
- `apps/favn/guides/adapters.md`
- `docs/architecture/manifest-first.md`
- `docs/architecture/runtime-model.md`
- `docs/contributing/documentation.md`
- `docs/operators/runs-and-schedules.md`
- `docs/adapters/storage-adapters.md`

## Documentation Type Per File

| File | Type | Notes |
| --- | --- | --- |
| `README.md` | tutorial plus overview entrypoint | Keep as root entrypoint; avoid complete reference. |
| `docs/DOCUMENTATION_GUIDE.md` | reference | Keep as writing rules for humans and agents. |
| `docs/DOCUMENTATION_PLAN.md` | explanation and reference checklist | Coordination document for this initiative. |
| `docs/FEATURES.md` | reference | Implemented capability inventory only. |
| `docs/ROADMAP.md` | explanation | Forward-looking direction only. |
| `docs/archive/README.md` | reference | Archive index; marks archived docs as historical, not current public docs. |
| `docs/archive/ai-planning/README.md` | reference | Index for historical AI-agent planning and implementation references. |
| `docs/archive/ai-planning/*.md` | historical reference | Preserved issue plans, broad plans, PR notes, tasklists, architecture/scope references, and refactor planning docs. Do not treat as current public docs. |
| `docs/architecture/manifest-first.md` | explanation | Why manifests are the compile-time/runtime seam. |
| `docs/architecture/runtime-model.md` | explanation | Runtime ownership, persisted truth, and lifecycle boundaries. |
| `docs/architecture/runner-boundary.md` | explanation | Runner execution responsibility and non-ownership of schedules. |
| `docs/adapters/storage-adapters.md` | reference | Stable callback and storage contract expectations when approved. |
| `docs/adapters/duckdb.md` | how-to guide plus reference | Plugin setup, config shape, external failure modes. |
| `docs/contributing/documentation.md` | how-to guide | How contributors update docs safely. |
| `docs/operators/runs-and-schedules.md` | how-to guide | Operator flows for runs, schedules, cancellation, and diagnostics. |
| `docs/operators/ui-overview.md` | explanation | UI ownership and page state model. |
| `docs/production/public_api_boundary.md` | reference | Existing public/internal package support boundary. |
| `docs/production/single_node_contract.md` | reference | Production support contract. |
| `docs/production/single_node_operator_runbook.md` | how-to guide | Operational procedures. |
| `docs/structure/*.md` | reference | App/path ownership maps. |
| `apps/favn/README.md` | tutorial plus overview entrypoint | HexDocs landing page for public package. |
| `apps/favn/guides/getting-started.md` | tutorial | First success path. |
| `apps/favn/guides/manifest-first.md` | explanation | Public explanation of manifest-first model. |
| `apps/favn/guides/authoring-assets.md` | how-to guide | Author one asset and one pipeline. |
| `apps/favn/guides/local-development.md` | how-to guide | Use public `mix favn.*` local tooling. |
| `apps/favn/guides/adapters.md` | how-to guide | Choose optional plugins and configure public connection shape. |
| `apps/favn/guides/runtime-model.md` | explanation | Public runtime model without internal APIs. |
| `apps/favn/guides/cheatsheet.cheatmd` | reference | DSL and task quick reference after public API stabilizes. |

## Public/Internal Boundary Decision

| App | Public HexDocs now? | Internal only? | Deferred? | Why |
| --- | --- | --- | --- | --- |
| `favn` | Yes, first target | No | No | It is the public user-facing dependency, DSL facade, `Favn.SQLClient` owner, and public Mix task entrypoint. |
| `favn_authoring` | No | Yes | Possible future extraction only if API is intentionally public | It implements authoring and manifest behavior behind `favn`; public docs should point to `Favn` and DSL modules, not the implementation app. |
| `favn_core` | No for package-level HexDocs | Yes for stable shared contract module docs when linked from `favn` | Public contract excerpts may move later | It owns shared compiler/domain/manifest contracts, but broad public docs would expose too many unstable internals. |
| `favn_runner` | No | Yes | Public runner contracts deferred | It owns execution runtime. Its boundary is important internally, but ordinary users should not depend on runner internals. |
| `favn_orchestrator` | No | Yes | Public operator API docs deferred until stable | It owns control plane and private HTTP/facade contracts. Operator docs can describe behavior, but not promise unstable modules as public API. |
| `favn_view` | No | Yes | UI public docs deferred | It is a thin UI/API boundary. Document operator behavior and Storybook states internally, not as a standalone public package. |
| `favn_local` | No | Yes | No broad HexDocs | It implements public `mix favn.*` behavior exposed through `favn`; document commands in `apps/favn` guides. |
| `favn_sql_runtime` | No | Yes | Adapter-author reference deferred until contracts settle | It is shared SQL runtime infrastructure. Public users should use `Favn.SQLClient` and adapter guides. |
| `favn_duckdb` | Not yet | Partly plugin-facing | Yes, until plugin package docs are planned | It is an optional supported plugin, but public docs should start with `apps/favn/guides/adapters.md`. |
| `favn_duckdb_adbc` | Not yet | Partly plugin-facing | Yes, until plugin package docs are planned | It is supported for explicit DuckDB driver control; document usage without exposing internals. |
| `favn_storage_postgres` | No | Yes | Capability docs only when stable | It is internal control-plane persistence; contributors need contracts and operators need deployment behavior. |
| `favn_azure` | No | Yes | Plugin docs deferred | Azure token/bootstrap behavior appears through DuckDB/DuckLake adapter documentation. |
| `favn_test_support` | No | Yes | No public docs | Shared fixtures are contributor-only. |

## HexDocs/ExDoc Follow-up Notes

No HexDocs or ExDoc configuration was changed in this cleanup pass. Root
`mix.exs` does not configure ExDoc or package docs, and this task did not inspect
`apps/favn/mix.exs` because it was outside the allowed scope.

Follow-up work for Subagent H:

- Inspect `apps/favn/mix.exs`, `apps/favn/README.md`, and `apps/favn/guides/**`
  before changing package docs configuration.
- Confirm whether `:ex_doc` is already present in the relevant app dependencies
  or must be added explicitly.
- Configure public HexDocs for `apps/favn` first only after the landing README
  and first guide set are stable.
- Keep internal apps out of public HexDocs unless this plan is updated and the
  public/internal boundary decision changes.
- Verify with `mix docs --warnings-as-errors` only after ExDoc is configured and
  dependencies are available.

## Subagent Work Packages

All subagents must follow these rules:

- Read `docs/DOCUMENTATION_GUIDE.md`.
- Read this document.
- Stay within assigned repo paths unless a clearly stated reason requires looking elsewhere.
- Do not read the whole repo.
- Do not run the full umbrella test suite.
- Do not expose unstable internal APIs as public docs.
- Do not move logic across app boundaries.
- Prefer small, accurate documentation over broad, speculative documentation.
- Return a concise completion report listing files read, files changed, assumptions, boundaries preserved, verification run, verification skipped, and follow-up risks.

### Subagent A: Documentation Inventory And Classification

Objective: map current documentation and classify what should be kept, updated, moved, merged, deprecated, or deleted.

Docs to read first: `docs/DOCUMENTATION_GUIDE.md`, `docs/DOCUMENTATION_PLAN.md`.

May inspect: `docs/`, `docs/structure/`, `README.md`, root `mix.exs`.

Should not inspect unless necessary: all `apps/*/lib/**` source code.

Output files: update the inventory section in this file, or create `docs/documentation_inventory.md` only if the inventory becomes too large.

Expected documentation type: reference inventory.

Questions to answer: which docs are product docs, which are planning/history, which are app structure maps, which are production/operator docs, and which files risk being mistaken for current public docs.

Verification steps: markdown-only review; no tests.

Risks and boundary warnings: do not delete historical plans; recommend movement or labeling when stale.

### Subagent B: Public `favn` Package And DSL Docs

Objective: produce the first public package docs for the supported `:favn` dependency.

Docs to read first: `docs/DOCUMENTATION_GUIDE.md`, `docs/DOCUMENTATION_PLAN.md`.

May inspect: `apps/favn/mix.exs`, `apps/favn/lib/**`, `docs/structure/favn.md`, `docs/structure/favn_authoring.md`, selected public DSL modules in `apps/favn_authoring/lib/favn/*.ex` if necessary, selected stable manifest structs in `apps/favn_core/lib/favn/manifest*.ex` if necessary.

Should not inspect: `apps/favn_view/**`, `apps/favn_orchestrator/**`, `apps/favn_runner/**`, storage adapters, except when a public guide explicitly needs a short adapter example.

Output files: `apps/favn/README.md`, `apps/favn/guides/getting-started.md`, `apps/favn/guides/authoring-assets.md`.

Expected documentation type: README entrypoint, tutorial, how-to guide.

Questions to answer: what should a new user install, what is the smallest asset example, how does public authoring become a manifest, what remains runtime-owned.

Verification steps: check links and examples for module/function names; run `mix format` only if code snippets are added to `.ex` files; do not run broad tests.

Risks and boundary warnings: do not expose orchestrator, runner, storage, or view internals as user-facing APIs.

### Subagent C: Manifest And Core Contract Docs

Objective: explain manifest-first concepts and stable shared contract shapes.

Docs to read first: `docs/DOCUMENTATION_GUIDE.md`, `docs/DOCUMENTATION_PLAN.md`.

May inspect: `apps/favn_core/mix.exs`, `apps/favn_core/lib/**`, `docs/structure/favn_core.md`, `apps/favn/lib/**` for public manifest production.

Should not inspect: UI code, adapter internals, unrelated runner implementation details.

Output files: `apps/favn/guides/manifest-first.md`, `docs/architecture/manifest-first.md`, small moduledoc or typedoc improvements only for stable structs if needed.

Expected documentation type: explanation.

Questions to answer: why compile-time output becomes runtime input, who produces manifests, who persists them, who consumes pinned work, and what data shapes should be shown.

Verification steps: markdown link review; focused doctests only if module docs are changed.

Risks and boundary warnings: `favn_core` owns shared contracts, not generic convenience or runtime ownership.

### Subagent D: Runtime/Orchestrator Docs

Objective: document control-plane runtime semantics at the right abstraction level.

Docs to read first: `docs/DOCUMENTATION_GUIDE.md`, `docs/DOCUMENTATION_PLAN.md`.

May inspect: `apps/favn_orchestrator/mix.exs`, `apps/favn_orchestrator/lib/**`, `docs/structure/favn_orchestrator.md`, `apps/favn_runner/lib/**` only for orchestrator/runner boundary contracts, storage adapter behaviours only where orchestrator calls them.

Should not inspect: `apps/favn_view/**` except public facade usage, public DSL implementation except manifest input assumptions.

Output files: `apps/favn/guides/runtime-model.md`, `docs/architecture/runtime-model.md`, `docs/operators/runs-and-schedules.md` if scope allows.

Expected documentation type: explanation, how-to guide for operator tasks.

Questions to answer: what owns persisted truth, how manifest versions are registered and activated, how runs/schedules/backfills/cancellation behave, and what can fail.

Verification steps: markdown review; focused orchestrator docs generation only if ExDoc is configured and module docs are changed.

Risks and boundary warnings: the orchestrator owns persisted truth, schedules, runs, admission, and control-plane state.

### Subagent E: Runner Docs

Objective: document runner execution responsibility and orchestrator/runner boundary.

Docs to read first: `docs/DOCUMENTATION_GUIDE.md`, `docs/DOCUMENTATION_PLAN.md`.

May inspect: `apps/favn_runner/mix.exs`, `apps/favn_runner/lib/**`, `docs/structure/favn_runner.md`, selected orchestrator boundary modules where runner contracts are called, core manifest structs needed for runner work input.

Should not inspect: UI, storage adapters except execution output contracts, scheduling internals.

Output files: `docs/architecture/runner-boundary.md` and module docs for stable runner contracts only if clearly stable.

Expected documentation type: explanation.

Questions to answer: what pinned work is, what runner owns, how success/failure/cancel/timeouts return, and what runner explicitly does not own.

Verification steps: markdown review; focused runner doctests only if module docs change.

Risks and boundary warnings: runner executes pinned work; it does not own schedules, operator state, or persisted control-plane truth.

### Subagent F: Adapter And Storage Docs

Objective: document adapter/plugin responsibilities and storage contract expectations.

Docs to read first: `docs/DOCUMENTATION_GUIDE.md`, `docs/DOCUMENTATION_PLAN.md`.

May inspect: relevant `apps/favn_storage_*`, `apps/favn_duckdb*`, `apps/favn_sql_runtime`, `docs/structure/*storage*.md`, `docs/structure/*duckdb*.md`, `docs/structure/favn_sql_runtime.md`.

Should not inspect: UI code, unrelated public DSL internals, unrelated runner lifecycle code.

Output files: `apps/favn/guides/adapters.md`, `docs/adapters/storage-adapters.md`, `docs/adapters/duckdb.md` if scope allows.

Expected documentation type: how-to guide and reference.

Questions to answer: which packages are optional plugins, what configuration belongs in public guides, what storage adapter contracts are internal, and what external failure modes matter.

Verification steps: markdown review; no adapter tests unless code or executable examples change.

Risks and boundary warnings: adapters own external integration details, not product scheduling policy, UI state, or control-plane lifecycle semantics.

### Subagent G: UI/Operator Docs

Objective: document operator-facing UI behavior without moving backend semantics into `favn_view`.

Docs to read first: `docs/DOCUMENTATION_GUIDE.md`, `docs/DOCUMENTATION_PLAN.md`.

May inspect: `apps/favn_view/mix.exs`, `apps/favn_view/lib/**`, `docs/structure/favn_view.md`, public orchestrator facades used by pages, Storybook files, focused Playwright tests if present.

Should not inspect: storage adapter internals, runner internals, scheduler internals except through public orchestrator facade docs.

Output files: `docs/operators/ui-overview.md`, page/component docs if needed, Storybook documentation improvements if relevant.

Expected documentation type: explanation.

Questions to answer: what each documented page renders, which orchestrator facade it uses, what UI states exist, and which commands it sends.

Verification steps: Storybook or focused Playwright only for changed documented UI flows; otherwise markdown review.

Risks and boundary warnings: `favn_view` renders and sends commands through orchestrator facades; it does not own runtime lifecycle, schedule calculation, or storage behavior.

### Subagent H: HexDocs/ExDoc Integration

Objective: maintain local `apps/favn` documentation generation without exposing
unstable internals or turning the umbrella into one public documentation surface.

Docs to read first: `docs/DOCUMENTATION_GUIDE.md`, `docs/DOCUMENTATION_PLAN.md`.

May inspect: root `mix.exs`, `apps/favn/mix.exs`, `apps/favn/README.md`, `apps/favn/guides/**`, selected app `mix.exs` files only for dependency/publishing constraints.

Should not inspect: app source code except public module docs that will appear in `apps/favn` docs.

Output files: `apps/favn/mix.exs` docs configuration, possible
`apps/favn/guides/cheatsheet.cheatmd`, notes in this plan.

Expected documentation type: reference configuration.

Questions to answer: are ExDoc deps/config present, which extras are included in
local `apps/favn` docs, which modules should be hidden, and what command should
verify docs.

Verification steps: run `cd apps/favn && mix docs --warnings-as-errors` only if ExDoc is configured and dependencies are available.

Risks and boundary warnings: do not publish HexDocs yet, do not configure public
HexDocs for internal apps, and do not add cross-package HexDocs links until
package boundaries are frozen.

## Dependency Order

1. Documentation inventory and public/internal classification.
2. Public `favn` package docs.
3. Manifest/core docs.
4. Runtime model docs.
5. Local development/tutorial docs.
6. Adapter docs.
7. UI/operator docs.
8. HexDocs integration.
9. Final consistency pass.

The first implementation pass should complete items 1 through 5 and create a small adapter overview. Full adapter reference, UI docs, runner-boundary docs, operator procedures, and HexDocs integration can follow once the first public guide set is coherent.

## Documentation Inventory

Initial classification based on current docs:

| Path | Action | Reason |
| --- | --- | --- |
| `README.md` | Update later | It is a broad entrypoint with useful status and quickstart content, but it is too large to be the only public path. Keep until `apps/favn` docs absorb stable user guidance. |
| `docs/DOCUMENTATION_GUIDE.md` | Keep | Current source of truth for Favn documentation standards. |
| `docs/DOCUMENTATION_PLAN.md` | Keep and update during this initiative | Coordination source for subagents. |
| `docs/FEATURES.md` | Keep | Implemented feature inventory. It should not become tutorial content. |
| `docs/ROADMAP.md` | Keep | Future work only. Keep separate from implemented behavior. |
| `docs/structure/*.md` | Keep and update | App/path ownership maps for contributors and agents. |
| `docs/production/*.md` | Keep | Production/operator contracts, runbooks, acceptance matrix, and SSE contract docs. Cross-link rather than move now. |
| `docs/report/*.md` | Keep as internal review reports | Mark as reports by location; do not present as public user docs. |
| `docs/refactor/*.md` | Keep as historical/planning docs | Do not delete casually; use for migration context only. |
| `docs/archive/README.md` | Keep | Archive index explaining that archived docs are historical references, not current public docs. |
| `docs/archive/ai-planning/README.md` | Keep | Index for archived AI-agent planning and implementation references. |
| `docs/archive/ai-planning/ISSUE_*.md`, `docs/archive/ai-planning/*_PLAN.md`, `docs/archive/ai-planning/*_PR.md` | Archive | Historical issue, implementation, and PR plans moved out of top-level `docs/`. Do not link from public docs unless historical context is explicitly needed. |
| `docs/archive/ai-planning/ASSET_SQL_PLAN.md`, `docs/archive/ai-planning/SQL_ADAPTER_ARCHITECTURE.md`, `docs/archive/ai-planning/CONNECTION_FOUNDATION_ARCHITECTURE.md`, `docs/archive/ai-planning/sql_adapter_scope.md` | Archive | Historical SQL/adapter background preserved for later merge review. Current adapter docs should live under `docs/adapters/`. |
| `docs/archive/ai-planning/*_ARCHITECTURE.md`, `docs/archive/ai-planning/*_TASKLIST.md`, `docs/archive/ai-planning/REFACTOR.md`, `docs/archive/ai-planning/refactor_review_standard.md` | Archive | Historical architecture, tasklist, and refactor planning references moved out of top-level `docs/`. |
| `docs/images/*` | Keep | Root README assets. |
| `docs/structure/favn_azure.md` | Keep | Structure map added for Azure token acquisition helper ownership based on allowed `favn_azure` files. |
| `apps/favn/README.md` | Create | Needed as public package entrypoint and HexDocs landing page. |
| `apps/favn/guides/**` | Create small first set | Needed to split tutorials, how-tos, and explanations. |
| `apps/favn/mix.exs` docs config | Keep | Local ExDoc generation is wired for `apps/favn` only with README and app-local guides as extras. Hex publishing remains deferred. |

Safest action for stale docs: keep archived, add links or labels from newer docs, and only delete when a maintainer confirms the historical plan is superseded and no longer useful.

## Verification Plan

Do not run the full umbrella test suite for documentation-only work.

Allowed focused checks:

- Markdown review for changed docs.
- `mix format` when Elixir code, Mix files, or doctest-bearing code examples in source files are changed.
- `cd apps/favn && mix docs --warnings-as-errors` for local `apps/favn` documentation generation.
- Focused doctests for public examples if doctests are added or changed.
- App-scoped compile checks only when module docs or ExDoc config change.
- Storybook checks only for UI documentation that changes stories.
- Focused Playwright checks only for documented UI flows that were changed.

Explicitly forbidden unless separately justified and approved:

- `mix test` from the umbrella root.
- Broad unrelated app test runs.
- Slow acceptance suites.
- Browser tests for markdown-only changes.

For the first documentation-only pass, expected verification is:

- Review changed Markdown for links, headings, and boundary language.
- Run `mix format --check-formatted` only if no code changes were made and the command is cheap in the current environment; otherwise state why skipped.
- Run `mix docs --warnings-as-errors` from `apps/favn` when public package docs,
  guide extras, or public module docs change.

## Open Questions

- Should `apps/favn` publish HexDocs later, after local generation has stabilized
  and the package boundary is frozen?
- Which plugin packages should get their own public HexDocs after `favn`: `favn_duckdb`, `favn_duckdb_adbc`, both, or neither before `v1`?
- Should `docs/architecture/runner-boundary.md` be created in the first pass, or deferred until runner contracts are stable enough to document without exposing internals?
- Archive index labels are enough for now. Add per-file archive status labels only
  if people keep landing directly on archived files and misreading them as current.
- Should public guides use runnable examples from `examples/basic-workflow-tutorial` as the canonical tutorial, or keep examples inline and shorter for HexDocs?
