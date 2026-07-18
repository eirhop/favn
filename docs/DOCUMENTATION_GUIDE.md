# Favn Documentation Guide

This guide tells AI agents and human contributors how to write high-quality documentation for Favn.

It is intentionally self-contained. Agents should be able to read this document and write useful documentation without first reading external documentation systems such as Diataxis, ExDoc, or HexDocs. External references may still be useful for deeper learning, but they are not required to apply this guide.

## Purpose

Favn documentation exists to help readers understand and safely use a manifest-first orchestration product.

Good Favn documentation should answer:

1. What is the reader trying to do?
2. Which Favn boundary are they interacting with?
3. Is this compile-time, runtime, operator-facing, adapter-facing, or UI-facing?
4. What input does the reader provide?
5. What output or behavior should they expect?
6. What owns the runtime behavior?
7. What can fail, and what should the caller or operator do?

Do not document code merely because it exists. Document public contracts, product concepts, runtime behavior, and contributor decisions that future agents need to preserve.

## Favn Documentation Doctrine

Favn is a manifest-first product with strict monorepo boundaries. Documentation must reinforce that architecture.

Core rules:

- Document boundaries before internals.
- Document contracts before implementation details.
- Keep public docs focused on supported public behavior.
- Keep internal docs explicit about ownership and migration status.
- Never make internal modules look like stable public API by accident.
- Prefer small, concrete examples over abstract descriptions.
- Explain data flow: input -> transformation -> output -> owner.
- State whether behavior happens at compile time or runtime.
- State failure shapes for public APIs and operational flows.
- Keep documentation close enough to code that agents can update both together.

## Favn App Boundaries In Documentation

When documenting any module, guide, feature, or architectural decision, name the owning app.

### `favn`

`favn` is the public user-facing dependency and DSL surface.

Document here:

- Public DSL modules and macros.
- User authoring workflows.
- Compile-time behavior that produces manifests.
- Stable user-facing examples.
- How user code should depend on Favn.

Avoid here:

- Orchestrator internals.
- Runner process details.
- Storage adapter implementation details.
- View/UI internals.
- Unstable migration scaffolding.

### `favn_core`

`favn_core` owns shared compiler, domain, manifest, and contract foundations.

Document here:

- Manifest structs and schemas.
- Shared domain contracts.
- Compiler diagnostics and validation semantics.
- Cross-app contract shapes that are intentionally shared.

Avoid here:

- Runtime process ownership.
- Scheduling behavior.
- Storage persistence implementation.
- UI-specific DTOs.
- Generic helper modules with unclear ownership.

### `favn_runner`

`favn_runner` owns compute execution runtime for user asset work.

Document here:

- Runner execution contracts.
- Work input/output shapes.
- Execution lifecycle from accepted work to result.
- Cancellation, timeout, retry, cleanup, and crash behavior when relevant.

Avoid here:

- Schedule ownership.
- Persisted control-plane truth.
- UI read models.
- Storage adapter internals except through explicit contracts.

### `favn_orchestrator`

`favn_orchestrator` owns the control plane.

Document here:

- Manifest registration and persisted manifest versions.
- Runs, schedules, leases, admission, and control-plane state.
- Operator-facing facades and DTOs.
- Storage-facing orchestration contracts.
- Runtime lifecycle semantics owned by the control plane.

Avoid here:

- User DSL details except as inputs to manifest registration.
- Runner implementation internals.
- LiveView component behavior.
- Direct plugin implementation details.

### `favn_view`

`favn_view` owns the UI/API boundary.

Document here:

- Page/component behavior.
- User interaction flows.
- UI state and form validation.
- Which orchestrator facade a page uses.
- Storybook and Playwright verification expectations.

Avoid here:

- Storage calls.
- Runner internals.
- Scheduler internals.
- Lifecycle semantics that belong behind orchestrator facades.

### Plugins and adapters

Plugins and adapters own external integrations.

Document here:

- Adapter configuration.
- Adapter callbacks and supported capabilities.
- External system assumptions.
- Failure modes specific to the external system.
- Compatibility and performance notes.

Avoid here:

- UI behavior.
- Product-level scheduling policy.
- Generic orchestration semantics that belong in `favn_orchestrator`.

## Documentation Types

Use four documentation types. Do not mix them unless there is a clear reason.

### 1. Tutorial

A tutorial teaches by walking the reader through a safe first success.

Use a tutorial when the reader is new and needs a guided path.

Tutorial rules:

- Optimize for first success, not completeness.
- Use one concrete scenario.
- Avoid deep architecture unless needed to avoid confusion.
- Include exact code and expected output.
- Keep choices minimal.
- End with the next useful step.

Good Favn tutorials:

- Build your first Favn pipeline.
- Define and compile your first manifest.
- Run a local DuckDB-backed asset flow.

Tutorial template:

```markdown
# Build Your First Favn Pipeline

## Goal

State what the reader will accomplish.

## Prerequisites

List only what is required.

## Step 1: Create the authoring module

Show code.

## Step 2: Compile the manifest

Show command or code.

## Step 3: Inspect the result

Show expected output or shape.

## What happened

Explain the data flow briefly.

## Common problems

List common errors and fixes.

## Next step

Link to the next guide.
```

### 2. How-to guide

A how-to guide solves one concrete task for a reader who already has some context.

Use a how-to guide when the reader asks, "How do I do X?"

How-to rules:

- Name the task in the title.
- State assumptions up front.
- Give the shortest safe path.
- Include tradeoffs only when they change the decision.
- Link to reference docs for details.

Good Favn how-to guides:

- Add a DuckDB-backed asset.
- Register a manifest version.
- Add an orchestrator facade for a new operator page.
- Add a storage adapter callback.

How-to template:

```markdown
# How To Add A DuckDB-Backed Asset

## When to use this

Describe the scenario.

## Assumptions

List required context.

## Steps

1. Do the first action.
2. Do the second action.
3. Verify the result.

## Expected result

Show the resulting code, manifest shape, UI state, or storage state.

## Failure modes

Explain likely failures and fixes.

## Related reference

Link to API or contract reference.
```

### 3. Reference

Reference documentation is precise, complete, and neutral.

Use reference docs for APIs, structs, callbacks, options, commands, config, lifecycle states, and storage contracts.

Reference rules:

- Do not teach from scratch.
- Do not hide edge cases.
- Include types, accepted values, return shapes, and failure shapes.
- Include examples only to clarify exact usage.
- Be exhaustive where the contract is stable.

Good Favn reference docs:

- `Favn.asset/2` options.
- Manifest struct fields.
- Runner work result contract.
- Orchestrator command DTOs.
- Storage adapter callback behavior.

Reference template:

```markdown
# Manifest Asset Reference

## Purpose

One paragraph defining the contract.

## Shape

Show the struct or map.

## Fields

| Field | Type | Required | Description |
| --- | --- | --- | --- |
| `key` | `String.t()` | yes | Stable asset key. |

## Valid states

List valid states or enum values.

## Return values

Show exact return tuples or structs.

## Failure modes

Show exact error shapes.

## Examples

Give minimal examples.
```

### 4. Explanation

Explanation documentation builds understanding. It answers "why".

Use explanation docs for architecture, tradeoffs, runtime semantics, and product direction.

Explanation rules:

- Explain the problem and the chosen design.
- Name alternatives and tradeoffs when relevant.
- Clarify ownership and boundaries.
- Do not turn it into a step-by-step guide.

Good Favn explanation docs:

- Why Favn is manifest-first.
- Why `favn_view` only calls orchestrator facades.
- Why persisted manifest versions are runtime truth.
- Why runner and orchestrator are separate apps.

Explanation template:

```markdown
# Why Favn Is Manifest-First

## Problem

Describe the design pressure.

## Decision

State the chosen model.

## How it works

Explain the flow.

## Tradeoffs

Explain what this improves and what it costs.

## Consequences for contributors

List rules contributors must preserve.
```

## Choosing The Right Documentation Type

Use this decision table:

| Reader need | Use |
| --- | --- |
| "Teach me from zero" | Tutorial |
| "Help me do this task" | How-to guide |
| "Tell me exactly what this API accepts/returns" | Reference |
| "Explain why the system is designed this way" | Explanation |
| "Help an AI agent preserve architecture" | Explanation plus reference checklist |
| "Help a user copy a pattern" | How-to with a runnable example |

If one document starts doing all four jobs, split it.

## Where Documentation Belongs

Use this placement rule:

- User-facing public package docs belong near `apps/favn` and in HexDocs extras.
- Architecture and contributor guidance belongs in top-level `docs/`.
- App-specific architecture belongs in `docs/structure/` or app-specific guides.
- Migration plans belong in `docs/refactor/` or issue-specific docs.
- UI component behavior belongs near `apps/favn_view` stories and related docs.
- API reference belongs in module docs and typespecs, not only Markdown.

Do not create new documentation locations casually. A predictable docs tree is more valuable than a perfect taxonomy.

## Module Documentation Rules

Every public module should have a `@moduledoc` unless it is intentionally hidden with `@moduledoc false`.

A public `@moduledoc` should include:

1. One-sentence purpose.
2. Ownership boundary.
3. Compile-time or runtime role.
4. Main input/output shape.
5. Important failure or lifecycle behavior.
6. Link to related public contracts.

Good example:

```elixir
defmodule Favn do
  @moduledoc """
  Public DSL for defining Favn assets and compiling authoring code into manifests.

  This module is used by user application code at compile time. Declarations made
  through this DSL contribute metadata to a manifest. They do not execute asset
  code, submit runs, mutate orchestrator state, or access storage adapters.

  Runtime execution is owned by `Favn.Orchestrator` and `Favn.Runner` contracts
  through persisted manifest versions.
  """
end
```

Weak example:

```elixir
defmodule Favn do
  @moduledoc """
  Helpers for Favn.
  """
end
```

Why the weak example is bad:

- It does not name ownership.
- It does not say whether the module is public.
- It does not explain compile-time versus runtime behavior.
- "Helpers" usually hides unclear design.

## Function Documentation Rules

Every public function, macro, callback, or facade should have `@doc` and `@spec` unless there is a deliberate reason not to.

A good function doc includes:

1. What it does.
2. Who should call it.
3. Input expectations.
4. Return shape.
5. Failure shape.
6. Side effects, if any.
7. Timeout, persistence, process, or storage behavior if relevant.
8. One minimal example for non-trivial behavior.

Template:

```elixir
@doc """
Registers a manifest version with the orchestrator.

This is an orchestrator-owned control-plane operation. It persists the manifest
before the manifest can be used for scheduling or run submission.

Returns `{:ok, version}` when the manifest is accepted, or `{:error, reason}`
when validation or persistence fails.

## Examples

    {:ok, version} = Favn.Orchestrator.register_manifest(manifest)

"""
@spec register_manifest(FavnCore.Manifest.t()) ::
        {:ok, Favn.Orchestrator.ManifestVersion.t()} | {:error, term()}
def register_manifest(manifest)
```

Avoid vague docs:

```elixir
@doc """Registers a manifest."""
```

That is not enough for a boundary function.

## Struct And Type Documentation Rules

Structs and types are contracts. Document them carefully.

For structs:

- Explain who owns the struct.
- Explain whether it is public, boundary-internal, or purely internal.
- Document each field if the struct crosses app boundaries.
- State whether callers may construct it directly or must use a constructor/validator.

Example:

```elixir
defmodule FavnCore.Manifest.Asset do
  @moduledoc """
  Manifest representation of one asset declared by user authoring code.

  This struct is compile-time output and runtime input. Authoring code produces
  it through the public DSL, the orchestrator persists it as part of a manifest
  version, and the runner consumes pinned asset work derived from it.

  Callers should not build this struct by hand unless they are inside compiler
  or manifest validation code. Prefer public DSL and validation functions.
  """

  @typedoc """
  A validated asset entry in a manifest.

  Fields:

  - `key` - stable asset key used in dependencies, storage, and operator views.
  - `deps` - upstream asset keys required before this asset can run.
  - `materialization` - how the asset output is persisted or exposed.
  """
  @type t :: %__MODULE__{
          key: String.t(),
          deps: [String.t()],
          materialization: atom()
        }
end
```

## Boundary Function Documentation

Boundary functions need stronger docs than private helpers.

A boundary function is any function that crosses one of these lines:

- user code -> `favn`
- `favn` -> `favn_core`
- `favn_view` -> `favn_orchestrator`
- `favn_orchestrator` -> `favn_runner`
- `favn_orchestrator` -> storage adapter
- `favn_runner` -> execution adapter
- plugin -> external system

Boundary docs must state:

- Caller.
- Owner.
- Input DTO/struct.
- Output DTO/struct.
- Error shape.
- Timeout or boundedness semantics where relevant.
- Persistence semantics where relevant.
- Whether the function is stable public API or internal contract.

Example:

```elixir
@doc """
Returns the operator-facing run detail projection for one run.

This is the public read facade used by `favn_view`. It is intentionally bounded:
it returns one run, summary fields, and a limited event/log projection. UI code
must not reconstruct this page by calling storage, runner, or scheduler internals.

Returns:

- `{:ok, detail}` when the run exists.
- `{:error, :not_found}` when no run exists for the ID.
- `{:error, reason}` for storage or projection failures.
"""
```

## Compile-Time Versus Runtime Documentation

Favn has a compile-time authoring surface and runtime orchestration/execution system. Docs must not blur this.

Use this language:

- "At compile time, the DSL records declarations and produces manifest data."
- "At runtime, the orchestrator operates on persisted manifest versions."
- "The runner executes pinned work derived from a manifest."
- "The view renders operator state exposed by orchestrator facades."

Avoid this language:

- "The asset runs when declared."
- "The UI loads runner state directly."
- "The manifest is generated dynamically from loaded modules at runtime."
- "The storage adapter decides scheduling behavior."

## Runtime Lifecycle Documentation

For runtime behavior, always document more than the success path.

When relevant, cover:

- success
- validation failure
- persistence failure
- timeout
- cancellation
- retry
- crash/restart
- duplicate messages
- cleanup
- partial work already submitted
- storage conflict
- external adapter error

Lifecycle documentation should name the owner.

Example:

```markdown
## Run cancellation lifecycle

Cancellation is owned by the orchestrator. The orchestrator records cancellation
intent before notifying runner-owned work. Runner cleanup is idempotent: repeated
cancel messages must not produce duplicate terminal results.

Flow:

1. Operator requests cancellation through an orchestrator facade.
2. Orchestrator persists cancellation intent.
3. Orchestrator notifies the runner owner for in-flight work.
4. Runner attempts cooperative cleanup.
5. Orchestrator records the terminal run state.
```

## Data Flow Documentation

Prefer data-flow explanations over vague architecture prose.

Good:

```text
User module
  -> Favn DSL declarations
  -> FavnCore.Manifest
  -> persisted manifest version in orchestrator
  -> pinned runner work
  -> run result
  -> orchestrator read model
  -> favn_view operator page
```

Bad:

```text
The system processes assets and shows results in the UI.
```

Whenever possible, show small shapes:

```elixir
%FavnCore.Manifest{
  assets: [
    %FavnCore.Manifest.Asset{
      key: "github.repos",
      deps: [],
      materialization: :table
    }
  ]
}
```

Then explain who produces and consumes the shape.

## Examples Policy

Examples should be:

- Realistic.
- Minimal.
- Copyable.
- Deterministic where possible.
- Focused on one concept.
- Updated when APIs change.

Avoid examples that require hidden setup unless the guide explicitly walks through that setup.

When an example depends on external systems, say so clearly:

```markdown
This example assumes DuckDB is available locally and the `favn_duckdb` adapter is configured.
```

Prefer named examples over generic placeholders:

Good:

```elixir
asset :github_repositories do
  source :github_api
end
```

Weak:

```elixir
asset :foo do
  source :bar
end
```

Use generic names only when the concept itself is generic.

## Linking Policy

Use links to connect related Favn docs and APIs.

Preferred links:

- Link from guides to public modules/functions.
- Link from explanation docs to reference docs.
- Link from reference docs to examples.
- Link from architecture docs to structure docs.
- Link from UI docs to Storybook stories when available.

In Elixir docs, use backticked references so ExDoc can auto-link:

```elixir
See `Favn`, `Favn.asset/2`, `t:FavnCore.Manifest.t/0`, and
`c:FavnRunner.Executor.run/1`.
```

Do not paste raw URLs into prose when an internal relative link is better.

## External Links Policy

This document is self-contained. Agents should not need external sources for normal Favn documentation work.

Use external links only when:

- The documentation depends on external product behavior, such as DuckDB, PostgreSQL, Phoenix, Ecto, ExDoc, Hex, or Azure.
- The external behavior is version-sensitive.
- The reader must verify installation or compatibility.
- The guide would become misleading without source-of-truth external docs.

When using external links:

- Summarize the relevant rule in Favn docs.
- Link the external source as supporting detail, not as a replacement for explanation.
- Prefer official documentation over blog posts.
- Include version context when relevant.

Good:

```markdown
Favn uses ExDoc extras to include Markdown guides in generated HexDocs. Keep the
main explanation here; link to ExDoc only for option-level details.
```

Bad:

```markdown
Read the ExDoc docs to understand how this works.
```

Optional further-reading links may be added at the end of a document, but the main document should still stand alone.

## HexDocs And ExDoc Rules

For public package documentation:

- The public `favn` package should be the first HexDocs target.
- Do not expose unstable internal apps as public documentation by accident.
- Use `@moduledoc false` for modules that should not appear in generated docs.
- Use `@doc false` for public functions that exist for internal reasons and should not be promoted.
- Use `@typedoc` for public types.
- Include `@spec` for public functions.
- Use ExDoc extras for guides.
- Use module/function/type references in backticks for automatic links.
- Keep the first paragraph of module docs short and clear.

Suggested package docs layout for `apps/favn`:

```text
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

Suggested guide order:

1. README / overview.
2. Getting started tutorial.
3. Authoring assets how-to.
4. Manifest-first explanation.
5. Runtime model explanation.
6. Adapter how-to/reference.
7. DSL cheatsheet.

## README Rules

A README is not a full manual. It is an entry point.

A good Favn README should include:

1. What Favn is.
2. Who it is for.
3. The smallest useful example.
4. Installation or local setup.
5. Links to the main guides.
6. Project status if APIs are unstable.
7. Boundary warning if internal apps are not public API.

README anti-patterns:

- Full architecture essay.
- Complete API reference.
- Long migration history.
- Unmaintained roadmap claims.
- Examples that do not run.

## Architecture Documentation Rules

Architecture docs should preserve decisions, not merely describe current files.

Include:

- Problem.
- Decision.
- Boundaries.
- Data flow.
- Runtime ownership.
- Tradeoffs.
- Consequences for contributors.
- Migration notes if relevant.

Architecture docs should explicitly reject unsafe shortcuts.

Example:

```markdown
`favn_view` must not call storage adapters directly. If a page needs additional
state, add or extend an orchestrator-owned facade/read model instead.
```

## UI Documentation Rules

For `favn_view`, document user-facing behavior and component contracts.

Include:

- Page purpose.
- Inputs from orchestrator facades.
- UI states: loading, empty, success, error, stale, disabled.
- User actions and resulting commands.
- Storybook story coverage.
- Playwright verification scope when relevant.

Avoid documenting UI pages as if they own backend lifecycle behavior.

Good:

```markdown
The schedule overview page displays schedule projections returned by the
orchestrator. Enable/disable actions are sent through orchestrator command DTOs.
The page does not calculate next occurrences locally.
```

## Storage And Query Documentation Rules

Storage documentation must describe access patterns and boundedness.

For storage/read-model functions, document:

- Query purpose.
- Filters.
- Ordering.
- Limit/cursor semantics.
- Index expectations if relevant.
- Whether the result is authoritative or derived.
- Repair/rebuild behavior for derived projections.

Avoid vague docs like:

```markdown
Lists runs.
```

Prefer:

```markdown
Lists runs for the operator overview page ordered by most recent update. The
query is bounded by `limit` and uses cursor pagination for follow-up pages.
The result is a read projection; authoritative run state remains in the run
lifecycle store.
```

## Testing Documentation Rules

Documentation should mention verification when behavior is non-trivial.

Use:

- Doctests for deterministic public examples.
- Focused tests for documented boundary behavior.
- Storybook stories for UI component states.
- Playwright only for important user flows and pixel/interaction verification.

Do not tell agents to run umbrella-wide tests by default when focused tests are enough. Prefer focused tests that match the touched app and behavior.

When documenting a feature request or implementation plan, include:

```markdown
## Verification

- Run the focused test file for the changed module.
- Run doctests if public docs changed.
- Run the relevant Storybook story if UI changed.
- Run the relevant Playwright scenario if browser behavior changed.
```

## Writing Style

Use:

- Direct language.
- Short paragraphs.
- Concrete nouns.
- Active voice.
- Exact module/function names.
- Exact return shapes.
- Tables for field lists and options.
- Code blocks for examples.

Avoid:

- Marketing language.
- Vague adjectives like "easy", "simple", or "powerful" without evidence.
- Long paragraphs with multiple concepts.
- Repeating the same architecture principle in every section.
- Saying "just" when describing setup or debugging.
- Hiding uncertainty.

Preferred phrasing:

- "This function returns `{:ok, result}` or `{:error, reason}`."
- "The orchestrator owns persisted run state."
- "The runner executes pinned work; it does not discover schedules."
- "This guide assumes the local PostgreSQL control plane is migrated and ready."

Weak phrasing:

- "This probably works with the runner."
- "This helper does some processing."
- "Simply configure the adapter."
- "The UI talks to the backend."

## Good Versus Bad Documentation Examples

### Bad public API doc

```elixir
@doc """
Runs an asset.
"""
def run_asset(asset)
```

Problems:

- Does not say who calls it.
- Does not state whether this is orchestrator or runner behavior.
- Does not describe manifest versioning.
- Does not show return shape.
- Does not document failures.

### Good public API doc

```elixir
@doc """
Submits a run for an asset in a persisted manifest version.

This is an orchestrator-owned command. The asset must exist in the selected
manifest version before the run can be accepted. The function records run intent
before dispatching executable work to the runner.

Returns:

- `{:ok, run}` when the run is accepted.
- `{:error, :unknown_asset}` when the asset key is not present in the manifest.
- `{:error, :manifest_not_found}` when the manifest version does not exist.
- `{:error, reason}` when persistence or dispatch fails.
"""
@spec submit_asset_run(manifest_version_id(), String.t()) ::
        {:ok, Run.t()} | {:error, term()}
def submit_asset_run(manifest_version_id, asset_key)
```

### Bad guide intro

```markdown
# Assets

Assets are an important part of Favn. This page explains assets.
```

### Good guide intro

```markdown
# How To Define An Asset

This guide shows how to define one asset in user authoring code and compile it
into a manifest entry. It covers compile-time DSL behavior only; run submission
and execution are handled by the orchestrator and runner.
```

## Documentation Review Checklist

Before approving documentation, check:

- Is the reader and use case clear?
- Is the documentation type clear: tutorial, how-to, reference, or explanation?
- Is the owning app named when relevant?
- Are public and internal contracts separated?
- Is compile-time versus runtime behavior clear?
- Are inputs and outputs shown concretely?
- Are failure modes documented for public APIs and runtime behavior?
- Are examples minimal, realistic, and copyable?
- Are links useful and not a substitute for explanation?
- Are unstable internals hidden or clearly marked internal?
- Does the doc reinforce Favn boundaries instead of blurring them?
- Is the document likely to stay maintainable as the code changes?

## Common Anti-Patterns

Reject or revise docs that:

- Document internal modules as public API by accident.
- Explain implementation files instead of product contracts.
- Mix tutorial, how-to, reference, and explanation into one long page.
- Use vague terms like "manager", "helper", "processor", or "service" without ownership.
- Hide failure behavior.
- Only document the success path.
- Mention runtime behavior without naming the owner.
- Tell `favn_view` to call internals directly.
- Tell the runner to own schedules or persisted control-plane truth.
- Use examples that cannot run or omit required setup.
- Link to external docs instead of explaining the required Favn-specific rule.

## Default Workflow For AI Agents

When asked to write or update Favn documentation:

1. Identify the reader: user, operator, adapter author, contributor, or AI agent.
2. Identify the documentation type: tutorial, how-to, reference, or explanation.
3. Identify the owning app and boundary.
4. Inspect the relevant code or structure docs.
5. Write the smallest document that fully serves the reader's task.
6. Include concrete examples and data shapes.
7. State compile-time/runtime ownership where relevant.
8. Document failure modes for public APIs and runtime behavior.
9. Add internal links to related Favn docs or modules.
10. Keep external links optional unless external behavior is the source of truth.
11. Add or update verification notes if examples/tests/stories are affected.
12. Re-read the doc as a new contributor and remove ambiguity.

## Optional Further Reading

Agents do not need these sources to apply this guide, but humans may find them useful:

- Diataxis: documentation types and information architecture.
- Elixir documentation guide: `@moduledoc`, `@doc`, examples, and cross-linking conventions.
- ExDoc documentation: HexDocs generation, extras, groups, cheatsheets, and source links.
- Hex publishing docs: package and documentation publishing behavior.
- Phoenix guides: example of layered framework documentation.
- Ecto docs: example of precise API and callback reference documentation.
