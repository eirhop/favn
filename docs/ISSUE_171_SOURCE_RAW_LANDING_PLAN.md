# Issue 171 Source-System Raw Landing Plan

Issue: <https://github.com/eirhop/favn/issues/171>

## Goal

Add a canonical, documented, and tested source-system raw landing pattern for Elixir assets that fetch records from an external source client, use resolved runtime config from `ctx.config`, write raw data into a SQL warehouse through `Favn.SQLClient`, and return structured run metadata for local run inspection.

This should be a dogfooding pattern, not a connector framework.

## Current State

- `Favn.Asset` already supports `source_config/2`, `env!/1`, and `secret_env!/1`; the runner resolves those values into `ctx.config` before asset execution.
- `Favn.SQLClient` is the public SQL client facade for connect/query/execute/transaction and `with_connection/3`.
- Runner metadata already supports `:ok`, `{:ok, map()}`, and `{:error, reason}` asset return shapes, stores returned maps on `Favn.Run.AssetResult.meta`, and redacts declared secret runtime config from returned metadata/errors.
- `examples/basic-workflow-tutorial` already has a standalone consumer-style graph with fake API raw assets, DuckDB JSON loading, staging/gold SQL transforms, manifest tests, and DuckDB execution tests.
- `mix favn.init --duckdb --sample` currently generates a minimal inline raw orders asset plus one gold SQL asset, but the generated raw asset does not model an external source client or runtime source config.

## Architectural Decision

Use the existing public APIs instead of adding a new DSL surface.

- Keep runtime config declaration on `Favn.Asset.source_config/2`; do not add a new `@config` annotation unless implementation proves `source_config/2` cannot express the pattern.
- Keep source client modules inside examples/generated consumer code. Core Favn should not own arbitrary source-system client logic.
- Keep raw ingestion in Elixir assets because it integrates with external clients and runtime config.
- Keep business transformations in SQL assets because relation-level transformations are already a strength of `Favn.SQLAsset` and dependency inference.
- Keep warehouse writes through `Favn.SQLClient`; do not call DuckDB adapter internals from example assets.
- Return explicit structured metadata from raw assets so existing orchestrator/web run inspection can display it without a new backend contract.

## Canonical Pattern

The preferred asset shape should look like this in docs and examples:

```elixir
defmodule MyApp.Warehouse.Raw.SourceItems do
  use Favn.Namespace
  use Favn.Asset

  alias MyApp.SourceClient
  alias MyApp.Warehouse.RawLanding

  source_config :source_system,
    segment_id: env!("SOURCE_SYSTEM_SEGMENT_ID"),
    token: secret_env!("SOURCE_SYSTEM_TOKEN")

  @meta owner: "data-platform", category: :source_system, tags: [:raw]
  @relation true
  def asset(ctx) do
    with {:ok, rows} <- SourceClient.fetch_all(ctx.config.source_system),
         :ok <- RawLanding.replace_json_rows(ctx.asset.relation, rows) do
      {:ok,
       %{
         rows_written: length(rows),
         mode: :full_refresh,
         relation: relation_name(ctx.asset.relation),
         source: %{
           system: :source_system,
           segment_id_hash: hash_identity(ctx.config.source_system.segment_id)
         },
         loaded_at: DateTime.utc_now()
       }}
    end
  end
end
```

Implementation may adjust helper names, but these properties should remain stable:

- `ctx.config.source_system` is the only place the asset reads source runtime config.
- The source client receives a narrow config struct/map, not the full runner context.
- The SQL loader receives `ctx.asset.relation` and rows, not the full runner context.
- Metadata contains row count, mode, relation, loaded timestamp, and a hashed/redacted source identity.
- The source token never appears in returned metadata, errors, logs, docs examples, or tests.

## Metadata Contract

Use a boring map shape that serializes cleanly through existing run storage and UI payloads:

```elixir
%{
  rows_written: non_neg_integer(),
  mode: :full_refresh,
  relation: "raw.source_items",
  loaded_at: DateTime.t(),
  source: %{
    system: :source_system,
    segment_id_hash: binary()
  }
}
```

Rules:

- Do not include raw segment IDs, tokens, URLs with credentials, request headers, or full source config.
- Hash source identities with SHA-256 and encode as lowercase hex.
- Prefer stable field names over UI-specific names.
- Keep `loaded_at` as a `DateTime` in asset code; verify current run serialization preserves or normalizes it safely.
- If storage/UI serialization rejects `DateTime`, normalize `loaded_at` to ISO 8601 in the example and document that choice.

## Implementation Scope

### 1. Upgrade the standalone tutorial pattern

Primary files:

- `examples/basic-workflow-tutorial/lib/favn_reference_workload/client/fake_api.ex`
- `examples/basic-workflow-tutorial/lib/favn_reference_workload/client/duckdb_json_loader.ex`
- `examples/basic-workflow-tutorial/lib/favn_reference_workload/warehouse/raw/*.ex`
- `examples/basic-workflow-tutorial/test/duckdb_execution_test.exs`
- `examples/basic-workflow-tutorial/README.md`
- `README.md`

Design:

- Extend the fake API client to accept a source config map for at least one canonical raw asset path.
- Add a deterministic segment/integration ID path that proves the value comes from `ctx.config`.
- Keep the fake API deterministic; the segment ID should select or annotate data, not call a network service.
- Update at least one raw asset, preferably `Raw.Orders`, to declare `source_config :source_system` and return structured metadata.
- Keep other raw assets either unchanged or migrate them only if it clarifies the tutorial without broad churn.
- Update the raw loader helper to return `{:ok, %{rows_written: count, relation: relation_name}}` or keep `:ok` and let the asset assemble metadata. Prefer asset-owned metadata assembly so the example is explicit.
- Preserve SQL assets as business transformations over raw tables.

### 2. Upgrade generated local sample only if it improves the first-run path

Primary files:

- `apps/favn_local/lib/favn/dev/init.ex`
- `apps/favn_local/test/dev_init_test.exs`
- `README.md`

Decision point:

- If issue 171 should affect the generated `mix favn.init --duckdb --sample` path, add a tiny generated `SourceClient` and `RawLanding` helper plus a raw asset that uses `source_config`.
- If the generated sample should stay minimal, leave `mix favn.init` unchanged and make the standalone tutorial the canonical detailed pattern.

Recommended direction:

- Keep generated sample minimal for now unless dogfooding specifically needs this path immediately from `mix favn.init`.
- Add a README pointer from generated sample docs to the standalone tutorial raw landing section.
- Revisit generation after the tutorial pattern settles.

### 3. Add focused runner-level metadata/failure coverage only if gaps remain

Primary files:

- `apps/favn_runner/test/worker_test.exs`

Existing coverage already proves:

- `ctx.config` resolution from runtime config refs.
- missing env failure diagnostics.
- secret redaction from returned metadata/errors/events.

Only add runner tests if implementation exposes a missing generic behavior, such as metadata `DateTime` handling or non-secret source identity redaction expectations not covered by the tutorial tests.

### 4. Add example-level acceptance tests

Primary files:

- `examples/basic-workflow-tutorial/test/duckdb_execution_test.exs`
- optional new `examples/basic-workflow-tutorial/test/raw_landing_test.exs`

Tests should cover:

- Successful full refresh writes expected raw rows.
- The raw asset returns metadata with `rows_written`, `mode: :full_refresh`, `relation`, `loaded_at`, and `source.segment_id_hash`.
- The metadata does not include the raw segment ID or token.
- Missing source segment env fails before any source-client call when run through the runner path.
- A source-client failure returns a structured `{:error, reason}` that the runner reports as an asset failure.
- SQL transforms still produce at least one gold table from the raw table.

Prefer tests that run through `FavnRunner.run/2` with a pinned manifest for runtime config behavior. Direct `apply(asset.module, :asset, [ctx])` tests are acceptable only for narrow helper behavior because they bypass runner config resolution and redaction.

### 5. Update product documentation

Primary files:

- `README.md`
- `docs/FEATURES.md` after implementation lands
- `docs/ROADMAP.md` while work remains planned
- `examples/basic-workflow-tutorial/README.md`

Documentation should explain:

- Use `source_config/2` for source IDs/tokens and read them through `ctx.config`.
- Keep source client logic outside the asset.
- Use `Favn.SQLClient` to land raw data into the owned relation.
- Return structured metadata for run inspection.
- Full refresh is the first supported dogfooding path.
- Windowed refresh/backfill is a compatible later extension, not required for this issue.

When implementation lands:

- Move the implemented behavior into `docs/FEATURES.md`.
- Remove or downgrade the issue-specific roadmap item.
- Keep `docs/ROADMAP.md` focused on remaining window/backfill follow-up work.

## Work Breakdown

- [x] Confirm whether generated `mix favn.init --duckdb --sample` should include the raw landing pattern in the first implementation or only link to the tutorial.
- [x] Update the tutorial fake source client to accept narrow runtime source config and provide deterministic full-refresh rows.
- [x] Add a small raw landing helper if needed to centralize SQL JSON write details without hiding the pattern from readers.
- [x] Update one canonical raw asset to use `source_config/2`, `ctx.config`, `Favn.SQLClient`, and structured metadata.
- [x] Keep SQL transformation assets downstream of the raw relation and verify dependency inference remains intact.
- [x] Add example tests for full refresh, metadata shape, redaction/no-leak behavior, failure diagnostics, and downstream gold output.
- [x] Optionally update generated sample scaffolding and `apps/favn_local/test/dev_init_test.exs` if the generated path is in scope.
- [x] Update README and tutorial docs with the recommended pattern.
- [x] Update `docs/FEATURES.md` and `docs/ROADMAP.md` after implementation status changes.
- [x] Run required Elixir verification after code changes: `mix format`, `mix compile --warnings-as-errors`, `mix test`, `mix credo --strict`, `mix dialyzer`, and `mix xref graph --format stats --label compile-connected`.
- [x] Run tutorial verification from `examples/basic-workflow-tutorial`: `mix deps.get`, `mix compile --warnings-as-errors`, and `mix test`.

## Acceptance Mapping

- Clear example/test project showing source-system raw landing: standalone tutorial raw asset and README section.
- Uses `ctx.config`: raw asset declares `source_config` and reads only resolved config from context.
- Writes raw data with `Favn.SQLClient`: raw landing helper or asset uses public SQL client only.
- Returns structured metadata: runner-path test asserts the canonical metadata shape.
- SQL assets transform raw table into gold table: existing gold SQL assets continue to run from raw relations.
- Works through local consumer project: tutorial remains a standalone Mix project and the local tooling section stays valid.
- Tests cover full refresh, metadata, and failure diagnostics: example test suite plus existing runner tests, with additions where needed.

## Non-Goals

- Do not build a connector framework.
- Do not add pagination, auth provider abstraction, cursor sync, or backfill orchestration.
- Do not introduce DuckDB-specific APIs into public Favn asset examples beyond SQL text that runs through `Favn.SQLClient`.
- Do not require web UI changes unless current run metadata display cannot show the returned map at all.

## Risks And Mitigations

- Risk: examples become too large for a beginner tutorial. Mitigation: make only one raw asset the canonical source-config example and keep the rest as simple deterministic raw loads.
- Risk: generated sample becomes noisy. Mitigation: keep `mix favn.init` minimal unless explicitly needed for dogfooding.
- Risk: metadata contains secrets by accident. Mitigation: assert no raw token or segment ID appears in returned runner result inspection.
- Risk: direct tutorial execution bypasses runner behavior. Mitigation: add at least one pinned-manifest runner-path test for the canonical raw asset.
- Risk: `DateTime` metadata serialization differs between direct tests and orchestrator storage. Mitigation: test through the runner and inspect stored/projected run metadata if needed.

## Decision

The first implementation keeps `mix favn.init --duckdb --sample` minimal and makes the richer standalone tutorial the canonical source-system landing example. Generated scaffolding can adopt this pattern later after the tutorial shape settles.
