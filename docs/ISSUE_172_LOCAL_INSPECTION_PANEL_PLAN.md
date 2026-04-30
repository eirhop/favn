# Issue 172 Local Inspection Panel Plan

> Status: implemented for the first curated local inspection slice.
> Scope: manifest-owned asset relation previews with run/manifest metadata, DuckDB row count/schema/sample support, runner-owned inspection dispatch, and web asset-detail preview UI.
> Richer DuckLake snapshot metadata, pagination, and any local-only SQL console remain future work.

Issue: <https://github.com/eirhop/favn/issues/172>

## Goal

Add a safe local inspection experience that lets a user verify landed data from a real local pipeline run without opening DuckDB CLI or using a general-purpose SQL editor.

The first implementation should answer the dogfooding questions after running a source-to-DuckLake or source-to-DuckDB pipeline:

- did the latest relevant run succeed or fail
- which asset relation was produced or updated
- how many rows were written or affected when metadata is available
- what columns exist on the produced relation
- what do a small number of sample rows look like
- what source runtime config keys were declared and whether non-secret values were present
- what window was used, if any
- what raw metadata, errors, and diagnostics were returned
- which upstream and downstream assets were involved in the run

This is a local verification feature, not a database IDE.

## Planning Baseline

This section records the baseline at the time the issue plan was written. The
first curated local inspection slice is now implemented; see the acceptance
mapping below for current status.

Implemented foundations already present:

- `Favn.Manifest.Asset` carries asset ref, type, relation, materialization, dependencies, runtime config declarations, SQL execution payload, and user metadata.
- `Favn.RelationRef` is the canonical relation identity with `connection`, `catalog`, `schema`, and `name`.
- `Favn.Run.AssetResult` carries per-asset `status`, timings, `meta`, `error`, attempts, and retry information.
- `Favn.Contracts.RunnerResult` carries `asset_results` plus run-level runner metadata.
- `FavnRunner.Worker` already records Elixir asset `{:ok, meta}` returns and SQL asset materialization metadata in per-asset results.
- SQL asset metadata currently includes fields such as `materialized`, `connection`, `rows_affected`, and `command`.
- `FavnOrchestrator.RunState.result` stores terminal `asset_results` and run metadata.
- `FavnOrchestrator.Projector.project_run/1` reconstructs `run.asset_results`, `run.node_results`, `run.metadata`, `run.result`, `run.pipeline`, and `run.pipeline_context` from stored run state.
- `Favn.SQL.Client` already supports `connect/2`, `query/3`, `execute/3`, `relation/2`, `columns/2`, `transaction/3`, and session admission control.
- `Favn.SQL.Adapter.DuckDB` already implements relation lookup, schema listing, relation listing, columns, and materialization.
- `favn_web` already has asset catalog/detail and run detail pages, BFF routes under `/api/web/v1/**`, server-side orchestrator client helpers, normalizers, Storybook stories, and E2E coverage.

Important gaps at planning time:

- The private orchestrator run detail DTO currently drops `run.result`, `run.metadata`, `asset_results`, `node_results`, `pipeline`, `pipeline_context`, `params`, and `trigger`.
- Active manifest target DTOs expose only asset target id and label for assets; they do not expose relation, type, metadata, dependencies, or runtime config declarations.
- There is no safe relation inspection contract for row count, sample rows, relation existence, or DuckLake-specific metadata.
- The SQL runtime has relation and column introspection but no generic sample or row-count helper.
- The DuckDB adapter has internal identifier quoting and query support, but no adapter-owned sampled relation API.
- The web UI can render some richer shapes defensively, but real backend payloads do not currently include enough landed-data information.

## Architectural Decision

Implement a curated local inspection path across the existing product boundaries.

- `favn_web` owns browser-facing UI, BFF endpoint shape, page loads, view-model normalization, Storybook, and browser tests.
- `favn_orchestrator` owns run, manifest, asset, relation identifier, and authorization metadata; it must not become a warehouse query engine.
- `favn_runner` owns live data-plane inspection commands because the runner owns consumer code, resolved connection modules/config, plugin loading, and the local SQL runtime context.
- `favn_sql_runtime` owns generic safe read-only inspection contracts and result structs.
- `favn_duckdb` owns DuckDB/DuckLake-specific SQL generation, identifier quoting, row count, sample rows, table metadata, and safe metadata redaction.
- `favn` remains the public authoring/tooling facade and should not gain broad UI or data-plane inspection behavior.

Do not add arbitrary SQL execution to the browser path in this issue.

## Inspection Contract V1

Represent relation inspection as a narrow read-only request rather than a SQL string.

```elixir
%Favn.Contracts.RelationInspectionRequest{
  manifest_version_id: String.t(),
  asset_ref: Favn.Ref.t() | nil,
  relation: Favn.RelationRef.t() | nil,
  include: [:relation, :columns, :row_count, :sample, :table_metadata],
  sample_limit: 20
}
```

The request should normalize to one concrete relation before it reaches SQL runtime code. For the normal UI path, the caller supplies an asset ref and the runner resolves the asset relation from the pinned manifest. Direct relation requests can remain internal for now.

Return a stable, JSON-friendly map or struct:

```elixir
%Favn.Contracts.RelationInspectionResult{
  asset_ref: Favn.Ref.t() | nil,
  relation_ref: Favn.RelationRef.t(),
  relation: Favn.SQL.Relation.t() | nil,
  columns: [Favn.SQL.Column.t()],
  row_count: non_neg_integer() | nil,
  sample: %{
    limit: non_neg_integer(),
    columns: [String.t()],
    rows: [map()]
  } | nil,
  table_metadata: map(),
  adapter: atom() | nil,
  inspected_at: DateTime.t(),
  warnings: [map()],
  error: map() | nil
}
```

Rules:

- Default `sample_limit` to 20 and cap it at 20 in every external-facing layer.
- Browser-facing layers clamp requested samples to `1..20`; the runner contract
  accepts `0` for internal schema/metadata-only inspection requests that should
  not fetch sample rows.
- Support only read-only operations: relation lookup, columns, row count, sample rows, and safe table metadata.
- Treat missing relation as a normal inspectable state, not a page crash.
- Normalize dates, decimals, binaries, atoms, and structs to JSON-safe values before exposing them to the web tier.
- Do not expose connection runtime values, source tokens, secret env values, request headers, or full database URLs.
- Use existing SQL admission control so local DuckDB file access remains serialized where required.
- Keep adapter-controlled SQL generation for row count and sampling to avoid identifier injection.

## Metadata Contract V1

Use existing per-asset `meta` maps as the first source for landed-data metadata.

Recommended keys for source/raw assets and SQL assets:

```elixir
%{
  rows_written: non_neg_integer(),
  rows_affected: non_neg_integer(),
  relation: "catalog.schema.table" | "schema.table" | binary(),
  loaded_at: DateTime.t() | String.t(),
  materialized_at: DateTime.t() | String.t(),
  window: map() | nil,
  source: %{
    system: atom() | binary(),
    segment_id_hash: binary()
  }
}
```

Rules:

- Preserve raw metadata under a separate `raw_metadata` or `asset_results[].meta` shape in the API so the UI can show diagnostics without inventing fields.
- Derive display fields such as rows written, loaded/materialized timestamp, and relation from known keys when present.
- Do not require all assets to return this metadata. The UI should show honest unavailable states.
- Do not expose raw source config values. Presence and secret flags should come from manifest declarations plus runner-side resolution diagnostics where available.

## Runtime Config Presence V1

Asset manifests already carry runtime config declarations. That is enough to show declared keys and secret flags, but not enough to know whether the runner resolved values for a particular run.

Implement in two steps:

- V1a: expose declared runtime config keys from active manifest targets with secret flags and descriptions where available. Display presence as `declared` or `not checked` unless run diagnostics explicitly prove missing values.
- V1b: extend runner context building to emit a safe resolution summary into asset metadata or runner metadata, for example `present: true`, `missing: true`, `secret: true`, and optional hashed non-secret identity. Never include raw values.

The UI should avoid claiming a config key was present unless the backend has run-scoped evidence.

## DuckDB And DuckLake Inspection V1

Add safe adapter-owned operations rather than generating SQL outside the adapter.

Proposed SQL runtime additions:

```elixir
Favn.SQL.Client.row_count(session, relation_ref)
Favn.SQL.Client.sample(session, relation_ref, limit: 20)
Favn.SQL.Client.table_metadata(session, relation_ref)
```

Proposed adapter callbacks:

```elixir
@callback row_count(conn(), RelationRef.t(), opts()) :: {:ok, non_neg_integer()} | {:error, Error.t()}
@callback sample(conn(), RelationRef.t(), opts()) :: {:ok, Result.t()} | {:error, Error.t()}
@callback table_metadata(conn(), RelationRef.t(), opts()) :: {:ok, map()} | {:error, Error.t()}
```

`table_metadata/3` should be optional. Unsupported metadata should return a clear unsupported warning, not fail the whole inspection.

DuckDB implementation notes:

- Reuse adapter-owned identifier quoting and relation qualification.
- Implement `row_count` as `select count(*) as row_count from <qualified relation>`.
- Implement `sample` as `select * from <qualified relation> limit <bounded integer>`.
- Keep catalog support explicit. If the current adapter cannot safely qualify catalog plus schema plus name, return a clear unsupported diagnostic for catalog-qualified sampling until this is fixed.
- Use `information_schema` for relation and column metadata as today.
- For DuckLake metadata, only expose fields known to be non-secret and useful. Snapshot id/time and table type are safe candidates; storage paths and catalog URLs need explicit redaction review before display.

## Orchestrator API Design

Add metadata exposure first, then inspection dispatch.

### Run detail DTO

Extend `GET /api/orchestrator/v1/runs/:run_id` to include normalized run details already available on `%Favn.Run{}`:

```json
{
  "id": "run_...",
  "status": "ok",
  "submit_kind": "pipeline",
  "manifest_version_id": "...",
  "target_refs": ["MyApp.Asset:default"],
  "params": {},
  "trigger": {},
  "metadata": {},
  "result": {},
  "pipeline": {},
  "pipeline_context": {},
  "asset_results": [
    {
      "asset_ref": "MyApp.Asset:default",
      "stage": 0,
      "status": "ok",
      "started_at": "...",
      "finished_at": "...",
      "duration_ms": 12,
      "meta": {},
      "error": null,
      "attempt_count": 1,
      "attempts": []
    }
  ],
  "node_results": []
}
```

Keep raw values normalized through the existing `normalize_data/1` approach, but add focused DTO helpers for asset results so atoms, refs, DateTimes, and errors stay consistent.

### Manifest asset target DTO

Extend active manifest target assets to include:

```json
{
  "target_id": "asset:Elixir.MyApp.Raw.Orders:default",
  "label": "{MyApp.Raw.Orders, :default}",
  "asset_ref": "MyApp.Raw.Orders:default",
  "type": "elixir",
  "relation": {
    "connection": "warehouse",
    "catalog": "raw",
    "schema": "sales",
    "name": "orders"
  },
  "metadata": {},
  "runtime_config": {},
  "depends_on": ["MyApp.Source:default"],
  "window": null
}
```

This lets the web show relation/config/lineage data without waiting for a data-plane sample query.

### Inspection endpoint

Add a private orchestrator endpoint that authorizes the actor and dispatches to the runner client:

```text
GET /api/orchestrator/v1/manifests/:manifest_version_id/assets/:target_id/inspection?sample_limit=20
```

or:

```text
POST /api/orchestrator/v1/inspection/relation
```

Recommended V1 route:

```text
GET /api/orchestrator/v1/manifests/:manifest_version_id/assets/:target_id/inspection?sample_limit=20
```

Rationale:

- The route stays manifest-pinned.
- The browser/web tier can use active manifest target ids already present in page data.
- The orchestrator resolves and validates asset target identity but delegates actual inspection to the runner/data-plane boundary.
- It avoids exposing direct relation-ref query input through the web by default.

Authorization:

- `viewer` can read inspection data in local development because sample rows are read-only but may contain business data.
- Revisit role requirements before any production web-facing release.

Errors:

- `404 not_found` for unknown manifest or target.
- `422 validation_failed` for invalid sample limit or asset without an inspectable relation.
- `424 inspection_unavailable` or `503 service_unavailable` when no runner/data-plane inspection client is available.
- `200` with `supported: false` and a warning for unsupported optional metadata sections.

## Runner Boundary Design

Extend the runner server/client contract with a synchronous inspection call:

```elixir
FavnRunner.inspect_relation(%RelationInspectionRequest{}, opts \\ [])
```

Runner responsibilities:

- Fetch the pinned manifest from its manifest store.
- Resolve asset target to one manifest asset.
- Validate that the asset has an owned relation with a connection.
- Open a SQL runtime session using the same connection registry/config path used for execution.
- Run relation lookup, columns, row count, sample, and optional table metadata through `Favn.SQL.Client`.
- Return a normalized `RelationInspectionResult` with warnings for unsupported pieces.
- Apply strict timeout handling and return structured diagnostics.

The runner should not mutate data and should not execute arbitrary SQL text supplied by the UI.

## Web/BFF Design

The web implementation should be handled through the `web-dev` workflow.

Browser-facing BFF route candidates:

```text
GET /api/web/v1/assets/:asset_ref/inspection?limit=20
GET /api/web/v1/manifests/:manifest_version_id/assets/:target_id/inspection?limit=20
```

Recommended V1 BFF route:

```text
GET /api/web/v1/manifests/:manifest_version_id/assets/:target_id/inspection?limit=20
```

Rationale:

- It aligns with the orchestrator route and avoids fragile asset-ref parsing in browser URLs.
- Asset detail pages already load active manifest targets and can retain target ids.

UI changes:

- Add a `Latest materialization` panel to asset detail using latest matching run and manifest target relation data.
- Add a `Data preview` panel with schema, row count, sample table, and explicit error/unsupported states.
- Add a `Runtime config` panel showing declared config keys, secret flags, and run-scoped presence only when available.
- Add a `Run metadata` panel showing normalized per-asset `meta` JSON and raw run detail JSON.
- Add per-asset inspection affordances on run detail, likely through `AssetDetailSheet` and `OutputRelationsTable`.

View-model additions:

- `AssetInspectionView`
- `RelationSchemaView`
- `RelationSampleView`
- `LatestMaterializationView`
- `RuntimeConfigPresenceView`
- `InspectionErrorView`

The UI should show honest states for missing relation, failed run, unsupported inspection, empty table, and backend errors.

## Implementation Slices

### Slice 1: Expose Existing Run And Manifest Metadata

- Extend orchestrator run detail DTO to expose `metadata`, `result`, `pipeline`, `pipeline_context`, `params`, `trigger`, `asset_results`, and `node_results`.
- Add DTO helpers for `Favn.Run.AssetResult` and node keys.
- Extend active manifest asset target DTOs with asset ref, type, relation, metadata, runtime config declarations, dependencies, materialization, and window.
- Update HTTP contract schemas and router tests.
- Update web normalizers/tests to consume real backend fields rather than relying on story-only shapes.

This slice unlocks latest status, relation display, rows written from metadata, windows, raw metadata, diagnostics, and lineage without data-plane sampling.

### Slice 2: Add SQL Runtime Inspection Primitives

- Add `Favn.SQL.Client.row_count/2`, `sample/3`, and `table_metadata/2` or one grouped `inspect_relation/3` helper.
- Add optional adapter callbacks for row count, sample, and table metadata.
- Add normalized sample result handling for JSON-safe scalar values.
- Add SQL runtime tests for limit validation, unsupported callback behavior, error normalization, and admission usage.
- Do not expose these as public `Favn.SQLClient` functions unless the implementation proves they are generally useful outside local inspection.

### Slice 3: Implement DuckDB/DuckLake Inspection

- Implement DuckDB adapter row count and sample using adapter-owned qualification and quoting.
- Add table metadata support only for fields that are safe and available with current DuckDB/DuckLake bootstrap state.
- Return explicit unsupported warnings for unavailable DuckLake snapshot/storage metadata.
- Add DuckDB tests for schema, row count, sample limit cap, missing relation, quoted identifiers, and redaction of connection/bootstrap secrets.

### Slice 4: Add Runner Inspection Command

- Add shared request/result contracts under `apps/favn_core/lib/favn/contracts/` or a dedicated `Favn.Inspection.*` namespace if multiple structs are clearer.
- Extend `FavnRunner` and `FavnRunner.Server` with a synchronous inspection call.
- Resolve asset relations from pinned manifests in the runner.
- Reuse connection registry/config loading and SQL admission paths.
- Add runner tests for success, missing relation, no relation on asset, missing connection env, unsupported adapter metadata, and timeout/error diagnostics.

### Slice 5: Add Orchestrator Dispatch Endpoint

- Extend the runner client behaviour if needed so orchestrator can call inspection without depending on runner internals.
- Add orchestrator facade function such as `inspect_manifest_asset/3`.
- Add private HTTP route `GET /api/orchestrator/v1/manifests/:manifest_version_id/assets/:target_id/inspection`.
- Authorize through the existing service + actor context flow.
- Keep orchestrator logic to identity validation, authorization, and dispatch; do not open SQL connections here.
- Add router tests for success, missing target, no relation, failed runner dispatch, sample limit validation, and authz.

### Slice 6: Add Web BFF And UI Panels

- Implement web BFF route matching the orchestrator inspection route.
- Add server orchestrator client helper and view-model normalizer for inspection responses.
- Add asset detail panels for latest materialization, data preview, runtime config, and run metadata.
- Add run detail per-asset inspection affordance through `AssetDetailSheet` and/or `OutputRelationsTable`.
- Add Storybook fixtures for success, missing relation, failed run, unsupported inspection, empty sample, and redaction.
- Add web unit and E2E tests for BFF behavior and UI states.

### Slice 7: Local Dogfooding Integration

- Verify the standalone tutorial or generated local sample can run a source-to-DuckDB/DuckLake pipeline and show inspection panels after `mix favn.run ... --wait`.
- Prefer the source/raw landing tutorial from issue 171 as the canonical first dogfooding path if available.
- Add an integration test or documented manual smoke path only if a full browser + runner + DuckDB flow is feasible in CI.

### Slice 8: Docs And Roadmap Cleanup

- Update `README.md` local development docs after the feature is implemented.
- Update `docs/FEATURES.md` only after user-visible inspection behavior lands.
- Remove or downgrade the roadmap item when implementation is complete.
- Update `docs/lib_structure.md` and `docs/test_structure.md` if new modules or test files are added.

## Test Plan

Backend tests:

- Orchestrator run detail exposes per-asset results, metadata, pipeline context, params, trigger, and raw result payloads.
- Active manifest target DTO exposes relation, runtime config declarations, dependencies, metadata, materialization, and window.
- Inspection endpoint validates manifest version, asset target, sample limit, relation existence, auth, and runner availability.
- Runner inspection resolves the pinned manifest asset and never inspects an unpinned/ad hoc relation by browser input.
- SQL runtime rejects limits above 20 at the external boundary and adapter code never receives unchecked limits.
- DuckDB inspection returns columns, row count, sample rows, and missing relation diagnostics.
- Missing SQL connection env values return structured redacted errors.

Web tests:

- Asset detail shows latest run status and relation from real normalized payloads.
- Asset detail shows rows written/affected when metadata is present and honest unavailable state when absent.
- Data preview renders schema, row count, sample rows, empty table, missing relation, unsupported backend, and failed inspection states.
- Runtime config display shows secret declarations without raw values.
- Run detail shows per-asset relation/rows/status/window and links or actions to inspect supported assets.
- BFF route caps `limit` at 20, relays auth, and preserves clear inspection errors.
- E2E mock covers success, missing relation, failed run, and redaction.

Verification commands after Elixir code changes:

```bash
mix format
mix compile --warnings-as-errors
mix test
mix credo --strict
mix dialyzer
mix xref graph --format stats --label compile-connected
```

For `favn_web` changes, use the web-dev workflow and run the workspace's relevant format, lint, test, Storybook, build, and E2E checks.

## Acceptance Mapping

Current status of the first curated local inspection slice:

- Latest run status is visible on asset/run surfaces: implemented by slices 1 and 6.
- Relation name/catalog/schema/table is visible when the active manifest carries an owned relation: implemented by slices 1 and 6.
- Rows written or affected are visible when asset metadata includes those fields: implemented by slices 1 and 6.
- Schema/columns are visible for supported inspected relations: implemented by slices 2, 3, 4, 5, and 6.
- Sample rows up to 20 are visible for supported inspected relations: implemented by slices 2, 3, 4, 5, and 6.
- Freshness or loaded/materialized timestamp is visible when metadata includes those fields: implemented by slices 1 and 6.
- Source config declarations are shown with secret flags and no raw secret values: implemented by slices 1 and 6. Run-scoped present/missing proof remains future V1b work unless existing run diagnostics prove a value is missing.
- Window context is visible when exposed by the run/manifest payload: implemented by slices 1 and 6.
- Raw run metadata JSON is visible: implemented by slices 1 and 6.
- Errors and diagnostics are visible without crashing pages: implemented across the local inspector path.
- Upstream/downstream assets are visible from manifest dependencies and run context: implemented by slices 1 and 6.
- DuckDB table metadata is exposed through the safe inspection path where available. Richer DuckLake snapshot/storage metadata remains future work.
- No arbitrary SQL editor is exposed by default: implemented by the architectural rule across all slices.
- Tests cover the backend/orchestrator/runner/adapter path and web BFF/UI preview states. Broader live browser-plus-runner DuckDB dogfooding coverage remains future hardening.

## Non-Goals

- Do not build a full BI or exploration UI.
- Do not expose a general SQL editor by default.
- Do not make orchestrator a data warehouse query engine.
- Do not stabilize the private orchestrator API as a public external API.
- Do not add production-grade arbitrary-query authorization in this issue.
- Do not replace DuckDB CLI or external SQL tools.
- Do not invent a connector framework.

## Risks And Mitigations

- Risk: sample rows contain sensitive business data. Mitigation: keep this local/dev-oriented, require authenticated viewer access, cap samples at 20, and do not expose arbitrary SQL.
- Risk: SQL injection through relation identifiers. Mitigation: only inspect manifest-owned relation refs and generate SQL inside adapters with adapter-owned quoting.
- Risk: orchestrator accumulates data-plane responsibilities. Mitigation: orchestrator validates identity and dispatches to runner; SQL connections stay runner/SQL-runtime owned.
- Risk: current run metadata is inconsistent across asset types. Mitigation: show raw metadata, derive known display fields opportunistically, and document recommended metadata keys.
- Risk: runtime config presence display overclaims. Mitigation: distinguish declared config from run-resolved presence until runner emits safe resolution summaries.
- Risk: DuckLake metadata leaks storage paths or account names. Mitigation: start with conservative metadata and add path display only after redaction review.
- Risk: catalog-qualified DuckDB sampling is incomplete. Mitigation: return clear unsupported diagnostics until qualification is proven safe.
- Risk: adding public `Favn.SQLClient` helpers expands API too early. Mitigation: keep inspection helpers internal unless a separate API decision promotes them.

## Open Decisions

- Should runtime config presence be V1a declaration-only or should V1 include runner-emitted resolution summaries from the start?
- Should `viewer` be enough for sample data in local mode, or should sample rows require `operator` until production auth policy is clearer?
- Should the first orchestrator inspection route be asset-target-only, or should an internal direct relation route exist for future tools?
- Which DuckLake metadata fields are safe enough to expose in V1?
- Should sample rows preserve backend column order separately from row maps for all adapters?

## Recommended First PR Shape

Start with Slice 1 before adding data-plane sampling.

That gives immediate user-visible value by exposing data already stored in orchestrator state and manifest versions, reduces frontend mock drift, and creates the stable asset/run metadata surface that later inspection endpoints can attach to. The next PR should add SQL runtime and DuckDB inspection primitives behind runner-owned commands, followed by the web preview panels.
