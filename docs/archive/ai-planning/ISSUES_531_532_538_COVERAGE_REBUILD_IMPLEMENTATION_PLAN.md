# Issues 531, 532, and 538: coverage and SQL rebuild implementation plan

Status: temporary normative implementation plan for issues #531, #532, and
issue #538.

This document is the implementation guide and review checklist for the combined
pull request. It records the complete contract agreed for asset coverage,
environment timezone defaults, pipeline lookback, SQL target generations, and
manual rebuilds. If implementation work discovers that this contract cannot be
followed, update this document and the affected GitHub issue before merging a
different design.

The implementation is a clean pre-v1 breaking change. PostgreSQL is the only
control-plane persistence backend. No SQLite or in-memory persistence behavior
is retained or reintroduced.

## 1. Locked decisions

1. One application-level timezone is the default for schedule cadence,
   pipeline anchors, asset windows, calendar freshness, and coverage. Each
   declaration may override only its own timezone.
2. Schedule, pipeline, and asset timezone overrides are independent. A schedule
   timezone never becomes the pipeline timezone, and a pipeline timezone never
   becomes an asset timezone.
3. Coverage is a standalone scalar declaration on assets and namespaces. It is
   not nested in `window`.
4. Namespace coverage uses closest-declaration-wins replacement. Coverage
   fields are never merged across namespaces and assets. `coverage nil`
   explicitly clears inherited coverage.
5. Coverage describes which canonical asset windows are expected. It is
   independent of freshness.
6. `availability_delay` delays when a closed window becomes expected. It does
   not delay execution, create a timer, submit work, or alter retry policy.
7. Development narrows declared history through a deployment-wide
   `coverage_scope` floor. Rebuild completeness is measured against the
   deployment's effective coverage, not always the portable declared history.
8. Operational lookback belongs to the pipeline window policy. Asset window
   specs do not contain lookback.
9. Scheduled pipeline selections apply pipeline lookback. Explicit manual
   windows and explicit backfill ranges are exact and are never widened.
10. A rebuild creates and validates a complete new SQL target generation before
    activation. A rebuild is not an in-place backfill.
11. The first rebuild workflow is manual. An operator must start it through an
    explicit CLI command or UI action. Deployment activation and scheduled runs
    never start a rebuild.
12. A target that requires rebuilding cannot materialize through an ordinary
    run. Its affected downstream execution paths are blocked, while unrelated
    paths remain runnable.
13. Rebuilds are limited to persisted SQL tables, including incremental table
    materializations. Views, source assets, and Elixir assets are not rebuild
    targets.
14. Downstream repair is conservative. Favn automatically chooses an exact
    backfill only when it can prove a partition-local one-to-one window mapping.
    It never guesses from the fact that both assets happen to be windowed.
15. The active target generation and its active freshness/coverage evidence do
    not change until candidate validation and data-plane activation succeed.
16. The runner owns data-plane SQL execution. The orchestrator owns durable
    operation state, admission, generation bindings, audit, and recovery.
17. Unknown outcomes are explicit. Favn never blindly retries a possibly
    committed materialization or activation.

## 2. Completion definition

The combined change is complete only when it can:

1. compile the final DSL and reject every invalid option or inheritance shape;
2. resolve all effective timezones and coverage scope into a deterministic
   manifest without runtime environment reads;
3. distinguish declared coverage from environment-effective coverage;
4. calculate bounded generation-aware coverage summaries and exact missing
   windows, including authoritative empty successes;
5. submit selected missing windows as an exact manual backfill;
6. apply pipeline lookback once for scheduled runs and never for manual or
   backfill requests;
7. record requested, expanded, and effective window selection in run metadata;
8. compare a desired persisted SQL target with its active generation and its
   physical relation;
9. mark incompatible targets `rebuild_required`, explain the diff, and reject
   ordinary writes to those targets;
10. produce and persist a complete, immutable rebuild and downstream-repair
    plan before execution;
11. build a shadow target for the complete effective coverage while the active
    target remains readable;
12. validate the candidate's coverage, ordered contract, authored checks, and
    physical fingerprint;
13. activate it atomically inside the data system and reconcile an unknown
    activation result without blind retry;
14. keep active state unchanged after build failure, failed validation, or
    cancellation before activation;
15. resume a failed operation without rerunning successful candidate windows;
16. expose the same persisted coverage and rebuild contracts through the public
    orchestrator facade, private API, Mix tasks, and LiveView UI;
17. pass the focused compiler, planner, PostgreSQL, orchestrator, runner,
    adapter, API, CLI, and UI test matrix in this plan; and
18. update the canonical public guides, moduledocs/typespecs, `Favn.AI`,
    feature status, and roadmap in the implementation pull request.

## 3. Final public DSL and configuration

### 3.1 Environment defaults

The application configuration is:

```elixir
config :favn,
  default_timezone: "Europe/Oslo"
```

When it is omitted, the fallback is `"Etc/UTC"`. The value must be a valid IANA
timezone known to the configured timezone database. Manifest construction fails
for an invalid value.

The old scheduler-only default is removed. There is one public default rather
than separate scheduler, pipeline, asset, freshness, and coverage defaults.

The effective timezone order is:

1. the explicit timezone on the declaration being resolved;
2. an inherited asset window declaration, when the asset inherits that entire
   window spec from a namespace;
3. `config :favn, :default_timezone`; and
4. `"Etc/UTC"`.

Resolution is independent for each owner:

| Owner | Explicit override | Meaning |
| --- | --- | --- |
| schedule | `schedule ..., timezone: value` | timezone used to interpret cron cadence |
| pipeline | `window ..., timezone: value` | timezone used to resolve pipeline anchor periods |
| asset | `Favn.Window.*(timezone: value)` | timezone used for canonical data windows |
| calendar freshness | `freshness ..., timezone: value` | timezone used for the freshness calendar |

If calendar freshness has no explicit timezone, a windowed asset uses its
effective asset-window timezone. A non-windowed asset uses the application
default. Freshness resolution never reads the schedule or pipeline timezone.

Manual run and backfill requests use an explicitly supplied request timezone
when one exists, otherwise the effective pipeline timezone. The request
timezone selects pipeline anchors; the planner still maps each anchor to the
selected asset's effective window timezone.

Every resolved manifest value carries provenance for operator display:
`local`, `namespace`, `application_default`, or `utc_fallback`. Provenance does
not affect semantic fingerprints. The effective timezone does.

The manifest compiler emits a warning when a pipeline selects an asset whose
effective window timezone differs from the pipeline's effective timezone. This
is a warning because the difference can be intentional. The UI displays both
values and the provenance next to each value.

### 3.2 Asset coverage

The final declaration is:

```elixir
coverage(
  from: ~D[2020-01-01],
  through: :latest_closed,
  availability_delay: {:hours, 6}
)
```

`coverage/1` is available in `Favn.Asset`, `Favn.SQLAsset`, `Favn.MultiAsset`,
and `Favn.Namespace`. It is a scalar declaration, like `window` and
`freshness`, not an accumulating declaration.

The accepted options are:

| Option | Required | Accepted values | Default |
| --- | --- | --- | --- |
| `from` | yes | `Date` or timezone-aware `DateTime` | none |
| `through` | no | `:latest_closed`, `:current`, `Date`, or timezone-aware `DateTime` | `:latest_closed` |
| `availability_delay` | no | non-negative `{unit, amount}` | zero |

Duration units are `:second`, `:seconds`, `:minute`, `:minutes`, `:hour`,
`:hours`, `:day`, and `:days`. The first tuple element is the unit and the second
is a non-negative integer. Durations are normalized to elapsed seconds;
`{:days, 1}` means 86,400 seconds after the close instant, not the next local
calendar midnight. `availability_delay` is valid only with
`through: :latest_closed`.

Coverage has no `timezone` or window-kind option. It uses the effective kind and
timezone from the asset window spec. A non-nil effective coverage declaration
on a non-windowed leaf is a compile error. A windowed asset without coverage is
valid and reports coverage as `unknown` with reason `coverage_not_declared`.

For hourly assets, authored fixed `from` and `through` boundaries must be
timezone-aware `DateTime` values. Daily, monthly, and yearly assets accept a
`Date` or timezone-aware `DateTime`. A `DateTime` is converted to the effective
asset timezone before finding its containing window. A `Date` is interpreted at
local midnight. The environment coverage floor may be a `Date` for every asset,
including hourly assets; for an hourly asset it means the first local hour that
starts on that date.

Both boundaries are normalized to canonical half-open windows `[start, end)`:

- `from` means the containing canonical window and is inclusive;
- a fixed `through` means the containing canonical window and is inclusive;
- `:current` means the canonical window containing the evaluation instant,
  even though it is not closed; and
- `:latest_closed` means the latest window for which
  `window.end_at + availability_delay <= now`.

A fixed authored `through` before authored `from` is a compile error. An
environment coverage floor later than a fixed `through` is valid and produces
an empty effective expectation with zero expected windows.

The evaluator takes `now` as an explicit input and records it in plans and
responses. DST gaps and overlaps are handled by `Favn.TimePeriod`; callers do
not add fixed seconds to find the next hour/day/month/year boundary.

### 3.3 Namespace replacement

A namespace may establish portable coverage once:

```elixir
defmodule MyApp.Raw do
  use Favn.Namespace

  coverage(
    from: ~D[2020-01-01],
    through: :latest_closed,
    availability_delay: {:hours, 6}
  )
end
```

Descendants inherit the closest complete coverage spec. A descendant replaces
the whole policy by declaring another `coverage(...)`. No option is inherited
from an earlier policy after replacement. `coverage nil` is an explicit leaf or
nested-namespace opt-out. Omitting `coverage` means inherit; declaring
`coverage nil` means do not inherit.

`Favn.Namespace.Config`, declaration import/export, compile diagnostics,
manifest serialization, rehydration, and tests must all preserve that
three-state distinction: absent, non-nil spec, and explicit nil override.

### 3.4 Environment coverage scope

Portable assets declare their full intended history. An environment may narrow
the beginning of that history:

```elixir
config :favn,
  coverage_scope: [from: ~D[2026-07-01]]
```

The initial configuration supports one optional global `from` only. It accepts
a `Date` or an ISO-8601 date string so `runtime.exs` can derive it from an
environment variable. Unknown keys and invalid values fail the manifest build.
There are no per-asset config overrides and no environment `through` override.

For each asset, the effective first window is the later of:

1. the normalized declared coverage `from`; and
2. the normalized environment coverage floor.

The manifest stores both declared and effective boundaries plus the applied
scope input. Runtime code does not read application configuration or OS
environment variables to calculate coverage.

Expanding environment scope earlier exposes missing windows. Narrowing it later
does not delete materialization or coverage evidence; reads simply exclude
evidence outside the current effective range. Changing only the scope does not
require a schema rebuild when the target identity remains compatible.

No CLI flag can weaken the effective range of a rebuild. A rebuild always uses
the effective coverage frozen in its pinned manifest.

### 3.5 Pipeline lookback and exact selections

Asset window specs become:

```elixir
window Favn.Window.monthly(
  required: true,
  refresh_from: :day,
  timezone: "Europe/Oslo"
)
```

`lookback` is removed from `Favn.Window.Spec` and from every
`Favn.Window.hourly/daily/monthly/yearly` option list.

Pipeline policies become:

```elixir
pipeline :monthly_refresh do
  schedule cron: "0 8 * * *"

  window :monthly,
    anchor: :current_period,
    lookback: 1,
    timezone: "Europe/Oslo"
end
```

`Favn.Window.Policy` owns `lookback`, a non-negative integer defaulting to zero.
It retains `kind`, `anchor`, `timezone`, and `allow_full_load`.

Core adds a serializable `%Favn.Window.Selection{}` contract with:

- `intent`: `:scheduled`, `:manual`, or `:backfill`;
- `requested_anchors`: the exact anchors resolved from the trigger/request;
- `expansion`: `:none` or `{:lookback, non_neg_integer()}`;
- `effective_anchors`: the deduplicated, ordered anchors after expansion; and
- `timezone`: the pipeline timezone used to resolve the anchors.

Selection behavior is exact:

| Intent | Requested | Expansion | Effective |
| --- | --- | --- | --- |
| scheduled | resolved schedule anchor | pipeline lookback | prior anchors followed by requested anchor |
| manual single | explicit anchor | none | exact explicit anchor |
| backfill range | explicit inclusive range | none | exact requested range |

There is no initial manual or backfill “include lookback” option. An operator who
wants a wider range requests that wider range explicitly.

The pipeline-policy resolver creates the selection. The asset planner consumes
only `effective_anchors`, maps them to each asset's canonical windows, and never
expands them. The runner executes the concrete planned nodes and never resolves
or expands windows.

Persisted run input and operator DTOs expose requested anchors, expansion, and
effective anchors separately. Existing run identity/fingerprints include the
effective selection so replay is deterministic.

### 3.6 Coverage and availability are not scheduling

An asset with `availability_delay: {:hours, 6}` may run before 06:00. Before its
latest closed window reaches the six-hour delay, that window is simply not yet
counted as expected or missing. Coverage does not submit or gate execution.

The existing manual and cron-triggered pipeline model remains. Retry policy
applies only after an execution starts and only to outcomes already classified
as safe to retry. It is not a timing or availability mechanism.

Manifest validation emits a non-fatal warning when a selected asset's recurring
cron occurrence is consistently earlier than that asset's latest-closed
availability delay in the relevant timezone. The warning explains that the
asset may run before data is expected and points to the cron and coverage
declarations. It does not suppress the occurrence.

## 4. Core and manifest contracts

### 4.1 New and changed domain types

`favn_core` owns these values and their validation:

- `%Favn.Coverage.Spec{from, through, availability_delay_seconds}` for the
  normalized authored policy;
- `%Favn.Coverage.Effective{declared_from, effective_from, through,
  availability_delay_seconds, kind, timezone, timezone_source,
  scope_source}` for a manifest asset;
- `%Favn.Coverage.Summary{status, evaluated_at, first_window,
  last_expected_window, expected_count, covered_count, missing_count,
  evidence_generation_id, active_target_generation_id}` for bounded read
  results;
- `%Favn.Window.Selection{}` for trigger-owned anchor selection;
- `%Favn.Manifest.TargetDescriptor{}` for the desired persisted SQL target;
- `%Favn.TargetGeneration{}` for stable generation identity returned across
  orchestrator boundaries; and
- `%Favn.Rebuild.Plan{}` plus typed target actions and frozen window items for
  deterministic plan hashing.

Public types get moduledocs, typedocs, stable validation errors, serializer and
rehydrator support, and focused unit tests. Orchestrator-only lifecycle records
remain in `favn_orchestrator`; they do not become public DSL API.

### 4.2 Manifest resolution

Manifest generation receives an explicit build environment containing:

```elixir
%{
  default_timezone: "Europe/Oslo",
  coverage_scope: %{from: ~D[2026-07-01]} | nil
}
```

The public `favn` build boundary reads and validates application configuration,
then passes the normalized environment to the authoring/compiler boundary.
`favn_core` functions remain pure and do not call `Application.get_env/3`.

Manifest assets contain effective window/freshness/coverage values, provenance,
and a desired target descriptor. Manifest pipelines and schedules contain
concrete effective timezones and provenance. Published manifests therefore
behave identically in the control plane and runner regardless of their boot
environment.

The manifest schema and runner contract advance from 10 to 11. Version 11 is
the only accepted shape after the clean breaking change. The execution-package
schema remains 2 unless implementation adds a field to its persisted payload;
target relation overrides belong in runner work, not authored execution
packages. If that payload must change, advance its schema in the same commit and
update this plan before implementation.

### 4.3 Desired target descriptor and fingerprints

Every persisted SQL table target receives a canonical descriptor containing:

- stable logical target id and relation identity;
- adapter and non-secret connection identity;
- materialization kind and write semantics;
- execution-package hash/transformation hash;
- ordered output contract fingerprint, including names, normalized types,
  nullability, and order;
- declared grain/key fingerprint;
- window identity fingerprint containing effective kind and timezone;
- coverage policy and effective-scope metadata; and
- the manifest and runner-contract versions that produced the descriptor.

Fingerprints are generated from canonical serialized data, never Erlang term
ordering or display strings. Secrets and transient provenance are excluded.

The window identity includes effective kind and timezone because existing
window keys cannot safely satisfy a different calendar. Coverage start,
`through`, availability delay, timezone provenance, requiredness, freshness,
pipeline lookback, display metadata, descriptions, and tags do not themselves
change physical target compatibility.

### 4.4 Compatibility classification

Manifest activation is allowed to publish a desired target even when that
target needs work. Admission calculates and persists one of:

- `ready`: desired descriptor and physical target are compatible;
- `uninitialized`: no active generation and no physical target exist; the first
  successful ordinary materialization may establish the initial generation;
- `rebuild_available`: only transformation semantics changed and physical
  writes remain compatible; ordinary work remains allowed, but an operator may
  choose a full rebuild;
- `rebuild_required`: contract, grain, materialization, relation, adapter/
  connection identity, or window identity is incompatible;
- `unexpected_drift`: the physical relation differs from the recorded active
  physical fingerprint without a matching desired change; and
- `operator_decision`: ownership or impact cannot be proven, including an
  unmanaged pre-existing physical relation without a generation binding.

`rebuild_required`, `unexpected_drift`, and `operator_decision` reject ordinary
writes to the target. `rebuild_required` may be resolved by the manual rebuild
workflow. Drift and ownership decisions remain blocked until the operator has a
safe, explicit path; they are never silently classified as compatible.

The compatibility result stores a stable reason code and structured field diff.
Metadata-only and provenance-only changes are `ready`. Moving coverage earlier
or later changes expected gaps but not target compatibility. Changing effective
window kind/timezone is `rebuild_required`.

Manifest activation recomputes status for the changed target and transitively
marks dependent paths blocked. It does not reject the whole manifest and does
not block unrelated assets.

## 5. Target-generation model

### 5.1 Identity and binding

A `target_generation_id` is an opaque UUID generated by the orchestrator. It is
scoped by workspace and logical target id and is stable for that physical data
generation. It is not the manifest id: multiple compatible manifests may use
one active generation.

The active binding is separate from the manifest. A manifest describes the
desired target; the binding identifies the currently readable generation.
Normal planning pins the active generation id for every persisted SQL input and
output. A materialization result without the pinned generation id is invalid.

For an uninitialized target, the orchestrator creates a `building` generation
before the first write and includes it in runner work. A successful,
reconciled first materialization records the physical fingerprint and binds it
active. If a physical relation already exists without a binding, Favn does not
adopt it automatically; admission reports `operator_decision`.

### 5.2 Generation-aware evidence

Materialization records carry `target_generation_id`. Asset-window and freshness
states carry a non-null `evidence_generation_id`:

- for a persisted SQL target it is the active or candidate
  `target_generation_id`; and
- for another windowed asset it is a deterministic manifest asset generation
  derived from the semantic asset/execution fingerprint and window identity.

The semantic generation lets source and non-persisted windowed assets use
coverage without inventing a physical SQL target. Metadata/provenance-only
manifest changes retain the same semantic generation; an execution or window
identity change creates a new one.

The authoritative asset-window key becomes:

```text
workspace_id + evidence_generation_id + target_id + window_key
```

`manifest_version_id` remains evidence metadata but is not generation identity.
Freshness state is likewise evidence-generation-scoped. Persisted-target reads
join through the active binding; other assets use the semantic generation from
the active manifest. Success from a retired, failed, discarded, stale semantic,
or candidate generation cannot satisfy active freshness or coverage.

Candidate materializations are persisted against the candidate generation so
they can be resumed and audited. They are visible only inside the rebuild
operation until activation. Switching the active binding makes their existing
generation-scoped evidence authoritative without copying or rewriting it.

### 5.3 Physical relation names

Runner work supplies both the stable active relation and a deterministic
candidate relation. Candidate names derive from the logical relation and the
generation id, are quoted by the adapter, and are shortened with a hash to stay
inside the target database identifier limit. User SQL and execution packages do
not construct these names.

The adapter keeps a small sidecar generation marker in the same data system as
the target. The marker contains logical target id, active physical relation,
active generation id, last activation operation id/token, and timestamp. It has
no secrets or control-plane credentials.

## 6. Coverage calculation and missing windows

### 6.1 Covered evidence

A canonical expected window is covered only when the asset's current evidence
generation has an authoritative successful result for that exact window key. A
success with zero rows is covered. Planned, queued, running, failed, cancelled,
skipped, invalidated, unknown-outcome, candidate, retired-target-generation, or
stale-semantic-generation results are not covered.

Coverage status is:

- `complete` when every currently expected window is covered, including the
  valid zero-expected-window case;
- `incomplete` when at least one expected window is absent or not successful;
  and
- `unknown` when coverage is not declared, the asset is non-windowed, an
  uninitialized persisted target has no active generation, or authoritative
  state cannot be read.

The response includes an explicit unknown reason. Freshness is evaluated and
displayed independently, so `fresh + incomplete` and `stale + complete` are
normal combinations.

### 6.2 Bounded evaluation

Coverage reads never load the whole state history into memory.

1. Core normalizes the effective first and evaluated last expected window from
   the manifest and explicit `now`.
2. Core calculates `expected_count` arithmetically through calendar iteration
   with a bounded counter. Evaluation fails with
   `coverage_window_limit_exceeded` above 100,000 expected windows.
3. PostgreSQL counts successful current-evidence-generation states inside the
   exact start/end range for `covered_count`.
4. `missing_count` is `expected_count - covered_count`; unique generation/window
   constraints prevent duplicate successes.
5. Exact missing windows are cursor-paged. Core emits at most one page of
   expected canonical windows and PostgreSQL batch-fetches matching successful
   keys. The difference is returned in canonical order.

The default missing-window page is 100 and the maximum is 500. Cursors contain
the evaluated boundary, last window key, evidence generation id, manifest
version id, and a checksum. A cursor is rejected if any pinned identity differs,
rather than mixing two evaluations.

Coverage summary and missing-window endpoints accept an optional `evaluated_at`
only on internal/test boundaries. Operator calls use the orchestrator clock and
return the chosen instant.

### 6.3 Missing-window action

The asset detail workflow can select the current missing-window result and
request an exact backfill. Submission freezes and revalidates the window keys,
manifest id, evidence generation id, active target generation id when present,
and evaluation checksum. If any changed, the operator receives
`coverage_selection_stale` and refreshes the plan.

The existing backfill safety limit remains 10,000 child windows. Larger missing
sets stay visible but cannot be submitted as one backfill; the operator selects
a bounded range/page. The submission never fills gaps that were not in the
approved plan.

No-date inference is supported for a single asset and for rebuild planning. A
pipeline with heterogeneous selected-asset kinds, timezones, or effective
coverage bounds does not invent one common range; it returns
`heterogeneous_coverage_requires_explicit_range`. Explicit pipeline ranges keep
their existing behavior.

## 7. PostgreSQL control-plane persistence

All tables live in `favn_storage_postgres`. Store contracts and result structs
live in `favn_orchestrator`; SQL/Ecto schemas and transactions remain private to
the PostgreSQL app.

### 7.1 New authoritative tables

`asset_target_generations` stores:

- workspace id, target id, and generation id primary identity;
- creating manifest id and candidate/active descriptor hashes;
- logical and physical relation descriptors;
- physical schema fingerprint;
- data-plane marker/token;
- status `building`, `active`, `retired`, `failed`, or `discarded`;
- creating rebuild operation id when applicable; and
- created, activated, retired, and updated timestamps.

`asset_target_bindings` stores one row per workspace/target:

- active generation id, nullable only for uninitialized targets;
- desired manifest id and desired descriptor hash;
- compatibility status, reason code, and bounded structured diff;
- active physical fingerprint;
- optimistic version; and
- updated timestamp.

`rebuild_operations` stores:

- operation, workspace, root target, pinned manifest, active generation, and
  candidate generation ids;
- immutable plan hash and plan version;
- manual trigger, operator actor/session, required reason, and idempotency key;
- frozen evaluation instant, effective coverage bounds, and item counts;
- lifecycle state, current phase, version, and timestamps;
- activation token, dispatched-at time, result marker, and unknown-outcome data;
- bounded validation result and terminal error payload; and
- cleanup state.

`rebuild_plan_actions` stores one topologically ordered row per affected target:

- action `no_action`, `backfill`, `rebuild`, or `operator_decision`;
- structured reason and upstream impact;
- exact mapping proof or reason mapping is unproven;
- pinned input generation ids;
- candidate generation id for rebuild actions;
- action status and child operation/run ids; and
- ordinal used for deterministic execution and plan hashing.

`rebuild_windows` stores frozen logical work items:

- operation id, action target, canonical window key/start/end, and ordinal;
- state `planned`, `ready`, `claimed`, `running`, `succeeded`, `failed`,
  `cancelled`, or `outcome_unknown`;
- child run/materialization id, attempt count, row count, and bounded error;
- candidate generation id; and
- timestamps/version.

For a non-windowed full-table target, the operation contains one explicit
`full_load` item rather than pretending it is a calendar window.

`target_operation_locks` stores one exclusive write-operation owner per
workspace/target, operation id/type, fencing token, lease owner/expiry, version,
and timestamps. The unique workspace/target key is the admission barrier shared
by ordinary materialization and rebuild execution.

### 7.2 Existing tables and projections

Add `target_generation_id` to materialization claims/materializations and their
payload codecs. Replace the `asset_window_states` identity with
`evidence_generation_id` as described above. Add the same identity to
`asset_freshness_states` and its indexes. Projection events and run snapshots
include both evidence and target generation ids where applicable.

Rebuild candidate events update generation-scoped evidence but do not update
the active target status, active coverage DTO, or active freshness DTO until the
binding changes.

Schema constraints bound identifiers, JSON payloads, plan action counts, window
states, legal transitions, and positive versions. Foreign keys use workspace
scope and `ON DELETE RESTRICT` for durable operation/generation evidence.

The storage-v2 exact migration list, reset/bootstrap schema, capability
registry, fixtures, codecs, instrumentation wrappers, and migration tests are
updated together. There is no SQLite migration or compatibility branch.

### 7.3 Store boundaries

Add focused persistence capabilities rather than widening a generic repo:

- `TargetGenerationStore` for binding, generation, compatibility, and physical
  fingerprint commands/queries;
- `RebuildStore` for plan creation, lifecycle transitions, item claims,
  progress, cancellation, retry, and recovery;
- `TargetOperationLockStore` for fenced acquisition/renewal/release and
  materialization admission; and
- bounded generation-aware coverage queries on the existing window-state read
  boundary.

Every mutating command has a stable command/idempotency id. State transitions
use expected version/state predicates and return `:conflict` rather than
silently overwriting newer state.

## 8. Rebuild planning

### 8.1 Request and authorization

A rebuild request identifies one active-manifest asset target and includes a
non-empty operator reason. Only an authenticated workspace operator may plan;
starting, cancelling, retrying, or reconciling requires workspace admin. The
same role rules apply through facade, API, CLI, and UI.

Planning is read-only in the data plane. It inspects the pinned desired
descriptor, active binding, physical relation, adapter capabilities, effective
coverage, and downstream graph. It fails before mutation if the root is not a
persisted SQL table or required generation/transaction capabilities are absent.

### 8.2 Frozen root work

For a windowed root, planning evaluates the complete effective coverage at one
recorded `evaluated_at` instant and persists every canonical expected window.
`through: :latest_closed` and `:current` are therefore frozen for the operation;
new time periods that become expected during a long rebuild are not silently
added. A later ordinary run or backfill handles them after activation.

For a non-windowed persisted SQL table, planning creates one full-load item.
Rebuild plans are rejected above 100,000 root logical window items. Items are
written and dispatched in batches of 500 so normal operation memory remains
bounded.

An authoritative empty candidate window is successful and contributes to
candidate completeness.

### 8.3 Pinned inputs

The plan pins:

- manifest id and content hash;
- runner release and execution-package hash;
- root desired target descriptor;
- active and candidate generation ids;
- every upstream target's active generation id and physical relation marker;
- runtime input pins and secret identifiers already used by ordinary runs;
- frozen root windows/full-load item;
- downstream actions and exact mapped windows; and
- adapter capability/fingerprint snapshot.

The runner reads upstream active generations while writing the candidate root.
If a required upstream generation changes before an undispatched item starts,
the operation pauses with `pinned_input_changed`; it does not silently mix
generations.

### 8.4 Downstream action algorithm

Traverse affected descendants in topological order and persist one action for
each. `no_action` is used only when the changed output cannot affect the
descendant under the manifest lineage/contract diff.

Use `backfill` only when all of these are proven:

1. the descendant is a persisted, windowed target that can accept an exact
   backfill;
2. the relevant lineage edge is direct and partition-local;
3. source and destination use the same canonical window kind and effective
   timezone;
4. each affected source window maps to exactly one equal destination window;
5. the descendant's materialization semantics do not replace unselected
   history; and
6. the compatibility diff does not change schema, grain, relation identity, or
   window identity.

Use `rebuild` for full-replacement consumers, non-windowed persisted SQL table
consumers, or downstream schema/grain/materialization/window incompatibility.
Use `operator_decision` when lineage or window impact cannot be proven. The plan
cannot start while any action is `operator_decision`.

The first version adds no window-mapping DSL. Matching kind/timezone and
partition-local lineage is the only automatic mapping proof. This deliberately
prefers a larger safe rebuild over a guessed partial repair.

### 8.5 Immutable approval

Planning returns an immutable `plan_id`, `plan_hash`, and expiry together with
the full root and downstream actions. Starting requires that exact id/hash.
Before creating the operation, the orchestrator rechecks manifest, bindings,
physical fingerprints, adapter capabilities, and active locks. Any difference
returns `rebuild_plan_stale`; the operator creates and reviews a new plan.

The start command itself is manual approval. There is no separate background or
deployment approval policy in this implementation.

## 9. Rebuild lifecycle and invariants

### 9.1 States

The persisted operation state machine is:

```text
planned -> queued -> building -> validating -> activating -> succeeded
                    |           |             |
                    +----------> failed <-----+
                    |
                    +----------> cancelling -> cancelled

activating -> activation_unknown -> reconciling -> activating | succeeded | failed
```

`planned` is immutable and not yet executable. `queued` means approval was
accepted and locks are being acquired. `building` dispatches candidate items.
`validating` performs candidate completeness and checks. `activating` has a
persisted activation intent/token before runner dispatch. Terminal states are
`succeeded`, `failed`, and `cancelled`. `activation_unknown` is non-terminal and
blocks all target writes until reconciliation proves the data-plane state.

Cleanup is an orthogonal `not_started`, `pending`, `running`, `complete`, or
`failed` field. Cleanup failure never changes a successfully activated rebuild
back to failed.

### 9.2 Execution order

1. Revalidate the immutable plan and acquire fenced locks for all planned write
   targets in canonical target-id order.
2. Create the candidate generation records and deterministic candidate relation
   names.
3. Execute root work into its shadow relation. Each logical window retains a
   separate outcome even if the adapter safely batches physical work.
4. Validate root completeness, physical schema, ordered contract, and authored
   checks against the candidate relation.
5. Persist activation intent/token, atomically activate the candidate in the
   data plane, and reconcile/record the active generation binding.
6. Execute downstream backfill/rebuild actions in topological order against the
   newly active upstream generation. Each downstream rebuild uses the same
   shadow/validate/activate protocol.
7. Mark the operation succeeded only after every required action succeeds.
8. Release operation locks and schedule idempotent retired-relation cleanup.

A downstream failure does not roll back an already activated upstream target.
The operation remains failed with explicit completed and remaining actions, and
retry resumes the remaining plan. This is a saga with durable checkpoints, not
a false cross-system transaction.

### 9.3 Candidate validation

Validation must prove:

- every frozen root item has an authoritative successful candidate result;
- candidate success records all reference the expected generation and pinned
  package;
- the physical relation exists and has the expected relation kind;
- ordered columns, normalized types, nullability, grain/key, and
  materialization fingerprint match the desired descriptor;
- all authored contract/data checks pass against the candidate relation; and
- the exact active-generation marker still identifies the pinned previous
  generation, while the deterministic candidate relation, orchestrator-owned
  generation record, and in-transaction candidate fingerprint together prove
  that the candidate is prepared but not active.

Validation failure stores bounded structured results, marks the candidate
generation failed, leaves the active binding and active relation unchanged, and
does not promote candidate freshness or coverage.

### 9.4 Cancellation

Cancellation is allowed before activation dispatch. It stops new item claims,
uses the existing run cancellation path for active child work, reconciles any
unknown child write, and then discards the candidate relation idempotently. The
active generation remains unchanged.

Once activation intent has been dispatched, cancellation becomes a request to
stop after reconciliation; Favn must first determine which generation is active.
It never deletes either physical relation while the outcome is unknown.

### 9.5 Retry and resume

Child executions use the existing effective retry policy only for outcomes
classified `safe_failure`. Successful candidate items are never rerun on
operation retry. Failed safe items return to `ready`; possibly committed writes
remain `outcome_unknown` until reconciled.

Retrying a failed operation creates a new attempt on the same operation and
plan, rechecks all pins, and resumes incomplete actions. If a pin or descriptor
changed, retry returns `rebuild_plan_stale` and requires a new plan. Activation
is redispatched only when marker reconciliation proves the previous generation
is still active and the candidate is intact.

## 10. Runner and SQL adapter contract

### 10.1 Runner work

Runner work for a persisted SQL target adds:

- operation type `normal_materialization` or `rebuild_candidate`;
- pinned target generation id;
- logical target id;
- active relation descriptor;
- write relation override for the candidate;
- pinned upstream generation/relation descriptors;
- rebuild operation/action/item ids; and
- activation/reconciliation request variants with idempotency token.

Runner validation rejects mismatched manifest/package/generation/relation
identity before SQL execution. Rendered `target()` references, materialization
writes, physical inspection, and authored checks use the write override for a
candidate. Dependency reads continue to use pinned active upstream relations.

The runner reports structured success, safe failure, and unknown outcome. It
never writes PostgreSQL control-plane tables and never decides that a candidate
becomes active.

### 10.2 Adapter capabilities

Add an explicit generation capability contract alongside `Favn.SQL.Adapter`.
Capabilities report:

- transactional DDL/rename support;
- isolated candidate relations;
- physical schema inspection/fingerprinting;
- atomic active/candidate swap;
- generation marker read/write in the same activation transaction;
- idempotent candidate discard; and
- optional snapshot support.

The focused generation behavior provides callbacks for physical inspection,
atomic activation, marker reconciliation, and discard. Ordinary candidate data
writes continue through existing materialization callbacks with the explicit
target override. Unsupported callbacks are never inferred from a general
`transactions: true` flag.

Planning rejects a rebuild before any mutation unless isolation, inspection,
atomic swap, marker reconciliation, and discard are supported. Snapshot support
is recorded when present but is not required.

### 10.3 Atomic data-plane activation

For DuckDB/DuckLake-capable adapters, one data-plane transaction:

1. verifies the expected active generation marker;
2. renames the stable active relation to a deterministic retired relation when
   it exists;
3. renames the candidate relation to the stable active relation name;
4. writes the new active generation and activation token to the sidecar marker;
5. commits; and
6. returns the observed marker and physical fingerprint.

The orchestrator persists activation intent before dispatch. PostgreSQL and the
user data system cannot commit atomically, so after runner return/recovery the
orchestrator reads the marker:

- candidate active with matching token: finish the binding transition;
- previous generation active and candidate intact: activation may be retried
  with the same token;
- neither provable: remain `activation_unknown` and block writes.

No code path assumes a lost runner reply means rollback.

### 10.4 Retired relation cleanup

After the PostgreSQL active binding is durably updated, cleanup removes the
retired relation through an idempotent adapter call. Until that point it is
retained for reconciliation. Cleanup failure is recorded and retried safely;
it does not obscure a successful activation. Candidate relations from failed or
cancelled pre-activation operations are also discarded only after unknown
outcomes are resolved.

## 11. Concurrency and admission

One active write operation may own a workspace/target at a time. Rebuild plans
acquire all targets they may mutate in sorted target-id order to avoid lock-order
deadlocks. Leases are fenced and renewed by the dispatcher; recovery may take
over only with a higher fencing token after expiry.

Ordinary materialization admission checks:

1. target compatibility is `ready`, `uninitialized`, or `rebuild_available`;
2. the requested output generation equals the binding/pending initial
   generation;
3. no target operation lock conflicts;
4. active input generations match the pinned plan; and
5. the existing materialization claim can be acquired.

Materialization claims still prevent duplicate work for one run/window. They do
not replace the generation operation lock, which protects the multi-run rebuild
lifecycle and activation.

Reads continue against stable active relations while a candidate builds.
Scheduled/manual writes to a rebuilding target or any target locked for a
downstream action return a stable `target_operation_in_progress` admission
error. Runs whose selected graph depends on a `rebuild_required` or unknown
target fail preflight with the exact blocked path. Unrelated graphs proceed.

Recovery scans non-terminal operations, expired dispatcher leases, unknown
child outcomes, activation intents without final bindings, and pending cleanup.
It resumes only from persisted checkpoints and observed data-plane markers.

## 12. Public orchestrator facade and private API

`FavnOrchestrator` remains the only boundary used by `favn_view`. Add typed
facade functions for:

- coverage summary and cursor-paged missing windows;
- missing-window backfill planning/submission;
- rebuild plan creation;
- rebuild start from a plan id/hash;
- rebuild detail/list/progress;
- cancel, retry, and explicit reconciliation; and
- target compatibility/generation detail included in asset catalogue/detail.

Facade inputs require `OperatorContext`; authorization happens before domain or
store work. Returned DTOs contain no Ecto structs, adapter sessions, secrets, or
unbounded payloads.

Add `/api/orchestrator/v1/coverage` and
`/api/orchestrator/v1/rebuilds` routers following existing auth, response,
pagination, audit, mutation-admission, and idempotent-command conventions:

```text
GET  /coverage/assets/:target_id
GET  /coverage/assets/:target_id/missing
POST /coverage/assets/:target_id/backfill/plan
POST /coverage/assets/:target_id/backfill

POST /rebuilds/plan
POST /rebuilds
GET  /rebuilds
GET  /rebuilds/:operation_id
GET  /rebuilds/:operation_id/items
POST /rebuilds/:operation_id/cancel
POST /rebuilds/:operation_id/retry
POST /rebuilds/:operation_id/reconcile
```

Mutations require the existing idempotency header. Rebuild start body includes
`plan_id`, `plan_hash`, and reason/approval confirmation. List/item endpoints
use opaque cursor pagination with default 100 and maximum 200, matching current
backfill API conventions.

Stable API conflicts include `rebuild_required`, `target_drift`,
`operator_decision_required`, `rebuild_plan_stale`,
`target_operation_in_progress`, `pinned_input_changed`,
`coverage_selection_stale`, `coverage_window_limit_exceeded`,
`rebuild_not_supported`, and `activation_outcome_unknown`. Validation errors are
422, missing resources 404, authorization 403, stale/conflicting state 409, and
accepted asynchronous commands 202.

All mutations record actor, session, service identity, idempotency replay state,
operation/target, reason, outcome, and bounded structured details in the
existing audit store.

## 13. Mix tasks

Add a public task with explicit plan/start separation:

```text
mix favn.rebuild plan MyApp.Assets.Monthly --reason "contract changed"
mix favn.rebuild start PLAN_ID --plan-hash HASH
mix favn.rebuild status OPERATION_ID
mix favn.rebuild cancel OPERATION_ID --reason "operator request"
mix favn.rebuild retry OPERATION_ID
```

For named multi-assets, the existing `Module:name` target syntax is used. The
task resolves through the active manifest and calls the private orchestrator API
through `favn_local`; it does not compile/execute SQL or access PostgreSQL
directly.

`plan` prints root identity, declared/effective coverage, evaluated range,
window count, active/candidate generations, compatibility diff, pinned inputs,
adapter capabilities, every downstream action, and the plan id/hash. `start`
accepts only the exact persisted plan id/hash. There is no `--from`, `--to`,
automatic approval, or deployment trigger for rebuild.

Extend `mix favn.backfill` with an asset missing-window plan/submit command that
prints the exact frozen keys and coverage checksum before mutation. Existing
explicit range submission stays exact and does not expose a lookback flag.

Task parsers, help text, output redaction, local HTTP client, timeout handling,
and public-task inventory tests are updated together.

## 14. Operator UI

### 14.1 Asset catalogue and detail

Catalogue rows show coverage `complete`, `incomplete`, or `unknown` separately
from freshness/health, and show `rebuild_required` as a blocking target state.

Asset detail shows:

- effective asset timezone and provenance;
- selected pipeline timezone and a mismatch warning when applicable;
- declared and effective coverage start;
- evaluated expected-through window and evaluation time;
- availability delay as explanatory text;
- expected, covered, and missing counts;
- cursor-paged exact gaps;
- active generation id and physical/desired compatibility;
- a missing-window backfill plan action when a valid run context exists; and
- a prominent blocking rebuild banner with reason, structured schema/identity
  diff, downstream impact, and “Plan rebuild” action.

When an asset is selected by multiple pipelines and no run context is selected,
the UI requires the operator to choose a context before backfill submission. It
does not guess dependency or pipeline semantics.

### 14.2 Rebuild workflow

Add authenticated rebuild list/detail LiveViews and routes. The plan flow:

1. operator enters a required reason and requests a plan;
2. UI renders the immutable root range, candidate identity, capability checks,
   downstream actions, and warnings;
3. admin confirms the exact plan hash;
4. UI starts the operation and navigates to progress;
5. progress shows phase, window/action counts, active work, failures, validation,
   activation/reconciliation, and cleanup; and
6. allowed cancel/retry/reconcile actions reflect server-provided permissions and
   state, never locally inferred state.

The existing asset run button is disabled with an actionable explanation while
the target is `rebuild_required`, drifted, undecided, or locked. The banner
remains after a manifest schema change until manual rebuild activation succeeds.

LiveViews call only `FavnOrchestrator` facade functions. Reusable page-level
components receive bounded DTOs. Use existing design-system primitives; add
component stories and responsive/accessible empty, blocked, progress, success,
failure, and unknown-outcome states. No browser code calls storage, scheduler,
runner, compiler, or adapter internals.

## 15. Observability, limits, and sensitive data

Telemetry covers coverage query latency/counts, compatibility classification,
plan size, lock contention, item dispatch/outcome, validation duration,
activation/reconciliation, cleanup, and terminal operation outcome. Labels use
bounded status/reason/action values; target ids, window keys, plan ids, SQL, and
errors are metadata/log fields rather than metric labels.

Structured logs include workspace, operation, target, generation, action/item,
run, attempt, fencing token, and activation token where safe. SQL text,
credentials, runtime-input values, and connection secrets are never logged or
persisted in descriptors, plans, DTOs, or audit.

Initial hard limits are:

- 100,000 expected/rebuild windows per asset evaluation;
- 10,000 windows per missing-window backfill submission;
- 500 windows per planning/dispatch batch;
- 500 maximum coverage-gap page size;
- 200 maximum API operation/item page size; and
- existing repository payload/identifier/error limits for all persisted JSON
  and strings.

Limit failures are explicit before data-plane mutation. Tests cover limit-1,
limit, and limit+1.

## 16. Implementation slices and ownership

Implement in this order so each slice has a testable owner and later slices do
not create compatibility shims for unfinished contracts.

### Slice 1: DSL, environment, and manifest version 11

- Add coverage declaration/import/inheritance to `favn_authoring`.
- Add coverage core structs, validation, boundary normalization, and duration
  handling.
- Remove asset lookback and add pipeline lookback/selection.
- Resolve application default timezone and coverage scope during manifest
  construction.
- Resolve freshness timezone from the effective asset timezone/default.
- Add provenance and target descriptors to manifest assets/pipelines/schedules.
- Update serialization/rehydration, contract locks, stable manifest identity,
  and manifest/runner contract versions.

### Slice 2: exact window planning

- Make schedule resolution return a scheduled selection with pipeline lookback.
- Make manual and backfill resolution return exact selections.
- Remove all lookback expansion from `Favn.Assets.Planner`.
- Persist requested/expansion/effective selection in run input and DTOs.
- Add timezone mismatch and early-cron/availability compile warnings.

### Slice 3: PostgreSQL generation foundation

- Add generation/binding/operation/plan-item/window/lock tables and constraints.
- Make materialization, window, and freshness evidence generation-aware.
- Add persistence command/query/result contracts, codecs, instrumentation,
  fixtures, and projector updates.
- Add active-generation reads to normal planning/admission.

### Slice 4: coverage read model and missing backfill

- Implement pure expected-bound/count/page logic in core.
- Add bounded PostgreSQL success counts and batch key lookup.
- Add orchestrator coverage service/facade and generation-aware DTOs.
- Add stale-safe missing-window backfill plan/submission.
- Add private API, CLI, catalogue/detail UI, stories, and tests.

### Slice 5: compatibility and blocking

- Inspect physical relations through the runner boundary.
- Compute desired/active/physical fingerprints and structured diffs.
- Persist target compatibility during manifest activation/reconciliation.
- Enforce target and dependent-path preflight blocking without affecting
  unrelated graphs.
- Surface blocking status and plan entry points in API/CLI/UI.

### Slice 6: runner and adapter generations

- Extend runner work/results with generation and relation overrides.
- Add explicit adapter generation capabilities and behavior.
- Implement candidate inspection, swap marker, reconciliation, and discard for
  supported DuckDB/DuckLake adapters.
- Add fault-injection tests around pre-commit, committed/lost-reply, rollback,
  marker mismatch, and cleanup.

### Slice 7: rebuild and downstream repair orchestration

- Implement immutable planning, mapping proof, plan hashing, and stale checks.
- Implement fenced multi-target locks, dispatcher, frozen item claims, progress,
  validation, activation saga, cancellation, retry/resume, reconciliation, and
  recovery.
- Reuse exact backfill execution for proven downstream backfill actions.
- Execute downstream rebuilds topologically with independent generations.

### Slice 8: complete operator surfaces and documentation

- Finish rebuild routers, facade, Mix task, LiveViews, components, stories, auth,
  audit, telemetry, and error mapping.
- Update canonical public and internal documentation listed below.
- Run the full focused and umbrella verification matrix.

## 17. Required tests

### Authoring and core

- Coverage accepted on assets, SQL assets, multi-assets, and namespaces.
- Closest replacement, omission inheritance, and explicit nil clearing.
- Compile error for non-windowed effective coverage and invalid options.
- Date/DateTime normalization for hour/day/month/year and named timezones.
- DST gap/overlap, month/year rollover, inclusive fixed through, current, latest
  closed, delay zero, delay boundary-1/exact/+1, and empty effective range.
- Environment floor later/earlier/equal to declared start and invalid config.
- Independent timezone resolution/provenance and mismatch warnings.
- Asset lookback rejection, pipeline lookback validation, and manifest roundtrip.
- Stable fingerprints proving provenance/delay/scope metadata behavior and
  effective kind/timezone incompatibility.

### Planner and scheduling

- Same monthly pipeline: scheduled July with lookback one plans June+July;
  manual July plans July; backfill March-May plans exactly March-May.
- Lookback applied once through mixed asset kinds and dependencies.
- Pipeline timezone never inherits schedule timezone.
- Asset timezone never inherits pipeline timezone.
- Requested/expanded/effective anchors survive serialization and run snapshots.
- Cron-before-availability emits a warning but still creates the occurrence.

### Coverage and PostgreSQL

- Complete, incomplete, and every unknown reason.
- Successful zero-row materialization covered; failure/cancel/invalid/unknown not
  covered.
- Fresh-but-incomplete and stale-but-complete integration cases.
- Candidate and retired generations excluded from active reads.
- Activation switches visible evidence without copying rows.
- Count and cursor gap paging at boundaries, stale cursor rejection, 100,000
  limit, and 10,000 submission limit.
- Moving scope/start does not delete evidence.
- Migration constraints, exact schema gate, payload bounds, query plans/indexes,
  workspace isolation, and transaction rollback.

### Compatibility and rebuild

- Metadata-only ready; SQL change rebuild available; ordered column/type/
  nullability, grain, materialization, relation, adapter/connection, kind, and
  timezone changes rebuild required; physical mismatch drift; unmanaged target
  operator decision.
- Incompatible affected path blocked while unrelated path runs.
- Plan hash determinism and stale detection for every pinned input.
- Full-table and monthly incremental successful rebuilds.
- Candidate authoritative empty windows.
- Failed build, failed authored check, failed physical validation, and
  cancellation leave the active target/evidence unchanged.
- Safe resume skips successes; unsafe/unknown write reconciles before retry.
- Concurrent normal write/rebuild conflict, fencing takeover, lock expiry, and
  sorted multi-target locking.
- Data-plane activation commit, rollback, lost reply after commit, marker says
  previous, marker says candidate, marker mismatch, and explicit unknown state.
- Cleanup success/failure/retry.
- Direct one-to-one partition-local downstream backfill, full-replacement
  downstream rebuild, incompatible downstream rebuild, unrelated no action, and
  unproven mapping operator decision.
- Upstream activation followed by downstream failure and checkpointed resume.
- Recovery after orchestrator and runner restart in every non-terminal phase.

### API, CLI, and UI

- Auth role matrix, workspace isolation, idempotent replay, validation/status
  mapping, bounded pagination, and audit records.
- CLI parse/help/output/redaction for every plan/start/status/cancel/retry path.
- Asset coverage and rebuild banner rendering across desktop/mobile states.
- Missing-window stale-plan handling and run-context selection.
- Rebuild plan confirmation, progress, cancel/retry/reconcile permissions,
  failure, success, activation unknown, and cleanup warning.
- LiveView boundary tests proving only the public orchestrator facade is used.
- Storybook stories plus browser acceptance for the critical manual rebuild
  path.

## 18. Documentation updates in the implementation PR

Update each contract once in its canonical location and link elsewhere:

- `apps/favn/guides/authoring-assets.md`: final coverage and window DSL,
  namespace replacement, coverage/freshness distinction, and pipeline lookback;
- `apps/favn/guides/configuration.md`: `default_timezone` and
  `coverage_scope`;
- `apps/favn/guides/local-development.md`: coverage/rebuild/backfill Mix tasks;
- manifest/deployment guide: environment resolution and schema 11;
- public moduledocs/typespecs and `Favn.AI` routes for every DSL/workflow;
- `docs/operators/runs-and-schedules.md`: exact selections, missing-window
  backfill, rebuild operation, cancellation/retry/reconciliation;
- `docs/architecture/`: target-generation, activation saga, and ownership
  invariants;
- `docs/storage/postgresql/`: new authoritative tables, generation-aware
  projections, constraints, indexes, and recovery;
- relevant `docs/structure/` pages for app ownership and public boundaries;
- `docs/FEATURES.md` for delivered behavior; and
- `docs/ROADMAP.md` plus issue links for remaining forward work.

After implementation is merged, archive or remove this temporary plan according
to the documentation guide; it must not become a second permanent contract.

## 19. Verification commands

Run the narrow owning-app tests during each slice. Before the pull request is
ready, run at least:

```bash
mix format
mix compile --warnings-as-errors

MIX_ENV=test mix do --app favn_authoring cmd mix test --no-compile --exclude acceptance --exclude container --exclude slow --exclude browser
MIX_ENV=test mix do --app favn_core cmd mix test --no-compile --exclude acceptance --exclude container --exclude slow --exclude browser
MIX_ENV=test mix do --app favn_orchestrator cmd mix test --no-compile --exclude acceptance --exclude container --exclude slow --exclude browser
MIX_ENV=test mix do --app favn_storage_postgres cmd mix test --no-compile --exclude acceptance --exclude container --exclude slow --exclude browser
MIX_ENV=test mix do --app favn_runner cmd mix test --no-compile --exclude acceptance --exclude container --exclude slow --exclude browser
MIX_ENV=test mix do --app favn cmd mix test --no-compile --exclude acceptance --exclude container --exclude slow --exclude browser
MIX_ENV=test mix do --app favn_local cmd mix test --no-compile --exclude acceptance --exclude container --exclude slow --exclude browser
MIX_ENV=test mix do --app favn_view cmd mix test --no-compile --exclude acceptance --exclude container --exclude slow --exclude browser

mix test --no-compile --timeout 1200000
mix test.acceptance
mix test.slow
elixir scripts/check_test_tag_tiers.exs
git diff --check
```

Run adapter integration and fault-injection tests against real supported
DuckDB/DuckLake services, and run the critical UI workflow through Storybook and
browser acceptance. Any intentionally skipped tier must be named in the pull
request with the exact reason and remaining risk.

## 20. Reviewer checklist

- [ ] The shipped DSL and config exactly match section 3.
- [ ] Effective values are resolved into the manifest; runtime code does not
      read environment coverage/timezone configuration.
- [ ] Schedule, pipeline, asset, and freshness timezone ownership is independent.
- [ ] Coverage inheritance replaces whole policies and supports explicit nil.
- [ ] Coverage delay affects expectedness only.
- [ ] Manual/backfill scope is exact and scheduled lookback is pipeline-owned.
- [ ] Manifest and runner contract version 11 reject old asset-lookback shapes.
- [ ] Active/candidate/retired generation evidence cannot be mixed.
- [ ] Incompatible targets block before ordinary mutation and explain why.
- [ ] The active relation survives every pre-activation failure/cancellation.
- [ ] Activation intent and sidecar reconciliation cover lost replies without
      blind retry.
- [ ] Downstream backfill is chosen only with a recorded mapping proof.
- [ ] All operation transitions, locks, attempts, actor actions, and unknown
      outcomes are durable and auditable.
- [ ] View uses only `FavnOrchestrator`; runner never writes control-plane state.
- [ ] PostgreSQL is the only control-plane backend touched by the change.
- [ ] Limits, pagination, payload bounds, auth, workspace isolation, telemetry,
      and secret redaction are tested.
- [ ] Public docs, typespecs, `Favn.AI`, feature status, roadmap, and issue scope
      agree with the implementation.
