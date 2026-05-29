# PostgreSQL Storage Foundation Plan (v0.5)

## Purpose

This document defines the recommended PostgreSQL storage architecture for Favn v0.5.

This is not a SQLite port.

PostgreSQL should become the production persistence foundation for:

- single-node production usage now
- future multinode scheduler and runtime coordination later
- additive queue/claim/lease features without a storage rewrite

The design is intentionally opinionated:

1. Important runtime state must be queryable and inspectable.
2. PostgreSQL correctness should come from normal transactional and relational patterns, not process-local assumptions.
3. Future distributed coordination should add tables and write paths, not replace the storage model.

---

## Summary Recommendation

Adopt a normalized PostgreSQL storage model with JSONB support:

- `favn_runs` stores one authoritative run row with indexed lifecycle fields and a versioned JSON snapshot cache.
- `favn_run_nodes` stores one row per persisted node key (`{asset_ref, window_key}`) with structured execution state.
- `favn_asset_window_latest` stores the latest successful materialization pointer per asset/window.
- `favn_scheduler_cursors` stores durable scheduler progress state.
- future multinode ownership should use additive tables such as `favn_scheduler_leases` and `favn_run_claims`, not overload the cursor rows.

Do not store Erlang term blobs in PostgreSQL.

If a full run snapshot is retained, store it as a versioned canonical JSONB payload and treat it as a compatibility/read-model cache, not the primary long-term contract.

---

## 1. Recommended Storage Architecture

## 1.1 Core persisted entities

Required in v0.5:

1. `favn_runs`
2. `favn_run_nodes`
3. `favn_asset_window_latest`
4. `favn_scheduler_cursors`

Explicitly deferred but designed for later additive introduction:

1. `favn_scheduler_leases`
2. `favn_run_queue`
3. `favn_run_claims`
4. `favn_run_keys`
5. `favn_materialization_history`

## 1.2 Authority model

The authority split should be:

- `favn_runs`: run-level authority
- `favn_run_nodes`: node/window execution authority
- `favn_asset_window_latest`: latest-success read model for freshness and missing-window checks
- `favn_scheduler_cursors`: durable scheduler progress cursor

The current SQLite model stores a full `%Favn.Run{}` blob and derives a few indexed tables. PostgreSQL should invert that priority:

- structured rows are primary
- JSONB is supplemental
- opaque binary blobs are not used

## 1.3 Run storage model

### Keep

Keep the public `%Favn.Run{}` shape for `get_run/1`, `list_runs/1`, rerun, and current runtime compatibility.

### Change

Do not make a serialized full run blob the durable source of truth.

Instead:

- store run summary fields in relational columns
- store complex but still inspectable payloads as JSONB
- store node results in a dedicated table
- optionally store a versioned `snapshot_json` JSONB column on `favn_runs` to reconstruct `%Favn.Run{}` cheaply

### Recommendation on full snapshots

Replace the current Erlang-term snapshot approach with this model:

- `snapshot_json` JSONB: allowed
- `snapshot_version` integer: required
- `snapshot_hash` text: required

Meaning:

- `snapshot_json` is a compatibility cache and debugging aid
- `snapshot_version` allows serializer evolution
- `snapshot_hash` enables idempotency and same-sequence conflict detection

`get_run/1` may initially deserialize from `snapshot_json` for simplicity.
That is acceptable in v0.5 as long as the important queryable state does not depend on the snapshot being opaque.

## 1.4 Relational columns vs JSONB

### `favn_runs` relational columns

These fields should be first-class columns because they are core query and concurrency keys:

- `id`
- `status`
- `submit_kind`
- `replay_mode`
- `event_seq`
- `write_seq`
- `started_at`
- `finished_at`
- `max_concurrency`
- `timeout_ms`
- `rerun_of_run_id`
- `parent_run_id`
- `root_run_id`
- `lineage_depth`
- `inserted_at`
- `updated_at`
- `snapshot_version`
- `snapshot_hash`

### `favn_runs` JSONB columns

These values are useful to retain, are inspectable, but are not worth full normalization in v0.5:

- `target_refs_json`
- `submit_ref_json`
- `params_json`
- `retry_policy_json`
- `pipeline_json`
- `pipeline_context_json`
- `plan_json`
- `backfill_json`
- `operator_reason_json`
- `error_json`
- `terminal_reason_json`
- `snapshot_json`

### `favn_run_nodes` relational columns

These fields should be normal columns:

- `id`
- `run_id`
- `ref_module`
- `ref_name`
- `window_key_text`
- `stage`
- `status`
- `attempt_count`
- `max_attempts`
- `next_retry_at`
- `started_at`
- `finished_at`
- `duration_ms`
- `inserted_at`
- `updated_at`

### `favn_run_nodes` JSONB columns

- `window_key_json`
- `meta_json`
- `error_json`
- `attempts_json`

This is the right split because:

- filters, ordering, joins, and conflicts use relational fields
- complex payloads remain inspectable without forcing premature table explosion

## 1.5 Window and latest-state modeling

The SQLite `window_latest_results` table should not be copied directly.

For PostgreSQL, the correct semantic read model is:

- latest successful materialization per `{asset_ref, window_key}`

So `favn_asset_window_latest` should only track successful node results.

That means failed retries or later failed runs do not erase knowledge of the latest successful materialization.

This better supports:

- freshness checks
- missing-window checks
- future operator inspection
- future materialization history additions

## 1.6 Scheduler state modeling

Current SQLite state mixes durable scheduler progress with fields that later look ownership-like.

For PostgreSQL, model scheduler persistence as a durable progress cursor, not as a lease record.

`favn_scheduler_cursors` should track:

- schedule stream identity
- last evaluation cursor
- last due cursor
- last submitted cursor
- currently known submitted run id
- queued due cursor
- schedule fingerprint
- optimistic `version`

Future ownership and election should be separate:

- `favn_scheduler_leases` will later track owner node, lease token, heartbeat, and expiry
- `favn_scheduler_cursors` remains the durable progress record

This separation is critical. Cursor state and lease ownership should not be the same row contract.

---

## 2. Recommended Behaviour Boundary Changes

## 2.1 Current contract is not quite sufficient

The current `Favn.Storage.Adapter` behaviour is enough for single-node memory and SQLite, but it is too weak as the long-term PostgreSQL foundation.

The two main problems are:

1. `put_run/2` has no explicit stale-write or sequence-conflict semantics.
2. scheduler state is keyed too narrowly and has no optimistic version semantics.

## 2.2 Recommended adapter changes now

Keep public `Favn` APIs stable.

Strengthen the internal adapter boundary in v0.5.

### Recommendation A: make run writes sequence-aware

Revise the adapter contract so run persistence is defined as a monotonic snapshot upsert.

Required semantics:

- new run id inserts the run
- higher `event_seq` replaces the stored run state
- same `event_seq` and same `snapshot_hash` is idempotent success
- same `event_seq` and different `snapshot_hash` is `{:error, :conflicting_snapshot}`
- lower `event_seq` is `{:error, :stale_write}` or an explicit no-op result

I recommend returning explicit conflicts, not silently succeeding.

That gives later multinode recovery and claim flows a safe storage primitive instead of assuming a single writer.

### Recommendation B: key scheduler persistence by schedule stream, not only pipeline module

Change scheduler storage identity from:

- `pipeline_module`

to:

- `{pipeline_module, schedule_id}`

Reason:

- the struct already contains `schedule_id`
- future multinode ownership and per-schedule leases need a stable schedule-stream key
- one-row-per-pipeline is too narrow for long-term scheduler evolution

### Recommendation C: add optimistic version semantics for scheduler writes

Add a `version` field to stored scheduler state and support compare-and-set updates internally.

Even if the single-node scheduler still uses simple writes in v0.5, the table and boundary should already support:

- `expected_version`
- increment-on-write
- stale update rejection

This allows later lease-based schedulers to become additive.

## 2.3 Recommended public/internal layering

Public Favn APIs should remain:

- `Favn.get_run/1`
- `Favn.list_runs/1`
- `Favn.rerun/2` behavior
- current freshness and scheduler-facing behavior

Internal storage should gain clearer semantics.

Recommended internal responsibilities:

1. `Favn.Storage` remains the stable facade.
2. Add a PostgreSQL serializer/mapper layer that converts `%Favn.Run{}` and scheduler structs into canonical DB payloads.
3. Treat PostgreSQL writes as transactional projection updates, not blob persistence.

## 2.4 Suggested module additions

Recommended new module tree:

```text
lib/
  favn/
    storage/
      adapter/
        postgres.ex
      postgres/
        repo.ex
        supervisor.ex
        migrations.ex
        migrations/
          create_foundation.ex
        run_serializer.ex
        run_mapper.ex
        scheduler_mapper.ex
        queries.ex
```

Keep serializer and mapper logic out of the adapter module. The adapter should orchestrate DB operations, not own all data-shape code.

---

## 3. Concurrency And Transaction Model

## 3.1 Core invariants

These invariants should hold in PostgreSQL from v0.5 onward:

1. A run row never regresses to an older `event_seq`.
2. Equal `event_seq` writes must be idempotent or rejected as conflicting.
3. `list_runs/1` ordering is deterministic by persisted write order, not timestamp coincidence.
4. Node rows for a persisted run snapshot are transactionally consistent with the run row.
5. Latest-window rows only move forward to a newer successful result.
6. Scheduler cursor updates are monotonic and versionable.

## 3.2 Run write transaction

Each `put_run` should execute in one transaction:

1. compute canonical serialized payloads and `snapshot_hash`
2. allocate a new `write_seq` from a Postgres sequence
3. upsert `favn_runs` with event-seq conflict rules
4. if the run row insert/update was accepted:
   - replace that run's `favn_run_nodes` rows
   - upsert affected `favn_asset_window_latest` rows for successful nodes only
5. commit

If the run row was rejected as stale/conflicting, do not touch node or latest rows.

## 3.3 Deterministic write ordering

Keep a dedicated global Postgres sequence, for example:

- `favn_run_write_seq`

Use its value as `favn_runs.write_seq` for every accepted snapshot write.

Then `list_runs/1` ordering is:

- `ORDER BY write_seq DESC, id DESC`

This is better than relying on timestamps alone and mirrors the intent of the SQLite `updated_seq` workaround using a PostgreSQL-native primitive.

## 3.4 Idempotency model

Idempotency rules should be explicit:

- same `run_id` + same `event_seq` + same `snapshot_hash` => success
- same `run_id` + same `event_seq` + different `snapshot_hash` => conflict
- same `run_id` + lower `event_seq` => stale
- same `run_id` + higher `event_seq` => success

This is important for:

- retrying a crashed persistence attempt safely
- future reconciliation workers
- future duplicate delivery from multinode coordination

## 3.5 Multi-node write expectations

We are not implementing distributed execution now, but the storage model should already assume:

- more than one BEAM node may talk to the same database
- future scheduler or recovery workers may race to write related state

Therefore:

- correctness should come from SQL constraints and transaction conditions
- not from BEAM process uniqueness assumptions
- not from a single-node supervisor tree assumption

## 3.6 Locking guidance

Recommended now:

- use normal row-level concurrency through `INSERT ... ON CONFLICT DO UPDATE ... WHERE ...`
- use transactions for run snapshot projection updates
- use unique constraints as correctness boundaries

Do not use advisory locks as the core correctness mechanism for run writes.

Advisory locks are acceptable later for very specific coordination cases, but they should not be the main storage contract.

## 3.7 Lease-ready patterns to prepare now

Prepare for future lease-based work by choosing patterns that compose well later:

- scheduler cursor rows include a `version`
- schedule identity is stable (`pipeline_module + schedule_id`)
- future ownership uses a separate lease table with fencing token semantics
- future queue/claim rows should use `FOR UPDATE SKIP LOCKED`, not boolean claimed flags

---

## 4. Migration And Repo Strategy

## 4.1 Repo ownership model

Favn should support both:

1. managed internal Repo
2. host-supplied external Repo

### Managed Repo

This should remain the easiest path.

Recommended modules:

- `Favn.Storage.Postgres.Repo`
- `Favn.Storage.Postgres.Supervisor`

Use this when the host just wants Favn to run its own storage connection.

### External Repo

This should also be supported in v0.5, not deferred indefinitely.

Reason:

- production Postgres deployments often already have a Repo
- migrations are often centralized
- observability and pool tuning often live in the host app

Recommended adapter config modes:

```elixir
config :favn,
  storage_adapter: Favn.Storage.Adapter.Postgres,
  storage_adapter_opts: [
    repo_mode: :managed,
    repo_config: [
      hostname: "localhost",
      port: 5432,
      database: "favn",
      username: "postgres",
      password: "postgres",
      pool_size: 10,
      ssl: true
    ],
    migration_mode: :manual
  ]
```

```elixir
config :favn,
  storage_adapter: Favn.Storage.Adapter.Postgres,
  storage_adapter_opts: [
    repo_mode: :external,
    repo: MyApp.Repo,
    migration_mode: :manual
  ]
```

## 4.2 Migration strategy recommendation

For PostgreSQL, do not auto-run migrations by default.

Recommended default:

- `migration_mode: :manual`

Startup behavior in manual mode:

- verify the required schema exists and is current enough
- fail fast with an actionable error if migrations are missing

Optional convenience mode:

- `migration_mode: :auto`

Use `:auto` only for local development, test, and simple embedded deployments.

This is the right tradeoff because serious PostgreSQL operations usually require explicit migration control.

## 4.3 Migration implementation recommendation

Use Ecto migrations under:

- `priv/favn/storage/postgres/migrations`

Provide:

- `Favn.Storage.Postgres.Migrations.migrate!/1`
- schema-version verification helper used at startup in manual mode

If the managed Repo path is used with `migration_mode: :auto`, the adapter supervisor may run migrations before starting the long-lived repo child, similar to SQLite.

If the external Repo path is used, Favn should never try to own the Repo lifecycle.

---

## 5. Schema Proposal

## 5.1 `favn_runs`

```text
id                    text primary key
status                text not null
submit_kind           text not null
replay_mode           text not null
event_seq             bigint not null
write_seq             bigint not null
started_at            timestamptz not null
finished_at           timestamptz null
max_concurrency       integer not null
timeout_ms            integer null
rerun_of_run_id       text null
parent_run_id         text null
root_run_id           text null
lineage_depth         integer not null default 0
target_refs_json      jsonb not null default '[]'
submit_ref_json       jsonb null
params_json           jsonb not null default '{}'
retry_policy_json     jsonb not null default '{}'
pipeline_json         jsonb null
pipeline_context_json jsonb null
plan_json             jsonb null
backfill_json         jsonb null
operator_reason_json  jsonb null
error_json            jsonb null
terminal_reason_json  jsonb null
snapshot_version      integer not null
snapshot_hash         text not null
snapshot_json         jsonb not null
inserted_at           timestamptz not null default now()
updated_at            timestamptz not null default now()
```

Recommended constraints and indexes:

- check constraint on `status`
- check constraint on `submit_kind`
- check constraint on `replay_mode`
- unique index on `write_seq`
- index on `(status, write_seq desc)`
- index on `(root_run_id, write_seq desc)`
- index on `(parent_run_id, write_seq desc)`
- index on `(rerun_of_run_id, write_seq desc)`
- index on `(started_at desc)`
- index on `(finished_at desc)`

Notes:

- keep `id` as `text` in v0.5 to preserve current run-id behavior and avoid coupling this feature to a UUID runtime change
- do not over-normalize `plan` or `pipeline_context` yet

## 5.2 `favn_run_nodes`

```text
id                    bigserial primary key
run_id                text not null references favn_runs(id) on delete cascade
ref_module            text not null
ref_name              text not null
window_key_text       text not null
window_key_json       jsonb null
stage                 integer not null
status                text not null
attempt_count         integer not null
max_attempts          integer not null
next_retry_at         timestamptz null
started_at            timestamptz null
finished_at           timestamptz null
duration_ms           bigint null
meta_json             jsonb not null default '{}'
error_json            jsonb null
attempts_json         jsonb not null default '[]'
inserted_at           timestamptz not null default now()
updated_at            timestamptz not null default now()
```

Recommended constraints and indexes:

- unique index on `(run_id, ref_module, ref_name, window_key_text)`
- index on `(run_id, stage, ref_module, ref_name, window_key_text)`
- index on `(ref_module, ref_name, window_key_text, finished_at desc)`
- index on `(status, finished_at desc)`
- partial index on successful rows:
  `(ref_module, ref_name, window_key_text, finished_at desc) where status = 'ok'`

Notes:

- store both `window_key_text` and `window_key_json`
- `window_key_text` is the canonical identity and uniqueness key
- `window_key_json` is for inspectability and later API responses

## 5.3 `favn_asset_window_latest`

```text
ref_module               text not null
ref_name                 text not null
window_key_text          text not null
window_key_json          jsonb null
last_run_id              text not null references favn_runs(id) on delete restrict
last_finished_at         timestamptz not null
last_write_seq           bigint not null
updated_at               timestamptz not null default now()
```

Recommended constraints and indexes:

- primary key or unique index on `(ref_module, ref_name, window_key_text)`
- index on `(last_finished_at desc)`
- index on `(last_run_id)`

Important semantic rule:

- only successful node results may populate this table

## 5.4 `favn_scheduler_cursors`

```text
pipeline_module          text not null
schedule_id              text not null
schedule_fingerprint     text not null
last_evaluated_at        timestamptz null
last_due_at              timestamptz null
last_submitted_due_at    timestamptz null
in_flight_run_id         text null
queued_due_at            timestamptz null
version                  bigint not null default 1
inserted_at              timestamptz not null default now()
updated_at               timestamptz not null default now()
```

Recommended constraints and indexes:

- primary key on `(pipeline_module, schedule_id)`
- index on `(schedule_fingerprint)`
- index on `(in_flight_run_id)`

Notes:

- `version` exists now because future lease-aware scheduler updates should be compare-and-set capable
- this table is durable progress state, not ownership state

## 5.5 Future-ready additive tables

Not part of v0.5 implementation, but the schema above is intentionally shaped so these can be added later without redesigning the adapter:

### `favn_scheduler_leases`

For future scheduler ownership and leader-election style coordination:

```text
pipeline_module
schedule_id
owner_node_id
lease_token
lease_expires_at
heartbeat_at
fencing_token
inserted_at
updated_at
```

### `favn_run_claims`

For future run ownership and recovery flows:

```text
run_id
owner_node_id
claim_token
claimed_at
claim_expires_at
heartbeat_at
fencing_token
inserted_at
updated_at
```

### `favn_run_queue`

For future queueing and admission control:

```text
run_id
queue_name
priority
queued_at
available_at
claimed_by
claimed_at
claim_token
status
inserted_at
updated_at
```

These are intentionally additive. The v0.5 core tables should not prevent them.

---

## 6. Implementation Plan

## Phase 1: internal storage contract hardening

1. Update `Favn.Storage.Adapter` semantics for monotonic run writes.
2. Introduce stale/conflicting snapshot result handling in the storage facade.
3. Update scheduler state identity to use schedule stream keys internally.
4. Define canonical JSON serializer/deserializer shapes for `%Favn.Run{}` and `%Favn.Run.AssetResult{}`.

Deliverable:

- storage boundary is ready for PostgreSQL-specific correctness rules

## Phase 2: PostgreSQL repo and migration foundation

1. Add PostgreSQL dependency (`postgrex`) and repo modules.
2. Add managed Repo supervisor.
3. Add migration runner and schema verification helpers.
4. Support both managed and external repo modes.
5. Default migration mode to `:manual`.

Deliverable:

- Favn can talk to PostgreSQL safely with explicit repo ownership and migration behavior

## Phase 3: core schema and adapter write path

1. Create `favn_runs`, `favn_run_nodes`, `favn_asset_window_latest`, and `favn_scheduler_cursors`.
2. Implement canonical serializer and mapper modules.
3. Implement transactional `put_run` with:
   - `event_seq` monotonicity
   - `snapshot_hash` conflict detection
   - `write_seq` generation
   - run-node replacement inside the same transaction
   - latest-success upserts
4. Implement `get_run` and `list_runs`.
5. Implement scheduler cursor read/write.

Deliverable:

- usable PostgreSQL adapter with current public run APIs working

## Phase 4: compatibility and correctness hardening

1. Make freshness and missing-window expectations pass against PostgreSQL.
2. Verify rerun and lineage behavior against PostgreSQL storage.
3. Harden scheduler state behavior and malformed data handling.
4. Improve storage error normalization for Postgres-specific failures.

Deliverable:

- current single-node production workflows behave correctly on PostgreSQL

## Phase 5: operational polish

1. Add docs for config, repo modes, and migration modes.
2. Add startup errors that clearly explain missing migrations or invalid config.
3. Add storage-focused telemetry metadata where useful.

Deliverable:

- production operators have a workable setup story

## First implementation PR vs follow-up PRs

### First implementation PR should include

- PostgreSQL adapter module
- PostgreSQL repo and supervisor foundation
- migration files for the four required tables
- managed repo mode
- external repo mode
- manual migration default and auto migration opt-in
- transactional run write path
- `get_run/1` and `list_runs/1`
- scheduler cursor persistence
- test coverage for correctness and concurrency basics
- roadmap/doc updates

### Follow-up PRs are reasonable for

- optional storage query helpers for future operator APIs
- materialization history table
- explicit scheduler compare-and-set API if not exposed in the first cut
- future queue/claim/lease tables
- later SQLite alignment to the JSON serializer if desired

I do not recommend splitting the first PR so aggressively that it lands a PostgreSQL Repo without the real transactional adapter behavior. The value is in the storage model, not just the connection.

---

## 7. Testing Plan

## 7.1 Correctness tests

- persist and fetch a run with JSONB snapshot reconstruction
- list runs newest-first by `write_seq`
- status filtering and limit filtering
- persist run nodes and latest-success rows correctly
- scheduler cursor read/write round trip
- rerun lineage fields persist correctly

## 7.2 Concurrency tests

- concurrent inserts of many different run ids preserve deterministic list ordering
- higher `event_seq` beats lower `event_seq`
- lower `event_seq` cannot overwrite higher `event_seq`
- same `event_seq` and same `snapshot_hash` is idempotent
- same `event_seq` and different `snapshot_hash` is rejected
- concurrent latest-success upserts for the same asset/window keep the newest success only

## 7.3 Recovery and persistence tests

- repo restart retains runs, node rows, and scheduler cursor rows
- stale write after restart is still rejected
- freshness checks survive restart
- rerun of a stored terminal run still works

## 7.4 Migration tests

- empty database migrates successfully
- manual mode fails fast when schema is missing
- auto mode bootstraps successfully in test/dev-style config
- repeated migration execution is safe

## 7.5 Compatibility tests

Run existing public storage behavior tests against PostgreSQL where practical:

- `Favn.get_run/1`
- `Favn.list_runs/1`
- `Favn.await_run/2`
- `Favn.rerun_run/2`
- freshness helpers
- scheduler runtime persistence expectations

Add dedicated adapter tests rather than relying only on broad integration tests.

---

## 8. Risks And Tradeoffs

## 8.1 Intentional tradeoffs

### Keep JSONB for complex payloads

Tradeoff:

- not every field is fully normalized

Reason:

- `%Favn.Run{}` and plan/pipeline payloads are still evolving
- JSONB keeps storage inspectable without exploding the schema too early

### Keep a run snapshot cache

Tradeoff:

- some data exists both in relational fields and in `snapshot_json`

Reason:

- preserves public compatibility cheaply
- avoids forcing a full relational reassembler in the first Postgres cut
- still far better than Erlang blobs because JSONB is inspectable and versioned

### Do not add queue/claim/lease tables yet

Tradeoff:

- multinode coordination is not immediately available

Reason:

- those features are out of scope
- the important goal here is to avoid blocking them later

## 8.2 Specific risks

### Serializer drift risk

If the canonical JSON serializer is poorly defined, later struct changes could become painful.

Mitigation:

- define explicit serializer modules
- add `snapshot_version`
- keep JSON shapes boring and stable

### Scheduler identity migration risk

Moving from pipeline-only keys to schedule-stream keys may require touching current scheduler internals.

Mitigation:

- make the change now, before PostgreSQL is shipped broadly

### Overreliance on snapshot JSON risk

If `get_run/1` only reads `snapshot_json` forever, the design could drift back toward snapshot-first storage.

Mitigation:

- treat `snapshot_json` as a compatibility cache in docs and code
- keep all important query patterns on real columns/tables

## 8.3 Decisions specifically made to avoid future redesign

These choices are deliberate so multinode work can be additive later:

- no Erlang term blobs
- monotonic `event_seq` run writes
- same-sequence conflict detection via `snapshot_hash`
- dedicated global `write_seq`
- node state in a dedicated table
- latest-success window table, not latest-anything
- schedule-stream identity, not pipeline-only identity
- cursor and lease responsibilities separated conceptually now
- managed repo and external repo both supported
- manual migration mode is the production default

---

## Recommended Final Feature Scope

The implementation feature request for this work should include:

1. PostgreSQL adapter with the normalized schema in this document
2. JSON serializer/deserializer for run snapshots
3. monotonic `event_seq` write semantics and conflict detection
4. `write_seq`-based deterministic run ordering
5. latest-success asset/window read model
6. scheduler cursor persistence keyed by schedule stream
7. managed repo support
8. optional external repo support
9. manual migrations by default with auto migration opt-in
10. focused correctness and concurrency test coverage

## Explicitly Excluded From This Implementation

To avoid overbuilding, explicitly exclude:

1. distributed execution
2. leader election
3. scheduler lease implementation
4. queueing and admission control implementation
5. run claim implementation
6. run deduplication / run keys implementation
7. materialization history implementation
8. persisted run event log / event sourcing
9. generic operator UI/query API layer

Those should be follow-on features built on this storage foundation.

## Clarifications Still Worth Confirming Before Coding

1. `schedule_id` should become part of the durable scheduler storage key. If current pipelines can omit it, define a canonical default rather than keeping pipeline-only identity.
2. External Repo support is recommended in v0.5. Confirm whether it belongs in the first PR or an immediate follow-up PR.
3. The first Postgres cut should use JSONB snapshots, not Erlang term blobs. This should be treated as a locked design choice before implementation starts.
