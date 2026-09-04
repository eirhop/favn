# Target Generations And Rebuilds

Reader: contributors changing persisted-target compatibility, rebuild planning,
or generation activation.

Documentation type: architecture explanation.

Favn keeps the current readable target available while building an incompatible
replacement. The control plane owns durable intent and recovery state; the
runner owns data-plane inspection, candidate writes, activation, marker reads,
and discard operations.

## Generation identity

Every managed persisted target has a workspace-scoped binding. The binding
records the desired manifest descriptor, the active physical generation, the
last authoritative physical fingerprint, and a compatibility classification.
Ordinary run plans and materialization claims pin that generation. Coverage and
freshness evidence are also generation-scoped, so retiring a generation cannot
make its evidence look current.

Every non-persisted logical asset has a separate workspace-scoped evidence
binding. Deployment initializes the binding once from the active manifest's
semantic generation and never replaces it during ordinary manifest activation
or runner release changes. Freshness and coverage therefore survive definition
changes until an operator explicitly recomputes the asset. The manifest's
semantic generation remains immutable definition provenance, not the active
freshness identity.

The migration that introduces these bindings seeds currently active
non-persisted assets from their existing manifest generation. Existing
freshness, coverage, materialization, and window rows remain untouched.
Deployment initializes bindings for newly selected assets in bounded batches.
Bindings remain when an asset is temporarily removed so re-adding the same
logical target does not discard its evidence. The materialization store verifies
the exact workspace, target, and evidence identity transactionally before
creating a non-persisted claim.

Non-persisted assets do not participate in physical candidate activation.
Persisted target bindings and rebuild generation isolation remain separate:
retiring a physical generation can never make its evidence current.

## Compatibility and admission

Manifest activation compares the desired descriptor with both the active
generation and a fresh physical inspection. `ready`, `uninitialized`, and
`rebuild_available` permit ordinary writes. `rebuild_required`,
`unexpected_drift`, and `operator_decision` block only dependency paths that
include the affected target. The active deployment and readable generation stay
available for diagnosis.

Admission rechecks compatibility immediately before a persisted write. A
deployment-time result is operator evidence, not permission to write forever.
Physical inspection always compares ordered column names and types. It compares
contract nullability only when the adapter marks that metadata reliable.
Recorded physical fingerprints remain exact observation signatures.

Runner release changes do not reject the whole manifest because the active
generation was created by an older runner release. The new runner inspects the
persisted active physical relation directly through the desired manifest; it
does not load old executable code. Missing or drifted physical state takes
precedence over descriptor differences. Once inspection proves the recorded
active generation is intact, immutable active-versus-desired descriptor
differences determine whether a rebuild is required.

The same rule applies when the active generation points to an immutable
manifest whose full runtime schema is no longer activatable. Compatibility
planning reads only the selected standalone target descriptors, in bounded
batches, and requires the persisted descriptor hash to match the active
binding. The persisted decoder preserves the original schema-1 canonical shape
and hash semantics used by RC9 while current manifests still require schema 2;
unknown descriptor schemas remain invalid. It never registers or executes the
historical manifest. A complete
descriptor hash may differ because of non-semantic envelope metadata; the named
compatibility fields still decide `ready`, `rebuild_available`, or
`rebuild_required`. Missing, malformed, or mismatched historical descriptor
evidence remains `operator_decision`.

When physical inspection is temporarily unavailable, activation persists the
bounded `physical_inspection_unavailable` decision and returns an unresolved
inspection summary. After correcting runner or data-system availability, the
operator repeats manifest activation with a new idempotency key. Reusing the
old key intentionally replays the old audited command result. The repeated
activation reuses the durable inspection-task identity, reads its live state,
and safely requeues a terminal `safe_to_retry` failure.

A target without an active generation has nothing to rebuild. Operator views
must offer activation/inspection retry guidance for that state, not a rebuild
action. Rebuild remains available only when an active generation exists.

## Interrupted initial-generation recovery

Initial activation happens after the first successful write. The run process
persists the step outcome and the completed materialization claim, then a
supervised worker inspects the physical relation through runner tasks and asks
the generation store to record the fingerprint and active binding. The run
process never blocks on those tasks, so its ownership lease keeps renewing while
an inspection is queued behind other runner work. If the run is cancelled while
the worker is pending, the binding stays `uninitialized` and the recovery
workflow below applies.

An initial materialization can commit in the data system while its control-plane
binding remains incomplete. Recovery is a separate ownership-restoration
workflow, not a rebuild and not general table adoption.

An immutable recovery plan pins the current binding version, original building
generation, successful materialization, source and desired descriptors,
physical relation, fresh fingerprint, and the exact pre-existing Favn
generation marker. Each new marker is transactionally bound to an opaque
identity stored on that physical table. The identity survives rename and
restart but disappears when the table is dropped and recreated. Start
revalidates that evidence and
acquires the same fenced target-operation lock used to exclude normal writes
and rebuild activation. Binding activation then rechecks the fence, evidence, fingerprint, and
exact marker in one PostgreSQL transaction.

Recovery never initializes a marker. Reconciliation only reads the
authoritative marker. Missing evidence, a changed relation or contract, or a
missing, unbound, or mismatched marker keeps the target blocked, including a
manually recreated table with an identical schema.

## Immutable planning and approval

Planning is read-only from the operator's perspective. It freezes:

- the active manifest and affected downstream graph;
- desired descriptors, active generations, physical fingerprints, and runtime
  input expectations;
- one topologically ordered action per affected target;
- every logical full-load, empty-generation, or exact-window work item; and
- the proof used to choose downstream backfill, downstream rebuild, no action,
  or operator decision.

The complete canonical payload produces one SHA-256 plan hash and expires after
one hour. Start requires the exact plan id and hash. Before accepting approval,
the orchestrator revalidates every pinned input and acquires sorted,
workspace-scoped target locks. Changed inputs return a conflict; Favn never
silently replaces the reviewed plan.

Windowed rebuild planning combines adjacent expected windows by default. The
plan still preserves their exact logical coverage, and an operator can request
separate runs. An explicit empty rebuild creates and activates only a schema-valid
empty root generation; ordinary backfills then fill the new active generation,
while retained downstream generations are stale until repaired.

## Execution and activation

The dispatcher claims one operation with a renewable fencing token. Candidate
generation work uses frozen items and normal run/materialization authority.
Successful items are checkpointed and are not repeated during safe recovery.
Candidate validation checks materialization evidence, authored assurances, and
the physical relation before activation.

Activation is an explicit saga:

1. Persist the activation intent and token.
2. Ask the runner to atomically activate the candidate.
3. Persist the returned marker and switch the control-plane binding.
4. Reconcile any lost reply by reading the authoritative data-plane marker.
5. Continue proven downstream repair in topological order.
6. Discard retired or abandoned physical relations and record cleanup outcome.

The active binding changes only after a validated marker proves the candidate
is active. A marker proving the previous generation permits safe resume. A
mismatch remains `activation_unknown`; operators must reconcile it and cannot
blindly retry activation.

## Recovery, cancellation, and retry

Operations, actions, items, leases, target locks, intent, markers, and cleanup
state are durable PostgreSQL records. Expired owners may be fenced out and work
may resume from the last checkpoint after an orchestrator or runner restart.
An unresolved runner write keeps the existing target lock or child claim held
regardless of lease expiry. Resolve it through the
[held-write procedure](../production/elastic_runners.md#resolve-a-held-write)
before ordinary recovery can acquire ownership.

Cancellation records intent first. It stops or reconciles active child work,
leaves the old active generation unchanged unless activation is already proven,
and cleans up inactive candidates when safe. Retry is available only for a
failed operation with no unknown outcome and no cleanup that would make the
original plan unsafe. Reconciliation is the only action offered for an unknown
activation outcome.

The operator contract, commands, states, and errors are documented in
[Operate Runs And Schedules](../operators/runs-and-schedules.md). PostgreSQL
tables and relationships are documented in
[PostgreSQL Data Model](../storage/postgresql/data-model.md).
