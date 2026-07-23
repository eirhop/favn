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

Non-persisted assets use deterministic semantic-generation identity. They do not
participate in physical candidate activation.

## Compatibility and admission

Manifest activation compares the desired descriptor with both the active
generation and a fresh physical inspection. `ready`, `uninitialized`, and
`rebuild_available` permit ordinary writes. `rebuild_required`,
`unexpected_drift`, and `operator_decision` block only dependency paths that
include the affected target. The active deployment and readable generation stay
available for diagnosis.

Admission rechecks compatibility immediately before a persisted write. A
deployment-time result is operator evidence, not permission to write forever.

Runner replacement does not reject the whole manifest because the active
generation was created by an older runner release. The new runner inspects the
persisted active physical relation directly through the desired manifest; it
does not load old executable code. Missing or drifted physical state takes
precedence over descriptor differences. Once inspection proves the recorded
active generation is intact, immutable active-versus-desired descriptor
differences determine whether a rebuild is required.

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
