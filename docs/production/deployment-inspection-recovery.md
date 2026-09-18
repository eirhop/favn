# Deployment inspection recovery

Use this runbook when local startup reports pending deployment cleanup or legacy
inspection work. Production archive deployments use the same task ownership and
activation fencing, but have no local-session expiry: disconnecting their HTTP
client does not cancel accepted work.

Local deployments retain the exclusive manifest slot and bounded preparation
workers, but do not require the finite container memory limit used by archive
admission. Lifecycle admission is delegated in memory to the exact local
workspace/operation; maintenance tokens are never persisted. Restart discards
these temporary permits and requires normal admission again.

## Upgrade

Quiesce deployment acceptance and stop old control-plane writers before running
the normal PostgreSQL bootstrap upgrade. Mixed old/new deployment writers are
unsupported. The migration retains existing archive operations and does not
guess owners for old hashed inspection identities. Use a forward fix; this
migration deliberately rejects automatic downgrade.

## Inspect an operation

Use an authorized workspace operator context with the orchestrator facade:

```elixir
FavnOrchestrator.ManifestDeployments.get_local(context, operation_id)
FavnOrchestrator.ManifestDeployments.inspections(context, operation_id,
  limit: 100, after_task_id: nil)
```

The operation reports activation state, cleanup state, original deadline,
expected runtime revision and the committed activation receipt. Inspection
counts cover all owned tasks; pages contain at most 100 task IDs and states.
Pass the last returned task ID as `after_task_id` for the next page.

A receipt proves that activation committed at its recorded runtime revision.
It does not claim that this revision is still current after another deployment.
A cancelled operation proves admission is closed, not that every assigned
inspection has stopped. Wait for cleanup to become `settled`.
The dispatcher retries bounded cleanup and cancellation notification after
restart. Unknown execution remains `unknown` and blocks new local acceptance.

## Resolve verified quiescence

First stop the relevant runner execution and verify that its database/backend
inspection has ended. Stopping the BEAM process alone is insufficient evidence
if the backend can continue the query. Inventory exact workspace/task IDs and
assignment generations; never select unrelated work merely by task kind.

For at most 100 verified assignments, submit:

```elixir
alias FavnOrchestrator.Persistence.Commands.ResolveDeploymentInspections

FavnOrchestrator.ManifestDeployments.resolve_inspections(
  %ResolveDeploymentInspections{
    workspace_context: context,
    operation_id: operation_id,
    task_assignments: %{task_id => assignment_generation},
    runner_stopped: true,
    backend_stopped: true,
    evidence_reference: "operator-incident-or-verification-reference",
    occurred_at: DateTime.utc_now()
  }
)
```

The command checks workspace, owner and assignment generation, fences the
resolved assignments and records an audit entry. Repeating the same verified
resolution is safe. It rejects live operations and incomplete evidence.
The next cleanup pass settles the owner once all work is resolved.

For pre-upgrade inspections with verified provenance, use `operation_id: nil`.
The command accepts only unowned relation inspections with no run or rebuild
owner. Keep legacy dispatch quiesced during inventory and settlement. If task
provenance or backend quiescence cannot be proved, preserve the blocker and
investigate; do not delete rows or invent an owner link.

Operation IDs, states and counts are suitable diagnostics. Do not put SQL,
credentials, customer paths or query payloads in evidence references or logs.
