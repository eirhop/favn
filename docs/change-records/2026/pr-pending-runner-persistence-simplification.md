# Change Record: Simplify runner data and history-conflict recovery

| Field | Value |
| --- | --- |
| Status | Plan reviewed |
| Type | Contract simplification and regression repair plan |
| Primary issue | None; the maintainer authorized this regression work without a separate issue. |
| Pull request | Pending draft creation |
| Related work | [#703](https://github.com/eirhop/favn/pull/703), [#711](https://github.com/eirhop/favn/pull/711), [#714](https://github.com/eirhop/favn/pull/714), [#716](https://github.com/eirhop/favn/pull/716), [#717](https://github.com/eirhop/favn/pull/717), [#722](https://github.com/eirhop/favn/pull/722), [#725](https://github.com/eirhop/favn/pull/725) |
| Compared versions | Main `ba3fa194`; proposed repair #725 at `f8fde8af` |
| Affected areas | Core runner contracts; Runner result construction; Orchestrator task preparation and completion; PostgreSQL history protection |
| Approved plan commit | To be recorded after independent approval |
| Last updated | 2026-09-17 |

## One-minute summary

Favn has two different problems. Its serializer mixes application data with
framework instructions, so an ordinary new metadata key can encounter rules
intended for execution controls. Separately, history cleanup introduced a lock
that makes ordinary writes compete, and existing failure handling can turn a
short database conflict into failed work and cancelled siblings.

We should separate those concerns and delete the special cases they require.
Keep the protections that prevent duplicate external writes. Do not preserve
old development formats or add a new workflow engine. This record proposes the
smaller boundaries and the experiments needed to decide how much of #725 is
necessary; it does not claim that a smaller replacement has already passed tests.

**This PR contains only this reviewed plan. Implementation is not authorized by
plan review alone.** It does not merge, close, replace or edit #725. Before code
work, confirm its current status and explicitly choose the implementation PR;
if the work is split, each implementation PR gets its own bounded record and
links here for the research rather than copying it.

## Three examples of the problem

### 1. Application metadata should not need framework registration

An Elixir asset can return `%{pages_written: 12, load_mode: :append}`. Those keys
and `:append` are already atoms in the application; Favn is not turning arbitrary
strings into atoms. The old replacement decoder only recognized names in its
approved dictionary. Adding a name repaired one example but could not cover
all names future applications might choose.

Application data should become `%{"pages_written" => 12, "load_mode" => "append"}`
at the runner boundary. No dictionary update is needed. Favn's own status such
as `:ok`, and its evidence about whether a write committed, stay separately
validated. **#717 already added application-result normalization.** The proposed
work removes the remaining guessing about which kind of metadata was returned;
it must not introduce a second normalization layer.

### 2. Two healthy tasks should not fight over permission to save history

Two backfill tasks finish at the same time. Both must protect their history from
being deleted. They should be able to hold that protection together. Instead,
#714 gave ordinary writers an exclusive lock for their shared execution group,
in the same lock namespace as run identity. One writer could reject the other
with `execution_history_owner_busy`, even with optional cleanup disabled.

#725 separates the namespace and shares writer protection while retaining
exclusive retirement. That addresses the source of unnecessary contention.
It does not prove that all retries or the required lock ordering can be removed.

### 3. Completed work is different from unfinished bookkeeping

Landing writes data successfully. Favn temporarily cannot record the resource
outcome. The state should mean **“the asset succeeded; bookkeeping is pending.”**
Retry recording the outcome, not the external write. Independent siblings keep
running. If the external write's outcome is unknown instead, Favn must retain
that uncertainty and require evidence before replay.

## What the research established

| Evidence | Finding | Limit |
| --- | --- | --- |
| [#703 record](issue-700-pr-703-crash-recovery.md), codec before/after merge `5b8a1124` | The old Erlang-term decoder depended on which atoms a restarted VM knew. The replacement omitted valid nested structures and treated application names as closed framework names. | The old happy path worked; the old crash-recovery behavior was not a safe baseline to restore wholesale. |
| [Current codec](../../../apps/favn_core/lib/favn/contracts/runner_task/persistence_data.ex) | Atom encoding and dictionary-based decoding accept different sets of values. Unknown structs/atoms still need rejection at the framework boundary. | A successful encode alone is not proof of a restorable task. |
| [Result normalization](../../../apps/favn_core/lib/favn/contracts/runner_task/persistence_result.ex) | SQL/Source/application treatment is chosen from metadata contents. | The heuristics are current-contract complexity, not an old-format decoder. |
| [Work construction](../../../apps/favn_orchestrator/lib/favn_orchestrator/run_server/execution/step_attempt_lifecycle.ex) | Run metadata is copied after removing a list of control-plane keys. | Another internal key can escape that list; the existence of a key does not prove the runner needs it. |
| [#714](issue-704-pr-714-postgresql-retention.md), `RunIdentity` and `Maintenance.History` | Introduced the exclusive history guard into ordinary writes; the guard runs independently of whether optional retention is enabled. | The original incident does not identify its exact competing transaction. |
| Git history of `terminalize_stage_admission_failure`, including `49ff99e4` | Blanket sibling cancellation predates #703. | Later locks exposed an older failure-path weakness. |
| [#725 reviewed repair](https://github.com/eirhop/favn/blob/f8fde8af/docs/change-records/2026/pr-725-history-conflict-lifecycle.md) | Broader exact-command retries and ownership tracking now pass composed tests. | That proves the tested behavior, not that this is the smallest design. Its process-owned bookkeeping continuations are not durable crash recovery. |

The five repair PRs add 8,934 lines gross: 2,952 production, 4,458 tests,
1,364 change records and 160 other documentation/security lines. Their net
production growth is 2,331 lines. #703 itself added 3,454 net production lines;
#714 added another 2,403 for retention. These are sums of per-PR diffs, not a
count of unique surviving lines or proof that the safety work is unnecessary.

### Why tests and reviews missed successive failures

#703 tested all task kinds and fresh processes, but its representative asset
fixture did not include the actual backfill pipeline context and Landing result.
Later repairs tested the next failing key or persistence boundary. #722's
contention test covered attempt-start and ownership renewal, not a new lock
acquired during successful completion bookkeeping or queue refill.

A reviewer finding no defects within those examples did not establish complete
lifecycle coverage or justify the total design. Future review must separately
answer “does this work?” and “can we delete this machinery by fixing its cause?”

## Current behavior and proposed boundary

The current contracts mix metadata from several owners. The serializer must
infer meaning; the #725 repair also carries partial execution state between
database writes. This diagram shows those contracts with the proposed #725
recovery behavior, rather than implying that every repair is already on main.

```mermaid
flowchart TD
    A[Run metadata] --> B[Copy fields and remove known internal keys]
    B --> C[Runner task and closed codec]
    D[Asset result metadata] --> E[Guess application or SQL or Source data]
    E --> C
    C --> F[Persist task or result]
    F --> G[Additional bookkeeping writes]
    G -->|Conflict| H[Pause and resume the failing phase]
```

The proposed boundary keeps meaning explicit. The last branch represents the
required behavior, not a decision to introduce another persistence subsystem.

```mermaid
flowchart TD
    A[Prepare work or result] --> B[Explicit framework fields]
    A --> C[Application data]
    B --> D[Validate framework contract]
    C --> E[Normalize bounded data with string keys]
    D --> F[Persist under one current format]
    E --> F
    F --> G{Outcome known}
    G -->|Successful external write| H[Complete remaining bookkeeping only]
    G -->|Unknown external write| I[Retain ownership and reconcile]
    H --> J[Allow independent siblings to progress]
```

## Proposed plan

### A. Give each field one owner

Use the existing runner request/result contracts. Do not build a general-purpose
object serializer, a plugin registration mechanism or a replacement scheduler.

| Data | Proposed rule | Example |
| --- | --- | --- |
| Task identity, status, deadline, write outcome and generation pins | Explicit framework fields with closed validation; reuse existing fields | An application's `"status"` key cannot change task status. |
| SQL/Source execution evidence currently mixed into result `meta` | One explicitly typed evidence field, selected by the trusted execution path, on the asset result and its attempt entries | Source relation evidence is constructed by Source execution; it is not detected by looking for `observed` in user metadata. |
| Application result `meta` | Normalize once with the existing bounded open-data rules | `%{pages_written: 12}` becomes `%{"pages_written" => 12}`. |
| Application context needed by the runner | Explicitly supplied application metadata, normalized by the same rules | Adding unrelated cancellation bookkeeping to a run cannot change runner work. |
| Required framework context currently carried in metadata | Inventory consumers and construct only the fields they actually require; reuse existing `RunnerWork`/pipeline fields first | Backfill identity needed by the asset remains available; cancellation outcomes stay in the control plane. |
| Parameters, runtime-input contracts, manifest references and execution packages | Keep their existing typed contracts | Do not stringify every field in a runner task as a shortcut. |

The first implementation artifact is a field inventory: producer, consumer,
owner, destination and replacement for every touched metadata field. Unknown
internal fields are not copied. Removing a field requires proving that no
supported execution path consumes it. Fields with overlapping names remain
separate by ownership, not precedence rules. Existing top-level write evidence
and new SQL/Source evidence must agree; the new field must not duplicate or
replace the authoritative write-outcome field.

For application metadata, keep `OpenData`'s bounds and permitted value types:
strings/binaries, numbers, booleans, nil, lists, string-keyed maps and its existing
approved date/time/Decimal values. Normalize atom keys/values to strings; do not
create atoms from stored names. Reject duplicate keys after normalization,
unsupported structs/functions/process handles and unsupported tuples. Keep
existing error-detail tuple handling separate. Preserve the existing byte,
depth and node limits; validation errors name the field without logging
its value.

Change result producers, persistence validation, attempt history, materialization
and freshness readers, operator projections and public result documentation
together. Remove metadata-shape guessing and the replaced metadata-copy path
in the same change. A current-format wrapper is not permission to keep both
representations indefinitely.

### B. Prove the smallest history-conflict repair

Keep #725's evidence and tests available at its pinned commit. On isolated
implementation candidates, compare main plus the root lock correction and
required lock-order changes with the complete #725 repair. Reuse its composed
PostgreSQL regression rather than writing a second large fixture.

Test shared sibling writes, exclusive retirement, completion bookkeeping,
admission queue/refill, cancellation, ownership loss, deadlines and lost replies.
For each failure of the smaller candidate, name the exact missing guarantee and
retain only the required mechanism. **A lock-only candidate is an experiment,
not an approved complete fix.** Simply disabling retention or removing its
writer guards is not acceptable: that does not resolve the installed contention
or preserve safe deletion.

Prefer grouping bookkeeping that is already owned by the same PostgreSQL
transaction where that removes a partial state. Never hold a database transaction
open around an asset callback or external write. Do not combine capacity waiting,
unknown task delivery and a rolled-back database write under one generic retry.

If safe progress still requires a durable completion phase, a new stored command
or a change to crash recovery, stop this slice and return a separate design with
its own budget. Do not quietly add another state machine to this plan.

### C. Make the implementation decision explicit

After the comparison, report which #725 production paths are retained, replaced
or unnecessary, with the failing/passing test for each decision and actual diff
counts. Its diagnostic, sibling-tracking and unknown-outcome safeguards remain
requirements even if their implementation changes. Replacing #725 requires a
new independently reviewed implementation; this planning PR cannot supersede it
by claiming that fewer lines are automatically safer.

## Failure rules in simple terms

| Situation | Required behavior |
| --- | --- |
| Application uses a new metadata key | Accept supported data without adding that key to a framework dictionary. |
| Metadata is unsupported before dispatch | Reject with the exact field/reason; no saved executable task or orphaned claim. |
| Result metadata is unsupported after execution | Preserve the existing unknown-outcome safeguards; never label a possibly completed write safe to replay. |
| Database confirms a transaction rolled back | Retry the same safe storage command, with its identity and original deadline. |
| Another writer legitimately owns the target | Use normal admission waiting; keep the attempt and original deadline. |
| Task enqueue/completion reply is lost | Reconcile the existing command/task identity; do not manufacture another execution. |
| External write succeeded, bookkeeping is busy | Preserve success and retry only remaining bookkeeping; let independent siblings run. |
| External write outcome is unknown | Keep ownership protection and require evidence before replay. |
| Operator cancels, deadline expires or ownership is lost | Respect that authority; preserve saved tasks/results and clean each owned resource once. |
| Retry budget expires or the process crashes | Expose the original operation/asset/error and the supported recovery disposition. Do not claim transparent crash recovery for an in-memory continuation. |

Diagnostics retain operation, asset/task/run ID, reason code and retry exhaustion.
Do not log application metadata, payloads or credentials. Reuse existing bounded
retry logging; do not add one log per failed polling attempt or render `nil` as
the error type.

## Scope, ownership and complexity limits

The reader is the maintainer deciding whether to approve a smaller implementation.
These are initial estimates, not promises of savings; slice 1 must validate them
before production edits. The record itself and generated/formatter-only changes
are excluded. Slices are alternatives/comparisons where stated, not permission
to accumulate every candidate in the final product.

| Slice | Owner and outcome | Production added | Production deleted | Supporting added | Supporting deleted |
| --- | --- | ---: | ---: | ---: | ---: |
| 1 | Core/Runner/Orchestrator: field inventory and reuse of end-to-end fixtures; comparison baseline | 0 | 0 | 80–160 | 0–40 |
| 2 | Core/Runner/Orchestrator: explicit evidence and application data, all producers/readers, one normalized representation | 180–350 | 200–400 | 200–400 | 80–200 |
| 3 | PostgreSQL/Orchestrator: isolate history lock fix and compare required lifecycle continuations | 80–180 | 40–140 | 100–200 | 40–100 |
| 4 | Owning guides/types and shared fixtures: adoption and complete qualification | 0–30 | 0–30 | 60–120 | 20–60 |

The desired direction is fewer special cases and a smaller production design
than #725, not a guaranteed net deletion from main. Include reused/cherry-picked
code in all counts. If slices 2 or 3 cannot meet their behavior within these
bounds, stop and re-review the design before expanding it. Apply the repository
variance rule: explain an overrun exceeding 25% or 100 lines, whichever is
smaller, and materially fewer deletions. Do not finish a large implementation
first and justify the overrun afterward.

Core owns data contracts and normalization. Runner owns constructing genuine
execution evidence. Orchestrator owns task preparation and lifecycle decisions.
PostgreSQL owns transactions and history locking. View consumes the public
orchestrator facade; no direct storage access is introduced.

Non-goals: arbitrary Elixir-term persistence, a framework-wide metadata rewrite,
a new retry engine, new retention features, a UI redesign, automatic repair of
old failed runs, or proving a live connector never duplicates writes.

## Deployment and compatibility

The maintainer confirmed that there are no production installations requiring
old data support. #703 already
removed its old decoder and dual-format rollout; this is not where most of the
current complexity resides. Use one current format after this change. Update
persisted-format/wire validation and runner-release requirements when the actual
contract changes; reject incompatible records and runner versions explicitly.
Do not retain an old decoder, dual writes or shape-detection fallbacks.

Stop old control-plane, maintenance and runner processes before adopting changed
locks or runner contracts. Any fresh disposable database adoption or old local
data disposal requires explicit operator action; this plan does not authorize
a reset. Existing external data and unknown writes are not cleared merely by
changing control-plane schema. Before any development reset, establish that old
workers have stopped and unresolved external writes are accounted for. Rollback
uses the matching prior build/database, not a decoder for both versions.

## Verification and review gates

| Acceptance criterion | Required proof |
| --- | --- |
| New application keys never need framework registration | Separate fresh writer/reader processes using multiple application keys absent from the registry; exact normalized values and no new atoms. |
| Application data cannot impersonate framework evidence | Keys such as `status`, `write_outcome`, `relation`, `observed` and `retryable?` remain application data; genuine SQL/Source success and failure evidence survives separately. |
| Task construction cannot leak internal metadata | Add arbitrary control-plane/cancellation metadata and prove the executable task context does not change; required backfill/window/schedule/application context still arrives. |
| Accepted tasks are restorable | Real BackfillDispatcher → fully populated task → PostgreSQL readback in a fresh process; bounds, unknown atoms/structs and normalization collisions remain rejected. |
| Complete success survives contention | Backfill enqueue → Landing-style result → real PostgreSQL contention at resource bookkeeping and admission refill/queue → lock release → whole pipeline completes once. |
| No hidden safety loss | No duplicate write effects, orphaned claims/leases, unintended cancellations or blocked descendants; unknown delivery and unknown external write remain distinct. |
| Real cancellation and deadlines remain effective | Contend, cancel/expire/lose ownership, then release the lock; no new dispatch past the original deadline and all saved tasks are accounted for. |
| Simpler implementation is actually smaller | Compare production/test/deletion counts, number of lifecycle phases and metadata special cases against main and #725; report any guarantees that still require added state. |

Run owning-layer tests first, then relevant full fast/slow/acceptance suites,
format, warnings-as-errors compilation, static checks and final-head CI.
Fresh-process codec tests and real PostgreSQL contention tests prove different
things; neither substitutes for the other. Live connector interruption, deployed
release recovery and workload performance require separate evidence and must
not be inferred from a green synthetic pipeline.

The final reviewer must compare the reviewed baseline with the implementation,
including everything removed. In particular, challenge whether each remaining
retry phase protects a real partial side effect or merely compensates for a
boundary that could be eliminated.

## Plan review and outcome

Independent **Astra xhigh** review approved this planning record on 2026-09-17.
The reviewer checked the pinned source, PR history, production/test line counts,
field ownership, failure safeguards and complexity limits.

| Review point | Resolution |
| --- | --- |
| No production installations must be evidence, not an inference from pre-v1 status | Attributed explicitly to the maintainer's statement. |
| The current-state diagram could imply #725 is already merged | Identified the diagram as existing contracts combined with proposed #725 recovery. |
| Smaller code is not evidence of complete recovery | Retained the field inventory, composed-test comparison, separate design for new durable phases, and budget re-review before expansion as approval conditions. |

**Verdict:** no remaining actionable findings; approved as a bounded research and
decision record. The reviewer did not authorize implementation or assert that
the smaller candidate is sufficient.

Documentation checks: all relative links resolve, GitHub's Markdown renderer
accepts the page, and both Mermaid diagrams render in Chrome. The planning diff
passes whitespace checks. No implementation, automated product tests or live
runtime validation were performed for this proposal. The record stays
`Plan reviewed` when its draft PR opens because implementation has not started.
