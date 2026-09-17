# Fix normal first-write generation registration

| Field | Value |
| --- | --- |
| Status | Plan reviewed |
| Primary issue | None; maintainer requested the repair without a new issue |
| Pull request | Pending |
| Related work | [#731](https://github.com/eirhop/favn/pull/731), [#726](https://github.com/eirhop/favn/pull/726) |
| Inspected baseline | Main at 5d27a519 |
| Last updated | 2026-09-17 |

## Problem in plain terms

An asset writes successfully, but Favn then refuses the small task that registers
its first generation. The task says it belongs to a rebuild operation that does
not exist. The retention guard rejects that claim, leaving a successful write
with an uninitialized binding and a failed run.

`InitialTargetGenerationReconciler` passes an `initial-marker:…` value as the
runner task's `operation_id`. That field identifies a retained rebuild/recovery
parent. Normal first writes have neither parent. The marker's independent write
identity already has its own `write_operation_id` field. This is an ownership
mix-up, not another application-result codec failure.

```mermaid
flowchart LR
    A[Successful asset write] --> B[Successful materialization saved]
    B --> C[Marker task claims nonexistent rebuild parent]
    C --> D[Retention guard rejects task]
    D --> E[Run fails and binding stays uninitialized]
```

The same parent guard protects later task transitions, so changing only the
enqueue guard would leave a broken lifecycle. Existing reconciler tests replace
the task store; existing storage reconciliation tests skip marker dispatch.
Neither test composes the failing PostgreSQL path.

## Reviewed scope carried from PR 731

This isolates the already reviewed marker repair from unfinished whole-run crash
recovery. Its original approved plan is the PR 731 marker addendum at `bd5a2f28`;
its reviewed implementation is `f5cd25ed`. The independent reviewer must recheck
this extraction against current main. The broader crash-recovery decision and
unintegrated prototypes stay in PR 731.

Remove the false parent reference from normal marker initialization. Preserve
its deterministic task ID, marker token, payload and write identity. Leave
`OperationRetention` unchanged: do not exempt task names or permit missing or
retiring real parents.

```mermaid
flowchart LR
    A[Successful write and materialization] --> B[Physical inspection and capability check]
    B --> C[Marker task with independent write identity]
    C --> D[Marker succeeds]
    D --> E[Activate generation and binding]
    E --> F[Run succeeds]
```

For existing affected writes, provide a checked script in an existing trusted
administrator console. It must require the original successful asset task,
matching committed materialization, original manifest and generation pins, and
an uninitialized unchanged binding. Reject unresolved write holds, changed
bindings and missing evidence. Reuse only registration; never rerun the asset,
clear unknown holds, or reopen the failed run.

The target must be quiescent throughout repair. A preflight query is not a
concurrent maintenance lock. Keep submissions and writes paused after a timeout
until any dispatched inspection/marker tasks settle or are reconciled. Keep
runners available for registration. Ordinary public target recovery needs an
existing marker and cannot repair this absent-marker case. Release hosts need
the reviewed script copied to an explicit path; it is not packaged in a release.

## Scope limits and tradeoffs

No new public API, schema, retention exception, state machine or compatibility
reader. No automatic replay of external writes. No repairs are run against the
user's data as part of implementation. The console procedure deliberately
requires operator-controlled quiescence instead of adding concurrent maintenance
coordination. It conservatively requires the earliest matching materialization;
a later source task is refused rather than guessed.

The original rough budget was 1–60 production additions, 1–10 deletions, and
150–350 supporting additions. PR 731's final review accepted the necessary
variance: application +5/-2, checked script +110, tests +496, operator guide +48.
The checks cover the exact original saved evidence and real parent lifecycles;
there is no new application state machine. Extraction should keep those code
counts unchanged. This record is excluded from the count.

## Verification and recovery acceptance

Use the real PostgreSQL store and RunServer to reproduce the original failure
and verify successful materialization, physical inspection, capability check,
marker task dispatch/completion, generation activation and successful run finish.
Exercise missing parents and retirement during later transitions, legitimate
rebuild/recovery tasks, repeated reconciliation, and repair from original success.
Negative repair cases include missing materialization, changed binding, unresolved
write holds and insufficient administrator permission.

The PostgreSQL tests supply runner outcomes through the actual task persistence
boundary. They do not execute external SQL callbacks. Those are separate runner
contracts; this repair qualifies the control-plane parent guard and registration
lifecycle. Whole-run crash recovery and fresh-process external-write qualification
remain outside this PR.

Re-run the PostgreSQL core-authority suite and focused reconciler/task tests on
current main; compile with warnings as errors, check formatting and documentation
links, and qualify the exact final PR head in CI. Independent Astra xhigh review
must compare this extraction with the approved marker plan and report any new
interaction with main before marking the PR ready.

Astra xhigh approved the extraction plan with no blocking findings. Apply the
selected patch so current main retains its schema-version expectation and runtime
catalog guard. Final review and current-main qualification remain required.

## Implementation outcome

Implementation and final verification will be recorded after the extraction.
