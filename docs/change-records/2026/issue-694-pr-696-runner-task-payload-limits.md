# Change Record: Raise asset runner task payload limits

| Field | Value |
| --- | --- |
| Status | Implemented |
| Type | Bug fix and migration |
| Primary issue | [#694](https://github.com/eirhop/favn/issues/694) |
| Pull request | [#696](https://github.com/eirhop/favn/pull/696) |
| Related work | SQL compaction #695 remains separate |
| Affected areas | Core runner contracts, PostgreSQL storage, runner assignment |
| Approved plan commit | `d1b36009` |
| Last updated | 2026-09-04 |

## One-minute summary

A valid published SQL package can exceed the runner task's 1 MiB limit and fail
before execution. Raise asset work to a bounded 8 MiB of uncompressed Erlang term,
with separate assignment and storage headroom. Keep package verification, claim
fencing, and conservative unknown outcomes intact. This changes a cross-process
contract and database constraint, so it requires coordinated deployment.

## Impact and problem analysis

Issue #694 reports a 1,267,440-byte task containing a 1,175,012-byte SQL package.
The complete work item is embedded in the assignment; changing persistence alone
still leaves assignment validation and wire decoding too small.

### Assumptions and evidence

| Evidence | Proves | Does not prove |
| --- | --- | --- |
| `PersistenceCodec` at main `5f3c163e` | Payload and result terms share a 1 MiB bound | Every published 4 MiB JSON package fits a particular Erlang term budget |
| `RunnerTask.Message` and `Codec` | Assignment defaults to 1 MiB; wire decoding to 2 MiB base64 | Larger payload execution |
| Latest orchestration-context migration | Stored task JSONB remains limited to 2 MiB; context is 8 MiB | Live database version |

The budget applies to complete work, including the package and task metadata.
Publication JSON and execution term sizes differ; this is bounded headroom, not
a guarantee that every legal published package fits. Tests use synthetic data.

## Current behavior

```mermaid
flowchart LR
    A[Verified SQL package] --> B[Build work item]
    B --> C{Within 1 MiB}
    C -->|No| D[Reject before enqueue]
    C -->|Yes| E[Store and claim]
    E --> F[Assignment and wire limits]
```

## Approved plan

Use one Core limits owner for an 8 MiB asset payload budget, a 64 KiB assignment
envelope allowance, and exact base64 sizing: `4 * ceil(raw_bytes / 3)`.
Assignment validation checks both the payload's kind-specific budget and the
complete assignment budget of 8,454,144 bytes. Wire encoding and decoding enforce
the corresponding 11,272,192-byte base64 budget for assignments, with raw checks
before term decoding. Other message limits remain unchanged.

Persisted asset envelopes receive a 12 MiB JSONB storage bound through a new
registered migration. The 8 MiB raw payload expands to 11,184,812 base64 bytes,
leaving over 1 MiB for JSONB envelope overhead. Existing payloads need no rewrite.
Non-asset payloads and results retain their existing 1 MiB raw bounds; result,
log, error, and orchestration-context database bounds stay unchanged.

```mermaid
flowchart LR
    A[Verified SQL package] --> B{Work within 8 MiB}
    B -->|No| C[Reject with actual size and limit]
    B -->|Yes| D[Persist in bounded JSONB envelope]
    D --> E[Claim with existing fences]
    E --> F[Validate payload and assignment budgets]
    F --> G[Bounded base64 encode and decode]
    G --> H[Execute verified package]
```

### Contracts, scope, and non-goals

- Maintain protocol 13 shapes, immutable package identities, pool/release binding,
  assignment fences, and unknown-outcome classifications.
- Retain uncompressed safe term decoding and reject oversized encoded input
  before allocation-heavy decoding.
- No SQL compaction, package-fetch protocol, retries, scheduling, or new tuning
  options. HTTP body limits are unrelated to distributed BEAM task transport.

### Implementation slices and complexity budget

| Slice | Owner and outcome | Production added | Production deleted | Supporting added | Supporting deleted |
| --- | --- | ---: | ---: | ---: | ---: |
| 1 | Core limits, payload/assignment/wire validation and boundaries | 60-130 | 10-45 | 130-250 | 0-20 |
| 2 | PostgreSQL migration and actual claim-to-execution regression | 35-75 | 0-5 | 140-300 | 0-20 |
| 3 | Canonical size and rollout documentation | 0 | 0 | 25-60 | 0-10 |

Supporting lines include tests, fixtures, and canonical docs, excluding this
record. Explain overruns exceeding 25 percent or 100 lines, whichever is smaller.
The actual assignment regression must include real PostgreSQL persistence and
claim, wire round trip, and SQL execution through the runner; reuse existing test
fixtures and SQL adapters where practical.

## Operational design

Oversized work remains a deterministic pre-enqueue error with byte count and
limit. Do not log task bodies. No new retries or recovery behavior are introduced.
Apply the migration first, then deploy matching updated control plane and runner
builds before publishing work relying on the new limit. Protocol 13 data shapes
are unchanged, but old binaries cannot process larger work; mixed builds are not
supported for that work. Rebuild runner release identities as usual. Before
rollback, drain larger tasks and handle retained oversized rows; old codecs cannot
read them and migration down must fail rather than truncate incompatible rows.

## Verification plan

| Acceptance criterion | Evidence |
| --- | --- |
| Larger SQL task executes | Synthetic package above 1 MiB persisted, claimed through orchestrator, assignment wire round trip, runner SQL execution |
| Raw and envelope bounds | Exact-limit and one-byte-over tests for payload and assignment, encoded expansion, malformed/compressed decode rejection |
| Storage consistency | Migration up/down and database constraint tests, with payload above old storage limit |
| Unrelated limits unchanged | Non-asset payload, result, and log boundary assertions |
| Identity and fencing retained | Existing package identity, assignment fence, cancellation/unknown tests plus relevant large-task checks |
| Compatibility documented | Canonical runner architecture and migration rollout instructions |

Run focused owning-layer tests first, then relevant compile/format/tag checks and
ordinary PR CI. Use a disposable bootstrap-owned PostgreSQL database. Local tests
and CI do not establish live deployment or constrained-memory performance.

## Risks

Larger terms increase per-assignment memory; the finite limit is deliberate.
A valid package may still exceed the full-task budget. PostgreSQL constraint
replacement takes a table lock. Rollback may be blocked by retained larger rows.

## Plan review

Independent agent `/root/review` compared issue #694, source, storage constraints,
transport, budget arithmetic, and this plan. Verdict: accepted with no blocking
findings. The review confirmed preservation of the latest 8 MiB context bound,
final struct validation after wire-map decoding, and the real claim-to-execution
regression requirement. Implementation has not started.

## Implementation outcome

Core now has one internal limits module. Persistence applies an 8 MiB asset-only
budget in both directions; operations and results keep 1 MiB. Assignments enforce
the nested kind budget plus 64 KiB envelope headroom. The wire codec checks encoded
size on both paths and decoded raw bytes before safe term decoding. PostgreSQL
migration `20260904000000` raises only the payload column bound to 12 MiB; schema
readiness uses the resulting verified definition fingerprint.

The synthetic SQL regression produces work above 1 MiB and a persistence envelope
above the old 2 MiB wire/storage budget. It traverses real PostgreSQL enqueue/get,
`RunnerTasks.claim`, assignment validation/encode/decode, manifest/package
verification, and `TaskExecutor`. A small test adapter executes the runner-rendered
SQL against PostgreSQL in a transaction-local table, checks its row, and returns
through durable task completion. It does not qualify a production SQL adapter.
The behavior matches the proposed diagram.

### Actual scope and complexity

| Slice | Production added | Production deleted | Supporting added | Supporting deleted |
| --- | ---: | ---: | ---: | ---: |
| 1: Core contracts and boundary tests | 64 | 10 | 151 | 0 |
| 2: Storage migration, claim/execution and migration tests | 31 | 2 | 300 | 2 |
| 3: Canonical runner size/rollout documentation | 0 | 0 | 37 | 0 |

No category exceeds its approved upper bound. Storage production is slightly
smaller than estimated because the existing migration replacement pattern and
diagnostics fingerprint suffice. Implementation complexity is low: explicit
budgets and one constraint replacement. Operational complexity is moderate:
matching binaries and migration ordering are required. Canonical documentation
is in `docs/architecture/elastic-runners.md#runner-task-size-budgets`.

## Deviations and decisions

| Planned | Implemented or clarified | Reason and impact | Reviewer verdict |
| --- | --- | --- | --- |
| Down migration refuses incompatible rows | It refuses rows exceeding the old stored-column constraint; operators must separately resolve all tasks above the old raw codec limit before rolling back binaries | JSONB storage size, base64, raw size and TOAST compression differ; no data truncation or new rollback protocol | Reviewer requested this narrower, accurate documentation |
| Registered migration | Migration also refreshes the schema definition fingerprint | The new constraint must be recognized by readiness diagnostics | Accepted |
| GitHub diagram rendering before implementation | GitHub Markdown API recognized both Mermaid diagrams before code edits; browser verification using GitHub's Mermaid renderer completed afterward | Verification timing deviation only; diagrams were unchanged and rendered successfully | Accepted |

## Verification evidence

| Check | Result | Evidence boundary |
| --- | --- | --- |
| Core runner-task contracts | 21 tests passed | Exact 8 MiB raw boundary, assignment round trip and envelope/raw/encoded rejections; operation/result/log bounds retained |
| PostgreSQL runner tasks and migration tests | 42 tests passed | Real storage, max raw payload, oversized JSONB rejection, claim/SQL/durable completion, fencing/recovery/logs; empty-database migration down/up and readiness |
| Orchestrator claim/recovery tests | 9 tests passed | Existing assignment/recovery invariants |
| Runner agent and SQL execution tests | 84 tests passed | Existing runner lifecycle, package and SQL execution behavior |
| Test-environment compile | Passed with warnings as errors | Local compilation |
| Formatting, tag-tier guard and diff whitespace | Passed | Static checks |
| GitHub Markdown and Mermaid | Both diagrams rendered using GitHub's Markdown API and browser Mermaid renderer | Rendered diagram inspection; no behavioral change |
| CI | Passed on implementation commit `93886861` | [CI](https://github.com/eirhop/favn/actions/runs/33845900274), [control-plane and runner images](https://github.com/eirhop/favn/actions/runs/33845900273), [HTTP boundary security](https://github.com/eirhop/favn/actions/runs/33845900364) |

The first combined storage run had one existing log test fail in setup with a
transient database connection-unavailable error. The unchanged full rerun passed
all 42 tests. CI initially failed an existing consumer-install slow test because
the npm esbuild download returned HTTP 504 before its assertion. Rerunning only
failed jobs on the unchanged implementation passed. No unrelated test behavior
was changed.

### Not verified

No live deployment, production database migration, constrained-memory stress,
or production SQL adapter was exercised. Migration rollback with retained
oversized rows was not executed; down/up coverage uses an empty database.

## Final review

Independent agent `/root/review` rechecked the complete implementation against
approved baseline `d1b36009`, the final record, verified schema fingerprint, test
evidence, corrected rollback wording and scope budgets. Final verdict: accepted,
with no remaining actionable findings. All listed deviations are accepted.
This verdict covers implementation and local evidence; CI completion and live
deployment are separate proof boundaries.
