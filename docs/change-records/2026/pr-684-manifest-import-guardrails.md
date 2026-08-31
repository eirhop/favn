# Change Record: Focused manifest import memory guardrails

| Field | Value |
| --- | --- |
| Status | Qualification in progress |
| Type | Bug fix |
| Pull request | [PR 684](https://github.com/eirhop/favn/pull/684) |
| Approved plan | Commit `d483fe8c` |
| Affected areas | Manifest upload, package persistence, and activation |
| Last updated | 2026-08-31 |

## Problem

Manifest import must not consume enough memory to terminate the Orchestrator.
Two deployments ended with exit 137 during upload, although the platform did
not prove that the kernel OOM-killed the process. The existing path could
overlap uploads and activations, retain large decoded batches, and read stored
package payloads back into the BEAM for conflict checks.

The fix is manifest-specific. It does not add a general memory manager or
require operators to configure the container size.

## Implementation contract

1. Resolve available memory from the current Linux cgroup v1 or v2 hierarchy.
   Use the smallest finite ancestor headroom. Missing, unlimited, unreadable,
   or malformed limits fail closed for manifest import.
2. Require 512 MiB of current cgroup headroom before an upload or activation
   phase starts and at importer stage boundaries. More container RAM permits
   admission but does not increase the fixed work bounds.
3. Allow one local upload or activation phase at a time. Keep the existing
   PostgreSQL upload lease as the durable cross-request upload guard.
4. Persist packages in batches of at most 8 packages and 4 MiB of canonical
   input, so package payload memory does not grow with total package count.
5. Decode packages and prepare activation indexes in monitored, read-only
   workers with fixed heap, result-size, and 30-second time limits. Wait for
   worker termination before persistence, acceptance, cache insertion, or
   activation continues.
6. Reject declared archive sizes, decoded results, or activation indexes above
   their fixed protocol limits before attempting unsafe work.
7. Compare package metadata and hashes in PostgreSQL without loading complete
   stored package payloads back into the Orchestrator.
8. Return a structured retryable response when upload cannot be admitted.
   Release an activation claim for later retry on transient pressure or worker
   timeout. Deterministic size failures are terminal.
9. Preserve the previously active manifest whenever a new import or activation
   is rejected.

The guarantee covers allocations controlled by manifest import. It cannot
prevent node eviction, an external SIGKILL, or failure caused by unrelated
native allocations.

## Fixed bounds

| Work | Bound |
| --- | --- |
| Admission reserve | 512 MiB current cgroup headroom |
| Package batch | 8 packages and 4 MiB canonical input |
| Package worker | 96 MiB heap, 64 MiB returned term, 30 seconds |
| Manifest/index worker | 256 MiB heap, 128 MiB returned `Version`, 128 MiB retained index, 30 seconds |
| Local concurrency | One upload or activation phase |

Returned terms use a conservative multiple of their external representation;
worker limits include shared binaries.

## Scope

Included:

- `PUT /api/orchestrator/v1/manifest-deployments/:operation_id`.
- Archive parsing, package persistence, and manifest acceptance.
- Activation preparation and dispatch.
- Generic Linux cgroup v1/v2 inspection.
- Focused unit and integration tests in the ordinary CI suite.

Excluded:

- Runners, schedules, backfills, recovery, and normal manifest reads.
- Global byte accounting, cache redesign, or database migrations.
- A dedicated constrained-memory workflow or recurring performance gate.
- Increasing Orchestrator memory as the fix.

## Stable outcomes

| Condition | Upload | Activation |
| --- | --- | --- |
| Local phase busy | HTTP 429 with `Retry-After` | Release claim and retry later |
| Headroom unknown or too low | HTTP 503 with `Retry-After` | Release claim and retry later |
| Declared or decoded fixed limit exceeded | HTTP 413; do not accept | Fail incompatible legacy operation |
| Worker timeout | HTTP 503; do not accept | Release claim and retry later |
| Invalid manifest | Existing HTTP 422 behavior | Existing terminal invalid-manifest behavior |

## Verification

The focused tests cover:

- cgroup v1, v2, hybrid hierarchies, finite ancestors, unlimited values, and
  malformed input;
- package batch, worker heap, result-size, timeout, and cleanup boundaries;
- upload/upload, upload/activation, and activation/activation exclusion;
- authentication before admission and stable HTTP pressure responses;
- claim release on transient activation failure and preservation of the prior
  deployment on deterministic rejection;
- SQL-side package conflict verification without payload reads.

Verification uses the existing formatter, compile, fast, acceptance, slow,
Dialyzer, image, and HTTP-security workflows. No bespoke memory workflow is a
merge gate. Runtime-heavy verification is not run in the user's WSL instance.

## Review and deviations

The approved baseline is preserved at commit `d483fe8c`. An independent
GPT-5.6-Sol xhigh static review required bounded worker results, cgroup ancestor
handling, pre-acceptance activation-index validation, bounded preparation for
older accepted operations, and explicit transient versus deterministic errors.

Implementation deviations from that baseline:

- Workers are linked to the phase owner so caller or guard death cannot leave
  overlapping manifest work.
- Activation preparation returns the already bounded immutable `Version` to an
  internal deploy path; it is not decoded again outside the worker.
- The proposed dedicated 1 GiB measurement workflow, Compose harness, fixture
  builder, and recurring multi-cycle gate were removed as outside the focused
  production fix. Focused behavior remains covered by ordinary CI.

Final qualification and the final independent baseline comparison remain
pending on the final commit.
