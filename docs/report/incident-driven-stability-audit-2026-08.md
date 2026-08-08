# Incident-Driven Stability Audit

Snapshot: `main` at `5b986693` on 2026-08-08.

This is a point-in-time review of closed defect and hardening issues, their
closing pull requests, and repository history. It is not production telemetry,
and code churn is used only to rank investigation—not as proof that a module
should be split or deleted.

## Findings

The review sampled 24 issues whose symptoms exposed a failed boundary or an
important missing test:

| Failure pattern | Issues | Count |
| --- | --- | ---: |
| Typed serialization and rehydration | [#136](https://github.com/eirhop/favn/issues/136), [#288](https://github.com/eirhop/favn/issues/288), [#499](https://github.com/eirhop/favn/issues/499), [#500](https://github.com/eirhop/favn/issues/500), [#502](https://github.com/eirhop/favn/issues/502), [#514](https://github.com/eirhop/favn/issues/514) | 6 |
| Execution, planned-window, and DAG semantics | [#144](https://github.com/eirhop/favn/issues/144), [#492](https://github.com/eirhop/favn/issues/492), [#508](https://github.com/eirhop/favn/issues/508), [#511](https://github.com/eirhop/favn/issues/511) | 4 |
| SQL resource and metadata handling | [#205](https://github.com/eirhop/favn/issues/205), [#516](https://github.com/eirhop/favn/issues/516), [#552](https://github.com/eirhop/favn/issues/552) | 3 |
| Local lifecycle and interface behavior | [#128](https://github.com/eirhop/favn/issues/128), [#129](https://github.com/eirhop/favn/issues/129), [#140](https://github.com/eirhop/favn/issues/140), [#157](https://github.com/eirhop/favn/issues/157), [#209](https://github.com/eirhop/favn/issues/209), [#286](https://github.com/eirhop/favn/issues/286), [#297](https://github.com/eirhop/favn/issues/297), [#451](https://github.com/eirhop/favn/issues/451) | 8 |
| Scheduler, runner, and storage failure semantics | [#196](https://github.com/eirhop/favn/issues/196), [#197](https://github.com/eirhop/favn/issues/197), [#200](https://github.com/eirhop/favn/issues/200) | 3 |

The strongest recurring pattern is not simply "too much code." It is that
invalid or incomplete state crosses a boundary and fails later, where the
original cause is harder to see. Manifest serialization is one such boundary:
unsupported values were previously converted with `inspect/1`, and atom/string
key collisions could silently overwrite one value.

## How tests missed the failures

- New-writer round trips did not always include persisted fixtures produced by
  an older writer.
- Isolated unit tests passed while the composed publication, storage, and
  execution path failed.
- Empty, equal, incompatible, retry, and partially completed cases were not
  consistently covered as a scenario matrix.
- Several fixes crossed many files, increasing the chance of unrelated
  regressions. For example, PRs #293, #493, #501, #512, and #513 changed between
  19 and 79 files.

## Change-amplification signals

History since 2026-05-01 identified these investigation hotspots:

| File | Commits | Added/deleted lines | Current lines |
| --- | ---: | ---: | ---: |
| `Favn.Manifest.Rehydrate` | 28 | +821 / -159 | 1,441 |
| `FavnOrchestrator` facade | 107 | +6,755 / -4,913 | 2,583 |
| PostgreSQL core-authority test | 76 | +11,183 / -1,468 | 9,715 |
| `Favn.SQL.Client` | 25 | +1,618 / -240 | 1,776 |
| `SubmissionBuilder` | 18 | +1,598 / -589 | 1,009 |

These numbers include feature work. They justify examining ownership and test
boundaries, but not cosmetic file splitting.

## Decision

The immediate change is to make manifest serialization reject unsupported
values, unsupported object keys, and duplicate normalized keys. This is a small
fail-fast correction at the publication boundary and preserves all explicitly
supported manifest and JSON-compatible values.

Larger work is tracked separately:

- [#617](https://github.com/eirhop/favn/issues/617): decompose manifest
  rehydration by owned contract without changing wire semantics.
- [#618](https://github.com/eirhop/favn/issues/618): make planned-window and
  execution terminalization ownership explicit.
- [#619](https://github.com/eirhop/favn/issues/619): reduce facade and authority
  test change amplification by contract ownership.
