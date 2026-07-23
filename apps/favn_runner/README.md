# `apps/favn_runner`

Purpose:

- internal execution runtime boundary for business-code asset execution

Visibility:

- internal

Allowed dependencies:

- `favn_core`

Must not depend on:

- `favn_orchestrator`
- `favn_storage_postgres`

Current status:

- implemented runner runtime boundary for manifest-backed execution, connection loading, SQL runtime work, and release-aware readiness checks
- packaged releases require an operator-supplied `FAVN_RUNNER_RELEASE_ID` and
  report it with runtime target/version compatibility; Favn does not inspect
  customer source or dependency provenance
- manifest registration, leasing, work, runtime-input resolution, and relation
  inspection require the exact configured runner release before cache/worker activity
- diagnostics expose only bounded release identity, readiness, and node name;
  results, events, and inspection results echo the configured release id, and the
  server discards lifecycle events and replaces results that do not match their
  stored work identity
