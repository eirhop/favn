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
- packaged releases verify the fixed private `runner-release.json` descriptor
  against runtime target/versions, packaged BEAM digests, and application
  version/lock fingerprints stamped into packaged `.app` files; stamped
  applications and configured plugins must exactly match descriptor inventories,
  and option-selected plugin applications/children must already be fingerprinted
  before any runner service starts
- manifest registration, leasing, work, runtime-input resolution, and relation
  inspection require the exact verified runner release before cache/worker activity
- diagnostics expose only bounded release identity, readiness, and node name;
  results, events, and inspection results echo the verified release id, and the
  server discards lifecycle events and replaces results that do not match their
  stored work identity
