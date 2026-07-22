# Upgrade and rollback

The first production topology has one control plane and one runner. Upgrades are
scheduled, drain-first maintenance operations; this release does not claim a
zero-downtime rolling upgrade.

Before every change, record this rollback tuple:

- control-plane image digest and control-plane build ID;
- runner image digest, runner release ID, and runner contract version;
- active manifest version and required runner release ID for each workspace;
- PostgreSQL schema version and a tested recovery point; and
- the environment revision without secret values.

Do not deploy mutable tags. Tags may locate an artifact, but the platform must
run the resolved digest.

## Control-plane upgrade

1. Select a qualified control-plane digest and run its release-safe
   `preflight-upgrade` operation against the existing database.
2. Confirm a current PostgreSQL backup/PITR point and that every active manifest
   has a current runner release binding.
3. Stop admission and terminate the current control plane, allowing its bounded
   drain to finish before the platform kills the container.
4. From the candidate image, run the required external `migrate`,
   `grant-runtime`, `verify-schema`, and workspace-provisioning operations with
   the correct database identities. Startup does not run them.
5. Start the candidate digest with the unchanged runner and runtime environment.
6. Require full readiness, sign in through View, and execute one SQL plus one
   Elixir smoke run.

Rollback is allowed only while the previous control-plane image is compatible
with the current schema. Stop and drain the candidate, run any documented
backward-compatible release operations, start the previous digest, require
readiness, and repeat the smoke run. If a migration is not backward compatible,
restore through the rehearsed PostgreSQL recovery procedure instead of guessing
or running an unproven downgrade.

The exact container qualification exercises a compatible control-plane upgrade
and image rollback against preserved PostgreSQL state. The database-specific
commands and compatibility checks are in
[`postgresql_operator_runbook.md`](postgresql_operator_runbook.md).

## Runner plus manifest upgrade

1. Build the new runner context and aligned manifest release.
2. Build, push, scan, and select the customer runner image by digest.
3. Publish the aligned manifest as staged; leave the current manifest active.
4. Stop admission, allow current work to drain, and replace the runner with the
   new digest.
5. Require the runner to self-verify and the control plane to report its exact
   release ID.
6. Activate the staged manifest version and resume admission.
7. Execute SQL and Elixir smoke runs.

If replacement or activation fails, keep admission stopped, restore the previous
runner digest, require its previous release ID, reactivate the previous manifest
version, verify the pair, and only then resume admission. Never combine an old
runner with a manifest that requires the new release, or vice versa.

## Manifest-only upgrade

Build against the existing verified runner descriptor. A successful
`mix favn.build.manifest` proves the runner fingerprint is unchanged. Publish the
new release as staged, activate its exact version, and execute a smoke run. No
container restart is required.

Rollback activates the previous immutable manifest version after the same
runner-alignment check. Publication and activation remain separate so a staged
release cannot change production accidentally.
