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

## Local tooling migration to project-owned Compose

Projects upgrading from the removed generated-Compose contract use this
data-preserving sequence:

1. Before upgrading, run the old `mix favn.stop`.
2. Upgrade Favn. If the project has local files under `.favn/data` and no
   `.data` directory, move them once with `mv .favn/data .data`. If both
   directories exist, reconcile them manually.
3. Run `mix favn.init`.
4. Review and commit the new files under `deploy/local/` and `deploy/runner/`.
5. Run `mix favn.install` to rewrite image-only installation metadata.
6. Run `mix favn.dev`.

After the new layout is adopted, the derived project name, default role names,
PostgreSQL volume name, secrets, and `.data` location remain stable. After the
first successful readiness check, Favn removes only the obsolete
`.favn/compose/compose.yml`; it does not remove the committed Compose file,
PostgreSQL volume, data, or containers. Old runtime state cannot safely identify
a consumer-owned deployment and is reported as pre-migration state.

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

The representative repository container gate does not exercise this full
upgrade/rollback drill. Qualify it against the target PostgreSQL service and
deployment image set before production use. The database-specific commands and
compatibility checks are in
[`postgresql_operator_runbook.md`](postgresql_operator_runbook.md).

## Runner plus manifest upgrade

1. Choose a new runner release ID and build the customer-owned runner image.
2. Build the manifest with that same ID, then push, scan, and select the runner
   image by digest.
3. Publish the aligned manifest as staged; leave the current manifest active.
4. Stop admission, allow current work to drain, and replace the runner with the
   new digest.
5. Require the runner to report its configured release ID and compatible
   runtime contract.
6. Activate the staged manifest version and resume admission.
7. Execute SQL and Elixir smoke runs.

If replacement or activation fails, keep admission stopped, restore the previous
runner digest, require its previous release ID, reactivate the previous manifest
version, verify the pair, and only then resume admission. Never combine an old
runner with a manifest that requires the new release, or vice versa.

## Manifest-only upgrade

Reuse the existing runner release ID only after the operator has established
that no executable runner input changed. Build with
`mix favn.build.manifest --runner-release-id ID`, publish the new release as
staged, activate its exact version, and execute a smoke run. Favn validates the
ID binding but does not inspect the customer build inputs. No container restart
is required.

Rollback activates the previous immutable manifest version after the same
runner-alignment check. Publication and activation remain separate so a staged
release cannot change production accidentally.
