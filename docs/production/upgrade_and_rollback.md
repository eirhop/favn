# Upgrade and rollback

The first production topology has one control plane and zero or more resident
or elastic runners. Control-plane upgrades are scheduled, drain-first
maintenance operations; this release does not claim a zero-downtime rolling
control-plane upgrade.

Before every change, record this rollback tuple:

- control-plane image digest and source revision;
- runner image digest, runner release ID, and runner contract version;
- active manifest version and runner release map for each workspace;
- PostgreSQL schema version and a tested recovery point; and
- the environment revision without secret values.

Do not deploy mutable tags. Tags may locate an artifact, but the platform must
run the resolved digest.

## Migration from Docker-based development

Stop the old stack with the version that created it. Preserve or export any
PostgreSQL data before removing old Compose resources. After upgrading:

1. remove obsolete generated `.favn/` Docker state;
2. provide a running PostgreSQL database;
3. export the required environment variables;
4. run explicit migrations and workspace provisioning;
5. start the Docker-free loop with `mix favn.dev`.

Favn does not infer ownership of old containers, networks, volumes, or
databases and does not delete them.

## Control-plane upgrade

1. Select a qualified control-plane digest.
2. Confirm a current PostgreSQL backup/PITR point and that every active manifest
   has a complete runner pool-to-release map.
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
4. Start capacity for the new pool/release beside the old capacity. Require at
   least one new runner to register with its exact release ID and compatible
   runtime contract before activation.
5. Activate the staged manifest version. New runs freeze its pool-to-release
   map; already-submitted and in-flight work remains pinned to the old map.
6. Execute SQL and Elixir smoke runs through the new release.
7. Query the exact old pool/release drain projection. Remove old capacity only
   after `durable_drained` is true and every reported blocker is zero.

Rollback reactivates the previous immutable manifest version and ensures its
exact runner capacity is available. New work then returns to the previous
pool/release map while work already pinned to the new release drains unchanged.
Remove the new capacity only after its exact drain projection is clear. A
runner-only or manifest-only rollback never requires globally stopping
admission; reserve a control-plane drain for control-plane or database changes
that explicitly require it. Never combine a task with a runner release other
than the immutable release frozen into that task.

## Manifest-only upgrade

Reuse each existing pool/release mapping only after the operator has established
that no executable runner input changed. Build with one
`--runner-release POOL=ID` option per effective pool, publish the new release as
staged, activate its exact version, and execute a smoke run. Favn validates the
bindings but does not inspect the customer build inputs. No runner restart is
required.

Rollback activates the previous immutable manifest version after the same
runner-alignment check. Publication and activation remain separate so a staged
release cannot change production accidentally.
