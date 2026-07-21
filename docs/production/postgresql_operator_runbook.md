# PostgreSQL Control-Plane Operator Runbook

Status: Storage V2 implementation runbook; managed-provider proof is tracked by #523
Scope: Favn-owned `favn_control` schema on PostgreSQL 18

Implementation architecture and ER diagrams live in
`docs/storage/postgresql/architecture.md` and
`docs/storage/postgresql/data-model.md`.

This runbook covers the shared Favn control plane. It does not back up or restore
customer blob data, DuckLake PostgreSQL metadata databases, DuckDB files, Key
Vault contents, or runner/plugin-owned data.

## Supported production shape

- PostgreSQL 18 on the latest tested minor release.
- One Favn-owned database and one `favn_control` schema.
- One migrator identity with DDL authority and a separate `favn_runtime` identity.
- Verified TLS with hostname checking on every production connection.
- At least two orchestrator nodes are required for application-node failure
  tolerance after the deployable multi-node topology in #522 exists. The database
  coordination foundation alone is not a supported deployment artifact.
- Provider-managed high availability and point-in-time recovery (PITR).
- Customer isolation through explicit workspace context and composite workspace
  keys; customers never receive database credentials.

PostgreSQL may be Azure Database for PostgreSQL Flexible Server or an equivalent
managed service. Do not place DuckLake customer metadata in the Favn control-plane
database.

## Required configuration

Runtime nodes require:

```text
FAVN_DATABASE_URL=ecto://favn_runtime:<secret>@<host>/<database>
FAVN_DATABASE_SSL_CA_FILE=/run/secrets/postgresql-ca.pem
FAVN_RUNTIME_INPUT_PIN_KEYS='{"1":"<base64-32-byte-key>","2":"<base64-32-byte-key>"}'
FAVN_RUNTIME_INPUT_PIN_KEY_VERSION=<current-positive-integer>
FAVN_INSTANCE_ID=<stable-node-or-replica-identity>
FAVN_WORKSPACE_IDS=<comma-separated-workspaces-enabled-for-scheduling-and-bootstrap>
```

Migration jobs use a separate URL for the migrator role. Never put database URLs
or runtime-input keys in application manifests, logs, support bundles, or command
history. `FAVN_RUNTIME_INPUT_PIN_KEYS` is a JSON object containing at most 32
version-to-key entries; the current version must be present. Retain every version
reported as referenced by readiness until the corresponding pins have been purged
or re-encrypted. Readiness fails closed when PostgreSQL references a version absent
from the configured keyring.

Production rejects disabled TLS. The configured CA file must be a regular file and
certificate hostname verification must succeed.

`FAVN_WORKSPACE_IDS` configures scheduler and explicit bootstrap scope; it is not a
run-recovery authority boundary. Recovery pages active workspace identities directly
from PostgreSQL so a provisioned workspace cannot lose orphan recovery merely because
one node's environment list is stale.

## Connection budget

Choose the pool before adding replicas:

```text
(orchestrator replica count × pool_size)
+ migration connections
+ monitoring and administrator headroom
<= PostgreSQL connection budget
```

The default pool is 15 connections per node. Keep at least 20% of the server limit
free for failover, migrations, monitoring, and incident response. PgBouncer may
protect connection count, but it cannot replace database capacity. This pool is
unrelated to DuckLake metadata connection or write-concurrency budgets.

## Local development

Docker and PostgreSQL client tools are prerequisites.

```bash
scripts/postgres/setup
```

This starts the digest-pinned PostgreSQL 18 container, migrates `favn_control`,
grants the runtime role, and provisions the `local-dev` workspace.
`scripts/postgres/stop` preserves the volume. The destructive
reset is explicit:

```bash
scripts/postgres/reset
```

Developers using another service set `FAVN_DATABASE_MIGRATOR_URL` before setup.
Credentials in `compose.postgres.yml` are local-only.

The local container does not enable TLS. Local development uses its explicit dev
configuration. A production-config acceptance launcher may use plaintext loopback
only with both `FAVN_DATABASE_SSL_MODE=disable` and
`FAVN_UNSAFE_ALLOW_PLAINTEXT_DATABASE=true`; never deploy that interlock.

## Deployment and migrations

Runtime nodes never migrate at boot.

1. Record the current image and active manifest IDs, then run candidate-image
   preflight with a read-capable database role:

   ```bash
   bin/favn_control_plane eval \
     'IO.inspect(FavnStoragePostgres.Release.preflight_upgrade())'
   ```

   An error has code `:runner_identity_upgrade_blocked`. Its
   `:active_legacy_manifests` sample field identifies active deployments that
   still point at pre-runner-identity manifests; `:nonterminal_legacy_runs`
   identifies unfinished runs still bound to them. Republish and activate an
   aligned current manifest, then explicitly finish or terminate every listed
   legacy run during the maintenance window before completing the upgrade. The
   result reports total and per-category blocker counts and at most 100 samples
   from each category, with `truncated?: true` when more remain.
2. Confirm a current backup/PITR recovery point and recent successful restore drill.
3. Prevent rollout of runtime code that requires the new schema.
4. Run the candidate image with the migrator identity to apply migrations and
   grants:

   ```bash
   bin/favn_control_plane eval 'IO.inspect(FavnStoragePostgres.Release.migrate())'
   bin/favn_control_plane eval 'IO.inspect(FavnStoragePostgres.Release.grant_runtime())'
   ```

   Then run exact schema verification from a separate one-off process configured
   with the runtime database identity. Production verification deliberately
   rejects a connection whose current database user is not the configured runtime
   role:

   ```bash
   bin/favn_control_plane eval 'IO.inspect(FavnStoragePostgres.Release.verify_schema())'
   ```

   The equivalent development wrappers are:

   ```bash
   FAVN_DATABASE_URL="$MIGRATOR_DATABASE_URL" mix favn.postgres.migrate
   FAVN_DATABASE_URL="$MIGRATOR_DATABASE_URL" \
     mix favn.postgres.grant_runtime --role favn_runtime
   ```

   Provision a workspace before adding it to a runtime's allowed workspace set.
   Production uses the candidate release image with the elevated database
   identity:

   ```bash
   bin/favn_control_plane eval \
     'IO.inspect(FavnStoragePostgres.Release.provision_workspace(%{workspace_id: "salmon-one", slug: "salmon-one", display_name: "Salmon One"}))'
   ```

   The development wrapper is:

   ```bash
   FAVN_DATABASE_URL="$MIGRATOR_DATABASE_URL" \
     mix favn.postgres.provision_workspace \
       --id salmon-one --slug salmon-one --name "Salmon One"
   ```

5. Start one canary and require readiness to report `ready?: true`.
6. Check database errors, lock waits, pool queue time, projection lag, and outbox lag.

Release operations emit start/stop telemetry at
`[:favn, :storage_postgres, :release_operation, :start | :stop]`. Completion
events contain a bounded duration measurement and metadata limited to operation,
status, and a stable error code; logs must never contain database URLs,
credentials, TLS material, or key values.

## Manifest activation and runner alignment

Manifest publication is safe while the runner is offline: it validates and stores
an immutable staged release without changing any workspace deployment. Activation
is the compatibility gate. For the exact staged manifest, the control plane:

1. validates the requested workspace target selection;
2. requires runner diagnostics to report both explicit readiness and a canonical
   runner release id;
3. requires that id to equal the manifest's `required_runner_release_id`;
4. verifies or registers the manifest in the runner cache; and
5. commits the immutable deployment and active pointer.

A runner outage or incomplete diagnostics returns `runner_unavailable` and leaves
the active deployment unchanged. A release mismatch returns
`runner_release_mismatch` with only the required and actual release ids. A cache
collision returns `runner_manifest_conflict`. Both are conflicts and also leave the
active deployment unchanged. Audit records and operator responses include release
ids, never runner paths, environment values, or secrets.

Monitor `manifest_publication_succeeded`, `manifest_publication_rejected`,
`manifest_activation_succeeded`, `manifest_activation_rejected`, and
`runner_release_diagnostics_checked` on the orchestrator telemetry prefix. The
diagnostic event reports bounded latency and readiness status. A mismatch reports
only the required and actual release ids. Activation rejection audit records use a
stable `rejection_reason`; they deliberately omit selection, deployment
configuration, environment values, and raw runner errors.

Every admitted run pins the deployment id, manifest id, manifest content hash, and
runner release id. Recovery rechecks this tuple before starting a run server. After
an upgrade, historical terminal snapshots without a release binding remain visible
for audit; a pending or running legacy snapshot fails closed with
`legacy_runner_release_unbound`. Do not edit those rows. Republish an aligned
manifest for new work and resolve or terminate legacy work through an explicit
maintenance decision before reopening admission.
7. Roll out remaining nodes gradually.

Readiness rejects PostgreSQL majors other than 18, a mismatched catalog-definition
fingerprint (column types/nullability/defaults plus every constraint and index on
owned tables), missing or unexpected columns, missing migration versions, unknown
future migration versions, a missing/blocked projector cursor, and—under production
configuration—an overprivileged runtime role. Projection lag is reported separately.
It also rejects a runtime-input keyring missing any version recorded in the compact
pin-key inventory. Do not bypass readiness. Rollback after a destructive migration
is restore of the matching backup plus the previous application release, not an
unreviewed down-migration.

### Retiring a runtime-input key

Do not remove a key version merely because a newer version is current. A persisted
pin remains encrypted with the version recorded on its row, and exact command replay
returns that canonical pin without rewriting it.

First inspect version numbers and pin counts. Neither command reads key material:

```bash
bin/favn_control_plane eval \
  'IO.inspect(FavnStoragePostgres.Release.runtime_input_key_inventory())'
```

The one-off process validates the complete keyring directly from
`FAVN_RUNTIME_INPUT_PIN_KEYS` and protects the version selected by
`FAVN_RUNTIME_INPUT_PIN_KEY_VERSION`; it does not depend on control-plane startup
having populated application state.

After the retention workflow has purged or re-encrypted every pin using the old
version, compact that explicit non-current version:

```bash
bin/favn_control_plane eval \
  'IO.inspect(FavnStoragePostgres.Release.compact_runtime_input_keys([1]))'
```

The equivalent development wrappers are:

```bash
FAVN_DATABASE_URL="$MIGRATOR_DATABASE_URL" \
  mix favn.postgres.runtime_input_key_inventory
FAVN_DATABASE_URL="$MIGRATOR_DATABASE_URL" \
  mix favn.postgres.compact_runtime_input_keys --version 1
```

Compaction briefly locks runtime-input pin writes. It atomically rejects the whole
request if any requested version still has a pin, rejects the configured current
version, and treats an already-absent version as a successful retry. Confirm
readiness remains healthy, then remove the retired version from the configured
keyring.

## Runtime privileges

The runtime role receives schema usage plus required table/sequence DML. The grant
task removes inherited role memberships and `CREATE` on the current database,
`public`, and `favn_control`; it also removes DML from
`favn_control.schema_migrations` and verifies the converged result. Because PostgreSQL
grants database/schema creation through `PUBLIC`, the task revokes those shared
defaults for this dedicated Favn database. Re-run it after a migration adds an object.

```sql
SELECT table_name, privilege_type
FROM information_schema.role_table_grants
WHERE grantee = 'favn_runtime' AND table_schema = 'favn_control'
ORDER BY table_name, privilege_type;
```

Never grant customer identities direct access.

## Backups and restore drills

The managed service owns physical backups, WAL archival, cross-zone HA, and PITR.
Configure production recovery objectives with the provider; Favn does not schedule
physical backups.

At least monthly, and before a major upgrade or destructive migration:

1. Restore the provider backup/PITR point into a new isolated server or database.
2. Apply the normal runtime grants, then run schema and authority verification
   with a read-capable restored runtime identity:

   ```bash
   bin/favn_control_plane eval \
     'IO.inspect(FavnStoragePostgres.Release.verify_restore())'
   ```

   The development wrapper is
   `FAVN_DATABASE_URL="$RESTORED_DATABASE_URL" mix favn.postgres.verify_restore`.
   Restore verification gives its checked-out database session a bounded
   ten-minute statement/client timeout for large authority scans and restores
   the normal session timeout before returning the connection.

3. Start an isolated canary with external dispatch disabled.
4. Backfill missing disposable projection rows in bounded batches and compare
   counts/lag. This does not repair corrupt current rows; a restore drill validates
   those rows from the restored snapshot instead.
5. Record backup time, requested/actual recovery points, duration, schema/PostgreSQL
   versions, verification result, and operator as release evidence.
6. Destroy the isolated target and temporary credentials.

For a logical development/CI drill, restore into a pre-created isolated database:

```bash
FAVN_DATABASE_SOURCE_URL="$SOURCE_DATABASE_URL" \
FAVN_DATABASE_RESTORE_URL="$EMPTY_RESTORE_DATABASE_URL" \
FAVN_RESTORE_ARTIFACT_DIR=/secure/evidence/restore-$(date -u +%Y%m%dT%H%M%SZ) \
scripts/postgres/verify_restore
```

The script refuses identical URLs, checks the dump checksum, and verifies exact
schema, authority relationships, and cursor bounds. A logical dump is test evidence,
not a replacement for PITR.

After a disaster, restore customer data-plane systems independently and decide their
compatible recovery points before enabling dispatch. A control-plane run may refer
to a data-plane object restored to another point in time.

## Retention and maintenance

Run bounded maintenance repeatedly until it returns less than its configured limit.
Never put unbounded delete, rebuild, or reconciliation work in an interactive request.

Use a stable job id and repeat the exact command while the returned status is
`:running`:

```bash
mix favn.postgres.maintenance backfill-missing \
  --job-id projection-backfill-salmon-one-20260717 \
  --workspace salmon-one --projection freshness --limit 250

mix favn.postgres.maintenance backfill-missing \
  --job-id asset-attempt-backfill-salmon-one-20260720 \
  --workspace salmon-one --projection asset-attempts --limit 250

mix favn.postgres.maintenance reconcile \
  --job-id capacity-audit-20260717 --invariant capacity-counters \
  --workspace salmon-one --repair --limit 500

mix favn.postgres.maintenance purge \
  --job-id expired-sessions-20260717 --target sessions \
  --workspace salmon-one --cutoff 2026-07-10T00:00:00Z --limit 1000

mix favn.postgres.maintenance purge \
  --job-id orphaned-execution-packages-20260717 --target execution-packages \
  --cutoff 2026-07-10T00:00:00Z --limit 1000
```

The command identity includes its full configuration. Reusing a job id with changed
scope, cutoff, target, or limit is rejected.

Run the `asset-attempts` backfill for each existing workspace after introducing or
recreating that disposable projection so historical run detail is available.

- completed idempotency records: purge after seven days;
- expired/revoked sessions: retain for the approved audit window;
- terminal claims and projection failures: bounded policy-driven retention;
- logs: purge in bounded batches after the approved audit window;
- unreferenced execution packages: purge at platform scope after a publication grace
  window; packages linked to any manifest are protected by the query and foreign key;
- canonical runs, run events, backfills, manifests, audit records, and published
  outbox rows are retained indefinitely in the initial production release. Monitor
  their growth and introduce deletion only with explicit referential and SSE replay
  watermarks.

Monitor table/index size, dead tuples, autovacuum, transaction age, and batch duration.
Tune per-table autovacuum only from production evidence. Do not use `VACUUM FULL`
during ordinary operation.

## Required monitoring and alerts

Collect at minimum:

- readiness and exact migration/schema status;
- Ecto checkout/queue duration and connection failures;
- statement timeout, deadlock, serialization failure, and lock-wait rate;
- PostgreSQL CPU, memory, storage, IOPS, connections, replication/failover state,
  transaction age, dead tuples, and autovacuum health;
- unsequenced outbox count/age and publication watermark;
- projector cursor lag, lease conflicts, batch duration, and failure rows;
- overdue ownership, schedule, admission, materialization, and backfill claims;
- maintenance backlog/duration, backup age, and last successful restore drill.

Page on readiness failure, backup/PITR failure, replication failure, sustained pool
saturation, growing outbox/projector lag, or repeated unknown outcomes.

## Incident procedures

### Database unavailable

Stop new dispatch and return typed unavailable errors. Do not switch to memory or
SQLite. Restore service, confirm readiness, then reconcile leases, ownership, outbox
sequencing, and projections before resuming.

### Pool saturation

Check slow/blocked statements and replica count before increasing the pool. Cancel
only statements whose outcome is known safe. Adding replicas without reducing the
per-node pool can worsen the incident.

### Lock contention or deadlock

Capture redacted `pg_stat_activity`/`pg_locks` evidence and operation telemetry.
Repeated deadlocks are correctness bugs; do not add broad retries to unknown-outcome
writes.

### Projection failure or lag

Authoritative commands continue without projections. Correct the projector, then run
a bounded missing-row backfill for the affected projection/workspace. It safely fills
absent rows but does not overwrite a row whose publication cursor is already current.
Corrupted current rows require restore or a future shadow-generation repair workflow.
Never repair authoritative history from a projection.

### Unknown command outcome

Retry only with the exact command/idempotency identity. Its transactional record or
domain command identity resolves whether the mutation committed. Never generate a
new identity merely because the client lost its connection.

## PostgreSQL upgrades

Minor upgrades require CI, restore-drill, and canary evidence. A major upgrade also
requires a production-size restored snapshot, high-growth query-plan comparison,
rollback/restore rehearsal, and explicit architecture approval. Storage V2 currently
accepts PostgreSQL 18 only.
