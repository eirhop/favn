# Single-Node Operator Runbook

This runbook documents the first production deployment path for Favn: one backend
node running the orchestrator, runner, and scheduler with SQLite control-plane
persistence and runner-owned DuckDB data-plane execution.

Use this with `docs/production/single_node_contract.md`, which remains the
source of truth for the supported topology and production boundaries.

## Scope

Supported in this runbook:

- One backend node.
- One active scheduler on that backend node.
- `FAVN_STORAGE=sqlite` for control-plane persistence.
- Durable attached storage for the SQLite control-plane database.
- Local DuckDB database files or external DuckLake/object-storage systems used by
  named DuckDB connections.
- The project-local backend launcher produced by `mix favn.build.single`.

Explicitly not supported here:

- Multiple backend nodes writing to one SQLite database.
- Shared SQLite on NFS, SMB, distributed filesystems, or object-storage mounts.
- Postgres production mode.
- Distributed runner pools or multi-node scheduler leadership.
- High-availability active/passive or active/active orchestrators.
- Browser/web production hardening beyond the backend API readiness surface.

## Prerequisites

On the backend host, provide:

- A private Favn checkout/runtime source root created by `mix favn.install`.
- `curl`, used by generated `bin/start` for readiness polling.
- Durable attached storage for the SQLite control-plane database.
- Durable storage for any local DuckDB database files that contain production
  asset data.
- A secret-management process for service tokens, bootstrap admin credentials,
  source-system credentials, and DuckDB/DuckLake credentials.

The current artifact is not a self-contained or relocatable release. It depends
on the recorded runtime source root from the install/build machine.

## Build The Artifact

From the consumer project:

```bash
mix favn.install --skip-web-install
mix favn.build.single
```

The build writes a single-node artifact under `.favn/dist/single/<build_id>/`.
Important files:

- `bin/start`: starts the backend runtime and waits for readiness.
- `bin/stop`: stops the backend runtime by PID file.
- `env/backend.env.example`: supported backend environment keys.
- `runner/manifest.json`: packaged manifest used by the backend launcher.
- `runner/metadata.json`: packaged manifest identity metadata.
- `OPERATOR_NOTES.md`: artifact-local notes and unsupported modes.

Treat the artifact directory as read-only after build. Mutable runtime state
belongs in `FAVN_SINGLE_NODE_HOME`, `FAVN_SQLITE_PATH`, DuckDB paths, and logs.

## Persistence Paths

Keep control-plane, data-plane, and artifact paths separate:

- `FAVN_SQLITE_PATH` is the SQLite control-plane database path.
- Named DuckDB connection database paths are data-plane paths.
- `FAVN_SINGLE_NODE_HOME` stores generated runtime files such as PID, boot file,
  and backend log.
- `.favn/dist/...` is build output, not production persistence.

Use absolute paths for production SQLite and DuckDB local-file storage. Parent
directories must exist and be writable before startup.

## Runtime Environment

From the artifact root, create a backend env file from the generated example:

```bash
cp env/backend.env.example env/backend.env
```

Replace placeholder secrets before starting. The example service token is
intentionally invalid, and the first-admin password is intentionally blank.

When running operator shell commands that reference values from `env/backend.env`,
load that file into the current shell first:

```bash
set -a
. env/backend.env
set +a
```

Supported backend environment keys are:

- `FAVN_STORAGE=sqlite`.
- `FAVN_SQLITE_PATH`, an absolute path on durable attached storage.
- `FAVN_SQLITE_MIGRATION_MODE`, `manual` or `auto`.
- `FAVN_SQLITE_BUSY_TIMEOUT_MS`, positive integer, default `5000`.
- `FAVN_SQLITE_POOL_SIZE=1`.
- `FAVN_ORCHESTRATOR_API_BIND_HOST`, IPv4 address, default `127.0.0.1`.
- `FAVN_ORCHESTRATOR_API_PORT`, `1..65535`, default `4101`.
- `FAVN_ORCHESTRATOR_API_SERVICE_TOKENS`, comma-separated
  `service_identity:token` entries.
- `FAVN_BOOTSTRAP_ORCHESTRATOR_SERVICE_TOKEN`, used by bootstrap tooling unless
  `--service-token` is passed.
- `FAVN_ORCHESTRATOR_BOOTSTRAP_USERNAME`, first admin username.
- `FAVN_ORCHESTRATOR_BOOTSTRAP_PASSWORD`, first admin password, 15 to 1,024
  characters.
- `FAVN_ORCHESTRATOR_BOOTSTRAP_DISPLAY_NAME`, optional, default `Favn Admin`.
- `FAVN_ORCHESTRATOR_BOOTSTRAP_ROLES`, optional, default `admin`.
- `FAVN_ORCHESTRATOR_AUTH_SESSION_TTL`, positive integer seconds, default
  `43200`.
- `FAVN_SCHEDULER_ENABLED`, boolean, default `true`.
- `FAVN_SCHEDULER_TICK_MS`, integer at least `100`, default `15000`.
- `FAVN_SCHEDULER_MAX_MISSED_ALL_OCCURRENCES`, positive integer, default `1000`.
- `FAVN_RUNNER_MODE=local`.
- `FAVN_VIEW_PUBLIC_ORIGIN`, the browser-facing absolute origin for `favn_view`.
  Production origins must use `https`; `http` is accepted only for localhost.
- `FAVN_VIEW_SECRET_KEY_BASE`, the Phoenix endpoint signing/encryption secret.
  Generate a unique value with `mix phx.gen.secret` or an equivalent secret
  generator and keep it in secret management, not Git. It must be at least 64
  characters.
- `FAVN_SINGLE_NODE_HOME`, optional runtime home override.
- `FAVN_ENV_FILE`, optional path to an env file loaded by `bin/start`.
- `FAVN_STARTUP_TIMEOUT_SECONDS`, optional startup readiness timeout.
- `FAVN_STOP_TIMEOUT_SECONDS`, optional stop timeout.
- Any runtime config keys required by authored assets or named SQL connections,
  such as DuckDB database paths and source-system credentials.

For a fresh node, use this minimum first-start shape unless the database has
already been migrated by an operator-controlled process:

```bash
FAVN_STORAGE=sqlite
FAVN_SQLITE_PATH=/var/lib/favn/control-plane.sqlite3
FAVN_SQLITE_MIGRATION_MODE=auto
FAVN_SQLITE_POOL_SIZE=1
FAVN_RUNNER_MODE=local
FAVN_VIEW_PUBLIC_ORIGIN=https://favn.example.com
FAVN_VIEW_SECRET_KEY_BASE=<replace-with-mix-phx-gen-secret-output>
FAVN_ORCHESTRATOR_BOOTSTRAP_USERNAME=admin
FAVN_ORCHESTRATOR_BOOTSTRAP_PASSWORD=<replace-with-15-plus-character-password>
```

With `manual`, startup requires a schema-ready SQLite database. Favn does not yet
ship a separate production migration command; track follow-up #350.

## First-Run Bootstrap

Start the backend from the artifact root:

```bash
set -a
. env/backend.env
set +a
bin/start
```

Then register and activate the packaged manifest from the consumer project root,
where the `mix favn.bootstrap.single` task is available:

```bash
artifact_dir=.favn/dist/single/<build_id>
set -a
. "$artifact_dir/env/backend.env"
set +a
mix favn.bootstrap.single \
  --manifest "$artifact_dir/runner/manifest.json" \
  --orchestrator-url http://127.0.0.1:4101 \
  --service-token "$FAVN_BOOTSTRAP_ORCHESTRATOR_SERVICE_TOKEN"
```

`mix favn.bootstrap.single` verifies service-token auth, validates the manifest,
registers the manifest, activates it by default, asks the orchestrator to
register the persisted manifest with the local runner, and verifies active
manifest selection.

The bootstrap workflow uses orchestrator APIs. It does not write SQLite directly.

## First Login And Validation Run

These examples assume the backend env file is loaded into the current operator
shell.

For browser operators, open `favn_view` at the configured
`FAVN_VIEW_PUBLIC_ORIGIN` and sign in at `/login` with the bootstrap operator
credentials. The Phoenix browser session stores only a random browser session id
and LiveView socket topic; the raw orchestrator session token
stays server-side in the web process. Actors, roles, password hashes, audit
entries, and session revocation state remain orchestrator-owned. `/logout`
revokes the current orchestrator session, clears the browser session, and
disconnects existing LiveView sockets for that session. Web health and readiness
routes remain unauthenticated.

After bootstrap, verify that the first admin can log in:

```bash
curl -fsS \
  -X POST \
  -H "Authorization: Bearer $FAVN_BOOTSTRAP_ORCHESTRATOR_SERVICE_TOKEN" \
  -H "Content-Type: application/json" \
  -d "{\"username\":\"$FAVN_ORCHESTRATOR_BOOTSTRAP_USERNAME\",\"password\":\"$FAVN_ORCHESTRATOR_BOOTSTRAP_PASSWORD\"}" \
  http://127.0.0.1:4101/api/orchestrator/v1/auth/password/sessions
```

Record the returned `actor.id` and `session_token` in the operator shell:

```bash
FAVN_ACTOR_ID=<actor-id-from-login-response>
FAVN_SESSION_TOKEN=<session-token-from-login-response>
```

Verify the active manifest through the authenticated operator path:

```bash
curl -fsS \
  -H "Authorization: Bearer $FAVN_BOOTSTRAP_ORCHESTRATOR_SERVICE_TOKEN" \
  -H "x-favn-actor-id: $FAVN_ACTOR_ID" \
  -H "x-favn-session-token: $FAVN_SESSION_TOKEN" \
  http://127.0.0.1:4101/api/orchestrator/v1/manifests/active
```

Submit a narrow validation run only after choosing a real manifest target id from
the active manifest payload:

```bash
curl -fsS \
  -X POST \
  -H "Authorization: Bearer $FAVN_BOOTSTRAP_ORCHESTRATOR_SERVICE_TOKEN" \
  -H "x-favn-actor-id: $FAVN_ACTOR_ID" \
  -H "x-favn-session-token: $FAVN_SESSION_TOKEN" \
  -H "Idempotency-Key: first-validation-$(date -u +%Y%m%dT%H%M%SZ)" \
  -H "Content-Type: application/json" \
  -d '{"target":{"type":"pipeline","id":"pipeline:Elixir.MyApp.Pipelines.ProductionSmoke"},"manifest_selection":{"mode":"active"}}' \
  http://127.0.0.1:4101/api/orchestrator/v1/runs
```

Replace the example pipeline id with a safe target from the deployed manifest.

## Start, Stop, And Restart

Run these commands from the artifact root. `bin/start` sources `env/backend.env`
automatically when that file exists.

Start:

```bash
bin/start
```

Stop:

```bash
bin/stop
```

Restart:

```bash
bin/stop
bin/start
```

Generated scripts use:

- PID file: `$FAVN_SINGLE_NODE_HOME/run/backend.pid`, or `var/run/backend.pid`
  under the artifact when `FAVN_SINGLE_NODE_HOME` is unset.
- Backend log: `$FAVN_SINGLE_NODE_HOME/log/backend.log`, or `var/log/backend.log`
  under the artifact when `FAVN_SINGLE_NODE_HOME` is unset.
- Readiness URL:
  `/api/orchestrator/v1/health/ready` on the configured API host and port.

## Health, Readiness, And Diagnostics

Liveness:

```bash
curl -fsS http://127.0.0.1:4101/api/orchestrator/v1/health/live
```

Readiness:

```bash
curl -fsS http://127.0.0.1:4101/api/orchestrator/v1/health/ready
```

Detailed diagnostics require a configured service token. From the artifact root:

```bash
set -a
. env/backend.env
set +a
curl -fsS \
  -H "Authorization: Bearer $FAVN_BOOTSTRAP_ORCHESTRATOR_SERVICE_TOKEN" \
  http://127.0.0.1:4101/api/orchestrator/v1/diagnostics
```

Readiness should fail before serving traffic when required config is invalid,
SQLite is unavailable or schema-not-ready, the scheduler cannot load required
state, or the local runner boundary is unavailable. Diagnostics redact secrets
and database paths.

## Backup And Restore Overview

SQLite control-plane backup and DuckDB data-plane backup are separate
responsibilities.

SQLite backup covers Favn control-plane state such as manifests, active-manifest
selection, runs, run events, scheduler state, auth/session/audit state,
idempotency records, and operational read models stored in SQLite.

SQLite backup does not cover:

- Local DuckDB database files.
- DuckLake metadata databases or object-storage data paths.
- External source systems.
- Runtime logs or crash dumps.
- Service-token/source/DuckDB credentials stored outside SQLite.
- Build artifacts.

The tested and recommended golden path is a stopped-backend backup and restore.
Favn does not yet provide a write-pause command, online backup command, or backup
verification command. Track follow-up #350 for SQLite backup/migration command
automation and follow-up #351 for DuckDB data-plane backup automation design.

## SQLite Control-Plane Backup

Use this conservative procedure from the artifact root when the backend can be
stopped.

1. Record the artifact version/build id and runtime env file used by the node.
2. Stop the backend:

```bash
bin/stop
```

3. Copy the SQLite database and any SQLite sidecar files from the same directory:

```bash
set -a
. env/backend.env
set +a
backup_dir=/backups/favn/$(date -u +%Y%m%dT%H%M%SZ)
mkdir -p "$backup_dir"
cp "$FAVN_SQLITE_PATH" "$backup_dir/control-plane.sqlite3"
[ ! -f "$FAVN_SQLITE_PATH-wal" ] || cp "$FAVN_SQLITE_PATH-wal" "$backup_dir/control-plane.sqlite3-wal"
[ ! -f "$FAVN_SQLITE_PATH-shm" ] || cp "$FAVN_SQLITE_PATH-shm" "$backup_dir/control-plane.sqlite3-shm"
```

4. Store the backend env file and operational notes separately, with secrets
   handled by the deployment secret manager.
5. Start the backend again:

```bash
bin/start
```

6. Check readiness and diagnostics.

Do not copy only the SQLite file while the backend is running. If an operator
uses an online SQLite backup API or filesystem snapshot instead, that procedure
must preserve SQLite consistency and is outside current Favn automation.

## SQLite Control-Plane Restore

Restore SQLite onto a fresh backend node using the same Favn artifact/release
version as the backup unless a later migration runbook explicitly supports the
version change. Run these commands from the artifact root.

1. Ensure the backend is stopped on the target node:

```bash
bin/stop
```

2. Restore the backend env file and secrets through the normal deployment secret
   process.
3. Create the SQLite parent directory and restore the database file:

```bash
set -a
. env/backend.env
set +a
mkdir -p "$(dirname "$FAVN_SQLITE_PATH")"
cp /backups/favn/<backup-id>/control-plane.sqlite3 "$FAVN_SQLITE_PATH"
[ ! -f /backups/favn/<backup-id>/control-plane.sqlite3-wal ] || cp /backups/favn/<backup-id>/control-plane.sqlite3-wal "$FAVN_SQLITE_PATH-wal"
[ ! -f /backups/favn/<backup-id>/control-plane.sqlite3-shm ] || cp /backups/favn/<backup-id>/control-plane.sqlite3-shm "$FAVN_SQLITE_PATH-shm"
```

4. Start the backend:

```bash
bin/start
```

5. Verify readiness and diagnostics:

```bash
set -a
. env/backend.env
set +a
curl -fsS http://127.0.0.1:4101/api/orchestrator/v1/health/ready
curl -fsS \
  -H "Authorization: Bearer $FAVN_BOOTSTRAP_ORCHESTRATOR_SERVICE_TOKEN" \
  http://127.0.0.1:4101/api/orchestrator/v1/diagnostics
```

6. Verify the active manifest, recent run history, scheduler status, and runner
   status through operator diagnostics or the web/operator client when available.

Supported restore shape:

- Fresh backend node.
- Same artifact/release version.
- One active backend writer.
- SQLite path on durable attached storage.

Unsupported restore shape:

- Restoring onto a running backend.
- Restoring a newer SQLite schema into an older release.
- Restoring inconsistent or manually edited SQLite rows.
- Treating SQLite restore as DuckDB asset-data restore.

## DuckDB Data-Plane Backup

DuckDB is runner/plugin-owned data-plane infrastructure. Back it up separately
from SQLite.

For local DuckDB database files:

1. Inventory every named DuckDB connection used by the active manifest and its
   resolved database path.
2. Stop the backend, or otherwise guarantee no Favn run or external process is
   writing those DuckDB files.
3. Copy each DuckDB database file and any DuckDB sidecar/write-ahead-log files
   that exist for that database according to the DuckDB storage mode in use.
4. Store the inventory of logical connection name to backup artifact, without
   embedding source-system or object-storage secrets.
5. Restart the backend and run readiness/diagnostics.

For DuckLake, object storage, or external systems:

- Back up object-storage data paths with the provider's tooling.
- Back up external DuckLake metadata databases with the owning database's backup
  procedure.
- Back up source systems according to the owning team's process.
- Store credentials in the secret manager, not in backup notes.

Do not assume SQLite backup includes DuckDB data. A restored control plane may
reference DuckDB paths whose files were not restored.

## DuckDB Data-Plane Restore

For local DuckDB files:

1. Stop the backend.
2. Restore each DuckDB database file and its sidecar/write-ahead-log files to the
   absolute path expected by the named connection.
3. If paths changed, update the connection runtime config and deploy/register a
   manifest compatible with those paths.
4. Start the backend.
5. Run readiness/diagnostics and submit a narrow validation run against the
   restored data-plane connection.

For DuckLake/object-storage deployments, restore the external systems first,
then restore or update the Favn runtime config that points to them. Favn does not
own those systems and cannot make SQLite restore recover them.

## Upgrade And Migration Notes

The supported restore path is same-version restore. Before upgrading:

- Take a stopped-backend SQLite backup.
- Back up DuckDB data-plane files and external systems separately.
- Record artifact build metadata and runtime env keys.
- Confirm the target release's migration expectations.

SQLite readiness diagnostics classify empty, ready, missing, upgrade-required,
newer-than-release, and inconsistent schemas. If readiness reports a schema
problem, do not serve traffic until the schema issue is resolved.

Rollback is backup restore, not down-migration. Restore the previous SQLite
backup with the matching previous artifact version.

## Logs, Secrets, And Redaction

Do not paste raw secrets into GitHub issues, logs, runbooks, or diagnostics
snapshots. Treat these as secret material:

- `FAVN_ORCHESTRATOR_API_SERVICE_TOKENS` values.
- `FAVN_BOOTSTRAP_ORCHESTRATOR_SERVICE_TOKEN`.
- Browser/session tokens.
- Source-system credentials referenced by authored assets.
- DuckDB/DuckLake/object-storage credentials.

Diagnostics and storage readiness are expected to redact configured SQLite paths
and secret values. If a diagnostic leaks a secret, treat it as a bug and rotate
the credential.

## Troubleshooting

`bin/start` says the backend did not become ready:

- Check `$FAVN_SINGLE_NODE_HOME/log/backend.log`.
- Verify `FAVN_STORAGE=sqlite`.
- Verify `FAVN_SQLITE_PATH` is absolute and its parent exists.
- Verify `FAVN_SQLITE_POOL_SIZE=1`.
- Verify service tokens are at least 32 characters and not placeholders.
- Check `/api/orchestrator/v1/health/ready` for failed readiness checks.

Readiness reports `storage` failure:

- Confirm the SQLite parent directory exists and is writable.
- Confirm the database is on durable attached storage, not NFS/object storage.
- Confirm the schema is ready for the running release.
- If using `manual` migration mode on a fresh DB, switch to an approved
  pre-migrated DB or use `auto` for the current fresh-node path.

Diagnostics reports no active manifest:

- Run `mix favn.bootstrap.single` with the packaged manifest and a valid service
  token.
- Confirm bootstrap output reports active manifest verification as `matched`.

Run fails with missing runtime config:

- Check the asset or connection runtime config key named in the error.
- Add the required environment variable or secret-manager value.
- Do not paste secret values into issue reports.

DuckDB connection fails:

- Confirm the named connection resolves to an explicit production-safe database
  path when using local-file storage.
- Confirm the parent directory exists and is writable.
- Confirm the file was restored separately from SQLite.
- Confirm DuckLake/object-storage credentials are available through the deployment
  secret process.

## Evidence And Known Limits

Evidence:

- `docs/production/single_node_acceptance_matrix.md` summarizes PR #348
  single-node acceptance evidence.
- `apps/favn_local/test/acceptance/single_node_production_acceptance_test.exs`
  verifies build, startup, readiness, bootstrap, manifest-pinned run submission,
  DuckDB-backed SQL execution, diagnostics, invalid config failure, restart
  survival, and artifact immutability.
- `apps/favn_storage_sqlite/test/sqlite_control_plane_restore_test.exs` verifies
  stopped-backend SQLite restore semantics for manifests, active manifest, runs,
  run events, scheduler state, and operational-backfill read models.
- `apps/favn_storage_sqlite/test/sqlite_single_node_bootstrap_acceptance_test.exs`
  verifies SQLite restart survival for bootstrap/scheduler state.
- `apps/favn_storage_sqlite/test/sqlite_readiness_test.exs` verifies SQLite path
  and schema readiness classification with redaction.
- `apps/favn_orchestrator/test/production_runtime_config_test.exs` verifies
  supported production env validation and rejected unsupported modes.
- `apps/favn_orchestrator/test/readiness_test.exs` verifies readiness behavior and
  redacted dependency failures.
- `apps/favn_duckdb/test/sql/adapter/*` and
  `apps/favn_duckdb_adbc/test/sql/adapter/*` verify DuckDB adapter hardening,
  production path checks, diagnostics, and redaction behavior.

Known limits:

- Favn does not yet ship a SQLite backup, backup-verification, or migration
  command. Track #350.
- Focused SQLite restore coverage for auth/session/audit and idempotency state is
  tracked by #349; do not overstate that coverage beyond existing tests.
- Favn does not yet automate DuckDB data-plane backup/restore. Track #351.
- Postgres production mode and distributed execution are out of scope.
