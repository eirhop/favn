# Single-Node Operator Runbook

This runbook adds launcher-specific steps to the PostgreSQL procedures in
`docs/production/postgresql_operator_runbook.md`. Follow that runbook first for
database provisioning, TLS, roles, migrations, workspace provisioning, backup,
restore, monitoring, and incidents.

## Build

From the customer project:

```bash
mix deps.get
mix favn.install
mix favn.build.single
```

The command prints the generated `dist` path. Review its `metadata.json`,
`config/assembly.json`, `OPERATOR_NOTES.md`, and `env/backend.env.example`.

## Configure

Copy the example environment outside the immutable artifact and replace every
placeholder. At minimum configure:

```text
FAVN_STORAGE=postgres
FAVN_DATABASE_URL=ecto://favn_runtime:<secret>@<host>/<database>
FAVN_DATABASE_SSL_CA_FILE=/run/secrets/postgresql-ca.pem
FAVN_RUNTIME_INPUT_PIN_KEY=<32-byte-raw-or-base64-key>
FAVN_WORKSPACE_IDS=<explicit-comma-separated-workspace-ids>
FAVN_ORCHESTRATOR_API_SERVICE_TOKENS=<bootstrap-id>|platform_operator:<secret>,<view-id>:<different-secret>
FAVN_ORCHESTRATOR_BOOTSTRAP_USERNAME=<operator>
FAVN_ORCHESTRATOR_BOOTSTRAP_PASSWORD=<secret>
FAVN_SINGLE_NODE_HOME=/var/lib/favn
```

Set `FAVN_ENV_FILE` to that file. Keep it owner-readable only. Never place it in
the artifact or repository.

## Start and bootstrap

```bash
<dist>/bin/start

mix favn.bootstrap.single \
  --manifest <dist>/runner/manifest.json \
  --orchestrator-url http://127.0.0.1:4101 \
  --workspace-id <workspace-id> \
  --service-token <service-token> \
  --operator-username <operator> \
  --operator-password <password>
```

Require readiness to pass before bootstrap or traffic. Repeat bootstrap safely to
confirm the manifest and runner registrations are idempotent.

## Stop and restart

```bash
<dist>/bin/stop
<dist>/bin/start
```

`bin/start` refuses a live PID and removes only stale PID files. `bin/stop` is
idempotent. After restart, verify readiness, active deployment, runner registration,
scheduler state, and at least one previously committed run.

## Upgrade

1. Build a new immutable artifact.
2. Follow the PostgreSQL migration/canary procedure if the schema changes.
3. Stop the old launcher.
4. Start the new launcher against the same PostgreSQL database and runtime home.
5. Verify readiness and durable state before resuming dispatch.

Do not copy or transform PostgreSQL state through the launcher. Rollback follows
the application/database compatibility and restore rules in the PostgreSQL
runbook.
