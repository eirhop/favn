# Local Development

Local development is intentionally Docker-free. Favn runs the View and
Orchestrator in the current Mix process and starts one separate runner BEAM
using the consumer project's compiled code.

The local runner uses a `source` identity with the actual host target. The
Linux-only `prod` identity remains reserved for deployable customer images.

You provide:

- a running PostgreSQL database;
- the required environment variables;
- the assets, pipelines, connections, and runner plugins in your Mix project.

Favn does not install or start PostgreSQL, parse `.env` files, build images, or
run Compose during source development.

The local operator, runner, and command processes communicate through the
reserved `favn-local.test` alias. Favn maps it to loopback inside each BEAM, so
source development does not require DNS or hosts-file changes.

## First-time setup

Add `:favn` and any data-plane plugins to the consumer project, then fetch and
compile dependencies:

```bash
mix deps.get
mix compile
```

Start PostgreSQL however your team normally does it. A local installation,
shared development server, managed database, or team-owned Compose stack are
all valid. The database setup must provide a schema-owning migrator role and a
separate restricted runtime role. Favn needs one connection URL for each.

Load the required variables into the shell:

```bash
export FAVN_DATABASE_URL='ecto://favn_runtime:runtime-secret@127.0.0.1/favn_dev'
export FAVN_DATABASE_MIGRATOR_URL='ecto://favn_migrator:migrator-secret@127.0.0.1/favn_dev'
export FAVN_RUNTIME_INPUT_PIN_KEY="$(openssl rand -base64 32)"
export DUCKDB_ADBC_DRIVER='/absolute/path/to/libduckdb.so'
```

PowerShell:

```powershell
$env:FAVN_DATABASE_URL = 'ecto://favn_runtime:runtime-secret@127.0.0.1/favn_dev'
$env:FAVN_DATABASE_MIGRATOR_URL = 'ecto://favn_migrator:migrator-secret@127.0.0.1/favn_dev'
$env:FAVN_RUNTIME_INPUT_PIN_KEY = '<32-byte value or base64-encoded 32-byte value>'
$env:DUCKDB_ADBC_DRIVER = 'C:\absolute\path\to\duckdb.dll'
```

Favn reads the process environment. It does not read `.env`. Teams commonly use
shell exports, `direnv`, their IDE's environment settings, or a command such as:

```bash
set -a
source .env
set +a
mix favn.dev
```

Do not commit secrets. If `config/runtime.exs` reads variables with
`System.fetch_env!/1`, load them before invoking Mix.

Apply the schema and explicitly provision the development workspace:

```bash
mix favn.postgres.upgrade
mix favn.postgres.provision_workspace \
  --config .favn/workspace-bootstrap.json
```

The JSON explicitly selects the initial Entra or local-password administrator;
password material is read from protected stdin or `--password-file`, never from
the JSON. See [Operator Authentication](operator-authentication.md).

The upgrade command uses the migrator role to apply migrations and runtime
grants, then reconnects with the runtime role to verify the exact schema. Both
commands are explicit. `mix favn.dev` never migrates or provisions the database
for you. If an upgrade stage fails, its changes may be partially applied; read
the reported failed stage and inspect PostgreSQL before retrying.

## Normal loop

Start development:

```bash
mix favn.doctor
mix favn.dev
```

After updating Favn, `mix favn.doctor` names any missing migration and tells you
to run `mix favn.postgres.upgrade`. The upgrade command preserves existing data;
it does not reset PostgreSQL or provision another workspace.

The command prints the View URL, normally `http://127.0.0.1:4173`, and stores
local credentials in `.favn/local/credentials.json`. Favn applies owner-only
mode bits on Unix; Windows uses the containing directory's ACLs. The file
includes the View workspace ID, username, and password. The UI password and
encrypted browser-session key are reused across normal stop/start cycles, so an
active login survives a local Favn restart. Source development also signs a
local browser in as the generated administrator automatically; the credential
file remains available for recovery and non-browser clients. Changing the
configured workspace, signing out, or reaching the normal session expiry
creates a fresh local browser session without presenting a password form.
Before starting the View, `mix favn.dev` builds its CSS and JavaScript from the
selected Favn dependency checkout so ignored files from an older checkout
cannot be served accidentally.

Source development logs at `info` by default in both the local control plane and
runner. Use `FAVN_LOG_LEVEL=debug mix favn.dev` for verbose troubleshooting or
`FAVN_LOG_LEVEL=warning mix favn.dev` for quieter output. The startup milestones
and final View URL remain visible at every supported log level.

After changing assets, pipelines, SQL, or ordinary Elixir runner code:

```bash
mix favn.reload
```

Reload compiles the project and derives the runner release ID from the compiled
BEAM closure. Changed compiled code replaces the runner. A manifest-only change
keeps the runner and deploys the new manifest, while an unchanged source and
manifest is a no-op when the expected deployment is still durably active. If
another action changed the active deployment, reload restores the expected local
manifest. Reload registers only execution packages that are absent from
PostgreSQL and prints build, package, publication, activation, and total timings.
It does not build a container image.

The success banner says `Favn source unchanged`, `Favn manifest reloaded`, or
`Favn runner reloaded`. Build time measures manifest construction; package time
measures checking and registering missing execution packages; publish and
activate measure the corresponding control-plane operations. An unchanged reload
reports zero package, publish, and activate time. Total reload time also includes
source identity calculation, durable state checks, and any runner startup/drain;
it excludes the Mix compilation that runs before reload starts. Timings are
observations for this invocation, not a fixed performance guarantee.

Only one reload can proceed at a time. If the previous runner is still draining,
wait until it finishes before reloading again. An interrupted deployment can
have an unknown outcome: use `mix favn.stop` followed by `mix favn.dev` when the
command requests it. A caller timeout does not cancel activation; inspect the
development process before deciding the next action. Reload never retries a
possibly completed deployment automatically.

Reload also retains existing asset freshness. A runner release, manifest, asset
implementation, metadata, or dependency declaration change does not by itself
make previously successful assets stale or schedule them to run. To execute one
changed asset explicitly:

```bash
mix favn.run MyApp.Assets.Example:asset --refresh force_selected
```

Add `--dependencies none` to plan only that asset. Use
`force_selected_upstream`, `force_all`, a forced backfill, or a target rebuild
when the intended repair scope is broader. A new window or freshness key still
requires matching evidence for that exact key, and a successfully refreshed
upstream still propagates its new result version to planned downstream assets.

Restart the full development process after changing:

- environment variables;
- `config/config.exs` or `config/runtime.exs`;
- PostgreSQL connection details;
- workspace or port configuration;
- dependencies or runner plugins.

```bash
mix favn.stop
mix favn.dev
```

Stop is idempotent and never deletes PostgreSQL data:

```bash
mix favn.stop
```

## Local configuration

The small non-secret development surface lives in application config:

```elixir
config :favn, :dev,
  workspace_id: "local-dev",
  orchestrator_port: 4101,
  view_port: 4173,
  scheduler_enabled: false,
  database_pool_size: 10
```

Set `FAVN_VIEW_PORT` or `FAVN_ORCHESTRATOR_API_PORT` to run more than one local
stack on the same machine; the environment overrides those two keys. See the
[Configuration guide](configuration.html).

PostgreSQL and secrets remain environment variables. Set
`FAVN_DATABASE_SSL_MODE=verify-full` and
`FAVN_DATABASE_SSL_CA_FILE=/absolute/path/to/ca.pem` when the database requires
verified TLS.

## Operator commands

These commands use the running local process when available. For a deployed
Orchestrator, set `FAVN_ORCHESTRATOR_URL`,
`FAVN_ORCHESTRATOR_SERVICE_TOKEN`, and `FAVN_WORKSPACE_ID`.

For an interactive workflow, keep `mix favn.dev` running in this terminal and
start `iex -S mix` from the same project in another terminal. The supported
`Favn.run/2`, `Favn.list_runs/1`, `Favn.get_run/2`, `Favn.run_events/2`,
`Favn.cancel_run/2`, and `Favn.diagnostics/1` functions use the same HTTP
boundary. See the [IEx Session Cheatsheet](iex-cheatsheet.html).

```bash
mix favn.run MyApp.Pipelines.Daily
mix favn.run MyApp.Pipelines.Daily --window day:2026-07-23
mix favn.backfill status BACKFILL_ID
mix favn.runs list
mix favn.runs show RUN_ID
mix favn.runs cancel RUN_ID
mix favn.schedules list
mix favn.schedules show SCHEDULE_ID
mix favn.schedules preview SCHEDULE_ID --limit 5
mix favn.schedules activate SCHEDULE_ID --reason "reviewed"
mix favn.schedules deactivate SCHEDULE_ID --reason "maintenance"
mix favn.inspect MyApp.Mart:orders
mix favn.diagnostics
```

`mix favn.backfill`, `mix favn.rebuild`, and `mix favn.recover` use the same
connection boundary. Recovery is only for a proven interrupted Favn-owned
initial generation; it does not adopt arbitrary tables.
Run `mix help TASK` for their exact options.

Pipeline backfills use the pipeline's authored window-combination choice. Pass
`--combine-windows` to run one adjacent historical range once instead of one
run per logical window. Rebuilds combine adjacent windows by default; pass
`--no-combine-windows` when smaller retry units matter. `mix favn.rebuild plan
ASSET --empty --reason "..."` explicitly plans an empty active root table for
later ordinary backfills; retained downstream tables remain stale until then.

When a backfill fails, `mix favn.backfill submit` prints its bounded failure
summary and the copyable `mix favn.backfill status BACKFILL_ID` inspection
command. Status output includes the root run, aggregate counts, and at most 20
failed windows by default. Use `--limit` up to 200; when more failures exist,
the command prints the paginated `windows` command for the next page. Admission
failures remain inspectable even when no child run was created.

Newly published schedules are inactive in every workspace. Preview and
explicitly activate each schedule that should submit future work.
Activation begins with the next due occurrence; it does not replay older missed
occurrences. Deactivation stops future submissions without cancelling runs that
were already accepted.

A windowed pipeline without `--window` runs one latest complete window. The
orchestrator evaluates it once using the pipeline timezone and selected assets'
availability delays, then persists the exact selection for retries and recovery.
An explicit `--window` runs exactly that one window. Use `mix favn.backfill` for
a range. Non-windowed pipelines keep their full-load behavior.

Favn intentionally does not expose an arbitrary SQL query command. Stop the
local Favn stack before opening a file-backed database with the DuckDB CLI.

## What is not part of development

Images are deployment artifacts. Source development does not require:

- Docker or Compose;
- a Favn control-plane image;
- a customer runner image;
- `mix favn.install`;
- `mix favn.maintainer.dev`;
- image rebuilds for ordinary code changes.

To test image changes, build the control-plane image from the Favn repository
and select it in a customer-owned deployment:

```bash
source <(scripts/release_metadata.sh --export)

docker build \
  --platform linux/amd64 \
  -f rel/control_plane/Dockerfile \
  --build-arg FAVN_CONTROL_PLANE_VERSION \
  --build-arg FAVN_MANIFEST_SCHEMA_VERSION \
  --build-arg FAVN_RUNNER_CONTRACT_VERSION \
  -t favn-control-plane:dev \
  .
```

See the production deployment guide for the separate runner image and manifest
release workflow.
