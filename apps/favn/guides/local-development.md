# Local Development

Local development is intentionally Docker-free. Favn runs the View and
Orchestrator in the current Mix process and starts one separate runner BEAM
using the consumer project's compiled code.

You provide:

- a running PostgreSQL database;
- the required environment variables;
- the assets, pipelines, connections, and runner plugins in your Mix project.

Favn does not install or start PostgreSQL, parse `.env` files, build images, or
run Compose during source development.

## First-time setup

Add `:favn` and any data-plane plugins to the consumer project, then fetch and
compile dependencies:

```bash
mix deps.get
mix compile
```

Start PostgreSQL however your team normally does it. A local installation,
shared development server, managed database, or team-owned Compose stack are
all valid. Favn only needs a connection URL.

Load the required variables into the shell:

```bash
export FAVN_DATABASE_URL='ecto://postgres:postgres@127.0.0.1/favn_dev'
export FAVN_RUNTIME_INPUT_PIN_KEY="$(openssl rand -base64 32)"
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
mix favn.postgres.migrate
mix favn.postgres.provision_workspace \
  --id local-dev \
  --slug local-dev \
  --name "Local Development"
```

Both commands are explicit. `mix favn.dev` never migrates or provisions the
database for you.

## Normal loop

Start development:

```bash
mix favn.doctor
mix favn.dev
```

The command prints the View URL, normally `http://127.0.0.1:4173`, and stores
local credentials in `.favn/local/credentials.json` with owner-only
permissions. The UI password is reused across stop/start cycles because the
administrator record is durable PostgreSQL state.

After changing assets, pipelines, SQL, or ordinary Elixir runner code:

```bash
mix favn.reload
```

Reload compiles the project, starts a fresh runner BEAM under a new runner
release ID, and publishes and deploys the aligned manifest. It does not build a
container image.

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

PostgreSQL and secrets remain environment variables. Set
`FAVN_DATABASE_SSL_MODE=verify-full` and
`FAVN_DATABASE_SSL_CA_FILE=/absolute/path/to/ca.pem` when the database requires
verified TLS.

## Operator commands

These commands use the running local process when available. For a deployed
Orchestrator, set `FAVN_ORCHESTRATOR_URL`,
`FAVN_ORCHESTRATOR_SERVICE_TOKEN`, and `FAVN_WORKSPACE_ID`.

```bash
mix favn.run MyApp.Pipelines.Daily
mix favn.runs list
mix favn.runs show RUN_ID
mix favn.runs cancel RUN_ID
mix favn.inspect MyApp.Mart:orders
mix favn.query "select * from mart.orders limit 10"
mix favn.diagnostics
```

`mix favn.backfill` and `mix favn.rebuild` use the same connection boundary.
Run `mix help TASK` for their exact options.

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
docker build \
  -f rel/control_plane/Dockerfile \
  -t favn-control-plane:dev \
  .
```

See the production deployment guide for the separate runner image and manifest
release workflow.
