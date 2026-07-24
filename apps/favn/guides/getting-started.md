# Getting Started

Favn source development uses normal Mix processes and a PostgreSQL database
that you provide. Docker is only needed when you choose to build or test
deployment images.

## 1. Add Favn

```elixir
def deps do
  [
    {:favn, "~> 0.5.0"}
  ]
end
```

During pre-release source testing, use the approved path, Git reference, or
private package version instead.

```bash
mix deps.get
mix favn.init --duckdb --sample
```

## 2. Provide PostgreSQL and environment variables

Start PostgreSQL using your team's preferred tooling, then export:

```bash
export FAVN_DATABASE_URL='ecto://postgres:postgres@127.0.0.1/favn_dev'
export FAVN_RUNTIME_INPUT_PIN_KEY="$(openssl rand -base64 32)"
```

Favn does not install PostgreSQL and does not load `.env`. The variables must
already exist in the environment of the `mix` process.

Initialize the control-plane schema and workspace once:

```bash
mix favn.postgres.migrate
mix favn.postgres.provision_workspace \
  --id local-dev \
  --slug local-dev \
  --name "Local Development"
```

## 3. Start Favn

```bash
mix favn.doctor
mix favn.dev
```

Open the printed View URL, normally `http://127.0.0.1:4173`.

## 4. Run and inspect work

In another terminal with the same environment:

```bash
mix favn.run MyApp.Pipelines.LocalSmoke
mix favn.runs list
mix favn.inspect MyApp.Mart:example
```

After changing runner code or authored definitions:

```bash
mix favn.reload
```

Reload compiles and replaces the runner BEAM. It does not rebuild an image.

After changing dependencies, plugins, environment, or runtime configuration:

```bash
mix favn.stop
mix favn.dev
```

## 5. Prepare a deployment

Deployment is a separate workflow. Copy the bounded example:

```bash
mix favn.init --target deployment
```

Favn writes `deploy/favn/` once and never overwrites it. Your team owns and
adapts those Docker and Compose files for its platform. PostgreSQL remains an
external service.

Build a customer runner with an explicit immutable release ID, then build the
manifest with the same ID:

```bash
export RUNNER_RELEASE_ID="rr_<64-lowercase-hex-characters>"

docker build \
  -f deploy/favn/runner.Dockerfile \
  --build-arg FAVN_CUSTOMER_APP=my_app \
  --build-arg FAVN_RUNNER_RELEASE_ID="$RUNNER_RELEASE_ID" \
  -t registry.example/customer-runner:"$RUNNER_RELEASE_ID" \
  .

MIX_ENV=prod mix favn.build.manifest \
  --runner-release-id "$RUNNER_RELEASE_ID"
```

The control plane is the reusable Favn image. The runner is customer-owned and
contains the consumer's code and plugins. Deploy immutable image digests and
the manifest created for that exact runner release.

Read [Local Development](local-development.md) for the full developer loop and
[Configuration](configuration.md) for environment and runtime configuration.
