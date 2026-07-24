# Control-plane and runner images

Images are production artifacts, not a source-development requirement.

Favn ships one reusable control-plane image containing the Orchestrator, View,
PostgreSQL storage adapter, and their runtime dependencies. Each customer builds
a separate runner image containing its own project code and plugins.

## Build the control plane

From the Favn repository root:

```bash
docker build \
  -f rel/control_plane/Dockerfile \
  -t favn-control-plane:dev \
  .
```

The Dockerfile consumes the repository directly. There is no generated build
context, build-ID registry, `mix favn.build.control_plane`, or maintainer
development mode.

CI performs the same root build, validates the image contract, scans it, and
publishes commit images. Production deployments use the resulting digest:

```text
ghcr.io/eirhop/favn-control-plane@sha256:<digest>
```

The image:

- runs as UID/GID `10001`;
- contains no Mix, runner, authoring, or local-development application;
- contains precompiled View assets;
- requires runtime secrets through the process environment;
- exposes View on `4000` and the Orchestrator API on `4101`;
- uses fixed BEAM distribution ports supplied by the deployment.

Run the static contract locally after building:

```bash
scripts/control_plane_image_contract.sh favn-control-plane:dev
```

## Build the customer runner

Copy the deployment example into a consumer project:

```bash
mix favn.init --target deployment
```

Choose one immutable runner release ID and use it for both the image and
manifest:

```bash
export RUNNER_RELEASE_ID="rr_<64-lowercase-hex-characters>"

docker build \
  -f deploy/favn/runner.Dockerfile \
  --build-arg FAVN_CUSTOMER_APP=my_app \
  --build-arg FAVN_RUNNER_RELEASE_ID="$RUNNER_RELEASE_ID" \
  -t registry.example/customer-favn-runner:"$RUNNER_RELEASE_ID" \
  .

MIX_ENV=prod mix favn.build.manifest \
  --runner-release-id "$RUNNER_RELEASE_ID"
```

The runner image is customer-owned because it contains customer code,
dependencies, native libraries, and plugins. Favn does not infer how it should
be built or published.

## Deployment contract

The supported first topology is:

- one control-plane container;
- one customer runner container;
- one externally supplied PostgreSQL database;
- a private network path between control plane and runner;
- ingress only to the View, and only the required private management paths.

The control-plane, runner, manifest, and PostgreSQL schema must come from one
coherent release candidate. Never combine a manifest with a runner release ID
different from the ID baked into the runner image.

Use immutable digests for both images. Tags are lookup aids, not deployment
identity.

## Runtime environment

Containers receive configuration from the deployment platform. Favn reads
ordinary environment variables through `System.get_env/1` and
`System.fetch_env!/1`; no `.env` loader runs inside the image.

Use the platform's secret store and map each secret to the environment variable
expected by Favn. For Azure Container Apps, a manual environment value or
`secretref:<name>` becomes a normal process environment variable at runtime.
Changing configuration creates a new revision; changing only a referenced
secret may require a new revision or restart for running replicas to observe it.

The copied `env.example` documents the minimum variables. Important groups are:

- PostgreSQL URL and verified TLS;
- runtime-input pin keys;
- Orchestrator service tokens and bootstrap identity;
- workspace IDs;
- View public origin, proxy CIDRs, and secret key base;
- control-plane and runner node names, distribution cookie, and fixed port.

Do not bake values into either image or pass secrets as Docker build arguments.

## Migrations and workspace provisioning

Run storage operations explicitly with an appropriate database identity before
starting the control plane:

```bash
mix favn.postgres.migrate
mix favn.postgres.grant_runtime --role favn_runtime
mix favn.postgres.provision_workspace \
  --id customer \
  --slug customer \
  --name "Customer"
mix favn.postgres.verify_schema
```

Application startup never migrates or provisions PostgreSQL.

## Maintainer image debugging

To test a control-plane change in a real consumer project:

1. build `favn-control-plane:dev` from the Favn repository;
2. build the consumer runner image from the consumer repository;
3. build a manifest with the same runner release ID;
4. select both images in the consumer-owned deployment;
5. run the deployment qualification.

No `favn.maintainer.dev` task is needed.
