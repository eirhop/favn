# Local Docker Compose development

The supported local topology mirrors production:

1. PostgreSQL 18;
2. the Favn-published control-plane image; and
3. a customer-built runner image.

Favn may start these images, but it builds only the maintainer control-plane
candidate. It never builds the customer runner.

## Prerequisites

- Linux amd64 or amd64 WSL2 with Docker Engine and Compose v2;
- Elixir `1.20.2` on Erlang/OTP `29` for host Mix commands;
- authenticated access to the private control-plane image; and
- a compatible customer runner image already present in the local Docker
  engine.

## First-time setup

```bash
mix favn.init --duckdb --sample
mix favn.init --target compose
mix favn.init --target runner
mix favn.install
```

Review and commit the generated Compose file and runner template. Both are
starting points owned by the customer and are never overwritten after edits.

Choose a release ID, build the runner, and start Favn:

```bash
export FAVN_RUNNER_RELEASE_ID="rr_$(openssl rand -hex 32)"

docker build \
  --platform linux/amd64 \
  --build-arg FAVN_RUNNER_RELEASE_ID \
  --file deploy/favn-runner/Dockerfile \
  --tag customer/favn-runner:dev \
  .

mix favn.dev --runner-image customer/favn-runner:dev
```

The image can instead be configured:

```elixir
config :favn, :local,
  runner_image: "customer/favn-runner:dev"
```

The command option overrides configuration, which overrides
`FAVN_RUNNER_IMAGE`. Favn inspects the selected image, validates its platform,
labels, Favn version, and runner contract, then pins the exact local Docker image
ID in generated Compose state. This makes reload rollback independent of a
mutable local tag.

Favn builds an aligned manifest with the image's release ID, starts existing
images with `docker compose up --no-build`, runs the PostgreSQL release
operations, and publishes and activates the manifest. Neither runtime container
mounts source code.

## Reload

Favn no longer classifies source changes or rebuilds a runner.

- If only manifest content changed, run `mix favn.reload` with the same selected
  runner image.
- If executable runner content changed, first build a new image with a new
  release ID, then run `mix favn.reload --runner-image <new-image>`.
- A bounded runner-environment change recreates the runner with the same pinned
  image.

Runner replacement remains drain-first and recoverable. A blocked drain leaves
the previous pinned image and active manifest unchanged.

## Ownership and cleanup

The customer owns the Compose file, runner Dockerfile, runner images, extra
services, networks, volumes, registry, and native dependencies. Favn owns only
generated state below `.favn/` plus lifecycle operations for the labeled
PostgreSQL, runner, and control-plane roles.

`mix favn.stop` stops the recorded Favn roles without deleting data.
`mix favn.reset --yes` removes generated `.favn/` state while preserving
`.favn/data`, the Compose file, and every customer image and Docker resource. It
never calls `docker compose down`.

Use:

```bash
mix favn.status
mix favn.doctor
mix favn.diagnostics --json
mix favn.logs --service runner --tail 200
```

Repository maintainers may build a control-plane candidate with
`mix favn.build.control_plane --load` or select it through
`mix favn.maintainer.dev`. Maintainer mode does not build the customer runner;
it requires the same `--runner-image` input as ordinary local development.
