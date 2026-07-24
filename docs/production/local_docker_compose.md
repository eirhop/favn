# Local Docker Compose development

The supported local topology mirrors production:

1. PostgreSQL 18 on the floating `postgres:18-trixie` tag;
2. the Favn-published control-plane image; and
3. a customer-built runner image.

For local convenience, Favn invokes the customer-owned runner Dockerfile. It
does not own that file or the production image pipeline.

## Prerequisites

- Linux amd64 or amd64 WSL2 with Docker Engine and Compose v2;
- Elixir `1.20.2` on Erlang/OTP `29` for host Mix commands;
- authenticated access to the private control-plane image.

## First-time setup

```bash
mix favn.init
mix favn.install
mix favn.dev
```

Review and commit the generated documented Compose file under `deploy/local/`
and runner template under `deploy/runner/`. Both are starting points owned by
the customer and are never overwritten after edits. The Compose default binds
the repository `.data/` directory into the runner and uses a Docker-managed
PostgreSQL volume.

New local PostgreSQL volumes are initialized with UTF-8 and PostgreSQL's
built-in `C.UTF-8` locale provider. They therefore do not inherit collation
versions from the image's operating-system libc. The init arguments run only
when PostgreSQL creates a new cluster. Favn does not delete, reindex, or refresh
existing databases. Existing libc-based volumes must be recreated deliberately
when their data is disposable, or repaired explicitly using the appropriate
PostgreSQL collation maintenance procedure.

Every service in the generated local template, including the one-shot
operations services, uses Docker's bounded `local` logging driver with three
10 MB files. This is a local scaffold default only. The single-host production
template leaves logging ownership to the deployment's external logging system.

`mix favn.dev` generates the opaque local release ID, runs `docker build --pull`
with the customer Dockerfile, and selects an automatic project-scoped image
name. An existing image can instead be selected:

```elixir
config :favn, :local,
  runner_image: "customer/favn-runner:dev"
```

The `--runner-image` command option overrides configuration, which overrides
`FAVN_RUNNER_IMAGE`; without any selection, the automatic local build is used.
Favn validates the image platform and labels, then pins the exact local Docker
image ID in generated Compose state.

Favn builds an aligned manifest with the image's release ID, starts existing
images with `docker compose up --no-build`, runs the PostgreSQL release
operations, and publishes and activates the manifest. Neither runtime container
mounts source code.

## Reload

Favn does not classify source changes during reload.

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
`.data`, the Compose file, and every customer image and Docker resource. It
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
`mix favn.maintainer.dev`. When the customer runner is built automatically, the
validated Favn source selection is materialized as a separate Docker build
context. The customer project's normal build context is not widened.
Maintainers can still bypass the automatic build with `--runner-image`.
