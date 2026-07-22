# Local Docker Compose development

The supported customer development loop deliberately mirrors production:
PostgreSQL 18, the version-matched prebuilt control plane, and a customer-built
runner run as three Docker Compose services on one project-scoped private
network.

## Supported hosts and prerequisites

- Linux amd64 with Docker Engine and Docker Compose v2;
- amd64 WSL2 using Docker Desktop's Linux container engine and Compose v2; and
- Elixir `1.20.2` running on Erlang/OTP `29` for the host Mix commands; and
- authenticated pull access to the private GHCR control-plane package.

Native Windows, arm64, macOS emulation, and Podman are not supported in v1. Host
compiler and engine probes fail before pulling images or starting services. The
host compiler must match the pinned runner compiler because authored modules are
compiled before they are vendored into the reproducible runner image context.

Authenticate Docker with a pull-only GitHub credential outside Favn:

```bash
docker login ghcr.io
```

Before Hex publication, check out Favn at an approved tag or commit, detach the
checkout, keep it clean, and use path dependencies from the customer project.
The generated runner context vendors its dependency closure; the checkout and
private Git credentials are not needed when that context is built elsewhere.

## Install and start

```bash
mix favn.init --duckdb --sample
mix favn.init --target compose
mix favn.install
mix favn.doctor
mix favn.dev
```

Compose initialization creates the consumer-owned default
`deploy/compose.local.yml`. Review and commit it. `mix favn.dev --compose-file`
overrides `config :favn, :local, compose_file:`, which overrides that default.
Favn validates versioned role labels and immutable image selections; extra
unlabeled services and ordinary Compose configuration remain consumer-owned.

Install pulls `ghcr.io/eirhop/favn-control-plane:v<matching-favn-version>`,
verifies its labels and platform, resolves its immutable repository digest, and
records that digest under `.favn/`. It never compiles View, Orchestrator,
PostgreSQL storage, Phoenix assets, or a source-built control plane. Repeating
install uses the cached exact digest; `--force` repulls and revalidates the
version tag.
Install probes Docker Engine but neither requires Compose nor writes deployment
topology or credentials.

Dev builds or reuses the customer runner, starts PostgreSQL, executes the
release-safe database operations, starts runner then control plane, verifies
full readiness, and publishes/activates the aligned manifest. Neither runtime
container mounts the Favn or customer source tree. PostgreSQL, EPMD, and BEAM
distribution remain private to Compose; only View and the private API bind to
`127.0.0.1`.

The generated environment, secrets, runner selection, PostgreSQL bootstrap, and
`.favn/data` bind source remain under `.favn/`. The Compose YAML is never
regenerated. The runner mounts `.favn/data` at `/var/lib/favn/data`; the image
creates that mount point but does not declare it as a Docker volume.

## Reload classification

`mix favn.reload` recomputes the release contract before changing the stack:

- a SQL, pipeline, schedule, or other manifest-only change publishes and
  activates a new manifest without rebuilding or replacing either container;
- an Elixir asset, helper, resolver, plugin, adapter, dependency, or other
  runner-affecting change builds a new immutable image with stable dependency
  layers eligible for BuildKit reuse, drains work, replaces only the runner,
  verifies its new release ID, and activates the aligned manifest;
- a bounded runner-environment change drains and recreates only the runner with
  the same image; and
- a blocked drain leaves the previous runner and active manifest in place.

If candidate validation or the active-manifest lookup fails before recovery
state is recorded, reload restores the previous runner image selection so the
next reload can retry from the active release.

The prebuilt control-plane container is never rebuilt or replaced by reload.

## State and cleanup

`mix favn.stop` stops only the recorded control-plane, runner, and local
PostgreSQL roles. It leaves unlabeled consumer services and every container,
network, volume, data file, and image in place. A later `mix favn.dev` reuses
that state. The last successful Compose selection remains recorded after stop.
After an interrupted start with no runtime record, stop discovers only
contract-labeled Favn containers in the derived Compose project, even when
other generated Compose state is missing.

`mix favn.reset` is a dry run that names generated `.favn/` state and verified
local runner image tags. `--yes` removes only that scope after proving known Favn
roles are stopped. It preserves `.favn/data`, the Compose file, containers,
services, networks, volumes, consumer resources, and the official control-plane
image. A selected Compose file below `.favn/` is explicitly excluded from
cleanup, and reset fails closed if partial-start role state cannot be inspected.
It never calls `docker compose down`.

Use these diagnostics before resetting:

```bash
mix favn.status
mix favn.doctor
mix favn.diagnostics --json
mix favn.logs --service control-plane --tail 200
mix favn.logs --service runner --tail 200
mix favn.logs --service postgres --tail 200
```

Targeted errors distinguish missing Docker, missing Compose v2, unsupported
host/engine architecture, GHCR authentication failure, missing version tag,
image-label or contract mismatch, partial Compose state, and runner/manifest
misalignment. Logs and diagnostics are bounded and redact generated secrets.

Repository maintainers may build and qualify an unpublished candidate directly
through `mix favn.build.control_plane --load` and the dedicated
`mix test.container` contract. A consumer project may use
`mix favn.maintainer.dev` with `FAVN_CHECKOUT` to build or reuse that checkout's
candidate by exact local image ID and run the ordinary development stack. That
path is explicitly non-production and cannot publish. Public `mix favn.install`
still has no arbitrary-image, candidate-image, or source-build option; it also
replaces maintainer selection with the version-matched official image.

The public command reference is
[`apps/favn/guides/local-development.md`](../../apps/favn/guides/local-development.md),
and implementation-owned state details are in
[`apps/favn_local/README.md`](../../apps/favn_local/README.md).
