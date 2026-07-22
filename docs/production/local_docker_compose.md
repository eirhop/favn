# Local Docker Compose development

The supported customer development loop deliberately mirrors production:
PostgreSQL 18, the version-matched prebuilt control plane, and a customer-built
runner run as three Docker Compose services on one project-scoped private
network.

## Supported hosts and prerequisites

- Linux amd64 with Docker Engine and Docker Compose v2;
- amd64 WSL2 using Docker Desktop's Linux container engine and Compose v2; and
- authenticated pull access to the private GHCR control-plane package.

Native Windows, arm64, macOS emulation, and Podman are not supported in v1. Host
and engine probes fail before pulling images or starting services.

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
mix favn.install
mix favn.doctor
mix favn.dev
```

Install pulls `ghcr.io/eirhop/favn-control-plane:v<matching-favn-version>`,
verifies its labels and platform, resolves its immutable repository digest, and
records that digest under `.favn/`. It never compiles View, Orchestrator,
PostgreSQL storage, Phoenix assets, or a source-built control plane. Repeating
install uses the cached exact digest; `--force` repulls and revalidates the
version tag.

Dev builds or reuses the customer runner, starts PostgreSQL, executes the
release-safe database operations, starts runner then control plane, verifies
full readiness, and publishes/activates the aligned manifest. Neither runtime
container mounts the Favn or customer source tree. PostgreSQL, EPMD, and BEAM
distribution remain private to Compose; only View and the private API bind to
`127.0.0.1`.

## Reload classification

`mix favn.reload` recomputes the release contract before changing the stack:

- a SQL, pipeline, schedule, or other manifest-only change publishes and
  activates a new manifest without rebuilding or replacing either container;
- an Elixir asset, helper, resolver, plugin, adapter, dependency, or other
  runner-affecting change drains work, replaces only the runner, verifies its
  new release ID, and activates the aligned manifest; and
- a blocked drain leaves the previous runner and active manifest in place.

The prebuilt control-plane container is never rebuilt or replaced by reload.

## State and cleanup

`mix favn.stop` stops the Compose application while preserving PostgreSQL data,
generated artifacts, and images. A later `mix favn.dev` reuses that state.

`mix favn.reset` is a dry run that names the current project-scoped containers,
network, PostgreSQL volume, generated runner images, and `.favn/` directory.
Only `mix favn.reset --yes` removes them. It cannot target another Compose
project and does not remove the installed official control-plane image.

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

Repository maintainers may build and inject an unpublished candidate only
through `mix favn.build.control_plane --load` and the dedicated
`mix test.container` contract. Public `mix favn.install` has no arbitrary-image,
candidate-image, or source-build option.

The public command reference is
[`apps/favn/guides/local-development.md`](../../apps/favn/guides/local-development.md),
and implementation-owned state details are in
[`apps/favn_local/README.md`](../../apps/favn_local/README.md).
