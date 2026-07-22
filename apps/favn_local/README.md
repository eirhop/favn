# favn_local

`favn_local` owns Favn's customer-side build and local Docker Compose tooling.
The public `mix favn.*` tasks live in `apps/favn` and delegate to `Favn.Dev`.
Authoring and manifest compilation remain owned by `favn_authoring`.

## Supported local runtime

Local development requires a reachable Linux amd64 Docker Engine and Docker
Compose v2 or newer. Docker Desktop with the WSL2 amd64 engine is supported.
There is one supported topology:

- digest-pinned PostgreSQL 18;
- the prebuilt, version-matched Favn control-plane image;
- the customer-built runner image.

All three services use one project-scoped private Compose network. PostgreSQL,
EPMD, and BEAM distribution ports are not published. Only the View and private
orchestrator API bind to `127.0.0.1`. No source tree is mounted into either
release container.

## First use

```bash
mix deps.get
mix favn.init --duckdb --sample
mix favn.install
mix favn.doctor
mix favn.dev
```

`mix favn.install` is explicit and does not start services. It:

- checks Docker Engine and Docker Compose;
- pulls `ghcr.io/eirhop/favn-control-plane:v<matching-favn-version>`;
- resolves the tag to its immutable repository digest;
- verifies its Linux amd64 target, non-root user, Favn version, build ID,
  manifest schema, and runner contract labels;
- writes the project-scoped Compose contract and generated local credentials.

A repeated install uses the exact cached digest. `mix favn.install --force`
repulls and revalidates the version tag. Registry credentials remain entirely
in Docker's credential store; Favn has no registry-password option.

`mix favn.dev` then:

1. compiles the customer project and builds an aligned runner/manifest release;
2. builds or reuses a project-scoped
   `favn-local-runner-<compose-project>:<runner_release_id>` image;
3. starts PostgreSQL and runs migration, grant, schema verification, and local
   workspace provisioning as one-shot control-plane release operations;
4. starts the runner and control plane and verifies liveness plus full remote
   runner readiness;
5. publishes and activates the aligned manifest;
6. streams prefixed Compose logs until interrupted.

Ctrl-C performs a bounded graceful stop and preserves the PostgreSQL volume,
generated artifacts, and cached images.

Install, start, reload, stop, and reset mutations are serialized by one
project-adjacent bounded lock. A second command fails instead of interleaving.
If a CLI process is killed, the next Linux process verifies the recorded PID,
boot ID, and process start identity before reclaiming its stale lock.
Start also fails before changing generated files or images when any part of the
project stack is already running; use reload or stop explicitly. Containers
left stopped by `mix favn.stop` may be started again normally.

## Day-to-day commands

```bash
mix favn.status
mix favn.run MyApp.Pipelines.Daily
mix favn.runs list --limit 20
mix favn.logs --service control-plane --tail 200
mix favn.logs --service runner --follow
mix favn.reload
mix favn.stop
mix favn.diagnostics
```

`mix favn.reload` recomputes the release contract before changing the running
stack:

- SQL, pipeline, schedule, or other manifest-only changes publish and activate
  a new manifest without rebuilding or restarting either image;
- customer Elixir, helper, resolver, plugin, adapter, runtime dependency, or
  runner-contract changes enter a resumable maintenance boundary, wait a
  bounded time for admitted mutations and work to drain, replace only the
  runner, verify its exact release ID, activate its aligned manifest, and then
  restore admission;
- the official control-plane image is never rebuilt or replaced by reload.

The owner-only recovery record snapshots the verified running image and active
manifest before a build can replace `latest.json`, then records the opaque
maintenance lease before the control plane is asked to acquire it. An
interrupted command therefore resumes with the original image/manifest pair
and the same lease. Admission is restored only after replacement succeeds or
both the old runner and old active manifest have been positively verified. If
rollback cannot be verified, maintenance remains active for a later recovery
attempt.

`mix favn.stop` preserves the volume and images. Destructive cleanup is always
explicit:

```bash
mix favn.reset
mix favn.reset --yes
```

Without `--yes`, reset prints the exact project-scoped containers, network,
volume, local runner images, and `.favn` directory it would remove. It never
removes the installed official control-plane image.

## Build and deployment operations

```bash
mix favn.build.runner
mix favn.build.manifest \
  --runner-release .favn/dist/runner/rr_.../runner-release.json

FAVN_ORCHESTRATOR_SERVICE_TOKEN=... mix favn.publish \
  --manifest .favn/dist/manifest/mv_.../manifest-index.json \
  --orchestrator-url https://control-plane.internal

FAVN_ORCHESTRATOR_SERVICE_TOKEN=... mix favn.activate \
  --manifest-version mv_... \
  --workspace-id production \
  --orchestrator-url https://control-plane.internal
```

Runner and manifest builds execute in `MIX_ENV=prod`. A production runner build
requires Favn to come from a clean, detached Git commit/tag so the packaged
source revision is provable. The runner context vendors the customer's OTP app,
`priv` files, exact runtime dependency closure, verified descriptor, aligned
manifest, digest-pinned Dockerfile, and bundle hashes. The user owns building
and publishing this runner image.

Declare executable code reached only through dynamic dispatch:

```elixir
config :favn,
  runner_build: [
    extra_modules: [MyApp.DynamicHelper],
    extra_applications: [:my_runtime_app]
  ]
```

Changing either list requires a new runner release. A manifest-only build
recomputes the executable fingerprint and fails if code, dependencies, plugins,
compile-time runtime values, or the toolchain no longer match the supplied
runner descriptor.

Publication uploads missing execution packages before staging the manifest. It
is content-addressed: a replay returns the existing canonical manifest version.
Activation always targets that exact staged version and succeeds only when the
connected runner advertises the required release.

## Configuration

Local tooling reads `config :favn, :local`:

```elixir
config :favn, :local,
  workspace_id: "local-dev",
  scheduler: false,
  orchestrator_port: 4101,
  web_port: 4173
```

PostgreSQL is managed by Compose; a customer does not configure a local storage
adapter or host database URL. The scheduler is disabled by default. Use
`mix favn.dev --scheduler`, `mix favn.dev --no-scheduler`, or the configuration
above to choose explicitly.

`mix favn.dev` and `mix favn.reload` load `<project-root>/.env` once before the
customer production build. Existing shell values win. The resulting bounded
customer environment is written to owner-only `.favn/compose/runner.env` and is
consumed only by the runner container. Favn-owned service values override
customer values. Values are literal: `$NAME`, `${NAME}`, quotes, backslashes,
`#`, and newlines reach the runner unchanged. Secrets are never placed in
process arguments, Compose YAML, install metadata, diagnostics, returned Docker
errors, persisted failure output, or streamed log messages.

The first release supports environment-variable secrets only. Rotation is a
manual overlap-and-restart procedure; automatic secret-provider integration and
hot rotation are outside this release.

## Managed files

All project-local state is under `.favn/` and must stay uncommitted:

- `install/control-plane.json`: exact installed control-plane image metadata;
- `compose/compose.yml`: generated topology with no embedded secrets;
- `compose/.env`: owner-only Favn service credentials and port selections;
- `compose/runner.env`: owner-only customer runner environment;
- `dist/runner/` and `dist/manifest/`: immutable customer artifacts;
- `history/`: bounded failure records;
- `logs/compose-failure.log`: preserved bounded startup diagnostics;
- `secrets.json`: owner-only generated local credentials;
- `maintenance.json`: owner-only runner/manifest recovery snapshot and
  maintenance lease, present only while completion remains unconfirmed;
- `runtime.json`: current Compose/deployment identity.

The PostgreSQL data volume is Docker-managed and named from the canonical
project root. Install does not create it; `dev` creates it and `reset --yes`
removes it.

## Maintainer-only control-plane build

`MIX_ENV=prod mix favn.build.control_plane --load` creates the repository-owned
Linux amd64 candidate image used by acceptance tests. Public install cannot
select arbitrary images or candidate tags. Protected main-branch CI is the only
publisher for `ghcr.io/eirhop/favn-control-plane`; deployments consume its
digest, not a mutable tag.
