# favn_local

`favn_local` owns Favn's customer-side build and local Docker Compose tooling.
The public `mix favn.*` tasks live in `apps/favn` and delegate to `Favn.Dev`.
Authoring and manifest compilation remain owned by `favn_authoring`.

## Supported local runtime

Local development requires a reachable Linux amd64 Docker Engine and Docker
Compose v2 or newer, plus Elixir `1.20.2` on Erlang/OTP `29` for host Mix
commands. Docker Desktop with the WSL2 amd64 engine is supported. There is one supported
topology:

- the latest patch release on the floating `postgres:18` tag;
- the prebuilt, version-matched Favn control-plane image;
- the customer-built runner image.

All three services use one project-scoped private Compose network. PostgreSQL,
EPMD, and BEAM distribution ports are not published. Only the View and private
orchestrator API bind to `127.0.0.1`. No source tree is mounted into either
release container.

## First use

```bash
mix deps.get
mix favn.init
mix favn.install
mix favn.dev
```

`mix favn.init` writes `deploy/local/compose.yml`, a secret-free environment
reference beside it, and the editable runner build under `deploy/runner/`. The
project owns, reviews, and may extend these files. Favn never overwrites a
modified scaffold. `--include duckdb-adbc[@VERSION]` adds the tested optional
native DuckDB driver when the customer project declares `favn_duckdb_adbc`.

`mix favn.install` is explicit and does not start services. It:

- checks Docker Engine;
- pulls `ghcr.io/eirhop/favn-control-plane:v<matching-favn-version>`;
- resolves the tag to its immutable repository digest;
- verifies its Linux amd64 target, non-root user, Favn version, build ID,
  manifest schema, and runner contract labels;
- writes image-only install metadata.

A repeated install uses the exact cached digest. `mix favn.install --force`
repulls and revalidates the version tag. Registry credentials remain entirely
in Docker's credential store; Favn has no registry-password option.

`mix favn.dev` then:

1. selects the configured Compose file and either an existing runner image or
   the generated customer Dockerfile;
2. generates a local release ID and invokes `docker build --pull` when no image
   was selected;
3. validates the image labels and pins its exact local Docker image ID;
4. builds a manifest aligned with the image's runner release ID;
5. validates the rendered labeled roles;
6. starts PostgreSQL and runs migration, grant, schema verification, and local
   workspace provisioning as one-shot control-plane release operations;
7. starts the runner and control plane with Compose `--no-build` and verifies
   liveness plus full remote runner readiness;
8. publishes and activates the aligned manifest;
9. records the selected deployment identity and streams prefixed Compose logs.

Compose selection uses `--compose-file`, then `config :favn, :local`, then
   `deploy/local/compose.yml` and validates the rendered labeled roles;
runner selection uses `--runner-image`, then local configuration, then
`FAVN_RUNNER_IMAGE`; without a selection, Favn uses an automatic project-scoped
image name and builds `deploy/runner/Dockerfile`.

Ctrl-C performs a bounded graceful stop and preserves the PostgreSQL volume,
generated artifacts, and cached images.

Install, start, reload, stop, and reset mutations are serialized by one
project-adjacent bounded lock. A second command fails instead of interleaving.
If a CLI process is killed, the next Linux process verifies the recorded PID,
boot ID, and process start identity before reclaiming its stale lock.
Start rejects a running or partial Favn role set before starting anything. It
may prepare generated environment and an immutable runner image first; use
reload or stop explicitly for an active stack. Containers left stopped by
`mix favn.stop` may be started again normally.

## Day-to-day commands

```bash
mix favn.status
mix favn.run MyApp.Pipelines.Daily
mix favn.runs list --limit 20
mix favn.rebuild plan MyApp.Assets.Orders --reason "contract changed"
mix favn.logs --service control-plane --tail 200
mix favn.logs --service runner --follow
mix favn.reload
mix favn.stop
mix favn.diagnostics
```

`mix favn.reload` validates the selected runner before changing the
running stack:

- SQL, pipeline, schedule, or other manifest-only changes publish and activate
  a new manifest without rebuilding or restarting either image;
- after the customer builds and selects an image with a new release ID,
  reload enters a resumable maintenance boundary, waits a
  bounded time for admitted mutations and work to drain, replace only the
  runner, verify its exact release ID, activate its aligned manifest, and then
  restore admission;
- the official control-plane image is never rebuilt or replaced by reload.

Favn does not classify source changes. The local automatic build happens during
`mix favn.dev`; production and explicit reload images remain customer-built. No
reload copies source or BEAM files into a running container.

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

Without `--yes`, reset prints the generated-state scope. Reset preserves
`.data`, the consumer Compose file, every customer image, container,
service, network, and volume. It never runs `docker compose down`. Stop and
reset recover project-scoped Favn roles
from their contract labels if a partial start failed before `runtime.json` was
written; reset fails closed if it cannot prove those roles are stopped.

## Build and deployment operations

```bash
mix favn.init --target runner
mix favn.build.manifest --runner-release-id rr_<64-hex>

FAVN_ORCHESTRATOR_SERVICE_TOKEN=... mix favn.publish \
  --manifest .favn/dist/manifest/mv_.../manifest-index.json \
  --orchestrator-url https://control-plane.internal

FAVN_ORCHESTRATOR_SERVICE_TOKEN=... mix favn.activate \
  --manifest-version mv_... \
  --workspace-id production \
  --orchestrator-url https://control-plane.internal
```

Manifest builds execute in `MIX_ENV=prod`. The customer owns the runner
Dockerfile, dependency closure, native libraries, CI build, scanning, registry,
and deployed digest. Favn treats the release ID as an opaque binding and does
not prove customer source provenance. Use a new ID whenever executable image
content changes.

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
  web_port: 4173,
  compose_file: "deploy/local/compose.yml",
  # Optional: omit this to build deploy/runner/Dockerfile automatically.
  runner_image: "customer/favn-runner:dev"
```

PostgreSQL is managed by Compose; a customer does not configure a local storage
adapter or host database URL. The scheduler is disabled by default. Use
`mix favn.dev --scheduler`, `mix favn.dev --no-scheduler`, or the configuration
above to choose explicitly.

`mix favn.dev --compose-file deploy/compose.team.yml` overrides the configured
path for that start. The recorded successful path remains authoritative for
reload, stop, status, logs, and diagnostics.

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
- `compose/.env`: owner-only Favn service credentials and port selections;
- `compose/selection.json`: last successful Compose deployment identity, retained
  across stop so status, logs, diagnostics, and reset keep the selected file;
- `compose/runner.env`: owner-only customer runner environment;
- `compose/postgres-init.sh`: generated local runtime-role bootstrap;
- `dist/manifest/`: immutable customer manifest artifacts;
- `runner.json`: exact inspected customer runner image and release identity;
- `history/`: bounded failure records;
- `logs/compose-failure.log`: preserved bounded startup diagnostics;
- `secrets.json`: owner-only generated local credentials;
- `maintenance.json`: owner-only runner/manifest recovery snapshot and
  maintenance lease, present only while completion remains unconfirmed;
- `runtime.json`: current running Compose/deployment identity.

`deploy/local/compose.yml` is project-owned and should normally be committed.
The default runner bind mount exposes the project-owned `.data/` directory at
`/var/lib/favn/data`; the generated inline comments explain how to change it or
replace it with a temporary filesystem.
The PostgreSQL volume name remains derived from the canonical project root.
Install does not create it, and Favn reset does not remove it.

## Maintainer-only control-plane build

`MIX_ENV=prod mix favn.build.control_plane --load` creates the repository-owned
Linux amd64 candidate image used by acceptance tests. Public install cannot
select arbitrary images or candidate tags. Protected GitHub Actions is the only
publisher for `ghcr.io/eirhop/favn-control-plane`; ordinary `main` merges publish
nothing. A maintainer must manually dispatch publication for the exact current,
green `main` revision. Deployments consume its digest, not a mutable tag.

A consumer project whose Mix dependencies already resolve coherently from one
`FAVN_CHECKOUT` can run `mix favn.maintainer.dev`. The command builds or reuses
that checkout's candidate, installs its exact local Docker image ID as explicit
maintainer state, and starts or reloads the ordinary local stack. If no runner
image is selected, it materializes the validated source selection as a separate
Docker build context while building the customer Dockerfile. This is a
development-only path; it does not publish, scan, attest, or accept arbitrary
candidate input.
`mix favn.install` switches the project back to its version-matched official
image.

Qualify the exact loaded candidate with:

```bash
FAVN_CONTROL_PLANE_CANDIDATE=favn-control-plane-candidate:<build-id> \
  mix test.container
```

The ordinary `mix test.acceptance` tier excludes these image-building Docker
scenarios. Pull-request candidate CI and manually dispatched publication CI run
the dedicated container tier explicitly, so missing candidate images cannot
turn a production qualification into a silent skip.

The generated Compose environment pins `FAVN_CONTROL_PLANE_IMAGE` to the exact
installed image identity and freezes the shutdown drain timeout. The repository
container gate proves that exact candidate starts with a representative
automatically built customer-owned runner and aligned manifest. Deployment
upgrade/rollback remains an operator drill; public install does not expose
arbitrary image selection.
