# Control-plane and runner images

Images are production artifacts, not a source-development requirement.

Favn ships one reusable control-plane image containing the Orchestrator, View,
PostgreSQL storage adapter, and their runtime dependencies. Each customer builds
a separate runner image containing its own project code and plugins.

## Build the control plane

From the Favn repository root:

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

The metadata script reads the version and compatibility contracts from the
checked-out source. The Docker build verifies those values against the compiled
release before applying image labels. It has no fallback labels that can drift
from the executable.

The Dockerfile consumes the repository through a BuildKit bind mount. There is
no generated build context, build-ID registry, `mix favn.build.control_plane`,
or maintainer development mode.

Pull-request CI performs a read-only root build, validates both image contracts,
and scans both images. Only a push to `main` receives registry and attestation
permissions; that job validates and scans before publishing the commit image.
Production deployments use the resulting digest:

```text
ghcr.io/eirhop/favn-control-plane@sha256:<digest>
```

The image:

- runs as UID/GID `10001`;
- keeps the root filesystem and root-owned release code read-only at runtime;
- contains no Mix, runner, authoring, or local-development application;
- contains precompiled View assets;
- requires runtime secrets through the process environment;
- exposes View on `4000` and the Orchestrator API on `4101`;
- uses fixed BEAM distribution ports supplied by the deployment.

Run the static contract locally after building:

```bash
scripts/control_plane_image_contract.sh favn-control-plane:dev "$FAVN_CONTROL_PLANE_VERSION"
```

## Promote a release candidate

The `Control-plane image` workflow builds, verifies, scans, attests, and
publishes `sha-<commit>` for every green push to `main`. A version tag promotes
that exact digest without rebuilding it. The promotion workflow rejects a tag
unless its version equals the source version and both CI and image qualification
passed for the tagged commit. It also requires a GitHub-verified signed tag,
binds both OCI attestations and the image revision label to that commit, and
refuses to change an existing release tag to another digest.

For `0.5.0-rc.5`, tag the already-qualified `main` commit:

```bash
git switch main
git pull --ff-only origin main
test "$(scripts/release_metadata.sh | sed -n 's/^FAVN_CONTROL_PLANE_VERSION=//p')" = "0.5.0-rc.5"
gh run list --workflow CI --branch main --commit "$(git rev-parse HEAD)"
gh run list --workflow control-plane-image.yml --branch main --commit "$(git rev-parse HEAD)"
git tag -s v0.5.0-rc.5 -m "Favn 0.5.0-rc.5"
git push origin v0.5.0-rc.5
gh run list --workflow control-plane-release.yml --limit 5
gh run watch <release-workflow-run-id> --exit-status
```

If a tag-triggered promotion fails before publishing because the workflow itself
needs a fix, keep the protected tag unchanged. After the fix reaches `main`, a
maintainer can run the corrected workflow against that existing tag:

```bash
gh workflow run control-plane-release.yml \
  --ref main \
  -f release_tag=v0.5.0-rc.5
```

The recovery path applies the same signed-tag, qualified-commit, attestation,
scan, and immutable-destination checks as the tag-triggered path. Re-running it
is safe only when any existing image tag and GitHub release already record the
same qualified digest.

The workflow creates a GitHub prerelease and the
`ghcr.io/eirhop/favn-control-plane:v0.5.0-rc.5` lookup tag. Deploy the digest
recorded in the release notes, not that mutable tag. This repository does not
currently define a Hex publishing workflow; source/Hex packaging is a separate
release decision. Protect `v*` tags in repository rules as defense in depth;
the workflow still checks the remote tag, its verified signature, and immutable
release destinations.

The scheduled security workflow rescans `main` plus every supported
control-plane digest promoted by this release workflow. Grype exceptions have a
machine-checked review deadline; CI fails after that date until every exception
is reviewed or removed.

## Build the customer runner

Copy the deployment example into a consumer project:

```bash
mix favn.init --target deployment
```

Choose an immutable release ID for each runner image and bind its logical pool
to the same ID in the manifest:

```bash
export RUNNER_RELEASE_ID="rr_<64-lowercase-hex-characters>"

docker build \
  --platform linux/amd64 \
  -f deploy/favn/runner.Dockerfile \
  --build-arg FAVN_CUSTOMER_APP=my_app \
  --build-arg FAVN_RUNNER_RELEASE_ID="$RUNNER_RELEASE_ID" \
  -t registry.example/customer-favn-runner:"$RUNNER_RELEASE_ID" \
  .

deploy/favn/runner-image-contract.sh \
  registry.example/customer-favn-runner:"$RUNNER_RELEASE_ID" \
  "$RUNNER_RELEASE_ID"

MIX_ENV=prod mix favn.build.manifest \
  --runner-release "default=$RUNNER_RELEASE_ID"
```

The generated runner installs the pinned DuckDB ADBC shared library plus
DuckLake, PostgreSQL scanner, and JSON extensions. This is the supported Favn
SQL runtime. The runner image remains customer-owned because it also contains
customer code, dependencies, and plugins; customer CI owns scanning, signing,
publication, and its final extension set.

## Deployment contract

The supported first topology is:

- one control-plane container;
- zero to N customer runner processes across user-defined logical pools;
- one externally supplied PostgreSQL database;
- a private network path from runners to the control plane;
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
expected by Favn. Follow that platform's restart or revision rules when changing
configuration or referenced secrets.

The copied `env.example` documents the minimum variables. Important groups are:

- PostgreSQL URL and verified TLS;
- runtime-input pin keys;
- general Orchestrator service tokens and dedicated capacity-reader tokens;
- workspace IDs;
- View public origin, proxy CIDRs, and secret key base;
- control-plane and runner node names, distribution cookie, and fixed port.

Do not bake values into either image or pass secrets as Docker build arguments.
Administrator bootstrap is a separate explicit operation; it is never configured
through the control-plane environment.

## PostgreSQL bootstrap and upgrades

Run Favn-owned PostgreSQL setup explicitly before starting the control plane:

```bash
bin/favn_control_plane_ops status
bin/favn_control_plane_ops bootstrap
bin/favn_control_plane_ops upgrade
```

The one-off process switches between explicit bootstrap, migrator, and runtime
profiles while keeping each connection separate. `bootstrap` finishes only after
a fresh restricted runtime connection verifies the database and workspace;
`upgrade` accepts no bootstrap profile. Release `eval` commands disable
distributed Erlang, so Jobs need no node name, cookie, EPMD, or distribution
port. Application startup never migrates or provisions PostgreSQL.

See [`postgresql_bootstrap.md`](postgresql_bootstrap.md) for the exact variables,
Job lifecycle, stable JSON/exit contract, Azure mapping path, and password path.

## Maintainer image debugging

To test a control-plane change in a real consumer project:

1. build `favn-control-plane:dev` from the Favn repository;
2. build the consumer runner image from the consumer repository;
3. build a manifest with the same runner release ID;
4. select both images in the consumer-owned deployment;
5. run the deployment qualification.

No `favn.maintainer.dev` task is needed.
