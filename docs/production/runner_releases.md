# Runner and manifest releases

The runner image belongs to the customer. It is built from the customer
repository with the customer's Dockerfile, dependencies, native libraries, and
CI policy. Local tooling may invoke that Dockerfile for development. Favn does
not generate a production OCI context, publish the image, or replace customer
CI policy.

Favn provides two interfaces:

1. `mix favn.init --target runner` writes an editable starting template under
   `deploy/runner/`.
2. The runner image and its manifests share one immutable runner release ID.
   Local tooling generates it; production CI supplies it explicitly.

## Identities

| Identity | Meaning |
| --- | --- |
| Runner contract version | Compatibility of the control-plane/runner protocol |
| Runner release ID | Opaque immutable ID assigned to one customer runner build |
| Runner image digest | Exact OCI image built and deployed by the customer |
| Manifest version ID | Immutable manifest and execution-package identity |
| Required runner release ID | Runner release that may execute a manifest |

The release ID has the form `rr_` followed by 64 lowercase hexadecimal
characters. Favn validates its syntax and exact alignment; it does not derive or
prove its meaning.

Choose a new ID whenever executable runner behavior may have changed, including
customer Elixir code, dependencies, plugins, adapters, native libraries,
compile-time configuration, Favn version, Elixir, OTP, or build target. CI may
derive the ID from its immutable build inputs or generate a unique ID and record
it with the image digest. Never assign one release ID to different image
contents.

Manifest-only changes may reuse an existing release ID. Because the build is
customer-owned, the operator is responsible for deciding that no executable
runner input changed.

## Create and build the customer image

Create the editable template once:

```bash
mix favn.init --target runner
```

The template compiles the customer project and creates a `favn_runner` release.
Edit it like any other application Dockerfile. Generate the tested optional
DuckDB ADBC native driver section with:

```bash
mix favn.init --target runner --include duckdb-adbc
```

Use `duckdb-adbc@VERSION` to select another version supported by the installed
Favn release.

The customer application is packaged and loaded for its modules, but its
supervision tree is not started automatically. Declare runner-local services
through the public runner-plugin contract.

The scaffold is customer-owned and is never overwritten. After upgrading Favn,
generate a comparison copy with the new compatibility labels and build shape:

```bash
mix favn.init --target runner \
  --output deploy/runner-next

diff -ru deploy/runner deploy/runner-next
```

Merge the relevant changes into the owned Dockerfile, or switch the deployment
to the fully rendered comparison directory. Then build with a new runner release
ID. Delete the comparison copy after the upgrade is recorded. If the canonical
template already differs, `mix favn.init --target runner` fails rather than
overwriting it.

Normal local development builds this Dockerfile automatically:

```bash
mix favn.dev
```

Production CI remains explicit:

```bash
export FAVN_RUNNER_RELEASE_ID="rr_$(openssl rand -hex 32)"

docker build \
  --platform linux/amd64 \
  --build-arg FAVN_RUNNER_RELEASE_ID \
  --file deploy/runner/Dockerfile \
  --tag customer/favn-runner:dev \
  .
```

The final image must:

- run the `favn_runner` release on Linux amd64;
- set `FAVN_RUNNER_RELEASE_ID`;
- carry the labels validated by the generated template:
  `io.favn.runner-release-id`, `io.favn.version`,
  `io.favn.runner-contract-version`, and `io.favn.target`; and
- contain every customer dependency and optional native driver needed at
  runtime.

Production CI builds, scans, pushes, and deploys this image using customer
registry credentials. Deploy the returned repository digest, not a mutable tag.

## Build and publish the aligned manifest

Build a manifest with the same release ID:

```bash
mix favn.build.manifest \
  --runner-release-id "$FAVN_RUNNER_RELEASE_ID"
```

The result is written below:

```text
.favn/dist/manifest/<manifest_version_id>/
```

Publish it as staged, then activate the exact returned version:

```bash
export FAVN_ORCHESTRATOR_SERVICE_TOKEN='<versioned-service-token>'

mix favn.publish \
  --orchestrator-url https://control-plane.internal \
  --manifest .favn/dist/manifest/<manifest_version_id>/manifest-index.json

mix favn.activate \
  --orchestrator-url https://control-plane.internal \
  --workspace-id production \
  --manifest-version <manifest_version_id>
```

The runner advertises the configured release ID at boot. Activation and dispatch
fail closed unless the connected ready runner advertises the exact ID required
by the manifest.

Keep at least the current and previous runner image digest, runner release ID,
and manifest version together as rollback tuples.
