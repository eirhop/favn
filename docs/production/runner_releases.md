# Runner and manifest releases

The runner is the customer-code artifact. Favn creates its reproducible build
context but neither builds the final production image nor pushes it to a
registry.

## Identities

| Identity | Meaning |
| --- | --- |
| Runner contract version | Compatibility of the control-plane/runner protocol |
| Runner release ID | Deterministic identity of the exact executable customer runtime |
| Runner image digest | Exact OCI image built by the customer from that runner release |
| Manifest version ID | Immutable manifest release and execution-package identity |
| Required runner release ID | Runner release that may execute a manifest version |

The runner release ID and OCI digest are deliberately different. Rebuilding an
unchanged context can produce a different OCI digest without changing logical
runtime compatibility; Orchestrator still pins the runner release ID while the
operator deploys the exact image digest.

## What requires a runner rebuild

A new runner release is required when executable behavior can change, including:

- Elixir assets or imported runtime helpers;
- SQL runtime-input resolver modules;
- configured runner plugins, adapters, or supervised children;
- explicitly declared dynamic modules or applications;
- selected runtime dependency or lock inputs;
- relevant compile-time runtime values; or
- the Favn runner contract, Elixir, OTP, or target.

SQL text, templates, checks, output contracts, materialization declarations,
graphs, pipelines, schedules, and other manifest-only metadata may reuse an
existing runner release when the executable fingerprint is unchanged.

## Build and publish a runner change

Before Hex publication, use a clean Favn checkout detached at the approved Git
tag or commit and reference its public packages with path dependencies. Do not
use a floating or dirty Favn checkout for a production build.

From the customer project:

```bash
mix favn.install
mix favn.build.runner
```

The build writes:

```text
.favn/dist/runner/<runner_release_id>/
.favn/dist/manifest/<manifest_version_id>/
```

The runner directory contains `runner-release.json`, an integrity-checked OCI
context, a digest-pinned Dockerfile, and `operator-notes.md`. The context vendors
the exact dependency closure and builds without the original Favn checkout or
private Git credentials.

Build and push it with customer-owned tooling, using immutable tags only as
lookup aliases:

```bash
docker buildx build \
  --platform linux/amd64 \
  --push \
  --tag registry.example/internal/favn-runner:<immutable-release-alias> \
  .favn/dist/runner/<runner_release_id>
```

Record the returned repository digest, deploy that digest to the runner role,
and keep `runner-release.json` with the deployment record. The runner validates
its baked descriptor at boot before advertising readiness.

Publish the aligned manifest as staged, then activate the exact returned
version:

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

Activation fails closed until the connected healthy runner advertises the exact
required runner release. Dispatch and the runner independently repeat the
alignment check.

## Manifest-only change

Point `mix favn.build.manifest` at the previously verified descriptor:

```bash
mix favn.build.manifest \
  --runner-release .favn/dist/runner/<runner_release_id>/runner-release.json
```

The command recomputes the executable fingerprint. It returns
`runner_rebuild_required` rather than producing a mismatched manifest when any
runner-affecting input changed. On success, publish and activate the new manifest
version; neither runtime image needs to change.

Keep at least the currently active runner image/descriptor/manifest tuple and
the immediately preceding compatible tuple for rollback. Never activate a
manifest merely because its schema is readable: its exact runner requirement
must be available and healthy.
