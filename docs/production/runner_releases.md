# Runner and manifest releases

The runner image belongs to the customer. It contains customer code,
dependencies, plugins, adapters, and native libraries. Favn does not build it
during development or publish it for the customer.

## Identities

| Identity | Meaning |
| --- | --- |
| Runner contract version | Control-plane/runner protocol compatibility |
| Runner release ID | Immutable ID assigned to one customer runner build |
| Runner image digest | Exact OCI image deployed by the customer |
| Manifest version ID | Immutable authored manifest and package identity |
| Required runner release ID | Exact runner release permitted to execute a manifest |

A runner release ID is `rr_` plus 64 lowercase hexadecimal characters. Assign a
new ID whenever executable runner behavior may have changed: code,
dependencies, plugins, native libraries, compile-time configuration, Favn,
Elixir, OTP, or build target.

Never reuse an ID for different image contents. A manifest-only change may use
an existing runner release only when the operator has established that
executable inputs did not change.

## Build the image

Copy the example once:

```bash
mix favn.init --target deployment
```

Favn writes `deploy/favn/runner.Dockerfile` and never overwrites it. The
customer reviews and owns that file.

```bash
export FAVN_RUNNER_RELEASE_ID="rr_$(openssl rand -hex 32)"

docker build \
  --platform linux/amd64 \
  --build-arg FAVN_CUSTOMER_APP=my_app \
  --build-arg FAVN_RUNNER_RELEASE_ID \
  --file deploy/favn/runner.Dockerfile \
  --tag registry.example/customer-favn-runner:"$FAVN_RUNNER_RELEASE_ID" \
  .
```

Customer CI scans, signs, publishes, and records the resulting digest. Deploy
the digest, not a mutable tag.

## Build the aligned manifest

Build in the consumer project with the same ID:

```bash
MIX_ENV=prod mix favn.build.manifest \
  --runner-release-id "$FAVN_RUNNER_RELEASE_ID"
```

The immutable bundle is written below
`.favn/dist/manifest/<manifest_version_id>/`. There is no mutable `latest.json`
pointer.

Publish and activate it against a configured Orchestrator:

```bash
export FAVN_ORCHESTRATOR_URL='https://control-plane.internal'
export FAVN_ORCHESTRATOR_SERVICE_TOKEN='<service-token>'
export FAVN_WORKSPACE_ID='production'

mix favn.publish \
  --manifest .favn/dist/manifest/<manifest_version_id>/manifest-index.json

mix favn.activate \
  --workspace-id production \
  --manifest-version <manifest_version_id>
```

The runner advertises its release ID at boot. Activation and dispatch fail
closed unless it exactly matches the manifest.

Keep the current and previous control-plane digest, runner digest, runner
release ID, manifest version, database schema version, and environment revision
as rollback tuples.
