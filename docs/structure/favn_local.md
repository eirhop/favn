# favn_local

Purpose: customer-side manifest tooling and production-like local Docker
Compose lifecycle implementation behind public `mix favn.*` tasks.

## Ownership

`favn_local` owns:

- official control-plane image resolution and immutable install state;
- non-overwriting customer Compose and runner-template scaffolds;
- rendered-Compose validation and generated local credentials/bootstrap state;
- automatic local invocation and exact image-ID pinning of the customer runner
  Dockerfile, with an explicit existing-image override;
- manifest building against that runner's release ID;
- local start, reload, recovery, status, logs, diagnostics, stop, and reset;
- manifest publication/activation and local operator commands; and
- maintainer-only control-plane candidate building.

It does not own or publish a customer runner image, construct its production
context, or inspect source provenance. It does not provision production
infrastructure.

## Primary code

- `apps/favn_local/lib/favn/dev/install.ex`: immutable control-plane install.
- `apps/favn_local/lib/favn/dev/init/compose.ex`: customer Compose templates.
- `apps/favn_local/lib/favn/dev/init/runner.ex`: editable runner Dockerfile and
  release wrapper template.
- `apps/favn_local/lib/favn/dev/runner_image.ex`: default local Dockerfile
  invocation, external image validation, and exact local image-ID selection.
- `apps/favn_local/lib/favn/dev/build/manifest.ex`: explicit release-ID manifest
  build.
- `apps/favn_local/lib/favn/dev/compose_lifecycle.ex`: lifecycle, deployment,
  drain, rollback, logs, status, and diagnostics.
- `apps/favn_local/lib/favn/dev/publish.ex` and `activate.ex`: staged deployment.

## Invariants

- Public install selects only the Favn-published control plane.
- `mix favn.dev` builds `deploy/runner/Dockerfile` when no image is selected,
  then uses the exact inspected image ID with Compose `--no-build`.
- The selected runner's labels, platform, Favn version, and runner contract are
  validated before start. Local Compose runs the inspected image ID, not a
  mutable tag.
- Manifest generation uses the image's exact runner release ID.
- Favn never infers whether customer source requires a new runner during reload.
  The customer selects a new image before reload when executable content changes.
- Stop and reset never call Compose `down`, remove customer images, remove
  volumes, or delete `.data`.
- Runtime state records only the current deployment. Compose selection remains
  available after stop for bounded diagnostics and restart.

## Verification

```bash
MIX_ENV=test mix do --app favn_local cmd mix test --no-compile \
  --exclude acceptance --exclude container --exclude slow --exclude browser

MIX_ENV=test mix do --app favn_local cmd mix test --no-compile --only acceptance
```

Use this document when changing customer artifacts, Compose lifecycle state, or
the boundary between customer code and the prebuilt control plane.
