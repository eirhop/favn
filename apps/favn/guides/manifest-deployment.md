# Deploying A Manifest Archive

`mix favn.build.manifest` produces one transport-ready `.tar.gz` file. Upload
that file unchanged to the private Orchestrator API. The uploader needs only an
HTTPS client such as `curl`; it does not need Elixir, Mix, Favn dependencies, a
customer image, or a mounted volume.

The network path is deployment-specific:

- CI may upload directly when it can reach the private API.
- CI may put the archive in blob storage and start a small private Linux Job
  that downloads and uploads it.
- An operator may upload it from any other authorized private host.

Blob storage and a relay Job are options, not parts of Favn's protocol. Favn's
contract starts with the archive produced by the build and ends with the
Orchestrator accepting and activating it.

## Build the archive

Bind every runner pool used by the project to its immutable release:

```console
MIX_ENV=prod mix favn.build.manifest \
  --runner-release default=rr_EXACT_RELEASE
```

The command prints both values needed for upload:

```text
archive: .favn/dist/manifest/mv_EXACT_ID.tar.gz
archive sha256: EXACT_LOWERCASE_SHA256
```

Favn creates a deterministic archive with the compact manifest index and its
content-addressed execution packages. It applies the transport limits while
building. Users do not choose compression, package chunks, or batch sizes.

## Upload from an authorized host

Choose a stable operation ID for this deployment attempt. If the HTTP result is
lost, repeat the request with the same operation ID, archive, and SHA-256:

```console
curl --fail-with-body \
  --request PUT \
  --header "Authorization: Bearer ${FAVN_MANIFEST_DEPLOY_TOKEN}" \
  --header "X-Favn-Workspace-Id: salmon" \
  --header "X-Favn-Archive-Sha256: EXACT_LOWERCASE_SHA256" \
  --header "Content-Type: application/gzip" \
  --upload-file .favn/dist/manifest/mv_EXACT_ID.tar.gz \
  https://orchestrator.internal/api/orchestrator/v1/manifest-deployments/release-2026-08-24
```

The Orchestrator authenticates and reserves bounded upload capacity before
reading the body. It then validates the gzip and tar streams without writing a
temporary archive, stores execution packages in bounded batches, atomically
accepts the manifest and durable deployment operation, and activates it in the
background.

The initial response is `202 Accepted`. Poll the returned `status_url`, or call:

```console
curl --fail-with-body \
  --header "Authorization: Bearer ${FAVN_MANIFEST_DEPLOY_TOKEN}" \
  --header "X-Favn-Workspace-Id: salmon" \
  https://orchestrator.internal/api/orchestrator/v1/manifest-deployments/release-2026-08-24
```

States are:

- `accepted`: the complete archive is durable and waiting for activation;
- `activating`: a fenced worker is activating it;
- `succeeded`: it is active with no unresolved inspection diagnostics;
- `needs_attention`: it is active, but diagnostics require operator attention;
- `failed`: activation failed with a known failure class; or
- `unknown`: activation may have completed and must be reconciled.

While activation runs, `progress.inspection_completed` and
`progress.inspection_total` report bounded durable target-inspection progress.
Physical inspections are queued against the manifest's exact runner pool and
release even when that pool currently has zero runners. The private runner-demand
endpoint then exposes the work so external scaling can start capacity. Activation
has one five-minute inspection budget for scaler polling, runner cold start,
claims, and completion. If no matching runner claims a task in that budget, the
operation ends as `needs_attention` with
`physical_inspection_runner_start_timeout`; restore that exact pool/release and
repeat deployment with a new operation ID.

The same operation ID and archive replay the existing result without reading
the body. Reusing the ID for different content returns
`409 deployment_operation_conflict`. A busy upload admission returns `429` with
`Retry-After`; retry that unchanged request later.

## Private relay example

When CI cannot reach the private API, CI may upload the produced archive to blob
storage and start a network-private Job. That Job performs only two transport
steps:

```console
curl --fail --location "${SIGNED_ARCHIVE_URL}" --output /tmp/manifest.tar.gz
curl --fail-with-body \
  --request PUT \
  --header "Authorization: Bearer ${FAVN_MANIFEST_DEPLOY_TOKEN}" \
  --header "X-Favn-Workspace-Id: ${FAVN_WORKSPACE_ID}" \
  --header "X-Favn-Archive-Sha256: ${FAVN_ARCHIVE_SHA256}" \
  --header "Content-Type: application/gzip" \
  --upload-file /tmp/manifest.tar.gz \
  "${FAVN_ORCHESTRATOR_URL}/api/orchestrator/v1/manifest-deployments/${FAVN_OPERATION_ID}"
```

The temporary path belongs to the short-lived relay container. The
Orchestrator itself requires no mounted volume.

## Credentials and limits

Operators configure workspace-scoped deployer credentials through
`FAVN_ORCHESTRATOR_MANIFEST_DEPLOYER_TOKENS`. A general API token with the
`platform_operator` role also works. Keep the private API off public ingress.

One archive may contain at most 10,000 execution packages. The compressed limit
is 256 MiB, the expanded limit is 1 GiB, the compact index limit is 64 MiB, and
each package is limited to 4 MiB. The Orchestrator reads at most 1 MiB at a time
and persists packages in batches of at most 8 or 4 MiB. The total upload
budget is 15 minutes. These are Favn protocol details, not uploader settings.

The Orchestrator automatically reads the finite Linux cgroup memory limit and
current usage on any cgroup-based container platform. If a bounded stage cannot
fit, it returns a retryable capacity error before reading that stage instead of
risking the control-plane process. Operators normally do not configure a memory
size in Favn; the optional ceiling is only for an unlimited host or a stricter
local policy.

The older `mix favn.publish` and `mix favn.activate` commands remain available
for interactive and local workflows. Production automation should prefer the
single archive deployment operation because it gives one durable status and one
safe replay identity.
