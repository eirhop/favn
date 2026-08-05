# Control-plane environment contract

The production control plane is one BEAM containing Favn View and Favn
Orchestrator. `FavnOrchestrator.ControlPlaneRuntimeConfig` reads the process
environment once, validates both applications without mutation, applies both
validated configs together, and retains only a redacted boot summary. A failed
validation starts neither supervision tree. The release evaluates
`config/runtime.exs` to enable this loader; that file does not parse deployment
values itself.

PostgreSQL is the only production persistence composition; the runtime exposes
no storage selector.

## PostgreSQL and durable secrets

| Variable | Contract |
| --- | --- |
| `FAVN_DATABASE_AUTH_MODE` | Optional; `password` by default or `azure_managed_identity`. The two modes have mutually exclusive connection inputs. |
| `FAVN_DATABASE_SSL_MODE` | Required; only `verify-full` or `verify_full` is accepted by the production loader. |
| `FAVN_DATABASE_SSL_CA_FILE` | Optional absolute readable CA bundle. Without it, Erlang's system trust store is used. |
| `FAVN_DATABASE_POOL_SIZE` | `1..200`, default `15`. |
| `FAVN_DATABASE_QUEUE_TARGET_MS` | `1..120000`, default `50`. |
| `FAVN_DATABASE_QUEUE_INTERVAL_MS` | `1..120000`, default `1000`. |
| `FAVN_DATABASE_TIMEOUT_MS` | `1..120000`, default `15000`. |
| `FAVN_RUNTIME_INPUT_PIN_KEYS` | Required bounded JSON object of version to 32-byte/base64 key. |
| `FAVN_RUNTIME_INPUT_PIN_KEY_VERSION` | Positive version used for new writes; default `1`, and it must exist in the key set. |

Password mode requires `FAVN_DATABASE_URL`. The URL is always redacted, and URL
query parameters are rejected so they cannot override the separately validated
TLS, pool, or timeout settings.

Azure managed-identity mode rejects `FAVN_DATABASE_URL` and requires ordinary,
non-secret connection components:

| Variable | Contract |
| --- | --- |
| `FAVN_DATABASE_HOST` | Azure Database for PostgreSQL hostname. |
| `FAVN_DATABASE_PORT` | Optional `1..65535`, default `5432`. |
| `FAVN_DATABASE_NAME` | PostgreSQL database name. |
| `FAVN_DATABASE_USERNAME` | PostgreSQL role mapped to the selected Entra identity. |
| `FAVN_AZURE_MANAGED_IDENTITY_CLIENT_ID` | Optional user-assigned managed-identity client ID. Omit it for the system-assigned identity. |

Every new Repo or notification connection obtains a fresh-enough token from an
independently supervised, bounded Azure cache. Existing authenticated sessions
remain open when the token used for their original handshake expires. Expected
identity-provider failures fail closed and enter connection backoff; Favn never
retries a database write or transaction. Release operations use the same path.
Use separate managed identities and PostgreSQL roles for runtime and migration.

Production rejects plaintext PostgreSQL, including loopback URLs and unsafe
interlock variables. Runtime code receives the validated connection and key-ring
values through frozen application configuration; it does not reread environment
variables.

## Initial administrator and break-glass recovery

Production startup never creates or changes an administrator. After migrations
and workspace provisioning, but before the initial control-plane start, run the
bootstrap operation once from a trusted operator shell:

```console
mix favn.admin.bootstrap --workspace WORKSPACE --username USERNAME
```

Repeat `--workspace` to grant the first administrator access to more than one
workspace. The command reads the password without echo. A protected Unix file
may be used with `--password-file`; it must be a regular file with no
group/other permission bits. Windows uses stdin because portable ACL
verification is unavailable.

A packaged release does not include Mix. Run its equivalent with an attached
interactive stdin:

```console
bin/favn_control_plane eval 'IO.inspect(FavnStoragePostgres.Release.admin_bootstrap_from_stdin(["WORKSPACE"], "USERNAME", "Favn Admin"))'
```

Bootstrap is serialized in PostgreSQL and succeeds only when no administrator
role exists. It creates one global actor, grants platform administration, adds
workspace-administrator membership to every listed workspace, and records
durable audit entries. Normal startup contains no bootstrap secret and cannot
repeat this action.

If an existing administrator loses access, use the explicit recovery operation:

```console
mix favn.admin.recover --username USERNAME
```

For a packaged release:

```console
bin/favn_control_plane eval 'IO.inspect(FavnStoragePostgres.Release.admin_recover_from_stdin("USERNAME"))'
```

Recovery is allowed only for an actor that already has an administrator role.
It activates the global actor, rotates its password, revokes all of its
sessions, and records a durable platform audit entry. It does not add or repair
workspace memberships. Durable authorization rejects those sessions
immediately; a passive LiveView connected to an unclustered control-plane
process leaves on its next revalidation, within 30 seconds. Run recovery only as
a documented break-glass operation.

For a non-administrator who has lost the password, use the separate global actor
reset. It preserves actor status and workspace memberships, rotates the
credential, revokes every session, and records a platform audit entry:

```console
mix favn.admin.password_reset --username USERNAME
```

For a packaged release:

```console
bin/favn_control_plane eval 'IO.inspect(FavnStoragePostgres.Release.admin_password_reset_from_stdin("USERNAME"))'
```

To disable or re-enable an exact global actor without changing memberships:

```console
mix favn.admin.actor --username USERNAME --status disabled
mix favn.admin.actor --username USERNAME --status active
```

The packaged-release equivalent is
`FavnStoragePostgres.Release.admin_actor_status/1`. Disabling revokes every
session across every workspace. See
[`operator_security.md`](operator_security.md) for the complete role, session,
and audit contract.

## Orchestrator and runner boundary

| Variable | Contract |
| --- | --- |
| `FAVN_LOG_LEVEL` | Optional Logger primary and default-handler level, default `info`. Accepted values are `debug`, `info`, `notice`, `warning`, `error`, `critical`, `alert`, and `emergency`; invalid values stop release startup before they can become VM arguments. |
| `FAVN_INSTANCE_ID` | Optional stable `1..160` byte identifier; defaults to the control-plane node name. |
| `FAVN_WORKSPACE_IDS` | Required unique comma-separated IDs; at most 1,000 IDs and 255 bytes per ID. |
| `FAVN_ORCHESTRATOR_API_BIND_HOST` | IPv4 bind address, default `0.0.0.0`. |
| `FAVN_ORCHESTRATOR_API_PORT` | `1..65535`, default `4101`. This listener is private. |
| `FAVN_ORCHESTRATOR_API_SERVICE_TOKENS` | Required bounded set of general platform `versioned_identity[|role+...]:secret` entries. `capacity_reader` and the identities `capacity-scaler` and `capacity-scaler-overlap` are rejected; use the dedicated variables below. Across this aggregate and the dedicated capacity credentials, at most 100 tokens are accepted and only hashes are retained. |
| `FAVN_ORCHESTRATOR_CAPACITY_READER_TOKEN` | Raw least-privilege scaler credential. Required when any runner pool is `elastic`; optional for resident-only deployments. Favn assigns the reserved `capacity-scaler` identity and only `capacity_reader`. |
| `FAVN_ORCHESTRATOR_CAPACITY_READER_PREVIOUS_TOKEN` | Optional raw overlap credential used only during rotation. Requires the primary credential. Favn assigns `capacity-scaler-overlap` and only `capacity_reader`. |
| `FAVN_ORCHESTRATOR_MANIFEST_COMPRESSED_LIMIT_BYTES` | `1 MiB..32 MiB`, default `8 MiB`. |
| `FAVN_ORCHESTRATOR_MANIFEST_DECOMPRESSED_LIMIT_BYTES` | At least the compressed limit and at most `128 MiB`, default `64 MiB`. |
| `FAVN_ORCHESTRATOR_AUTH_SESSION_TTL` | `1..2592000` seconds, default `43200`. |
| `FAVN_ORCHESTRATOR_ACTIVE_RUN_PLAN_MAX_BYTES` | `64 MiB..8 GiB`, default `512 MiB`. |
| `FAVN_SCHEDULER_ENABLED` | Strict `true` or `false`, default `true`. |
| `FAVN_SCHEDULER_TICK_MS` | `100..86400000`, default `15000`. |
| `FAVN_SCHEDULER_MAX_MISSED_ALL_OCCURRENCES` | `0..100000`, default `1000`. |
| `FAVN_RUN_SUBMISSION_CONCURRENCY` | Global bounded preparation/admission workers, `1..256`, default `8`. |
| `FAVN_RUN_SUBMISSION_WORKSPACE_CONCURRENCY` | Per-workspace worker cap, `1..256`, default `2`; it cannot exceed the global cap. |
| `FAVN_SHUTDOWN_DRAIN_TIMEOUT_MS` | `1000..3600000`, default `120000`. |
| `FAVN_CONTROL_PLANE_NODE` | Required long distributed-BEAM node name on private DNS. |
| `FAVN_RUNNER_POOLS` | JSON object of operator-defined pool names to `elastic` or `resident` lifecycle policy; defaults to one elastic `default` pool with `15000` ms idle grace. Any elastic pool requires `FAVN_ORCHESTRATOR_CAPACITY_READER_TOKEN`. |
| `FAVN_DISTRIBUTION_COOKIE` | Required high-entropy secret shared by the control plane and trusted runner releases. |
| `FAVN_BEAM_DISTRIBUTION_PORT` | Required fixed private control-plane distribution port. |
| `ERL_EPMD_PORT` | Optional private EPMD port, default `4369`. |

## Lifecycle, readiness, and shutdown

Each BEAM owns an in-memory lifecycle authority with monotonic states:
`starting`, `accepting`, `draining`, and `stopping`. This state is not durable;
PostgreSQL ownership and fencing remain the recovery authority after a crash.
Readiness is true only in `accepting`. Liveness remains true while draining so
the platform can distinguish an orderly drain from a failed process.

Control-plane readiness checks configuration, API/View, storage/schema,
scheduler, lifecycle, active manifest pool bindings, and durable runner
queue/counter health. Zero registered runners is healthy and expected when
elastic pools have no work. HTTP readiness reads bounded state and never starts
infrastructure or performs runner work.

On `SIGTERM`, the application callback enters `draining` before OTP stops the
supervision tree. New HTTP mutations, run/rerun and backfill submissions,
scheduler/backfill claims, manifest publication/activation, durable task
enqueue, runtime-input resolution, and executable inspection are
rejected. Read-only operator and health requests remain available until the
listeners stop. Work admitted before the transition is tracked by monitored
permits and may finish for up to `FAVN_SHUTDOWN_DRAIN_TIMEOUT_MS`. At the
deadline, control-plane runs request cancellation through the durable task
authority. A runner forced to stop a
worker records `native_cancel_unknown` as an unknown-outcome interruption rather
than claiming safe cancellation. Unresolved outcomes remain explicit and are
handled by fenced recovery after restart; shutdown never invents success.

Configure the container platform's termination grace period longer than the
drain timeout. Allow at least an additional 50 seconds for the control plane's
single 30-second post-drain cancellation/settlement budget, bounded listener
shutdown, repository teardown, and a small platform safety margin. The drain
election is process-local and idempotent, so the View and
Orchestrator application callbacks share one long drain window. After it, each
HTTP listener and worker child has a separate five-second teardown bound.
Production upgrades are drain-first; zero-downtime rolling replacement is not
supported by this release.

## View, proxy, and HTTP limits

| Variable | Contract |
| --- | --- |
| `FAVN_VIEW_PUBLIC_ORIGIN` | Required absolute HTTPS origin. The production loader has no plaintext HTTP interlock. |
| `FAVN_VIEW_SECRET_KEY_BASE` | Required secret of at least 64 bytes. |
| `FAVN_VIEW_BIND_HOST` | IPv4 bind address, default `0.0.0.0`. Unified boot freezes this address for container health probes; wildcard maps to loopback. |
| `FAVN_VIEW_PORT` | `1..65535`, default `4000`. |
| `FAVN_VIEW_TRUSTED_PROXY_CIDRS` | Required comma-separated canonical private IPv4/IPv6 proxy allowlist, maximum 32 entries. Prefer exact `/32` or `/128` peers; wider subnets emit a startup warning. |
| `FAVN_VIEW_FORWARDED_FOR_POLICY` | `replace`, `append`, or `ignore`; default `replace`. Describes how an authorized proxy represents the client address. |
| `FAVN_VIEW_AUTH_MODE` | `password` (default) or `azure_container_apps_entra`. The selected mode is frozen at startup. |
| `FAVN_VIEW_ENTRA_TENANT_ID` | Required UUID in Entra mode. Only principals from this immutable tenant are accepted; diagnostics redact it. |
| `FAVN_VIEW_ENTRA_WORKSPACE_ID` | Required Favn workspace for initial Entra sign-in; diagnostics redact it. |
| `FAVN_VIEW_ORCHESTRATOR_READINESS_TIMEOUT_MS` | `100..30000`, default `1000`. |
| `FAVN_HTTP_MAX_CONNECTIONS` | Exact per-listener connection ceiling `1..100000`, default `1024`. |
| `FAVN_HTTP_REQUEST_TIMEOUT_MS` | Request-body read deadline `1000..120000`, default `30000`; configure an equal or shorter total deadline at the reverse proxy. |
| `FAVN_HTTP_IDLE_TIMEOUT_MS` | Idle connection deadline `1000..300000`, default `60000`. |
| `FAVN_HTTP_BODY_LIMIT_BYTES` | Ordinary request-body limit `64 KiB..8 MiB`, default `1 MiB`. Manifest publication keeps its separate limits. |

Only an immediate peer inside `FAVN_VIEW_TRUSTED_PROXY_CIDRS` may supply
forwarded client IP or scheme. `replace` requires one exact
`X-Forwarded-For` value. `append` trusts only the rightmost value in a bounded
chain. `ignore` retains the immediate peer. Favn strips `Forwarded`,
`X-Forwarded-For`, `X-Forwarded-Host`, `X-Forwarded-Port`, and
`X-Forwarded-Proto` before routing and never uses forwarded host or port; the
fixed public origin controls redirects. The reverse proxy must terminate TLS,
forward LiveView WebSocket upgrades, and have no public route to the
orchestrator API, EPMD, BEAM distribution, or PostgreSQL. The complete exposure
and trusted-network checklist is
[`network_and_proxy.md`](network_and_proxy.md).

The Entra mode is specific to Azure Container Apps Easy Auth. Configure the
provider-to-actor link with `mix favn.admin.entra`; do not use email, display
name, groups, or provider roles as authorization keys. See the HexDocs
[Operator Authentication](../../apps/favn/guides/operator-authentication.md)
guide for Azure settings, logout, denial behavior, and break-glass switching.

## Secret rotation

Long-running release configuration reads secrets only from environment
variables. It does not read mounted secret files, call Azure Key Vault, or
hot-reload credentials. Platforms may resolve vault references into environment
values, but changing a value requires a manually controlled drain and service
restart. General platform tokens overlap through versioned aggregate entries;
capacity-reader tokens overlap through their dedicated primary and previous
variables. Automatic secret rotation remains future work.
Administrator bootstrap and recovery are explicit exceptions: their one-off
commands read the password from protected operator input and persist only its
password hash. The password is never a control-plane environment variable.

Every rotation uses a maintenance window: drain admission, wait up to
`FAVN_SHUTDOWN_DRAIN_TIMEOUT_MS`, change the platform environment, restart only
the affected revision, require readiness, run a smoke execution, and then resume
admission. Never remove an overlapping credential until its replacement has
been exercised successfully.

For a general platform service token:

1. Add a new versioned identity beside the old entry in
   `FAVN_ORCHESTRATOR_API_SERVICE_TOKENS` and restart the control plane.
2. Move clients to the new token and prove an authenticated operation.
3. Remove the old entry, restart again, and verify the old token is rejected.

Never place `capacity_reader` or either reserved capacity-scaler identity in
the aggregate variable. The dedicated primary and previous variables provide
the overlap contract described in the
[capacity-reader rotation runbook](secret_rotation.md#capacity-reader-token).

For a runtime-input encryption key:

1. Add the new version to `FAVN_RUNTIME_INPUT_PIN_KEYS`, retain every referenced
   old version, set `FAVN_RUNTIME_INPUT_PIN_KEY_VERSION` to the new version, and
   restart the control plane.
2. Run `favn_control_plane_ops runtime-input-key-inventory` and confirm the new
   current version without exposing key material.
3. Remove an old version only after inventory proves it is unreferenced, then
   restart and require readiness again. See
   [`postgresql_operator_runbook.md`](postgresql_operator_runbook.md) for
   compaction and retirement commands.

Changing `FAVN_VIEW_SECRET_KEY_BASE` requires a control-plane restart and
invalidates all existing browser sessions. Operators must announce that users
will sign in again; PostgreSQL application state is unaffected.

The consolidated operator procedures, including distribution-cookie and database
credential handling, are in [`secret_rotation.md`](secret_rotation.md).
