# Control-plane environment contract

The production control plane is one BEAM containing Favn View and Favn
Orchestrator. `FavnOrchestrator.ControlPlaneRuntimeConfig` reads the process
environment once, validates both applications without mutation, applies both
validated configs together, and retains only a redacted boot summary. A failed
validation starts neither supervision tree. The release evaluates
`config/runtime.exs` to enable this loader; that file does not parse deployment
values itself.

PostgreSQL is the only production persistence composition. There is no storage
selector and `FAVN_STORAGE` has no meaning.

## PostgreSQL and durable secrets

| Variable | Contract |
| --- | --- |
| `FAVN_DATABASE_URL` | Required PostgreSQL URL. It is always redacted. URL query parameters are rejected so they cannot override the separately validated TLS, pool, or timeout settings. |
| `FAVN_DATABASE_SSL_MODE` | Required; only `verify-full` or `verify_full` is accepted by the production loader. |
| `FAVN_DATABASE_SSL_CA_FILE` | Optional absolute readable CA bundle. Without it, Erlang's system trust store is used. |
| `FAVN_DATABASE_POOL_SIZE` | `1..200`, default `15`. |
| `FAVN_DATABASE_QUEUE_TARGET_MS` | `1..120000`, default `50`. |
| `FAVN_DATABASE_QUEUE_INTERVAL_MS` | `1..120000`, default `1000`. |
| `FAVN_DATABASE_TIMEOUT_MS` | `1..120000`, default `15000`. |
| `FAVN_RUNTIME_INPUT_PIN_KEYS` | Required bounded JSON object of version to 32-byte/base64 key. |
| `FAVN_RUNTIME_INPUT_PIN_KEY_VERSION` | Positive version used for new writes; default `1`, and it must exist in the key set. |

Production rejects plaintext PostgreSQL, including loopback URLs and unsafe
interlock variables. Runtime code receives the validated connection and key-ring
values through frozen application configuration; it does not reread environment
variables.

## Orchestrator and runner boundary

| Variable | Contract |
| --- | --- |
| `FAVN_INSTANCE_ID` | Optional stable `1..160` byte identifier; defaults to the control-plane node name. |
| `FAVN_WORKSPACE_IDS` | Required unique comma-separated IDs; at most 1,000 IDs and 255 bytes per ID. |
| `FAVN_ORCHESTRATOR_API_BIND_HOST` | IPv4 bind address, default `0.0.0.0`. |
| `FAVN_ORCHESTRATOR_API_PORT` | `1..65535`, default `4101`. This listener is private. |
| `FAVN_ORCHESTRATOR_API_SERVICE_TOKENS` | Required bounded set of `versioned_identity[|role+...]:secret` entries. Up to 100 identities may overlap during manual rotation. Only hashes are retained. |
| `FAVN_ORCHESTRATOR_MANIFEST_COMPRESSED_LIMIT_BYTES` | `1 MiB..32 MiB`, default `8 MiB`. |
| `FAVN_ORCHESTRATOR_MANIFEST_DECOMPRESSED_LIMIT_BYTES` | At least the compressed limit and at most `128 MiB`, default `32 MiB`. |
| `FAVN_ORCHESTRATOR_AUTH_SESSION_TTL` | `1..2592000` seconds, default `43200`. |
| `FAVN_ORCHESTRATOR_BOOTSTRAP_USERNAME` | Required normalized initial operator username. |
| `FAVN_ORCHESTRATOR_BOOTSTRAP_PASSWORD` | Required `15..1024` byte initial operator secret. |
| `FAVN_ORCHESTRATOR_BOOTSTRAP_DISPLAY_NAME` | Bounded display name, default `Favn Admin`. |
| `FAVN_ORCHESTRATOR_BOOTSTRAP_ROLES` | Required known comma-separated role set, default `admin`. |
| `FAVN_ORCHESTRATOR_ACTIVE_RUN_PLAN_MAX_BYTES` | `64 MiB..8 GiB`, default `512 MiB`. |
| `FAVN_SCHEDULER_ENABLED` | Strict `true` or `false`, default `true`. |
| `FAVN_SCHEDULER_TICK_MS` | `100..86400000`, default `15000`. |
| `FAVN_SCHEDULER_MAX_MISSED_ALL_OCCURRENCES` | `0..100000`, default `1000`. |
| `FAVN_SHUTDOWN_DRAIN_TIMEOUT_MS` | `1000..3600000`, default `120000`. |
| `FAVN_CONTROL_PLANE_NODE` | Required long distributed-BEAM node name on private DNS. |
| `FAVN_RUNNER_NODE` | Required distinct runner long node name on private DNS. |
| `FAVN_DISTRIBUTION_COOKIE` | Required high-entropy secret shared only by the two trusted nodes. |
| `FAVN_BEAM_DISTRIBUTION_PORT` | Required fixed private distribution port. |
| `ERL_EPMD_PORT` | Optional private EPMD port, default `4369`. |
| `FAVN_RUNNER_RPC_TIMEOUT_MS` | `100..120000`, default `15000`. |
| `FAVN_RUNNER_DIAGNOSTICS_TIMEOUT_MS` | `100..30000`, default `5000`. |
| `FAVN_RUNNER_AWAIT_TIMEOUT_BUFFER_MS` | `0..120000`, default `2000`. |

## Lifecycle, readiness, and shutdown

Each BEAM owns an in-memory lifecycle authority with monotonic states:
`starting`, `accepting`, `draining`, and `stopping`. This state is not durable;
PostgreSQL ownership and fencing remain the recovery authority after a crash.
Readiness is true only in `accepting`. Liveness remains true while draining so
the platform can distinguish an orderly drain from a failed process.

The control-plane readiness response has the stable checks `config`, `api`,
`view`, `storage`, `schema`, `scheduler`, `lifecycle`, `runner_connection`,
`runner_release`, and `active_manifests`. A configured workspace may have no
deployment, so a clean installation can become ready before its first publish.
Every workspace that does have an active manifest must align with the connected,
self-verified runner release and be present in its manifest cache.

HTTP readiness reads cached snapshots only. A bounded background runner probe
checks the runner server, required supervisors and registries, extensions, and
data-plane adapters. A separate bounded reconciliation pass re-registers active
manifests after runner-cache restarts. Pending, failed, timed-out, malformed, or
stale snapshots fail closed; the readiness request itself never performs remote
adapter or PostgreSQL mutation work.

On `SIGTERM`, the application callback enters `draining` before OTP stops the
supervision tree. New HTTP mutations, run/rerun and backfill submissions,
scheduler/backfill claims, manifest publication/activation, runner manifest
leases, runner work, runtime-input resolution, and executable inspection are
rejected. Read-only operator and health requests remain available until the
listeners stop. Work admitted before the transition is tracked by monitored
permits and may finish for up to `FAVN_SHUTDOWN_DRAIN_TIMEOUT_MS`. At the
deadline, control-plane runs use the durable cancellation path and runner
executions are stopped through the runner result path. A runner forced to stop a
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
| `FAVN_VIEW_BIND_HOST` | IPv4 bind address, default `0.0.0.0`. Container health probes this address; wildcard maps to loopback. |
| `FAVN_VIEW_PORT` | `1..65535`, default `4000`. |
| `FAVN_VIEW_TRUSTED_PROXY_CIDRS` | Required comma-separated private IPv4/IPv6 proxy allowlist, maximum 32 entries. |
| `FAVN_VIEW_ORCHESTRATOR_READINESS_TIMEOUT_MS` | `100..30000`, default `1000`. |
| `FAVN_HTTP_MAX_CONNECTIONS` | Exact per-listener connection ceiling `1..100000`, default `1024`. |
| `FAVN_HTTP_REQUEST_TIMEOUT_MS` | Request-body read deadline `1000..120000`, default `30000`; configure an equal or shorter total deadline at the reverse proxy. |
| `FAVN_HTTP_IDLE_TIMEOUT_MS` | Idle connection deadline `1000..300000`, default `60000`. |
| `FAVN_HTTP_BODY_LIMIT_BYTES` | Ordinary request-body limit `64 KiB..8 MiB`, default `1 MiB`. Manifest publication keeps its separate limits. |

Only an immediate peer inside `FAVN_VIEW_TRUSTED_PROXY_CIDRS` may supply
`X-Forwarded-For`, `Host`, `Port`, or `Proto`. Favn strips those headers from
untrusted peers before routing. The reverse proxy must terminate TLS, forward
LiveView WebSocket upgrades, remove client-supplied forwarded headers, and have
no public route to the orchestrator API, EPMD, BEAM distribution, or PostgreSQL.

## Secret rotation

This release reads secrets only from environment variables. It does not read
mounted secret files, call Azure Key Vault, or hot-reload credentials. Platforms
may resolve vault references into environment values, but changing a value
requires a manually controlled drain and service restart. Configure overlapping
versioned service-token identities before removing an old token. Automatic secret
rotation remains future work. After successful initial-actor provisioning, the
control plane removes the bootstrap password from its application configuration;
the durable credential store retains only its password hash.

Every rotation uses a maintenance window: drain admission, wait up to
`FAVN_SHUTDOWN_DRAIN_TIMEOUT_MS`, change the platform environment, restart only
the affected revision, require readiness, run a smoke execution, and then resume
admission. Never remove an overlapping credential until its replacement has
been exercised successfully.

For an orchestrator service token:

1. Add a new versioned identity beside the old entry in
   `FAVN_ORCHESTRATOR_API_SERVICE_TOKENS` and restart the control plane.
2. Move clients to the new token and prove an authenticated operation.
3. Remove the old entry, restart again, and verify the old token is rejected.

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
