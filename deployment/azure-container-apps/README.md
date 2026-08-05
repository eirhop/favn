# Azure Container Apps reference

Use `control-plane.bicep` once and one `elastic-runner-job.bicep` deployment per
exact pool/release. The environment must use private networking and a workload
profile that supports the required TCP ingress. The runner image entrypoint
starts `RELEASE_NODE=undefined@<FAVN_RUNNER_NODE_HOST_ALIAS>`. OTP assigns a
bounded dynamic name after the runner explicitly connects outward; the runner
does not listen for BEAM connections and never uses the job execution ID in an
atom or node name.

The templates accept TLS options, CA, certificate, and key as secure
parameters and mount them as a read-only secret volume at `/etc/favn`. Replace
plain deployment parameters with Key Vault references or your chosen secret
delivery mechanism. The control-plane certificate SAN must match the stable DNS
host portion of `FAVN_CONTROL_PLANE_NODE`; OTP sends that host as TLS SNI.
Runner certificates must chain to the trusted CA and be valid for client
authentication. The dynamic runner is non-listening, so infrastructure does not
route to its host alias.

`control-plane.bicep` includes every mandatory production boot input for the
orchestrator and its same-BEAM web application. Supply `platformServiceTokens`
with general platform entries only; Favn rejects `capacity_reader` in that
aggregate. The template maps the raw `capacityReaderToken` separately to
`FAVN_ORCHESTRATOR_CAPACITY_READER_TOKEN`. It may be omitted only when every
configured pool is resident. Deploy every elastic runner job with the exact
same raw value, preferably through the same pinned Key Vault secret version;
otherwise KEDA demand reads fail closed. During rotation, use
`capacityReaderPreviousToken` as described in the
[capacity-reader rotation runbook](../../docs/production/secret_rotation.md#capacity-reader-token).
`runnerPools` is the JSON lifecycle-policy map and must contain every pool
deployed as a runner job.

The control-plane template uses Azure Database for PostgreSQL managed identity.
It creates two deterministic, distinct user-assigned identities and derives each
client ID from the resource it assigns:

- `${name}-postgres-runtime` is assigned only to the long-running control plane
  and maps to `runtimeDatabaseUsername`;
- `${name}-postgres-migration` is assigned only to the manual
  database-operations job and maps to `migrationDatabaseUsername`.

Use different identity resources and PostgreSQL roles. The template contains no
database URL/password secret. It still mounts the database CA for verified TLS.
The manual job invokes only an allowlisted `favn_control_plane_ops` operation;
deploy or override `databaseOperation` for the exact controlled operation being
performed.

The web proxy settings are generic and must match the actual ingress behavior:

```text
viewTrustedProxyCidrs=<canonical private source CIDR>
viewForwardedForPolicy=replace|append|ignore
```

Use `replace` when the last proxy removes incoming forwarding headers and
writes one client address. Use `append` only if the actual ingress appends the
address it observed to the right of any incoming chain; Favn ignores earlier
entries. Use `ignore` if the application does not need the original client IP.
Exact `/32` or `/128` peers are safest. A changing managed-ingress source may
require a private subnet, but every workload in that subnet is then authorized
to claim HTTPS. Restrict the View port to ingress sources and verify the source
range and header behavior in the deployed environment.

These settings do not expose the View listener by themselves. This reference
currently routes private ingress to the orchestrator API on port `4101`, not
the operator View on port `4000`; add and qualify a separate private operator
route before treating it as an operator deployment.

The template deliberately has no administrator bootstrap secret. After
database migration and workspace provisioning, run the one-time interactive
bootstrap operation documented in
[`../../docs/production/control_plane_environment.md`](../../docs/production/control_plane_environment.md).

For Microsoft Entra SSO through Container Apps Easy Auth, set
`enableEntraEasyAuth=true`, `viewEntraTenantId`, `viewEntraClientId`,
`viewEntraClientSecret`, and `viewEntraWorkspaceId`. The template then enables
Easy Auth, requires authentication, and redirects browser requests to Entra.
It deliberately does not create or own the company app registration or its
enterprise-application assignments. Configure that existing registration's
callback URL and require assignment for the intended users. Then link each
immutable Entra object ID with `mix favn.admin.entra`. The complete flow,
no-bypass requirement, logout, and break-glass procedure is in
[`../../apps/favn/guides/operator-authentication.md`](../../apps/favn/guides/operator-authentication.md).

Favn keeps assigned tasks in `outstanding` until their durable result is
accepted. This matches event-job queue guidance: active work remains visible
while the platform accounts for running executions. Qualify this exact behavior
in the Azure environment before production because Container Apps does not
expose KEDA's ScaledJob strategy controls directly.

Before production use, qualify the exact Azure environment for:

- private EPMD and raw TCP distribution routing;
- managed KEDA `metrics-api` bearer authentication and subtraction of running
  executions from outstanding demand;
- DNS/SAN validation and certificate rotation;
- cold start, polling load, duplicate execution, timeout, drain, rollout, and
  rollback behavior.

No live Azure qualification was performed by this PR, so this remains an
unqualified reference.
