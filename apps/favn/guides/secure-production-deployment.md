# Secure Production Deployment

This guide describes the safest currently supported way to run Favn in
production.

The short version is:

- run one control-plane container;
- keep it on a private network;
- put one trusted HTTPS ingress in front of it;
- use PostgreSQL with verified TLS, backups, and a separate runtime account;
- use Azure Container Apps Easy Auth with Microsoft Entra for SSO and MFA, or
  let operators reach a trusted proxy through an equivalent identity-aware
  access service; and
- expose no database, internal API, EPMD, or BEAM distribution port publicly.

`mix favn.dev` is a local-development tool, not a production server. Production
uses the immutable control-plane image and explicit database operations.

Favn's repository documentation contains the complete operational contracts.
This guide collects the important decisions and checks in one place for
HexDocs readers.

## Is the supported shape right for you?

The first supported production shape has one control-plane BEAM. That BEAM
contains the web application and the orchestrator.

Use this shape when:

- the operator UI is an internal administrative system;
- a short drain-and-restart maintenance window is acceptable;
- PostgreSQL provides durable state and managed backup or recovery; and
- runners can reach the control plane over a private network.

Do not deploy yet when:

- you require multiple active control-plane replicas;
- you require zero-downtime control-plane upgrades;
- operators must sign in directly from the public internet without an
  MFA-capable access layer; or
- you cannot isolate the internal ports from untrusted networks.

Multi-node control-plane failover is not part of the current security contract.
Login throttling and immediate LiveView disconnect are node-local. Durable
authorization remains in PostgreSQL, but that does not make an unsupported
multi-node deployment safe.

## Recommended architecture

```text
Operator
   |
   | VPN or identity-aware access with MFA
   v
Trusted HTTPS ingress
(reverse proxy or Container Apps Easy Auth)
   |
   | private HTTP and LiveView WebSocket
   v
One Favn control-plane container
   |                         ^
   | verified PostgreSQL TLS | private mutual-TLS BEAM
   v                         |
Managed PostgreSQL        Favn runners
```

Only the trusted HTTPS ingress receives operator traffic. PostgreSQL, the
container's HTTP listener, the private orchestrator API, EPMD, and BEAM
distribution stay private.

### Option 1: one dedicated container host

This is the simplest first deployment and is closest to Favn's manual
production qualification.

Use:

- one hardened Linux host or VM;
- one rootless or otherwise restricted control-plane container;
- Caddy, Nginx, HAProxy, or a managed edge proxy;
- a private firewall between the proxy, control plane, runners, and database;
  and
- managed PostgreSQL when possible.

The repository's Docker Compose files are a qualification harness, not a
production template. They use short-lived test certificates, include
test-oriented lifecycle helpers, and do not provide the complete operator
HTTPS edge. Do not deploy that Compose stack unchanged.

### Option 2: one replica on a managed container platform

Kubernetes, ECS, Nomad, Azure Container Apps, and similar platforms can host
the same topology.

Configure:

- exactly one control-plane replica;
- private container ingress;
- one managed HTTPS ingress or reverse proxy;
- secrets from the platform's secret store;
- a managed PostgreSQL service; and
- private runner-to-control-plane networking.

Before production, test the exact platform's WebSocket behavior, forwarded
headers, certificate rotation, secret rotation, runner networking, drain,
rollback, and database recovery.

Favn does not ship an Azure deployment. Operators own and qualify their cloud
infrastructure against the provider-neutral deployment contract.

For Azure Container Apps, the recommended first authentication option is
Microsoft Entra through Easy Auth. Container Apps ingress and Easy Auth act as
the managed authentication proxy, so a separate Nginx-style proxy is not
required solely for SSO. See [Operator Authentication](operator-authentication.md)
for the exact trust boundary, environment, identity-link command, logout, and
break-glass procedure.

## Network checklist

Complete every item:

- [ ] Operators reach Favn through a VPN or identity-aware access layer.
- [ ] The access layer supplies MFA when the operator surface is reachable
  outside a tightly controlled administrative network.
- [ ] Only the reverse proxy can reach the View container port.
- [ ] The reverse proxy terminates HTTPS and supports LiveView WebSocket
  upgrades.
- [ ] The reverse proxy removes client-supplied forwarded headers before
  adding its own trusted values.
- [ ] `FAVN_VIEW_PUBLIC_ORIGIN` is the exact external HTTPS origin.
- [ ] `FAVN_VIEW_TRUSTED_PROXY_CIDRS` contains only the real proxy peers.
- [ ] `FAVN_VIEW_FORWARDED_FOR_POLICY` matches what the last trusted proxy
  actually does.
- [ ] PostgreSQL accepts connections only from the control plane and trusted
  one-off database operations.
- [ ] EPMD and BEAM distribution accept connections only from the private
  runner network.
- [ ] The private orchestrator API is not routed through public ingress.
- [ ] Runners have no public inbound listener.

For example, if the proxy has the fixed private address `10.42.0.5`, prefer:

```text
FAVN_VIEW_PUBLIC_ORIGIN=https://favn.example.com
FAVN_VIEW_TRUSTED_PROXY_CIDRS=10.42.0.5/32
FAVN_VIEW_FORWARDED_FOR_POLICY=replace
```

In simple terms, the CIDR setting answers "who may speak for the proxy?" The
policy setting answers "where did that proxy put the client address?"

| Proxy behavior | Policy | What Favn trusts |
| --- | --- | --- |
| Removes the incoming chain and writes one address | `replace` (default) | One exact address |
| Keeps the incoming chain and adds its observed address on the right | `append` | Only the rightmost address |
| Does not provide a useful client address | `ignore` | The proxy's socket address |

Use `replace` whenever you control the proxy. For example:

```text
# Proxy 10.42.0.5 removes Forwarded and X-Forwarded-* from the request,
# then writes one X-Forwarded-For value and X-Forwarded-Proto.
FAVN_VIEW_TRUSTED_PROXY_CIDRS=10.42.0.5/32
FAVN_VIEW_FORWARDED_FOR_POLICY=replace
```

Some managed ingress systems append their observed client address and do not
offer a replace setting. Use `append` only after verifying that contract in the
target environment:

```text
# The platform routes only its ingress subnet to the View port and appends the
# address it observed as the rightmost X-Forwarded-For entry.
FAVN_VIEW_TRUSTED_PROXY_CIDRS=10.42.8.0/24
FAVN_VIEW_FORWARDED_FOR_POLICY=append
```

Favn ignores every earlier entry in that chain because a client may have
supplied it. The platform must send the chain as one header line; duplicate
`X-Forwarded-For` header lines are rejected as ambiguous. A same-replica
sidecar is simpler:

```text
FAVN_VIEW_TRUSTED_PROXY_CIDRS=127.0.0.1/32
FAVN_VIEW_FORWARDED_FOR_POLICY=replace
```

Do not use a broad range such as `172.16.0.0/12` merely because containers use
addresses inside it. Favn warns at startup for every trusted subnet that is
wider than one host. Such a subnet can be legitimate when a managed ingress
has changing source addresses, but every workload in it is authorized to claim
HTTPS and a client address. Network rules must ensure that only the real
ingress can reach the View port.

The setting accepts canonical network addresses only. Write `10.42.8.0/24`,
not `10.42.8.7/24`. This prevents a misleading value whose host bits would have
been silently ignored.

Favn always removes `Forwarded`, `X-Forwarded-For`, `X-Forwarded-Host`,
`X-Forwarded-Port`, and `X-Forwarded-Proto` before routing. It never trusts a
forwarded host or port: redirects use the fixed `FAVN_VIEW_PUBLIC_ORIGIN`. A
direct non-proxy HTTP request remains HTTP and is redirected to that fixed
origin even if it supplies forged forwarding headers.

One narrow exception keeps the immutable container health check useful: an
exact loopback peer may call `GET /api/web/v1/health/ready` over plaintext.
Changing the `Host` header to `localhost` does not create this exception for a
network peer.

CIDR trust is network authorization, not cryptographic proxy authentication.
For a higher-assurance deployment, put the hop on an exclusive private network
or use a local sidecar. If another workload can enter the trusted network,
terminate authenticated transport such as mutual TLS at a sidecar before the
request reaches Favn.

A simple firewall model is:

```text
Internet or company network -> proxy:443            allow
Proxy                       -> control-plane:4000   allow
Control plane               -> PostgreSQL:5432     allow
Runners                     -> private BEAM ports  allow
Everything else                                      deny
```

Port numbers are examples. Use the exact configured ports and the smallest
possible source ranges.

## PostgreSQL checklist

Production uses PostgreSQL 18 as the durable authority.

- [ ] Use a currently supported PostgreSQL 18 minor release.
- [ ] Require TLS with hostname verification.
- [ ] Give migrations a separate DDL-capable identity.
- [ ] Give the running control plane the least-privilege runtime identity.
- [ ] Enable provider-managed high availability when required.
- [ ] Enable point-in-time recovery.
- [ ] Perform and record a restore drill before launch.
- [ ] Monitor connection usage, storage growth, failed backups, and
  certificate expiry.
- [ ] Keep database administrator and monitoring connection headroom.

Password authentication uses:

```text
FAVN_DATABASE_URL=ecto://favn_runtime:<secret>@postgres.internal/favn
FAVN_DATABASE_SSL_MODE=verify-full
FAVN_DATABASE_SSL_CA_FILE=/run/secrets/postgresql-ca.pem
```

Azure Database for PostgreSQL can instead use managed identity without a
database password or URL secret:

```text
FAVN_DATABASE_AUTH_MODE=azure_managed_identity
FAVN_DATABASE_HOST=<server>.postgres.database.azure.com
FAVN_DATABASE_PORT=5432
FAVN_DATABASE_NAME=favn
FAVN_DATABASE_USERNAME=favn_runtime
FAVN_AZURE_MANAGED_IDENTITY_CLIENT_ID=<runtime-user-assigned-identity-client-id>
FAVN_DATABASE_SSL_MODE=verify-full
FAVN_DATABASE_SSL_CA_FILE=/run/secrets/postgresql-ca.pem
```

Use a different user-assigned identity and `favn_migrator` PostgreSQL role for
the one-off migration job. Tokens are requested for each new physical
connection through a bounded cache; they are never placed in application
configuration, URLs, logs, or diagnostics.

Do not add TLS settings to the URL query string. Favn validates TLS, pool, and
timeout settings separately and rejects URL query parameters in production.

Never put customer DuckLake metadata in the Favn control-plane database. The
control-plane backup covers Favn's own durable state, not every customer data
store or external secret.

## Secrets checklist

- [ ] Generate a unique `FAVN_VIEW_SECRET_KEY_BASE` containing at least 64
  random bytes.
- [ ] Generate unique, high-entropy general platform service tokens, a dedicated
  capacity-reader token for elastic scaling, and the BEAM distribution cookie.
- [ ] Store database passwords, service tokens, cookies, keys, and
  certificates in a secret manager.
- [ ] Mount certificate and CA files read-only.
- [ ] Never bake secrets into an image.
- [ ] Never pass secrets as Docker build arguments.
- [ ] Never commit a real `.env` file.
- [ ] Never put an administrator password in an environment variable or
  command-line argument.
- [ ] Document who may read, rotate, and revoke each secret.

One way to generate the browser-session secret is:

```bash
openssl rand -base64 64
```

Store the output directly in the secret manager. Do not paste it into source
control, tickets, chat, or deployment logs.

Long-running Favn releases read environment-backed secrets at startup. They do
not hot-reload secrets. Rotate them through a controlled procedure:

1. stop new work;
2. allow active work to drain;
3. update the secret reference;
4. restart the affected release;
5. require readiness;
6. run a smoke operation; and
7. remove the old overlapping credential only after the new one works.

## First deployment

Runtime startup never migrates PostgreSQL, provisions a workspace, or creates
an administrator.

Use this order:

1. Choose and record the exact immutable image digest.
2. Confirm the database backup and restore point.
3. Run migrations with the migrator database identity.
4. Grant the runtime database privileges.
5. Provision each intended workspace explicitly.
6. Start one control-plane replica with the runtime database identity.
7. Require health and readiness before exposing the proxy route.
8. Create the first platform administrator through the trusted one-off
   bootstrap command.
9. When using Entra, link the administrator's immutable tenant and object IDs.
10. Sign in through the real HTTPS origin.
11. Run one harmless smoke operation and inspect its audit record.

From a trusted source checkout with production database authority:

```console
mix favn.admin.bootstrap --workspace WORKSPACE --username USERNAME
```

The command reads the password without echo. A protected Unix password file is
also supported:

```console
mix favn.admin.bootstrap \
  --workspace WORKSPACE \
  --username USERNAME \
  --password-file /run/secrets/favn_admin_password
```

Do not write the password after the command or place it in an environment
variable.

For a packaged release, use the no-echo entry point:

```console
bin/favn_control_plane eval \
  'IO.inspect(FavnStoragePostgres.Release.admin_bootstrap_from_stdin(["WORKSPACE"], "USERNAME", "Favn Admin"))'
```

Normal application startup never invokes this operation.

## Administrator and incident examples

Disable a compromised actor globally:

```console
mix favn.admin.actor --username USERNAME --status disabled
```

This blocks login and revokes every session across all workspaces while
preserving the memberships for investigation.

Reset an ordinary actor's password:

```console
mix favn.admin.password_reset --username USERNAME
```

This preserves the actor's status and memberships but revokes every session.

Recover an existing administrator:

```console
mix favn.admin.recover --username USERNAME
```

Recovery is a break-glass operation. It works only for an actor that already
has an administrator grant. It reactivates the actor, replaces the password,
and revokes every session.

Run these commands only from a trusted host with production database authority.
They are platform-global operations and are deliberately unavailable in the
workspace administration UI.

## Security smoke test

Before allowing operators to use the deployment, verify:

- [ ] Plain HTTP redirects to the intended HTTPS origin.
- [ ] Direct access to the container port is blocked from the operator network.
- [ ] A client cannot influence forwarded scheme, host, or client IP headers.
- [ ] Login sets a `Secure`, `HttpOnly`, `SameSite=Lax` cookie.
- [ ] A state-changing request without a valid CSRF token is rejected.
- [ ] A workspace administrator cannot view or modify another workspace.
- [ ] The workspace switcher is hidden for a user with one workspace.
- [ ] Disabling a test actor invalidates its existing session.
- [ ] Resetting a test actor's password invalidates its existing session.
- [ ] Logs and audit summaries contain no password, raw token, cookie,
  connection URL, or password hash.
- [ ] A LiveView WebSocket remains connected through the real proxy.
- [ ] Readiness fails when PostgreSQL or required cryptographic configuration
  is unavailable.

Use disposable actors and credentials for this test. Remove the test secrets
and sessions afterward.

## Monitoring and maintenance

At minimum, alert on:

- control-plane readiness failure;
- PostgreSQL connection or TLS failure;
- failed database backups and restore checks;
- database storage and connection pressure;
- proxy TLS certificate expiry;
- repeated login failures;
- runner disconnection or growing outstanding work; and
- failed migrations, drains, or rollbacks.

Review redacted authorization audit records during incidents. Record image
digests, schema versions, configuration changes, and operator actions so a
rollback or investigation does not depend on memory.

Production upgrades are drain-first. Do not run two active control-plane
revisions as a zero-downtime strategy; that would create an unsupported
multi-node topology.

## Current limitations

Understand these before launch:

- Operator SSO supports Microsoft Entra only through Azure Container Apps Easy
  Auth. Native OIDC, other identity providers, and Entra group/role
  authorization are not implemented. This is separate from managed-identity
  authentication to Azure Database for PostgreSQL.
- Only one control-plane BEAM is supported.
- Zero-downtime control-plane replacement is not supported.
- Login throttling and immediate PubSub disconnect are node-local.
- A release command running in a separate VM cannot immediately publish to the
  running node. Durable authorization blocks new reads and mutations
  immediately, while an idle LiveView may remain visible for at most 30
  seconds.
- Secret changes require a controlled restart.
- The content security policy currently allows inline styles, but not inline
  scripts.
- Every cloud deployment requires live qualification in its target environment.

These limits do not prevent a controlled single-node deployment. Public direct
login without MFA, broad proxy trust, plaintext PostgreSQL, exposed internal
ports, or an unsupported multi-replica topology are deployment blockers.

## Detailed runbooks

The repository contains the full versioned contracts:

- [Production documentation index](https://github.com/eirhop/favn/blob/main/docs/production/README.md)
- [Control-plane environment](https://github.com/eirhop/favn/blob/main/docs/production/control_plane_environment.md)
- [Network and reverse-proxy contract](https://github.com/eirhop/favn/blob/main/docs/production/network_and_proxy.md)
- [Operator authorization and security](https://github.com/eirhop/favn/blob/main/docs/production/operator_security.md)
- [PostgreSQL operator runbook](https://github.com/eirhop/favn/blob/main/docs/production/postgresql_operator_runbook.md)
- [Secret rotation](https://github.com/eirhop/favn/blob/main/docs/production/secret_rotation.md)
- [Upgrade and rollback](https://github.com/eirhop/favn/blob/main/docs/production/upgrade_and_rollback.md)
