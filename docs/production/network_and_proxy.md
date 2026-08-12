# Network and reverse-proxy contract

View, Orchestrator, and runners are trusted distributed-BEAM cluster members.
Production requires both mutual-TLS distribution and a high-entropy cookie.
A private address or cookie alone is not sufficient.

View and Orchestrator each have a stable long node name and fixed private
distribution port. View connects to Orchestrator and uses the public facade
through bounded `:erpc`; Phoenix PubSub uses the same cluster connection. Each
runner starts as a hidden dynamic node named
`undefined@<stable-host-alias>`, connects outbound to the control plane, and
does not listen for inbound distribution. Runner host aliases need not be
predeclared in the control plane.

Certificates must chain to an operator-controlled CA. The control-plane
certificate SAN must match the host portion of `FAVN_CONTROL_PLANE_NODE`
because OTP uses it for TLS SNI. Both sides verify peers, and the control-plane
server requires runner client certificates.

Distributed Erlang provides code-execution trust, not a narrow RPC sandbox. A
compromised View can invoke code on Orchestrator. The View's missing database
identity blocks direct database connections but is not containment against that
remote execution authority. Keep View ingress authenticated and allowlisted,
keep both distribution listeners private, require mutual TLS and the cookie,
and treat both roles as one security trust zone.

## Exposure matrix

| Listener | Allowed peers | Public exposure |
| --- | --- | --- |
| View HTTPS origin | Authenticated operators through VPN, reverse proxy, or Container Apps Easy Auth | Trusted ingress only |
| View container HTTP port | Trusted ingress and private health system | Never directly |
| Private orchestrator API | Scalers and trusted operator tooling | Never |
| Control-plane EPMD | View and trusted dynamic runners | Never |
| Orchestrator BEAM distribution port | View and trusted dynamic runners | Never |
| View BEAM distribution port | Orchestrator on the private control network | Never |
| PostgreSQL | Control plane and one-off database operations | Never |
| Runner | No inbound application listener required | Never |

View and runners require outbound access to the control-plane
EPMD/distribution listeners. Runners additionally need access to any explicitly
configured data-plane services. The control plane requires PostgreSQL and API
ingress from the trusted scaler/operator network. Use explicit rules; do not
rely on a container runtime's default bridge policy.

## Reverse proxy

The reverse proxy owns public TLS and must:

- route only the View listener;
- use the exact HTTPS origin configured in `FAVN_VIEW_PUBLIC_ORIGIN`;
- support LiveView WebSocket upgrades and long-lived connections;
- remove client-supplied forwarded headers before adding trusted values;
- appear inside `FAVN_VIEW_TRUSTED_PROXY_CIDRS`;
- implement the behavior selected by `FAVN_VIEW_FORWARDED_FOR_POLICY`;
- apply an equal or shorter deadline than `FAVN_HTTP_REQUEST_TIMEOUT_MS`; and
- expose no route to the private API, BEAM ports, or database.

`FAVN_VIEW_TRUSTED_PROXY_CIDRS` authorizes the immediate socket peer. Prefer an
exact IPv4 `/32` or IPv6 `/128`. Favn accepts a canonical private subnet when a
platform's ingress source address can change, but emits a startup warning
because every peer in that subnet receives the same authority. CIDR trust is
network authorization, not cryptographic proxy authentication.

`FAVN_VIEW_FORWARDED_FOR_POLICY` has three provider-neutral values:

- `replace` is the default. The proxy must remove the incoming chain and write
  exactly one client IP.
- `append` is for an ingress that appends the address it observed. Favn bounds
  one header-line chain and trusts only its rightmost entry. Duplicate header
  lines are rejected as ambiguous.
- `ignore` keeps the immediate proxy address as the remote identity.

Favn consumes only client IP and scheme. It strips standard `Forwarded` plus
`X-Forwarded-For`, `X-Forwarded-Host`, `X-Forwarded-Port`, and
`X-Forwarded-Proto` before routing, ignores forwarded host and port, and uses
`FAVN_VIEW_PUBLIC_ORIGIN` as the fixed redirect authority. Untrusted peers
cannot turn plaintext into HTTPS with these headers.

The immutable container's own `GET /api/web/v1/health/ready` probe is the only
plaintext exception. It is accepted only when the immediate socket peer is the
exact IPv4 or IPv6 loopback address. A `Host: localhost` value from any network
peer grants no exception.

Use `replace` with a fixed proxy or same-replica sidecar:

```text
FAVN_VIEW_TRUSTED_PROXY_CIDRS=10.42.0.5/32
FAVN_VIEW_FORWARDED_FOR_POLICY=replace
```

Use `append` only when target-environment evidence proves both the append
contract and an exclusive ingress subnet:

```text
FAVN_VIEW_TRUSTED_PROXY_CIDRS=10.42.8.0/24
FAVN_VIEW_FORWARDED_FOR_POLICY=append
```

For higher assurance, isolate this hop or terminate mutually authenticated TLS
at a local sidecar. Favn's listener does not itself authenticate a proxy
certificate. Validate the chosen proxy, real WebSocket upgrade, source
addresses, network exclusivity, certificate rotation, runner scale-out, and
runner egress in the target deployment.

The repository container gate does not qualify an operator proxy or managed
network. Validate the chosen proxy, forwarded-header allowlist, real LiveView
WebSocket upgrade, certificate rotation, runner scale-out, and runner egress in
the target deployment.

### Azure Container Apps Easy Auth

For the supported Entra option, Container Apps ingress and Easy Auth are the
trusted authentication proxy. A second reverse proxy is not required solely
for SSO. Easy Auth must require authentication, use the intended single Entra
tenant, and replace the client-principal header before Favn receives the
request.

No network route may bypass Container Apps ingress and reach the Favn listener.
Favn validates the configured tenant and immutable object ID but deliberately
does not validate or store the provider token. Enterprise application
assignment is a useful outer allowlist; Favn actor status, workspace membership,
and roles remain the authorization authority.
