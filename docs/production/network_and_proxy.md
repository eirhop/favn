# Network and reverse-proxy contract

The v1 topology is supported only in a dedicated trusted network segment. The
control plane and customer runner are both trusted cluster members.

Plain distributed Erlang is not encrypted. The shared cookie authenticates a
node to the cluster but is not a least-privilege or cryptographic transport
boundary; a connected node has broad BEAM-level trust. A private address alone
does not satisfy this contract. Use firewall isolation and, where the underlying
network is not already encrypted and isolated, a trusted encrypted private
overlay.

Encrypted mutually authenticated runner transport, least-privilege runner RPC,
secret providers, and automatic rotation are deferred to
[#530](https://github.com/eirhop/favn/issues/530).

## Exposure matrix

| Listener | Allowed peers | Public exposure |
| --- | --- | --- |
| View HTTPS origin | Authenticated operators through VPN or reverse proxy | Reverse proxy only |
| View container HTTP port | Reverse proxy/health system on the private network | No direct public route |
| Private Orchestrator API | Trusted operator tooling and local control-plane workflows | Never |
| EPMD | Control plane and runner only | Never |
| Fixed BEAM distribution port | Control plane and runner only | Never |
| PostgreSQL | Control plane and one-off database operations only | Never |
| Runner | Control plane through private BEAM ports only | Never |

Use explicit inbound and outbound rules. Do not rely on a container runtime's
default bridge policy. The node names in `FAVN_CONTROL_PLANE_NODE` and
`FAVN_RUNNER_NODE` must resolve over private DNS, and both nodes must use the
same private `ERL_EPMD_PORT`, `FAVN_BEAM_DISTRIBUTION_PORT`, and strong
`FAVN_DISTRIBUTION_COOKIE` values.

## Reverse proxy

The reverse proxy owns public TLS and must:

- route only the View listener;
- use the exact HTTPS origin configured in `FAVN_VIEW_PUBLIC_ORIGIN`;
- support LiveView WebSocket upgrades and long-lived connections;
- remove client-supplied forwarded headers before adding its own `Forwarded` or
  `X-Forwarded-*` values;
- appear inside `FAVN_VIEW_TRUSTED_PROXY_CIDRS`;
- apply an equal or shorter total request deadline than
  `FAVN_HTTP_REQUEST_TIMEOUT_MS`;
- enforce request-size and connection limits compatible with Favn's stricter
  application limits; and
- have no route to the private API, runner, BEAM ports, or database.

Favn accepts forwarded scheme, host, port, and client IP only when the immediate
peer is in the configured private proxy allowlist. It strips those headers from
untrusted peers. Session cookies use the configured HTTPS origin and secure
browser settings. Browser responses include a restrictive Content Security
Policy; scripts must come from the View origin, while LiveView connections may
use same-origin WebSocket or long-poll transports.

The container acceptance suite starts the pinned Nginx reference proxy on the
private Compose network and requires an HTTP `101` LiveView WebSocket upgrade
through it. This proves the documented proxy headers and upgrade behavior; it
does not publish or manage a proxy image for operators.

For direct operator access without a reverse proxy, use a VPN that enters the
private network and still terminate browser TLS at a trusted endpoint. Do not
make the BEAM or database listeners reachable through the VPN merely because
the View is reachable.

## Operator verification

Before routing traffic:

1. scan the public address from outside the private network and confirm only the
   intended TLS proxy is reachable;
2. scan from each private role and confirm the exposure matrix above;
3. send forged forwarded headers from an untrusted peer and confirm they are
   ignored;
4. verify the configured public origin, secure session cookie, WebSocket path,
   request limits, and readiness route through the proxy; and
5. capture deployment evidence without recording cookies, service tokens,
   database URLs, or the distribution cookie.
