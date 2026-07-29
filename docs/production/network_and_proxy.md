# Network and reverse-proxy contract

The control plane and runners are trusted distributed-BEAM cluster members.
Production requires both mutual-TLS distribution and a high-entropy cookie.
A private address or cookie alone is not sufficient.

The control plane has one stable long node name and one fixed private
distribution port. Each runner starts as a hidden dynamic node named
`undefined@<stable-host-alias>`, connects outbound to the control plane, and
does not listen for inbound distribution. Runner host aliases need not be
predeclared in the control plane.

Certificates must chain to an operator-controlled CA. The control-plane
certificate SAN must match the host portion of `FAVN_CONTROL_PLANE_NODE`
because OTP uses it for TLS SNI. Both sides verify peers, and the control-plane
server requires runner client certificates.

## Exposure matrix

| Listener | Allowed peers | Public exposure |
| --- | --- | --- |
| View HTTPS origin | Authenticated operators through VPN or reverse proxy | Reverse proxy only |
| View container HTTP port | Reverse proxy and private health system | Never directly |
| Private orchestrator API | Scalers and trusted operator tooling | Never |
| Control-plane EPMD | Trusted runner network only | Never |
| Control-plane BEAM distribution port | Trusted dynamic runners only | Never |
| PostgreSQL | Control plane and one-off database operations | Never |
| Runner | No inbound application listener required | Never |

Runners require outbound access to the control-plane EPMD/distribution
listeners and any explicitly configured data-plane services. The control plane
requires PostgreSQL and API ingress from the trusted scaler/operator network.
Use explicit rules; do not rely on a container runtime's default bridge policy.

## Reverse proxy

The reverse proxy owns public TLS and must:

- route only the View listener;
- use the exact HTTPS origin configured in `FAVN_VIEW_PUBLIC_ORIGIN`;
- support LiveView WebSocket upgrades and long-lived connections;
- remove client-supplied forwarded headers before adding trusted values;
- appear inside `FAVN_VIEW_TRUSTED_PROXY_CIDRS`;
- apply an equal or shorter deadline than `FAVN_HTTP_REQUEST_TIMEOUT_MS`; and
- expose no route to the private API, BEAM ports, or database.

Favn accepts forwarded scheme, host, port, and client IP only from the
configured private proxy allowlist. Browser responses use secure cookies and a
restrictive content security policy.

The repository container gate does not qualify an operator proxy or managed
network. Validate the chosen proxy, forwarded-header allowlist, real LiveView
WebSocket upgrade, certificate rotation, runner scale-out, and runner egress in
the target deployment.
