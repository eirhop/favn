# Trusted proxy boundary

Status: approved implementation plan for
[issue #581](https://github.com/eirhop/favn/issues/581).

## Purpose

Favn terminates public HTTPS at a reverse proxy and receives HTTP from that
proxy on the private View listener. The application therefore needs a safe way
to distinguish:

- transport and client identity supplied by the intended proxy; from
- client-supplied headers sent by an untrusted peer.

This design hardens that distinction without coupling Favn to one proxy product
or hosting platform. Configuration describes the proxy's observable header
behavior. It never selects a provider-specific mode.

The production deployment contract remains canonical in
[`../production/network_and_proxy.md`](../production/network_and_proxy.md).
The public, copyable operator guidance remains canonical in
[`../../apps/favn/guides/secure-production-deployment.md`](../../apps/favn/guides/secure-production-deployment.md).
This document records the security decisions and implementation sequence.

## Current problem

The current implementation authorizes forwarded headers when the immediate
socket peer is inside `FAVN_VIEW_TRUSTED_PROXY_CIDRS`. This is the correct
boundary, but three details make it too easy to configure unsafely:

1. shipped examples use broad private ranges such as `10.0.0.0/8` and
   `172.16.0.0/12`;
2. startup diagnostics report only the number of configured ranges; and
3. `Plug.RewriteOn` always selects the leftmost `X-Forwarded-For` value.

The third behavior is safe only when the last trusted proxy removes the
client's entire incoming header and replaces it with a sanitized value. Some
managed ingress products append their observed sender to an existing chain
instead. In that topology, a client controls the leftmost value.

Favn also currently rewrites host and port from forwarded headers even though
`FAVN_VIEW_PUBLIC_ORIGIN` is already the authoritative public origin.

## Threat model

The design addresses these actors:

- an internet client that sends forged `Forwarded` and `X-Forwarded-*`
  headers;
- a private workload that can reach the View port but is not the proxy;
- a workload inside an overly broad trusted subnet;
- a correctly authorized proxy that replaces incoming forwarded identity;
- a correctly authorized proxy that appends its observed sender;
- a misconfigured proxy that preserves unsafe host, port, scheme, or client
  identity; and
- an operator who writes a non-canonical CIDR whose host bits hide the actual
  trusted network.

The following are outside this change:

- authenticating browser users, service tokens, or runner nodes;
- making a compromised trusted proxy harmless;
- cloud firewall, private-link, service-mesh, or WAF configuration; and
- implementing native proxy-to-View mTLS in the Phoenix listener.

## Security invariants

1. Socket peer authorization and forwarded-chain interpretation are separate
   decisions.
2. No forwarded header is consumed unless the immediate peer is authorized.
3. `Forwarded` is unsupported and removed from every request.
4. `X-Forwarded-Host` and `X-Forwarded-Port` are removed from every request.
   The configured public origin is the authority for external host and port.
5. `X-Forwarded-Proto` changes the scheme only when it contains one exact
   supported value from an authorized peer. Invalid or ambiguous values fail
   closed as plaintext and are redirected.
6. `X-Forwarded-For` never supplies an address selected from client-controlled
   text unless the configured policy explicitly matches the proxy's behavior.
7. Raw forwarded headers are removed after the normalized connection fields
   are computed so downstream code cannot reinterpret them differently.
8. An HTTP redirect always targets `FAVN_VIEW_PUBLIC_ORIGIN`, never a request
   or forwarded host.
9. A CIDR with non-zero host bits is rejected instead of silently widening to
   its containing network.
10. Every trusted subnet containing more than one peer is visible as a startup
    warning and in redacted readiness diagnostics.

## Generic configuration contract

### Immediate proxy authorization

`FAVN_VIEW_TRUSTED_PROXY_CIDRS` remains the private IPv4/IPv6 allowlist for the
immediate socket peer.

Preferred values identify exact peers:

```text
FAVN_VIEW_TRUSTED_PROXY_CIDRS=10.42.0.5/32
```

Multiple exact peers are allowed for highly available proxies:

```text
FAVN_VIEW_TRUSTED_PROXY_CIDRS=10.42.0.5/32,10.42.0.6/32
```

A stable proxy subnet remains supported for managed ingress or private
load-balancer deployments. It produces a warning because every peer in that
subnet receives the same authority. The warning does not prevent startup.

Only canonical networks are valid. For example, `10.42.0.0/24` is canonical,
while `10.42.0.5/24` is rejected because it disguises the same `/24` trust as a
host-looking value.

### Forwarded client policy

`FAVN_VIEW_FORWARDED_FOR_POLICY` describes what the authorized proxy does with
`X-Forwarded-For`.

| Value | Proxy contract | Favn behavior | Use |
| --- | --- | --- | --- |
| `replace` | Remove the incoming header and set one IP literal | Accept exactly one IP; otherwise keep the socket peer | Default; preferred for controlled reverse proxies |
| `append` | Append the sender observed by the proxy | Use only the rightmost IP; ignore all earlier client-controlled values | Managed ingress that documents append behavior |
| `ignore` | No trusted client-IP contract exists | Keep the socket peer and discard the header | Safest fallback when client identity is unnecessary |

The default is `replace`. There is no compatibility alias for the old implicit
leftmost behavior. Favn is private pre-v1 software, and retaining an ambiguous
security default would be more dangerous than requiring an explicit correction.

The `append` policy deliberately does not walk farther left through a chain of
proxies. The rightmost value is the only value the last proxy can add after
untrusted input. If the original browser address is required through several
proxies, the final trusted proxy must validate that chain and replace it with
one normalized address before forwarding to Favn.

The implementation bounds accepted header length and entry count before
splitting. Invalid, oversized, or ambiguous values do not fail the request;
they leave `conn.remote_ip` equal to the authorized proxy peer.

### Scheme, host, and port

An authorized proxy must remove any client-supplied scheme header and set one
`X-Forwarded-Proto: https` value after terminating TLS. Both `http` and `https`
are recognized so plaintext proxy paths still redirect honestly.

Favn does not consume forwarded host or port. The external authority is the
validated absolute HTTPS value in `FAVN_VIEW_PUBLIC_ORIGIN`. This also supplies
the fixed host used by `Plug.SSL` redirects.

## Deployment patterns

### Fixed reverse proxy

Give each proxy an exact stable private address, trust its `/32` or `/128`, and
use `replace`.

```text
FAVN_VIEW_TRUSTED_PROXY_CIDRS=10.42.0.5/32
FAVN_VIEW_FORWARDED_FOR_POLICY=replace
```

The proxy must:

1. remove `Forwarded` and every `X-Forwarded-*` header from the client;
2. set one normalized `X-Forwarded-For` address;
3. set one `X-Forwarded-Proto` value;
4. route only the View listener; and
5. preserve WebSocket upgrades.

### Managed ingress

Use the smallest stable private source subnet actually used by the ingress
fleet and select the policy documented by that ingress.

For an ingress that overwrites scheme but appends its observed sender:

```text
FAVN_VIEW_TRUSTED_PROXY_CIDRS=10.42.8.0/24
FAVN_VIEW_FORWARDED_FOR_POLICY=append
```

This is supported only when the platform prevents direct access to the target
port outside its ingress path. The subnet warning is expected and records that
network authorization is broader than one peer. The exact source subnet and
header behavior must be verified in the target environment; they must not be
guessed from a generic private address range.

### Same-replica proxy

A trusted sidecar proxy may connect to View over loopback:

```text
FAVN_VIEW_TRUSTED_PROXY_CIDRS=127.0.0.1/32
FAVN_VIEW_FORWARDED_FOR_POLICY=replace
```

This avoids depending on a managed ingress fleet's internal source addresses.
Only trusted containers may share the replica or network namespace.

## Diagnostics and failure behavior

Validation returns the existing redacted invalid-environment shape for:

- an unknown forwarded-client policy;
- public proxy networks;
- malformed addresses or prefixes;
- non-canonical networks;
- empty lists; and
- existing count and byte limits.

Successful diagnostics add:

- the forwarded-client policy;
- exact-peer and subnet counts;
- a warning count; and
- bounded warning details containing the address family and prefix, but no
  secrets.

Startup emits one actionable warning per trusted subnet. Readiness remains
ready because broad private subnets are legitimate in some managed deployments.
The warning states that CIDR trust is network authorization, not cryptographic
proxy authentication.

## Executable acceptance topology

The Docker qualification gains a real HTTPS reverse proxy on a dedicated
internal hop. The proxy receives a fixed address and View trusts only that
address. A one-shot attacker container receives a different address on the
same hop so it can exercise the application defense even if deployment
firewalls are wrong.

The executable assertions are:

| ID | Actor and request | Required result |
| --- | --- | --- |
| `TP-001` | Real proxy sends sanitized HTTPS request | View serves the health route without an HTTPS redirect loop |
| `TP-002` | Client sends forged headers through the real proxy | Proxy overwrites/removes them and View still uses the configured origin |
| `TP-003` | Non-proxy peer sends forged `Forwarded` and `X-Forwarded-*` directly | View redirects to the configured HTTPS origin |
| `TP-004` | Non-proxy peer supplies an attacker host | Redirect location never contains that host |
| `TP-005` | Append-policy request includes a forged leftmost address | Favn selects only the rightmost address |
| `TP-006` | Replace-policy request contains multiple addresses | Favn keeps the socket peer |

The Docker script writes a small machine-readable result with the assertion
IDs and returns non-zero on failure. It targets only the explicitly named local
Compose project. This is a focused precursor to the broader security
qualification in issue #578, not a replacement for it.

## mTLS decision

CIDR trust proves network placement, not proxy identity. Higher-assurance
deployments should prefer one of:

- a same-replica loopback hop;
- platform or service-mesh authenticated encryption;
- private-link origin isolation; or
- proxy-to-View mTLS when the deployment can own certificate issuance,
  rotation, revocation, and failure recovery.

Native proxy-to-View mTLS is not added in this issue. The current View listener
is HTTP behind a terminating proxy; adding application mTLS would expand the
configuration, certificate lifecycle, health probes, local development, and
deployment templates substantially. A shared secret header is not an
acceptable substitute because it is replayable and provides no protected
channel by itself.

## Implementation sequence

1. Add this design and implementation plan as a documentation-only commit.
2. Replace `Plug.RewriteOn` with bounded deterministic normalization in
   `FavnView.Plugs.TrustedProxyHeaders`.
3. Add generic forwarded-client policy validation and immutable runtime config.
4. Reject non-canonical CIDRs and add subnet diagnostics/startup warnings.
5. Bind HTTPS redirects to the configured public origin.
6. Update focused View tests for trusted, untrusted, replace, append, ignore,
   malformed, oversized, and host-spoof cases.
7. Replace broad shipped example CIDRs and pass the generic policy through the
   portable and Azure deployment references.
8. Add the fixed-proxy/attacker Docker topology and executable assertions.
9. Update the public HexDocs security guide with simple copyable examples,
   decision guidance, and “when not to use it” warnings.
10. Update the canonical production proxy/environment references without
    duplicating the public tutorial.
11. Run focused format, compile, View tests, deployment conformance, Docker
    acceptance, docs/link checks, and repository security checks proportional
    to the changed surface.
12. Request an independent development review with explicit focus on spoofing,
    fail-open behavior, header ambiguity, trust expansion, diagnostics leakage,
    and deployment portability.

## Review checklist

- Can any untrusted peer cause `conn.scheme` to become `:https`?
- Can a client-controlled leftmost address become `conn.remote_ip`?
- Can multiple header lines or comma-separated values bypass the selected
  policy?
- Can an attacker influence an HTTPS redirect host or port?
- Are unsupported forwarding headers removed before routing?
- Can a CIDR spelling silently authorize a larger network than it appears to?
- Do diagnostics reveal secrets or silently normalize risky trust?
- Does the managed-ingress example require only generic behavioral guarantees?
- Does the Docker negative case prove application behavior, not only firewall
  behavior?
- Is every documented proxy configuration copyable and consistent with the
  executable contract?
