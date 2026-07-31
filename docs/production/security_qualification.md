# HTTP boundary security qualification

This is the HTTP-boundary phase of Favn's attacker-oriented production security
qualification. It supplements unit, acceptance, dependency, and image scanning
checks; it does not claim that unknown vulnerabilities cannot exist.

It is not yet the complete issue #578 release verdict. Runner mutual-TLS and
distribution attacks, the full role/cross-workspace matrix, private-API
resource-pressure cases, PostgreSQL privilege/pressure attacks, and exact-image
OCI/SBOM/secret inspection remain required later phases. A passing
`http_boundary_v1` verdict approves only the scope described below.

## Security claim

A passing local `http_boundary_v1` verdict means:

- every production browser and private API route present in source is in the
  versioned security surface catalogue;
- anonymous, forged-token, and wrong-scope requests fail closed at every
  applicable API route;
- every protected browser route redirects anonymous users to sign-in and is
  reachable by an authorized administrator without a server error;
- serious and critical automated WCAG 2.2 findings are absent on every
  authenticated browser route;
- login CSRF protection, secure session-cookie attributes, logout revocation,
  trusted-proxy header replacement, canonical LiveView WebSocket acceptance,
  hostile WebSocket Origin rejection, and HTTPS security headers work in a real
  browser and proxy;
- rejected or malformed probes do not change canonical fingerprints of the
  sampled durable business rows;
- the browser attacker, private-API client, proxy, control plane, runner, and
  PostgreSQL occupy explicit isolated networks; and
- the browser and private-API attacker probes are read-only, capability-free
  containers without the Docker socket.

The automated accessibility scan cannot replace keyboard, screen-reader, or
other manual accessibility testing. A live Azure Container Apps deployment is
also a separate external gate: managed Entra authentication, platform ingress,
certificate ownership, WAF/rate-limiting policy, and Azure-generated headers
must be qualified in the target subscription.

## Threat model

| Boundary | Attacker capability | Required invariant |
| --- | --- | --- |
| Public edge to reverse proxy | Send arbitrary methods, paths, bodies, cookies, and forwarding headers | Only the proxy is published; spoofed forwarding headers are replaced |
| Browser to View | Steal/replay a stale cookie, omit CSRF, navigate directly to protected routes | Secure cookie, CSRF, authorization, and revocation fail closed |
| Operator client to private API | Omit, forge, or use a valid token with the wrong scope | Authentication and scope checks precede domain authority |
| Workspace to workspace | Supply foreign identifiers and actor context | Persistence authorization remains workspace-scoped |
| Runner to control plane | Reach control-plane distribution only | No database, public-edge, or operator-API reachability |
| Control plane to PostgreSQL | Use the runtime database role | TLS and runtime grants remain authoritative |
| Browser/API probes to host | Abuse capabilities, writable roots, or the Docker socket | No socket on these attacker probes; read-only roots and no capabilities |
| Build to deployment | Replace a tested image or dependency | Pinned test images, immutable release digests, SBOM and vulnerability scan |

The test catalogue uses stable `WEB-*`, `API-*`, `TOPO-*`, `STATE-*`,
`HARDEN-*`, and existing `TP-*` assertion identifiers. The catalogue drift
check fails whenever a production route is added, removed, or changed without
an explicit security classification.

## Run the gate

From a clean checkout on a Linux Docker host:

```sh
sh deployment/docker-compose/run-security-qualification.sh
```

The command builds the actual release image, creates disposable credentials and
PostgreSQL data, applies migrations and runtime grants, provisions a workspace,
bootstraps a disposable administrator from a mounted secret file, then runs the
proxy, API, and Playwright/axe probes. It removes containers, networks, and
volumes on exit unless `FAVN_SECURITY_KEEP_STACK=1` is set for diagnosis.

Redacted JSONL assertion evidence and the final verdict are written under
`deployment/docker-compose/security-results/`. That directory and all generated
credentials are ignored. Never publish `.env.local`, `security/secrets/`,
traces, screenshots, raw headers, or container inspection output.

Set `FAVN_SECURITY_BROWSER_DIAGNOSTICS=1` only for a local diagnostic rerun when
screenshots or Playwright traces are needed. Those diagnostics can contain
operator data and are deliberately excluded from CI artifacts.

## Verdict rules

The gate passes only when every expected assertion appears exactly once and
passes. A duplicate assertion, tool crash, missing route, unreachable
dependency, incomplete evidence file, or unknown test outcome is a failure, not
a skipped check. A reusable verdict requires a clean checkout whose `HEAD`
matches the recorded source revision. Dirty runs require
`FAVN_SECURITY_ALLOW_DIRTY=1` and produce only `diagnostic_pass`.

Before writing a verdict, the harness scans the retained result tree for every
generated password, token, cookie, and key from the disposable Compose
environment. CI uploads only successful, allow-listed JSONL evidence and the
verdict.

For final issue #578 release approval, combine this phase verdict with the
remaining local phase verdicts, the image digest and scan/SBOM attestations from
the image workflow, the manual accessibility record, and the
environment-specific Azure qualification. Do not reuse a verdict for another
revision or deployment topology.
