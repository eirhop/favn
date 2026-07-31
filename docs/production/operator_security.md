# Operator authorization and security

This document is the security contract for the production operator surface.
It covers the functional and security baseline implemented for issue #524 and
the first external-auth adapter from issue #580.
Visual polish remains continuous discovery work. Automated browser and
accessibility acceptance is intentionally split into
[issue #579](https://github.com/eirhop/favn/issues/579).

The supported topology is one production control-plane BEAM behind one trusted
HTTPS ingress. This may be a conventional reverse proxy or Azure Container
Apps ingress with Easy Auth. Multi-node application failover is not claimed.

## Roles and boundaries

Roles are ordered: `admin` includes `operator`, and `operator` includes
`viewer`. Every request and LiveView mount reauthorizes the opaque PostgreSQL
session inside the workspace selected by the encrypted cookie.

| Surface or action | Viewer | Operator | Workspace admin | Platform command |
| --- | --- | --- | --- | --- |
| Sign in, sign out, change own password | Yes | Yes | Yes | No |
| Read assets, pipelines, schedules, runs, logs, rebuilds, and recoveries | Yes | Yes | Yes | No |
| Switch among own active workspace memberships | Yes | Yes | Yes | No |
| Submit run/backfill, cancel/retry run, enable/disable schedule | No | Yes | Yes | No |
| Plan rebuild or target recovery | No | Yes | Yes | No |
| Start/cancel/retry/reconcile rebuild or recovery | No | No | Yes | No |
| `/admin`: actors, memberships, current-workspace sessions, redacted audit | No | No | Yes | No |
| Create an actor or attach an exact existing username to this workspace | No | No | Yes | No |
| Change another actor's current-workspace membership | No | No | Yes | No |
| Revoke another current-workspace session | No | No | Yes | No |
| Bootstrap/recover an administrator | No | No | No | Yes |
| Reset any exact global actor password or enable/disable the actor | No | No | No | Yes |

A workspace administrator cannot enumerate or modify other workspaces. Attaching
an existing username proves only that the exact username exists; it does not
return other memberships. Global actor status and credentials cross workspace
boundaries, so only explicit trusted-host commands can change them.

The UI blocks changing the current administrator's own membership or revoking
the current session through `/admin`. PostgreSQL also prevents removal of the
last active workspace administrator. Logout and self-service password rotation
are the explicit self-service paths.

## Session and workspace behavior

The browser cookie is encrypted, secure in production, and contains only the
selected workspace ID, one opaque session token, and the LiveView socket ID.
The raw token is stored only in that cookie; PostgreSQL stores its hash.

Sessions are bound to one actor, one workspace, an absolute expiry, and the
credential version that issued them. They are revalidated on each HTTP request,
each LiveView mount, and every 30 seconds while a LiveView remains connected.
Every read and mutation facade also reauthorizes durable state before accessing
workspace data.

Switching workspaces is a CSRF-protected POST. It verifies that the actor has an
active target membership, rotates the durable session and cookie, and
disconnects the old LiveView socket. The switcher is hidden when the actor has
only one active workspace.

Actor disable, password change/reset, administrator recovery, membership
suspension/revocation, and explicit session revocation invalidate the affected
durable sessions. In-process PubSub disconnects matching LiveViews immediately.
A separate release-command VM cannot publish into the running node, so a
passive connected view may remain visible until its next durable check, bounded
to 30 seconds. Any intervening read refresh or mutation is rejected immediately
by durable authorization.

## Password and login controls

Passwords are hashed with Argon2id. Invalid usernames still perform a dummy
Argon2 verification, and the login response does not distinguish an unknown
actor, wrong password, disabled actor, or denied workspace membership.

Login attempts are throttled by normalized username plus remote identity and by
remote identity alone. The default is five failures followed by a 60-second
backoff. These counters are deliberately node-local and disposable. That is
correct for the supported one-control-plane topology; a future load-balanced
multi-node topology must add a shared or edge limiter before it is supported.

Azure Container Apps deployments may use Microsoft Entra through Easy Auth.
Easy Auth authenticates the request and owns token validation, MFA, Conditional
Access, provider session lifetime, signing-key rotation, replay, and clock
skew. Favn accepts only the immutable `tid` and `oid` claims from the
platform-injected principal, resolves a pre-created provider link, then applies
its own durable actor status, workspace membership, roles, and sessions.

Email, display name, provider groups, and provider roles cannot grant Favn
authorization. The adapter is valid only when the Favn listener cannot be
reached without passing through Container Apps ingress. Native OIDC and other
proxy assertion formats are not supported. The complete setup and break-glass
contract is in the HexDocs
[Operator Authentication](../../apps/favn/guides/operator-authentication.md)
guide.

## Mutation, audit, and redaction

Browser-originated mutations reserve a durable command intent before execution.
The reservation binds the workspace, actor, session, action, and redacted
request fingerprint to one idempotency key. Replaying the same request returns
the recorded result; changing the request under the same key returns a conflict.
Accepted outcomes and resulting identifiers are durably completed beside the
owning command.

Authorization audit records include actor, session, workspace, action, redacted
detail, outcome, subject, command ID, and timestamp. Platform lifecycle
operations use a separate platform audit table. `/admin` intentionally displays
only bounded redacted summaries.

Phoenix parameter filtering covers password, secret, token, and credential
names. Cookies, password hashes, raw service tokens, connection URLs,
runtime-input values, and encryption keys must never be rendered or logged.

## Trusted platform operations

Normal production startup never creates or changes an administrator. Run these
only from a trusted host with the production database authority:

```console
mix favn.admin.bootstrap --workspace WORKSPACE --username USERNAME
mix favn.admin.recover --username USERNAME
mix favn.admin.password_reset --username USERNAME
mix favn.admin.actor --username USERNAME --status active|disabled
mix favn.admin.entra --username USERNAME --tenant-id UUID --object-id UUID --action link|unlink
```

Bootstrap, administrator recovery, and ordinary actor password reset read the
password without echo. They may use `--password-file` only for a protected Unix
regular file; Windows uses stdin. Passwords are never command-line arguments or
environment variables.

Packaged-release equivalents are:

```console
bin/favn_control_plane eval 'IO.inspect(FavnStoragePostgres.Release.admin_bootstrap_from_stdin(["WORKSPACE"], "USERNAME", "Favn Admin"))'
bin/favn_control_plane eval 'IO.inspect(FavnStoragePostgres.Release.admin_recover_from_stdin("USERNAME"))'
bin/favn_control_plane eval 'IO.inspect(FavnStoragePostgres.Release.admin_password_reset_from_stdin("USERNAME"))'
bin/favn_control_plane eval 'IO.inspect(FavnStoragePostgres.Release.admin_actor_status(%{username: "USERNAME", status: "disabled"}))'
```

Administrator recovery is break-glass: it is restricted to an actor that
already has an administrator grant, reactivates that actor, rotates its
password, and revokes every session. Ordinary actor password reset preserves
the actor's status and all memberships while rotating the password and revoking
every session. Actor disable preserves memberships but blocks login and revokes
every session across every workspace.

## Security qualification

On 2026-07-30 the issue branch passed:

- strict Credo over the changed production sources with no release-blocking
  findings;
- strict Sobelow scans of the View and Orchestrator applications;
- `mix hex.audit` and `mix deps.audit` with no advisories;
- Gitleaks over the working tree, with all 19 matches classified as dependency,
  build-output, or explicit non-production example values;
- the production image content contract;
- Grype `v0.116.0` with no High or Critical vulnerabilities after the
  repository's reviewed exception policy.

Docker Scout could not run because the local Docker client was not signed in.
Grype used the same pinned version and policy as CI instead.

The production image built from commit `0532e7e7` was also exercised manually
against PostgreSQL with verified TLS and an HTTPS Caddy edge. The check covered
the production security headers and cookie flags, CSRF rejection, successful
login and administrator access, the hidden single-workspace switcher, actor
disable, password reset, session invalidation, logout protection, and secret
redaction. No browser automation was used. A normal direct HTTP request was
redirected to HTTPS.

## Known limits

- Automated browser and accessibility release acceptance is deferred to
  [#579](https://github.com/eirhop/favn/issues/579). Elixir tests and a manual
  production-artifact check are the gate for this change.
- The first external identity adapter is intentionally limited to
  single-tenant Microsoft Entra through Azure Container Apps Easy Auth.
- Forwarded scheme and client-address headers are accepted only from an
  authorized immediate peer. Exact peers are preferred; wider private subnets
  produce a startup warning and require exclusive network routing. CIDR trust
  is network authorization, not cryptographic proxy authentication. Use an
  isolated hop, a local sidecar, or authenticated transport for deployments
  where other workloads could enter the trusted network.
- The supported topology is one control-plane BEAM. Login throttling and
  immediate PubSub disconnect are node-local; the durable authorization check
  remains PostgreSQL-backed.
- CSP permits inline styles for the current component stack, but not inline
  scripts. Removing `style-src 'unsafe-inline'` is defense-in-depth rather than
  an executable-script exposure.
- Visual polish is deliberately outside this contract. Missing authorization,
  audit, error, or lifecycle behavior is not considered polish and remains a
  release blocker.
