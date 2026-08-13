# Operator Authentication

Favn supports two explicit operator authentication modes:

- `password` — Favn verifies a local password and creates a Favn session.
- `azure_container_apps_entra` — Azure Container Apps Easy Auth verifies the
  Microsoft Entra sign-in; Favn maps the immutable Entra identity to an
  existing Favn actor and creates the same kind of Favn session.

For a small internal Azure Container Apps deployment, use the Entra option.
It gives you company SSO and MFA policy without implementing OpenID Connect
token validation inside Favn.

## What happens during sign-in?

```text
Browser -> Container Apps ingress + Easy Auth -> Favn identity link
                                                -> Favn workspace/roles
                                                -> Favn session cookie
```

1. The browser reaches the Container App.
2. Easy Auth redirects the user to Microsoft Entra when necessary.
3. Entra authenticates the user and applies your tenant's MFA and Conditional
   Access policies.
4. Easy Auth validates the provider token before forwarding the request.
5. Easy Auth replaces the `x-ms-client-principal` header with the validated
   principal.
6. Favn accepts only the immutable tenant ID (`tid`) and object ID (`oid`).
7. PostgreSQL must already contain an exact link from that pair to a Favn
   actor.
8. Favn checks the actor status, workspace membership, and Favn roles, then
   issues a revocable, workspace-bound Favn session.

Entra proves who the person is. Favn still decides what the person may do.
Entra groups, app roles, email, display name, and username cannot grant Favn
authorization.

## Do I need another reverse proxy?

Not for authentication. Container Apps ingress and Easy Auth are the managed
edge and authentication proxy for this option. They terminate HTTPS, handle
the Entra redirect, validate the provider token, and pass an authenticated
principal to Favn.

Do not add Nginx or another proxy merely for SSO. Add another gateway only when
you need a separate feature such as a company-wide WAF, private connectivity,
or centralized routing.

The important security rule is that nobody can bypass Container Apps ingress
and reach Favn's HTTP listener directly. Favn deliberately does not validate
the Entra token itself; it trusts the principal inserted by Easy Auth.

## Azure configuration

Configure Container Apps authentication with:

- Microsoft Entra as the identity provider;
- the app registration in the same tenant as the deployment;
- unauthenticated browser requests set to **redirect to Microsoft Entra**
  (`RedirectToLoginPage`); use `/.auth/login/aad` explicitly instead if you
  intentionally choose `Return401` for an API-oriented deployment;
- token store according to your organizational policy;
- the enterprise application set to require assignment; and
- only the intended internal users or a small assigned group (for example,
  two to four operators) assigned to the application.

Requiring assignment is the outer allowlist. It does not replace Favn actor
status, workspace membership, or roles.

Use the official Azure references for the exact platform controls:

- [Container Apps authentication and authorization](https://learn.microsoft.com/azure/container-apps/authentication)
- [Access user identity claims](https://learn.microsoft.com/azure/app-service/configure-authentication-user-identities)
- [Microsoft Entra application assignment](https://learn.microsoft.com/entra/identity/enterprise-apps/what-is-application-management)

## Favn configuration

Set these variables on the control-plane Container App:

```text
FAVN_VIEW_AUTH_MODE=azure_container_apps_entra
FAVN_VIEW_ENTRA_TENANT_ID=11111111-1111-4111-8111-111111111111
FAVN_VIEW_ENTRA_WORKSPACE_ID=your-workspace-id
```

`FAVN_VIEW_ENTRA_TENANT_ID` is the Entra directory/tenant ID.
`FAVN_VIEW_ENTRA_WORKSPACE_ID` is the Favn workspace selected for initial
sign-in. The values are validated at startup and redacted from runtime
diagnostics.

The deployment still needs the normal production web settings, including
`FAVN_VIEW_PUBLIC_ORIGIN`, secure cookie configuration, and the exact private
Container Apps proxy CIDRs in `FAVN_VIEW_TRUSTED_PROXY_CIDRS`.

## Provision the initial operator

Provision the workspace and its first administrator in one supported operation.
Put the immutable Entra tenant and object IDs in the tagged configuration and
run:

```console
/app/bin/favn_control_plane_ops provision-workspace \
  --config /run/config/workspace-bootstrap.json
```

Use the Entra object ID, not an email address, display name, group, or provider
role. The transaction creates the workspace, actor, membership, platform grant,
and provider link together. See the
[PostgreSQL bootstrap guide](../../../docs/production/postgresql_bootstrap.md#initial-workspace-administrator)
for the complete Entra/password configuration, retry, and reconciliation
contract. Later link changes remain explicit audited administrator operations.

## Denial and logout

Favn returns a generic `403 Access denied` when the Easy Auth principal is
missing, malformed, in the wrong tenant, unlinked, disabled, or not an active
member of the configured workspace. The response does not reveal whether an
actor or link exists.

Signing out revokes the Favn session, disconnects its LiveViews, clears the
encrypted browser cookie, and redirects to `/.auth/logout` so Easy Auth also
clears the Azure session.

Disabling a Favn actor or suspending its workspace membership continues to
block access even while the Entra account remains valid. Existing Favn
sessions are revalidated against PostgreSQL.

## Break-glass access

Keep a tested local administrator password, but do not expose the password
form alongside Entra mode.

For break-glass recovery:

1. restrict ingress to the trusted administrator network;
2. change Container Apps authentication so the recovery request may reach
   Favn;
3. set `FAVN_VIEW_AUTH_MODE=password`;
4. replace or restart the View role without starting an overlapping View node;
5. use the local administrator credential;
6. restore Entra Easy Auth and `azure_container_apps_entra` mode; and
7. verify sign-in and audit records.

This procedure depends on Azure control-plane access. If local access must
survive a wider Azure or Entra outage, provide a separately protected private
recovery route; that is an infrastructure responsibility, not an automatic
second login path in Favn.

## Security boundaries and limitations

Easy Auth owns token signature validation, issuer/audience validation, replay
handling, clock skew, signing-key rotation, Entra session lifetime, MFA, and
Conditional Access. Favn does not store or log provider tokens.

Favn owns the durable provider-to-actor link, actor status, workspace
membership, Favn roles, session revocation, workspace switching, and audit.

Do not use this option:

- outside Azure Container Apps Easy Auth;
- when a network path can bypass the managed ingress;
- for multiple Entra tenants;
- when you need Entra groups or application roles to administer Favn roles; or
- as a generic OIDC integration.

Native OIDC and other reverse-proxy assertion formats are not implemented by
this first adapter.
