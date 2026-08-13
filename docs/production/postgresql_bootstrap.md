# PostgreSQL bootstrap and upgrade Jobs

Favn owns its PostgreSQL roles, database policy, `favn_control` schema,
migrations, runtime grants, and initial workspace administrator. A deployment owns the
PostgreSQL server, networking, TLS, backups, identities or passwords, temporary
bootstrap authorization, and the Job that runs Favn's command.

The resident control plane receives only the runtime identity. It never creates
roles, repairs grants, provisions a workspace, or migrates at startup.

## The three commands

All commands use the same immutable control-plane image and fixed entrypoint:

```text
/app/bin/favn_control_plane_ops status
/app/bin/favn_control_plane_ops bootstrap
/app/bin/favn_control_plane_ops upgrade
```

`status` is read-only. With the bootstrap profile present it can also report a
missing database or identity mapping; without that profile it inspects an
already-bootstrapped target through the migrator and runtime identities.

`bootstrap` is the first-install and repair command. It:

1. acquires a PostgreSQL advisory lock;
2. creates missing Favn roles and the target database when authorized;
3. maps provider identities when required;
4. removes `CREATEROLE`, `CREATEDB`, inheritance, memberships, and other unsafe
   role attributes;
5. creates `favn_control` with the migrator as its only owner;
6. runs pending migrations and exact runtime grants;
7. atomically creates the initial workspace, actor, workspace membership,
   platform grant, and selected Entra link or password credential; and
8. opens a new runtime connection and verifies the completed result.

Every stage first inspects durable PostgreSQL/provider state. Running
`bootstrap` again after success therefore verifies or skips exact state instead
of creating a second database, role, workspace, or administrator.

`upgrade` is the normal later-deployment command. It accepts only migrator and
runtime profiles, refuses bootstrap authority, applies pending migrations and
grants, and verifies through a new runtime connection. It does not create roles,
databases, schemas, or workspaces.

## Identity profiles

The profiles are deliberately separate even though one process uses them in
sequence:

| Profile | Temporary or persistent | Authority |
| --- | --- | --- |
| Bootstrap | Temporary | Create/map/harden roles and create/harden the Favn database |
| Migrator | Persistent one-off Job identity | Own only `favn_control`, migrate, and grant Favn objects; never `CREATEROLE` |
| Runtime | Persistent control-plane identity | Connect and use Favn tables; never own or create objects |

Read-only status starts and stops one connection profile at a time. Write-enabled
bootstrap keeps its maintenance connection and advisory lock alive while it opens
an isolated target-database lifecycle for the same bootstrap identity. Every
lifecycle has distinct, compile-time process names. Credentials are read from
environment variables, never command arguments, and are omitted from Favn JSON,
application logs, telemetry, and inspected configuration.

### Password authentication

```text
FAVN_DEPLOYMENT_MODE=production
FAVN_DATABASE_SSL_MODE=verify-full
FAVN_DATABASE_SSL_CA_FILE=/run/secrets/postgresql-ca.pem

FAVN_DATABASE_BOOTSTRAP_AUTH_MODE=password
FAVN_DATABASE_BOOTSTRAP_URL=ecto://favn_bootstrap:<secret>@<host>/postgres

FAVN_DATABASE_MIGRATOR_AUTH_MODE=password
FAVN_DATABASE_MIGRATOR_URL=ecto://favn_migrator:<secret>@<host>/favn

FAVN_DATABASE_RUNTIME_AUTH_MODE=password
FAVN_DATABASE_RUNTIME_URL=ecto://favn_runtime:<secret>@<host>/favn

FAVN_WORKSPACE_PROVISIONING_CONFIG_FILE=/run/config/workspace-bootstrap.json
FAVN_OPERATOR_COMMAND_HMAC_SECRET=<deployment-stable-secret-at-least-32-bytes>
```

`FAVN_DATABASE_URL` may replace `FAVN_DATABASE_RUNTIME_URL`, allowing the Job and
resident control plane to reference the same runtime secret. Existing roles are
never given a new password implicitly; a wrong credential fails authentication
instead of silently rotating it.

For a missing password role, Favn derives a salted PostgreSQL SCRAM-SHA-256
verifier in memory. Only that verifier reaches PostgreSQL role-creation SQL;
the plaintext password never becomes SQL text or a SQL bind parameter. For an
existing role, Favn first proves the configured credential and exact
`current_user` through a fresh connection. A wrong credential fails before any
hardening, and Favn never rotates an existing password implicitly.

Credential proof prefers the target database. It does not require migrator or
runtime access to the `postgres` maintenance database. When target `CONNECT`
has drifted, Favn may use maintenance access as a fallback; PostgreSQL's
post-authentication database-access rejection is also sufficient proof, so the
bootstrap administrator can restore the target ACL without manual SQL.

Favn reads the effective PostgreSQL `scram_iterations` setting and uses that
iteration count when deriving a new verifier. Password-profile values must use
non-space printable ASCII (`!` through `~`); URL-reserved characters must be
percent-encoded. This deliberately narrow boundary is unchanged by SASLprep,
so Favn and conforming PostgreSQL clients derive the same verifier.

### Azure managed identity

Assign the bootstrap Job all three user-assigned managed identities. The
deployment must first make the bootstrap identity a temporary Microsoft Entra
administrator for the Azure Database for PostgreSQL server. Favn then maps the
two application identities by exact Entra object ID:

```text
FAVN_DEPLOYMENT_MODE=production
FAVN_DATABASE_SSL_MODE=verify-full
FAVN_DATABASE_HOST=<server>.postgres.database.azure.com
FAVN_DATABASE_PORT=5432
FAVN_DATABASE_NAME=favn
FAVN_DATABASE_MAINTENANCE_NAME=postgres

FAVN_DATABASE_BOOTSTRAP_AUTH_MODE=azure_managed_identity
FAVN_DATABASE_BOOTSTRAP_USERNAME=<bootstrap-admin-role>
FAVN_DATABASE_BOOTSTRAP_AZURE_MANAGED_IDENTITY_CLIENT_ID=<bootstrap-client-id>

FAVN_DATABASE_MIGRATOR_AUTH_MODE=azure_managed_identity
FAVN_DATABASE_MIGRATOR_USERNAME=favn_migrator
FAVN_DATABASE_MIGRATOR_AZURE_MANAGED_IDENTITY_CLIENT_ID=<migrator-client-id>
FAVN_DATABASE_MIGRATOR_AZURE_OBJECT_ID=<migrator-object-id>

FAVN_DATABASE_RUNTIME_AUTH_MODE=azure_managed_identity
FAVN_DATABASE_RUNTIME_USERNAME=favn_runtime
FAVN_DATABASE_RUNTIME_AZURE_MANAGED_IDENTITY_CLIENT_ID=<runtime-client-id>
FAVN_DATABASE_RUNTIME_AZURE_OBJECT_ID=<runtime-object-id>
```

Client IDs select which assigned identity obtains a token. Object IDs are the
stable principals that Azure PostgreSQL maps to roles. Favn uses Azure's
`pgaadauth_create_principal_with_oid` provider adapter, then applies the same
PostgreSQL role policy as password deployments. An exact application mapping is
a non-admin `service` principal with the configured object ID and MFA disabled.
The common workflow is therefore PostgreSQL-specific but not Azure-Job-specific;
Kubernetes Jobs, Docker, CI, and other PostgreSQL providers use the same
commands.

The built-in Entra token provider currently targets Azure public cloud with the
`https://ossrdbms-aad.database.windows.net` resource. Sovereign Azure clouds use
a different resource value and are not supported by this workflow yet.

Favn never claims an existing plain PostgreSQL role for Entra authentication.
An existing role must already have the exact object-ID mapping; otherwise the
Job reports a conflict without changing attributes, passwords, or labels. This
prevents a role-name collision from taking over deployment-owned authority.

Password-role creation keeps plaintext out of PostgreSQL, but its salted SCRAM
verifier can appear in privileged PostgreSQL statement, audit, or error logs.
Treat those logs like other database password-hash stores: restrict access and
retention. The verifier is not sufficient for normal client authentication, but
it is sensitive because it permits offline password guessing.

For an existing password-role hardening transition, the deployment temporarily
grants the bootstrap administrator `ADMIN OPTION` on the configured migrator
and runtime roles. Every membership reported by the read-only
`role_memberships` finding must also be revocable by the bootstrap identity.
On PostgreSQL 18, `ADMIN OPTION` on the parent alone does not necessarily permit
revoking a membership recorded under another grantor. The deployment must make
the bootstrap identity the grantor for that transition membership or use
provider administrator authority that can revoke it. Otherwise Favn fails with
`role_membership_hardening_not_authorized`. The authorization belongs to the
disposable bootstrap identity and disappears when the deployment removes it.
New password roles do not need these transition grants: Favn creates them with
their final attributes.
The migrator creates `favn_control` as itself through a temporary database
`CREATE` grant that Favn revokes and verifies inside the locked workflow; the
bootstrap identity does not need permanent `SET ROLE` membership.

## Job lifecycle

A first deployment needs one no-ingress, run-to-completion bootstrap Job:

```text
image:      ghcr.io/eirhop/favn-control-plane@sha256:<digest>
entrypoint: /app/bin/favn_control_plane_ops
args:       ["bootstrap"]
restart:    never
timeout:    deployment-defined and bounded
identity:   bootstrap + migrator + runtime identities
```

Each identity used by the command must be available to the Job's main
container. If the Container Apps Job sets `identitySettings`, use lifecycle
`Main` or `All` for these identities; `Init` and `None` make the token endpoint
unavailable to the Favn process. See Microsoft's
[managed-identity availability contract](https://learn.microsoft.com/azure/container-apps/managed-identity#control-managed-identity-availability).

The release `eval` entrypoint disables distributed Erlang, so this Job needs no
BEAM node name, cookie, EPMD port, or distribution port. It also needs no HTTP
ingress or Easy Auth.

## Initial workspace administrator

`bootstrap` and the standalone `provision-workspace` operation use the same
provider-neutral transaction. The JSON file contains an explicit operation ID
and exactly one tagged administrator mode. For Entra:

```json
{
  "contract_version": 1,
  "operation_id": "install-salmon-one-v1",
  "workspace": {
    "id": "salmon-one",
    "slug": "salmon-one",
    "display_name": "Salmon One"
  },
  "administrator": {
    "mode": "entra",
    "username": "salmon-admin",
    "display_name": "Salmon administrator",
    "tenant_id": "11111111-1111-4111-8111-111111111111",
    "object_id": "22222222-2222-4222-8222-222222222222"
  }
}
```

For a local administrator, set `mode` to `password`, omit the Entra IDs, and
supply the username and display name. Password material never belongs in JSON,
an argument, or an environment variable. Provide it through protected stdin or
a private regular file:

```text
/app/bin/favn_control_plane_ops provision-workspace \
  --config /run/config/workspace-bootstrap.json \
  --password-file /run/secrets/initial-admin-password
```

The file must be readable only by its owner on Unix. `bootstrap` uses the same
`FAVN_WORKSPACE_ADMIN_PASSWORD_FILE` input when password mode is selected.
Windows callers must use protected stdin because portable ACL verification is
unavailable.

`FAVN_OPERATOR_COMMAND_HMAC_SECRET` binds operation identity to the complete
request, including protected password input, without persisting the password.
It must remain stable for exact retries. A repeated operation with identical
input returns the existing redacted ready result; changed input returns a
bounded conflict without changing authorization state.

If the client loses the result after commit, reconcile without retrying a
possibly completed write:

```text
/app/bin/favn_control_plane_ops workspace-status --workspace salmon-one
```

The readiness result includes the workspace, actor, username, authentication
mode, and assigned roles. It never contains password material, password hashes,
provider claims, tokens, or raw Entra identifiers. No browser visitor is ever
promoted automatically.

The deployment sequence is:

1. create PostgreSQL, networking, identities/secrets, and a recovery point;
2. temporarily authorize the bootstrap identity;
3. create and start the immutable bootstrap Job;
4. require exit `0`, JSON state `ready`, and `runtime_verified: true`;
5. remove the temporary administrator assignment, bootstrap identity attachment,
   and bootstrap Job; and
6. start the resident control plane with only the runtime identity.

For later releases, create or update a no-ingress Job using the candidate image,
`args: ["upgrade"]`, and only migrator plus runtime identities. The deployment
owns when to start and delete these Jobs. Favn does not create cloud resources or
remove administrator assignments.

## Result contract

The command writes one JSON document to stdout. Stderr contains a bounded
operation summary and stage progress at `FAVN_LOG_LEVEL`; release startup
progress never shares stdout with the JSON result.
Important fields are `contract_version`, `operation`, `outcome`, `state`,
`safe_to_retry`, `release.favn_version`, `release.latest_migration_version`,
`duration_ms`, `completed_stages`, and `runtime_verified`.

`completed_stages` is reserved for stages proved complete by post-write reads.
Read-only `status` therefore leaves it empty; its bounded stage logs and
telemetry carry completed-check progress without implying that a write was
verified.

| Exit | Meaning |
| --- | --- |
| `0` | Ready and runtime-verified |
| `2` | Read-only `status` found changes required |
| `64` | Invalid configuration |
| `69` | Server or authentication provider unavailable |
| `70` | Deterministic workflow failure |
| `75` | Another bootstrap/upgrade holds the lock |
| `76` | A write may have completed but verification was lost; inspect `status` before any retry |

Only a verified ready result authorizes deployment cleanup. A failed stage does
not authorize removal of bootstrap access. An unknown outcome must be reconciled
with `status`; automation must not blindly retry it.

An unexpected workflow-worker exit keeps the same exit-code and retry-safety
rules. Its finding and stderr diagnostic identify the operation, active stage,
completed stages, a correlation ID, the failure kind/class, and a safe
application frame. They never include the unrestricted exception message,
database URL, username, token, or provider response. Use the correlation ID to
match the JSON finding to stderr before reconciling with `status`.

The production image wrapper exposes `bootstrap`, `status`, `upgrade`,
`provision-workspace`, and `workspace-status` for normal database/workspace
lifecycle work. Older granular Mix/release functions remain
for local development and transition code, but production mode rejects a
superuser, `CREATEDB`, `CREATEROLE`, inherited, replication, bypass-RLS, or
member role before those functions can write. Production automation uses the
composed commands so sequencing and runtime verification cannot be skipped.
