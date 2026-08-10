# PostgreSQL bootstrap implementation plan

Status: temporary normative implementation plan for
[issue #611](https://github.com/eirhop/favn/issues/611).

Reader: contributors implementing or reviewing the PostgreSQL bootstrap,
status, and upgrade workflow.

This document records the agreed design, implementation slices, tests, and
rollout checks. If implementation proves that a locked decision is unsafe or
not supported by PostgreSQL or Azure Database for PostgreSQL, update this plan
and issue #611 before implementing a different contract.

This is not permanent operator documentation. Before issue #611 closes, move
the implemented behavior into the canonical production, storage, and structure
pages listed in [Documentation work](#documentation-work), then remove this
temporary plan. The optional operator UI remains separate
[issue #612](https://github.com/eirhop/favn/issues/612).

## 1. Outcome in simple terms

Today an operator must run several Jobs and some manual SQL in the correct
order. The target workflow is one explicit Job command:

```text
/app/bin/favn_control_plane_ops bootstrap
```

That command performs only Favn-owned PostgreSQL setup:

1. check every input before changing the database;
2. connect with the temporary bootstrap administrator;
3. create or check the target database;
4. map deployment identities to PostgreSQL roles when the provider requires it;
5. create, inspect, and harden the migrator and runtime roles;
6. make the migrator the owner of only the `favn_control` schema;
7. run migrations as the migrator;
8. apply runtime grants as the schema owner;
9. provision the initial workspace as the restricted runtime role; and
10. open a fresh runtime connection and verify the exact ready state.

The deployment then removes the bootstrap administrator assignment and deletes
the temporary identity and Job. The resident control plane receives only the
runtime connection and continues to fail closed if the schema is not ready. It
never migrates or repairs PostgreSQL during startup.

Later releases use:

```text
/app/bin/favn_control_plane_ops status
/app/bin/favn_control_plane_ops upgrade
```

`status` only reads and explains state. `upgrade` uses the migrator and runtime
identities, never the bootstrap administrator, and does not create a workspace.

### Responsibility boundary

| Owner | Responsibility |
| --- | --- |
| Deployment/operator | PostgreSQL server, network, TLS, backups, Azure or password identities, secrets, temporary administrator authorization, Job creation/triggering, and cleanup |
| Favn common PostgreSQL workflow | Validate configuration, inspect state, enforce role policy, create the Favn database/schema when authorized, migrate, grant, provision the first workspace, and verify runtime |
| Favn provider adapter | Translate a deployment identity into the provider's PostgreSQL mapping operation; Azure uses exact Entra object IDs and `pgaadauth` |
| Resident Favn control plane | Use only the restricted runtime identity and fail closed when verification is not ready |

The operator should not need to write Favn-specific SQL. Favn should not create
or delete cloud infrastructure or decide when temporary external authority is
granted or removed.

## 2. Assumptions

1. PostgreSQL 18 remains the only Favn control-plane database.
2. The deployment creates the PostgreSQL server, network path, firewall rules,
   DNS, TLS trust, backup policy, and provider identities.
3. The deployment supplies temporary bootstrap authority to a one-off Job and
   removes it only after Favn returns successful runtime verification.
4. The bootstrap Job may temporarily use three credentials in one operating
   system process, but every database stage opens a connection as the exact
   identity intended for that stage.
5. Azure Container Apps may attach the bootstrap, migrator, and runtime
   user-assigned managed identities to the temporary Job. The configured client
   ID selects which identity receives each token.
6. Password deployments supply the administrator, migrator, and runtime
   credentials through deployment-managed secrets. Favn never generates,
   returns, or persists those passwords.
7. One explicit initial workspace is required by `bootstrap`. Provisioning is
   already idempotent for an exact existing workspace.
8. PostgreSQL and provider state are the bootstrap source of truth. Version one
   does not add a Favn bootstrap-history table because the database or schema
   may not exist when the operation begins.
9. The deployment platform remains authoritative for who started a Job. Favn
   emits bounded, redacted operation and stage evidence only.

## 3. Locked decisions

### 3.1 Supported commands

The supported production database lifecycle becomes:

```text
favn_control_plane_ops bootstrap
favn_control_plane_ops status
favn_control_plane_ops upgrade
```

- `bootstrap` is the only command allowed to use bootstrap administrator
  authority.
- `status` performs no writes, grants, mapping changes, migrations, or workspace
  provisioning.
- `upgrade` requires only migrator and runtime connections.
- All three commands use the same implementation from the same immutable
  control-plane image.
- The deployment may run them as an Azure Container Apps Job, Kubernetes Job,
  one-off Docker container, CI task, or equivalent batch process.
- Favn does not implement a cloud-specific Job launcher in issue #611.

The existing low-level operations remain internal implementation stages while
the new workflow is introduced. Once shipped deployment examples have moved to
the three commands, remove `migrate`, `grant-runtime`, `provision-workspace`,
and runtime `verify-schema` from the supported release wrapper rather than
maintaining two public production workflows. Keep unrelated maintenance
operations such as restore verification and key inventory explicit.

### 3.2 Privilege boundary

The three database identities have different jobs:

| Identity | May do | Must not remain able to do |
| --- | --- | --- |
| Bootstrap administrator | Provider identity mapping, role creation and hardening, database creation/ownership setup, `PUBLIC` hardening, initial schema ownership | Run in the resident control plane or remain attached after bootstrap verification |
| Migrator | Own `favn_control`, run Favn migrations, manage Favn objects, grant Favn object access and default privileges | `CREATEROLE`, `CREATEDB`, superuser, replication, bypass RLS, inherited memberships, ownership outside `favn_control` |
| Runtime | Connect, use `favn_control`, read and mutate ordinary control-plane tables and sequences, read migration state | Create roles/databases/schemas, own Favn objects, change grants, mutate `schema_migrations`, inherit memberships |

The target database remains owned by a deployment/bootstrap administrator, not
by the migrator or runtime role. The migrator owns only the `favn_control`
schema and objects it creates inside that schema.

The normal control-plane image receives only the runtime profile. The normal
upgrade Job receives migrator and runtime profiles. Only the temporary
bootstrap Job receives all three profiles.

### 3.3 General workflow, provider adapters

The workflow is PostgreSQL-specific but not Azure-deployment-specific.

- `favn_storage_postgres` owns command orchestration, named connection
  profiles, state inspection, PostgreSQL role policy, database/schema
  ownership, grants, locks, migrations, workspace provisioning, verification,
  result classification, and redaction.
- `favn_azure` owns managed-identity token acquisition and Azure Entra principal
  mapping through the `pgaadauth` extension.
- The built-in password path uses ordinary PostgreSQL roles and credentials; it
  has no Entra mapping step.
- A future provider may implement the same bounded identity callbacks without
  changing command names, status shapes, role policy, migrations, or runtime
  grants.

Do not put Azure branches into the common workflow. Extend the existing
internal dynamic authentication-provider contract in
`FavnStoragePostgres.Authentication` with identity-inspection and
identity-convergence callbacks. `favn_azure` implements those callbacks without
depending on `favn_storage_postgres`; storage validates and invokes the module
dynamically, as it already does for connection tokens. This avoids both an app
dependency cycle and putting a PostgreSQL deployment contract in `favn_core`.

### 3.4 No automatic startup work

Preserve `FavnStoragePostgres.SchemaGate` as a read-only startup gate. Runtime
startup:

- never calls `bootstrap`, `upgrade`, migrations, grants, role repair, mapping,
  or workspace provisioning;
- opens only the runtime connection;
- verifies the exact schema, role authority, and configured workspace; and
- fails readiness with an actionable status when setup or upgrade is required.

### 3.5 Idempotency is verified state, not blind retry

Every stage follows this pattern:

```text
inspect -> classify -> apply the smallest required change -> inspect again
```

An exact desired state is a no-op. A safe missing state is created. A conflict
or authority wider than policy fails closed. A timeout or connection loss after
a write may have committed returns `unknown_outcome`; the command never assumes
failure and never blindly repeats the write.

The operator resolves an unknown outcome by running `status`. A later
`bootstrap` or `upgrade` is safe only after fresh inspection identifies a known
current state.

### 3.6 No bootstrap ledger in version one

Do not add a durable bootstrap-operation table in the first implementation.
Such a table cannot cover failures before the database/schema exists and would
create a second truth beside PostgreSQL catalog state. Use:

- PostgreSQL catalog and Favn rows as completion evidence;
- one structured final result plus bounded stage events;
- deployment Job history for launcher identity and timestamps; and
- `status` for reconciliation.

Reconsider a durable ledger only if a later UI needs history that the platform
cannot provide. That belongs to issue #612 and must not delay the Job workflow.

## 4. Non-goals

Issue #611 does not:

- add an Easy Auth, LiveView, or other operator UI;
- create Azure Container Apps, managed identities, Entra administrators,
  PostgreSQL servers, networks, private endpoints, DNS, or firewall rules;
- grant or remove the external bootstrap administrator assignment;
- provide arbitrary SQL execution;
- support another database engine;
- rotate existing password-role credentials;
- migrate automatically at application startup;
- provision workspaces during `upgrade`;
- make normal runtime or migrator identities repair unsafe role authority;
- hide a provider limitation by broadening privileges; or
- automatically retry a Job after an unknown outcome.

## 5. Current state and main gaps

The current release already has useful pieces:

- `FavnStoragePostgres.Release` runs bounded one-off operations with redacted
  results and telemetry;
- `FavnStoragePostgres.StorageV2.Migrations` owns migrations and exact schema
  diagnostics;
- `FavnStoragePostgres.RuntimePrivileges` grants runtime access and inspects part of
  the runtime authority;
- `FavnStoragePostgres.Config` validates password or Azure managed-identity
  connection configuration;
- `Favn.Azure.ControlPlanePostgresAuth` supplies fresh managed-identity tokens;
  and
- `FavnStoragePostgres.SchemaGate` prevents startup against an unready schema.

The gaps are:

1. operators must compose multiple release commands and manual provider SQL;
2. one process can configure only one database identity at a time;
3. database/role administration and schema grants are mixed together;
4. current role hardening issues a blanket `NOSUPERUSER`, which Azure Database
   for PostgreSQL rejects even when the role is already non-superuser;
5. the migrator can be deployed with `CREATEROLE` even though migrations do not
   need it;
6. migrations create `favn_control` themselves, which requires database-level
   `CREATE`; the new migrator must not have that permission;
7. there is no provider identity-mapping boundary;
8. release output is intended for humans, not stable Job monitoring; and
9. current production documentation describes several manually sequenced Jobs.

## 6. Configuration contract

### 6.1 Named connection profiles

Introduce one validated bootstrap configuration containing three independent,
redacted connection profiles:

```text
bootstrap administrator -> maintenance database and target database
migrator                -> target database
runtime                 -> target database
```

Each profile contains:

- authentication mode;
- host, port, target database, username, and verified TLS configuration;
- provider options such as managed-identity client ID;
- external identity facts needed for mapping, such as Azure object ID; and
- a private profile name used to isolate authentication supervisors and caches.

Configuration validation must reject:

- missing required profiles for the selected command;
- the same PostgreSQL role name used for bootstrap, migrator, and runtime;
- the same Azure object ID assigned to migrator and runtime;
- role names that are not safe PostgreSQL identifiers;
- target database or workspace values outside existing bounds;
- plaintext or unverified TLS in production;
- secret-bearing values in command arguments;
- mixed URL/component fields inside one profile; and
- provider fields used with the wrong authentication mode.

`bootstrap` validates all three profiles and the initial workspace before its
first connection or write. `upgrade` validates migrator and runtime profiles.
`status` validates every supplied profile but can make a bounded partial report
when administrator configuration is intentionally absent.

### 6.2 Environment shape

Keep environment configuration because deployment platforms can bind each
value directly to a secret or identity resource. Use consistent profile
prefixes:

```text
FAVN_DATABASE_NAME
FAVN_DATABASE_MAINTENANCE_NAME       # default: postgres
FAVN_DATABASE_HOST
FAVN_DATABASE_PORT                  # default: 5432

FAVN_DATABASE_BOOTSTRAP_AUTH_MODE
FAVN_DATABASE_BOOTSTRAP_USERNAME
FAVN_DATABASE_BOOTSTRAP_URL          # password mode alternative
FAVN_DATABASE_BOOTSTRAP_AZURE_MANAGED_IDENTITY_CLIENT_ID

FAVN_DATABASE_MIGRATOR_AUTH_MODE
FAVN_DATABASE_MIGRATOR_USERNAME
FAVN_DATABASE_MIGRATOR_URL           # password mode alternative
FAVN_DATABASE_MIGRATOR_AZURE_MANAGED_IDENTITY_CLIENT_ID
FAVN_DATABASE_MIGRATOR_AZURE_OBJECT_ID

FAVN_DATABASE_RUNTIME_AUTH_MODE
FAVN_DATABASE_RUNTIME_USERNAME
FAVN_DATABASE_RUNTIME_URL            # password mode alternative
FAVN_DATABASE_RUNTIME_AZURE_MANAGED_IDENTITY_CLIENT_ID
FAVN_DATABASE_RUNTIME_AZURE_OBJECT_ID

FAVN_WORKSPACE_ID
FAVN_WORKSPACE_SLUG
FAVN_WORKSPACE_NAME
```

Reuse the existing canonical TLS, timeout, and certificate variables rather
than creating profile-specific TLS policy. A password bootstrap URL identifies
the maintenance database; migrator and runtime URLs must identify the exact
target database. Azure profiles use the common endpoint and target names.

During implementation, confirm these names against the production loader and
replace the old single-profile names atomically in release documentation and
deployment examples. Favn is pre-v1; do not add a permanent alias layer merely
to preserve the cumbersome workflow.

No command accepts a URL, password, token, object ID, or certificate through
its argument list. Redacted diagnostics may include profile name,
authentication mode, role name, target database name, TLS enabled/verified,
and whether an external ID was supplied. They never include URLs, host
credentials, tokens, passwords, certificate contents, or provider endpoints
containing secrets.

### 6.3 Password-role creation

The deployment owns password values; Favn owns Favn role policy. For a missing
role, Favn derives a salted SCRAM-SHA-256 verifier locally and sends only that
verifier to the fixed administrative statement. Plaintext password material is
never part of SQL text or SQL bind parameters. For an existing role, Favn first
proves the configured credential and exact `current_user` through a fresh
connection, then hardens attributes and memberships. A wrong credential fails
before any role mutation, and implicit password rotation is forbidden.

Credential proof prefers the target database and may fall back to the
maintenance database only when target `CONNECT` is denied. A PostgreSQL
post-authentication database-access rejection proves the supplied role
credential without granting normal roles hidden maintenance-database access.

Verifier derivation uses the server's effective `scram_iterations` setting.
Password profiles accept only non-space printable ASCII (`!` through `~`), a
deliberately narrow SASLprep-stable boundary; URL-reserved characters remain
percent-encoded in connection URLs.

If an existing password role rejects the supplied credential, fail with
`credential_reconciliation_required`. Do not reset an existing password as an
incidental bootstrap repair. Explicit credential rotation remains deployment
work.

## 7. Internal module layout

Prefer small modules with explicit data and no global environment rereads:

| Proposed owner | Responsibility |
| --- | --- |
| `FavnStoragePostgres.Bootstrap.Config` | Parse one environment map into immutable named profiles and workspace input; redact safely |
| `FavnStoragePostgres.Bootstrap` | Compose `status`, `bootstrap`, and `upgrade` stages and classify their bounded results; no provider SQL |
| `FavnStoragePostgres.Bootstrap.Connection` | Start and stop isolated raw/admin and Repo connections for one profile |
| `FavnStoragePostgres.Bootstrap.Lock` | Acquire and release bounded PostgreSQL advisory locks |
| `FavnStoragePostgres.Bootstrap.Database` | Inspect/create the target database and verify its owner/access |
| `FavnStoragePostgres.Bootstrap.RolePolicy` | Inspect exact role attributes and memberships; apply common role policy through administrator authority |
| `FavnStoragePostgres.Bootstrap.Database` | Coordinate temporary database authority, migrator-owned `favn_control` creation, and database/schema ownership verification |
| `FavnStoragePostgres.RuntimePrivileges` | Converge runtime grants/default privileges using the schema owner, without server-role administration |
| `FavnStoragePostgres.Authentication` | Validate/invoke dynamic token and external-identity callbacks |
| `FavnStoragePostgres.Release` | Stable release API delegating to the workflow and existing maintenance operations |
| `FavnStoragePostgres.ReleaseCLI` | Fixed command dispatch, stable exit mapping, JSON result, human progress |
| `Favn.Azure.ControlPlanePostgresAuth` | Azure tokens plus Azure Entra mapping callbacks and redacted provider failure classes |

Names may be adjusted to match existing conventions, but the responsibilities
must remain separate. Keep `FavnStoragePostgres.RuntimePrivileges` limited to
schema grants and diagnostics; server-role policy remains in
`Bootstrap.RolePolicy`. Migration diagnostics consume the read-only runtime
grant result.

`FavnStoragePostgres.Release` must not keep one globally configured Repo alive
while pretending to switch users through environment changes. The parsed
configuration is explicit data, and each stage receives its exact profile.

## 8. Provider identity contract

### 8.1 Common callback contract

Extend the existing validated dynamic provider callbacks with bounded identity
operations equivalent to:

```text
identity_status(executor, mapping) -> exact | missing | conflict | unavailable
ensure_identity(executor, mapping) -> exact | created | error | unknown_outcome
```

`executor` is a bootstrap-administrator query boundary owned by
`favn_storage_postgres`. The provider implementation may execute only its
compiled, fixed statements with bound values. Deployment input cannot supply
SQL, function names, or arbitrary options.

The mapping passed to the provider contains only validated facts:

```text
PostgreSQL role name
external object ID
principal type
profile purpose: migrator or runtime
```

The provider returns bounded classifications and redacted facts. It never
returns an access token or a raw database error to workflow output.

### 8.2 Azure Entra mapping

For Azure, keep three identities distinct:

| Value | Meaning |
| --- | --- |
| PostgreSQL username | Role name used when connecting to PostgreSQL |
| Managed-identity client ID | Selects which attached user-assigned identity receives a token |
| Managed-identity object ID | Stable Entra principal ID stored by Azure PostgreSQL mapping |

The Azure adapter uses the bootstrap Entra administrator connection and the
Azure PostgreSQL `pgaadauth` functions. Conceptually it:

1. inspects mappings with `pgaadauth_list_principals(false)`;
2. compares exact normalized role name, object ID, and service-principal type;
3. treats an exact mapping as a no-op;
4. creates a missing mapping with
   `pgaadauth_create_principal_with_oid(role_name, object_id, 'service', false, false)`;
5. re-inspects and requires an exact result; and
6. rejects a role-name/object-ID/type conflict without deleting or remapping it.

An existing plain role is not a missing mapping Favn may claim. It is a conflict
until the deployment removes it or establishes the exact provider mapping
outside this workflow. This protects unrelated roles before any hardening.

Do not infer the object ID from the client ID or display name. Do not search
Microsoft Graph during bootstrap. The deployment supplies the exact object ID.

The exact function result columns and supported principal-type spelling must be
confirmed against the target Azure PostgreSQL 18 service in an acceptance test.
If the service contract differs, update the adapter and this section before
merging; do not make matching looser.

### 8.3 Password identity path

Password mode has no external mapping. Its identity check is:

1. inspect whether the PostgreSQL role exists;
2. create a missing role only during `bootstrap` using the supplied credential;
3. prove the role name and credential with a fresh connection; and
4. report an existing authentication mismatch without rotating it.

The common PostgreSQL role policy is then identical to Azure mode.

## 9. PostgreSQL role policy

### 9.1 Exact inspection

Inspect at least these catalog facts for migrator and runtime:

- `rolcanlogin`;
- `rolsuper`;
- `rolcreatedb`;
- `rolcreaterole`;
- `rolinherit`;
- `rolreplication`;
- `rolbypassrls`;
- direct and inherited memberships from `pg_auth_members`;
- target database owner and `CONNECT`/`CREATE` authority;
- `public` and `favn_control` schema ownership/authority; and
- owned objects outside `favn_control` within the target database.

The diagnostic result must expose individual booleans/findings, not only one
`safe?` flag. This makes a failed Job actionable without exposing credentials.
Do not query `pg_authid`, read password verifiers, or include credential
metadata in role diagnostics. Authentication is proved only with a fresh
connection using the deployment-supplied profile.

### 9.2 Desired attributes

Both migrator and runtime roles must be login roles with:

```text
NOSUPERUSER
NOCREATEDB
NOCREATEROLE
NOINHERIT
NOREPLICATION
NOBYPASSRLS
no role memberships
```

They differ through ownership and grants, not server-wide attributes:

- migrator owns `favn_control` and Favn objects in it;
- runtime owns none of them and receives only explicit use/DML grants.

### 9.3 Safe convergence

Never issue a blanket `ALTER ROLE ... NOSUPERUSER ...` statement.

For each attribute:

1. inspect the current value;
2. emit no SQL when it already matches;
3. apply only the minimal provider-supported alteration when it differs;
4. re-inspect; and
5. fail if the desired value cannot be proved.

This directly fixes the Azure PostgreSQL 18 failure where a delegated Entra
administrator cannot execute a redundant `NOSUPERUSER` alteration because only
a real PostgreSQL superuser may change the `SUPERUSER` attribute.

If `rolsuper` is true, fail `unsafe_authority`; never attempt to repair it on
Azure. Apply the same fail-closed rule to any dangerous attribute or membership
the active administrator cannot safely remove. A provider may declare an
attribute alteration unsupported, but it may not declare unsafe authority
acceptable.

Normal migrator/runtime connections only inspect this policy. They never alter
their own roles or other roles. `upgrade` fails with `bootstrap_required` if
unsafe or stale role authority requires the temporary administrator.

### 9.4 Ownership and grants

Bootstrap administrator stages:

1. revoke target-database `CREATE` from `PUBLIC`;
2. revoke `public` schema `CREATE` from `PUBLIC`;
3. remove database-level `CREATE` from migrator and runtime;
4. temporarily grant database `CREATE`, connect as migrator, and create
   `favn_control AUTHORIZATION CURRENT_USER` when missing;
5. verify an existing `favn_control` owner is the exact migrator;
6. reject unrelated schema/object ownership rather than silently taking it; and
7. prove migrator/runtime do not own the target database.

Migrator stages:

1. migrate only inside `favn_control`;
2. revoke all Favn schema/table/sequence access from `PUBLIC`;
3. converge runtime access on all current Favn tables and sequences;
4. keep `favn_control.schema_migrations` read-only for runtime;
5. set default privileges for future migrator-owned tables and sequences; and
6. re-inspect the exact grant contract.

Remove `CREATE SCHEMA IF NOT EXISTS favn_control` from the normal migration
path. `bootstrap` creates the schema with the correct owner. `upgrade` requires
it to exist with the exact owner and fails before migration otherwise.

## 10. Command contract

### 10.1 Common output

Each command writes human-readable, secret-free progress to standard error and
exactly one final JSON object to standard output. The final object has a stable
top-level shape:

```json
{
  "contract_version": 1,
  "operation": "bootstrap",
  "outcome": "ready",
  "safe_to_retry": true,
  "completed_stages": ["configuration", "database", "identities"],
  "state": "ready",
  "findings": [],
  "runtime_verified": true
}
```

Rules:

- `completed_stages` contains only stages proved complete by post-write reads;
- `safe_to_retry` is false for `unknown_outcome`;
- findings contain stable code, stage, and bounded redacted details;
- never serialize exception text, SQL text, URLs, passwords, tokens,
  certificates, or unrestricted PostgreSQL messages;
- JSON field names and finding codes are strings; and
- telemetry contains operation, stage, outcome, stable code, and duration only.

Use fixed exit codes:

| Exit | Meaning |
| --- | --- |
| `0` | command completed and runtime is verified ready, or `status` found ready |
| `2` | `status` completed and found deterministic work required |
| `64` | invalid or incomplete configuration; no write attempted |
| `69` | dependency, server, or authentication unavailable before any possible write |
| `70` | deterministic conflict, unsafe authority, or failed verification |
| `75` | another operation holds the bounded lock; no write attempted |
| `76` | unknown outcome after a write may have completed; do not auto-retry |

The release shell wrapper must return these codes directly. Do not use an
uncaught Elixir exception as the machine contract.

### 10.2 `status`

`status` never acquires a write lock and never changes state. It returns all
findings it can safely inspect, plus one summary state using this precedence:

1. `invalid_configuration`;
2. `server_unreachable`;
3. `authentication_unavailable` or `authentication_rejected`;
4. `database_missing`;
5. `identity_mapping_missing` or `identity_mapping_conflict` when an
   administrator profile can prove it;
6. `unsafe_authority`;
7. `role_hardening_required`;
8. `schema_upgrade_required`;
9. `runtime_grants_missing`;
10. `workspace_missing` or `workspace_conflict`;
11. `unknown_outcome` when reads cannot establish a complete answer; or
12. `ready`.

When no bootstrap administrator is supplied, successful fresh migrator/runtime
connections are sufficient evidence that their provider mappings work.
If authentication is rejected, `status` reports `authentication_rejected`
rather than guessing `identity_mapping_missing`. Administrator-backed status
may refine that result through provider mapping inspection.

`status` exits zero only after a fresh runtime connection verifies:

- the exact current migration/schema fingerprint;
- runtime role identity and least privilege;
- runtime table/sequence/default-grant contract; and
- the configured workspace when workspace input is supplied.

### 10.3 `bootstrap`

`bootstrap` requires bootstrap, migrator, runtime, and initial-workspace input.
Its stages and identities are:

| Order | Stage | Database identity |
| --- | --- | --- |
| 1 | Parse and validate every input | none |
| 2 | Acquire bounded maintenance lock | bootstrap administrator |
| 3 | Inspect/create target database | bootstrap administrator |
| 4 | Inspect/create provider mappings | bootstrap administrator through provider adapter |
| 5 | Inspect/create/harden migrator and runtime roles | bootstrap administrator |
| 6 | Harden database/`PUBLIC`; temporarily grant schema-create authority | bootstrap administrator |
| 7 | Create missing `favn_control` as its natural owner | migrator |
| 8 | Revoke temporary database `CREATE` and verify exact policy | bootstrap administrator |
| 9 | Connect and apply migrations | migrator |
| 10 | Apply and verify current/default runtime grants | migrator/schema owner |
| 11 | Connect and provision exact initial workspace | runtime |
| 12 | Close it, open a fresh runtime connection, verify all runtime-visible state | runtime |
| 13 | Emit `ready`, release locks, close every connection/provider lifecycle | each owner |

Runtime verification is part of success, not a follow-up suggestion. If it
does not pass, `bootstrap` does not return success and the deployment must not
remove temporary authority merely because earlier stages completed.

An exact already-ready installation performs no writes. Existing installations
may run `bootstrap` once to remove migrator `CREATEROLE`, correct ownership, and
move to the new workflow. Exact existing workspaces remain unchanged.

When repairing pre-existing password roles, the deployment gives the temporary
bootstrap identity `ADMIN OPTION` on those exact roles. Every parent membership
reported by status must also be revocable by that identity. PostgreSQL 18 tracks
the membership grantor, so parent-role `ADMIN OPTION` alone may not permit
revoking a grant recorded under a different grantor; the deployment must make
the bootstrap identity the transition grantor or use provider administrator
authority that can revoke it.
Favn creates a missing control schema by
temporarily granting database `CREATE` to the migrator, connecting as the
migrator so it owns the schema naturally, then revoking and verifying the grant.
This avoids relying on implicit `SET ROLE` membership from `CREATEROLE`.

### 10.4 `upgrade`

`upgrade` requires only migrator and runtime profiles. It:

1. validates configuration;
2. opens a fresh migrator connection;
3. inspects migrator authority, schema ownership, and target state;
4. fails `bootstrap_required` if role/database/schema administration is needed;
5. acquires the target-database operation lock;
6. runs pending migrations as migrator;
7. converges current and default runtime grants as migrator;
8. closes the migrator connection;
9. opens a fresh runtime connection and performs exact verification; and
10. returns success only when runtime is ready.

It never creates a database, role, provider mapping, schema, or workspace. It
never receives bootstrap administrator configuration in normal use.

## 11. Locking and concurrent Jobs

Use session-level PostgreSQL advisory locks with a constant namespace and a
deterministic key derived from the validated target database name. Do not use a
Favn table because the database/schema may not exist.

- Lock acquisition has a bounded wait and no retry loop.
- PostgreSQL advisory locks are scoped to the database of the locking session.
  `bootstrap` therefore holds the maintenance-database lock across database
  creation and also takes the target-database lock before target writes. The
  maintenance lock serializes bootstrap Jobs; the target lock serializes an
  active bootstrap with upgrade.
- `upgrade` takes the same target-database key from its migrator connection. It
  contends with bootstrap's target lock, not the maintenance-database lock.
- `status` remains lock-free and returns available read evidence.
- A second writer exits `75` without making a change.
- Connections own locks. Connection loss releases the lock but may also create
  an unknown write outcome, which must be reported as such.

PostgreSQL integration tests must prove contention within both lock domains,
that maintenance and target locks do not self-contend, and that releasing or
losing an owning connection makes its lock available again.

## 12. Transaction and unknown-outcome rules

Use a transaction or `Ecto.Multi` only where one connection and one database
can make the whole stage atomic. Do not present the complete bootstrap as one
transaction; database creation, provider mapping, role changes, migrations,
and fresh connection verification cross transaction and connection boundaries.

| Stage | Completion evidence | Retry rule |
| --- | --- | --- |
| Create database | fresh catalog lookup and target connection | connection loss before proof is unknown; run `status` |
| Create provider mapping | provider re-inspection exactly matches | loss after provider call is unknown; inspect before any later create |
| Create/harden role | fresh `pg_roles` and membership inspection | repeat only the missing minimal change |
| Create schema/change owner | fresh namespace and ownership inspection | conflict fails; no blind owner takeover |
| Run migrations | exact migration versions and schema fingerprint | use Ecto migration locking; connection loss requires diagnostics |
| Apply grants | complete current/default grant inspection | repeat only after known catalog state |
| Provision workspace | exact row identity and values | existing exact row is success; conflict fails |
| Runtime verification | entirely fresh restricted connection | read-only and safe to repeat |

Classify failures as:

- `not_started`: validation or connection failed before a write;
- `known_incomplete`: reads prove the write did not complete;
- `known_conflict`: state exists but is not the requested identity/authority;
- `known_complete`: postcondition is proved;
- `unknown_outcome`: a write may have committed but the postcondition could not
  be read.

Only `not_started` and a freshly inspected `known_incomplete` are safe for
automatic orchestration to retry. The Job itself has platform retry count zero.

## 13. Runtime verification

The final runtime connection must be new; do not reuse a connection opened
before grants or migrations. Verify:

- `current_user` is exactly the configured runtime role;
- PostgreSQL major version is supported;
- TLS is enabled and verified in production;
- target database is exact;
- runtime role attributes and memberships satisfy policy;
- target database and schema ownership are not runtime-owned;
- database/schema `CREATE` is absent;
- all required tables, indexes, columns, constraints, migrations, and schema
  fingerprint match the image;
- all current table/sequence permissions match runtime policy;
- runtime cannot mutate `favn_control.schema_migrations`;
- default privileges will cover new migrator-owned objects; and
- the initial/configured workspace exists and is active.

Where safe and transactionally reversible, add negative capability probes for
forbidden runtime actions. Roll back every probe and keep catalog inspection as
the primary evidence. Never create durable test objects in production
verification.

## 14. Removing `CREATEROLE` from the migrator

This is the first security milestone and a required acceptance result.

### Fresh deployment

The bootstrap administrator creates or maps the migrator directly with no
`CREATEROLE`, verifies the exact role state, and only then connects as migrator.
No manifest, template, example, or runtime environment grants `CREATEROLE` to
the migrator. Temporary `ADMIN OPTION` is needed only for an existing-role
hardening transition.

### Existing deployment

An existing deployment performs one explicit transition:

1. deploy the candidate immutable image as the temporary bootstrap Job;
2. attach/re-authorize a bootstrap administrator;
3. run `status` and retain the redacted result;
4. run `bootstrap` with the exact existing workspace values;
5. let the administrator remove `CREATEROLE` and other unsafe role state;
6. let migrator migrations/grants and fresh runtime verification complete;
7. require final `state=ready` and `runtime_verified=true`; and
8. remove the administrator assignment, bootstrap identity, and Job.

After this transition, `upgrade` refuses to operate if `CREATEROLE` or any other
unsafe authority reappears. The fix is to investigate drift and explicitly run
an authorized bootstrap repair; never give `CREATEROLE` back to the normal
migrator.

## 15. Azure Job reference workflow

The Favn image contract is provider-neutral. The Azure reference deployment
should instantiate it as follows.

### Bootstrap Job

- exact immutable Favn image digest;
- fixed command `favn_control_plane_ops bootstrap`;
- no ingress;
- manual trigger only;
- replica timeout bounded to the documented maximum;
- replica retry limit `0`;
- bootstrap, migrator, and runtime user-assigned identities attached;
- bootstrap identity temporarily configured as or authorized by the Azure
  PostgreSQL Entra administrator;
- role names, client IDs, and object IDs supplied as non-secret configuration;
- any password/certificate material supplied from secret references; and
- logs retained long enough to capture the final structured result.

After success, deployment automation—not Favn—removes the bootstrap
administrator assignment, bootstrap identity attachment, bootstrap identity,
and Job definition. Cleanup must be conditional on the exact candidate Job
execution returning `0`, `state=ready`, and `runtime_verified=true`.

### Upgrade Job

- same image contract and fixed `upgrade` command;
- only migrator and runtime identities attached;
- no bootstrap identity or administrator assignment;
- no ingress, manual/pipeline trigger, and retry limit `0`; and
- runtime control-plane rollout starts only after successful verification.

The temporary bootstrap Job may be deleted after use. The upgrade Job may
remain as an operator-owned deployment resource because it has no server/role
administrator identity.

## 16. Release packaging

Update `rel/control_plane/overlays/bin/favn_control_plane_ops` to expose the new
fixed commands and exit codes. It must not accept a module, function, SQL
fragment, or arbitrary `eval` expression from user input.

Investigate and, if safe, remove the requirement for distributed Erlang node,
cookie, EPMD, and distribution-port configuration when invoking this one-off
operations entrypoint. Database Jobs do not communicate with runners or a
resident control plane. If the release tooling cannot disable distribution for
`eval` safely in the first slice, keep the current requirement temporarily and
track its removal inside issue #611; do not weaken cookie validation.

The control-plane image must continue to include `favn_azure` so Azure provider
callbacks are available. Password mode must work without starting Azure token
services.

## 17. Implementation slices

Each slice must leave a coherent, tested boundary. Do not publish the new
workflow until the complete bootstrap path and runtime verification work.

### Slice 1: role-policy split and Azure blocker

1. Replace the blanket role alteration with inspect-first minimal convergence.
2. Add every required role attribute and membership to diagnostics.
3. Separate server/role administration from schema runtime grants.
4. Add migrator-role policy and ownership diagnostics.
5. Create/verify `favn_control` through the migrator under a temporary database
   grant and remove schema creation from normal migration execution.
6. Add regression coverage proving no redundant `NOSUPERUSER` is issued.
7. Prove the migrator performs migrations/grants with `NOCREATEROLE`.

### Slice 2: explicit profiles and status

1. Parse immutable bootstrap/migrator/runtime profiles from one environment map.
2. Give every dynamic-auth profile an isolated provider lifecycle.
3. Add redacted config diagnostics and cross-profile validation.
4. Implement complete read-only PostgreSQL status inspection.
5. Add stable finding codes, JSON output, and exit-code mapping.
6. Preserve existing startup schema-gate behavior against the runtime profile.

### Slice 3: provider identity adapters

1. Extend the internal dynamic authentication-provider callback validation.
2. Implement exact Azure Entra mapping inspection/create/re-inspection in
   `favn_azure`.
3. Implement the password role-existence/SCRAM-verifier creation/fresh-login
   path.
4. Add conflict, unavailable, rejected, and unknown-outcome classifications.
5. Add provider contract and redaction tests.

### Slice 4: bootstrap workflow

1. Implement one cluster-wide advisory lock held by the maintenance connection.
2. Implement target database inspect/create/reconnect.
3. Compose identity mapping, role hardening, schema ownership, migrations,
   grants, runtime workspace provisioning, and fresh runtime verification.
4. Make every stage inspect/apply/re-inspect and return bounded evidence.
5. Expose `bootstrap` through the release wrapper.
6. Test fresh, partial, ready, conflict, concurrent, timeout, and unknown paths.

### Slice 5: upgrade workflow and cutover

1. Compose inspect, the shared lock, migrate, grant, and runtime verification with
   no administrator profile.
2. Make unsafe authority return `bootstrap_required`.
3. Expose `upgrade` through the release wrapper.
4. Move development upgrade helpers onto the same underlying workflow where
   their local password assumptions remain valid.
5. Remove old supported production verbs after every shipped deployment
   example uses the new commands.

### Slice 6: deployment qualification and permanent docs

1. Update the Favn production-shaped password/Compose qualification.
2. Update the operator-owned Azure reference deployment in its repository.
3. Run live Azure PostgreSQL 18 bootstrap and existing-deployment tests.
4. Capture evidence that temporary authority was removed after verification.
5. Update canonical documentation and feature/roadmap status.
6. Remove this temporary plan before closing issue #611.

## 18. Test plan

### 18.1 Pure/configuration tests

Cover:

- all required profiles by command;
- password and Azure modes;
- invalid/mixed fields;
- duplicate role/object identities;
- identifier and workspace bounds;
- production TLS enforcement;
- profile-specific provider process names;
- redacted `Inspect` and JSON output; and
- stable exit-code classification.

### 18.2 PostgreSQL 18 integration tests

Use real PostgreSQL roles/databases. Cover:

- fresh password bootstrap;
- exact second bootstrap with no writes;
- existing database with missing schema;
- partial migrations;
- missing/stale runtime grants;
- exact existing workspace and conflicting workspace;
- migrator/runtime role creation and hardening;
- no migrator `CREATEROLE` before, during, or after migration;
- role memberships and each dangerous attribute;
- target database and schema ownership;
- runtime forbidden DDL and migration-table DML;
- current/default grants after a migration creates a table and sequence;
- concurrent bootstrap/bootstrap, bootstrap/upgrade, and upgrade/upgrade;
- lock timeout with zero writes by the loser;
- connection loss before a write;
- simulated loss after each non-transactional write;
- status reconciliation after unknown outcomes;
- upgrade without administrator configuration; and
- runtime startup still failing closed before setup and succeeding after it.

Where PostgreSQL permits fault injection only through a privileged test harness,
keep that privilege in the test setup process, not in code under test.

### 18.3 Azure adapter tests

Unit tests use a fixed query executor and cover:

- exact role/object-ID/type match;
- missing mapping create and re-inspection;
- role name mapped to a different object ID;
- object ID mapped under a different role;
- wrong principal type;
- unavailable `pgaadauth` functions;
- insufficient Entra administrator authority;
- timeout after the create call;
- token/provider failure classification; and
- no object ID, token, or raw database message in output/logs.

### 18.4 Live Azure acceptance

The target Azure Database for PostgreSQL 18 environment must prove:

1. a fresh server/database path or an operator-approved missing-database path;
2. exact migrator/runtime Entra mapping by object ID;
3. no redundant `NOSUPERUSER` statement;
4. provider-supported role hardening and no unsafe memberships;
5. migrations with migrator `NOCREATEROLE`;
6. grants and initial workspace through their intended identities;
7. fresh runtime verification;
8. idempotent second bootstrap;
9. upgrade with no bootstrap identity;
10. concurrent Job rejection;
11. a controlled unknown-outcome/reconciliation drill where feasible;
12. bootstrap Job retry limit zero; and
13. removal of the bootstrap administrator assignment, identity attachment,
    identity, and Job only after verified success.

If Azure requires a provider-owned role membership or prevents one of the
locked safety invariants, stop. Record the exact live evidence and revisit the
issue; do not silently weaken the common policy.

### 18.5 Focused repository checks

Start with the owning app tests, then expand in proportion to changes:

```bash
MIX_ENV=test mix do --app favn_storage_postgres cmd mix test \
  test/release_cli_test.exs \
  test/storage_v2/config_test.exs \
  test/storage_v2/privileges_test.exs \
  test/storage_v2/release_operations_test.exs

MIX_ENV=test mix do --app favn_azure cmd mix test \
  test/favn/azure/control_plane_postgres_auth_test.exs

mix format
mix compile --warnings-as-errors
```

Add focused bootstrap workflow files rather than overloading the existing
release-operation test. Before merge, run the relevant broader PostgreSQL,
acceptance, container, and security tiers required by the changed slices.

## 19. Documentation work

Implementation must update each owning page once and link rather than duplicate
the contract:

| Page | Required change |
| --- | --- |
| `docs/storage/postgresql/architecture.md` | Permanent identity, ownership, role-policy, idempotency, and unknown-outcome design |
| `docs/production/postgresql_operator_runbook.md` | Copyable bootstrap/status/upgrade procedures, findings, recovery, and authority cleanup |
| `docs/production/control_plane_image.md` | Supported release commands and one-off image contract |
| `docs/production/control_plane_environment.md` | Exact named profile variables, secrets, TLS, and provider fields |
| `docs/production/deployment_topology.md` | Bootstrap/upgrade/runtime identity lifecycle |
| `docs/production/upgrade_and_rollback.md` | `upgrade` sequencing, backup gate, runtime verification, rollback/unknown handling |
| `docs/production/security_qualification.md` | Password and Azure qualification evidence |
| `docs/structure/favn_storage_postgres.md` | New module/path ownership |
| `docs/structure/favn_azure.md` | Entra mapping callback ownership and tests |
| `docs/FEATURES.md` | Mark only implemented command/provider capabilities |
| `docs/ROADMAP.md` | Remove or narrow completed forward work and retain live qualification gaps |

Update release wrapper examples and any local/development setup that currently
teaches the old sequence. Keep provider infrastructure instructions clearly
deployment-owned.

Do not add issue #612 UI behavior to these pages until it exists. The future UI
must invoke and monitor this same Job contract; it must not duplicate the
database workflow in `favn_view` or `favn_orchestrator`.

## 20. Rollout and rollback

### 20.1 Fresh installation

1. Deployment creates PostgreSQL, network/TLS, identities, secrets, and
   temporary administrator authorization.
2. Deployment creates the fixed-command bootstrap Job from an immutable image.
3. Operator runs `status`; missing state is expected.
4. Operator runs `bootstrap` once.
5. Deployment parses the final JSON and requires verified ready state.
6. Deployment removes temporary authority and Job resources.
7. Deployment starts the resident control plane with runtime identity only.
8. Readiness independently confirms the same exact state.

### 20.2 Existing installation

Use the [migrator hardening transition](#removing-createrole-from-the-migrator).
Take the normal backup/PITR checkpoint before changing schema or role
ownership. Do not remove the old deployment commands until the new bootstrap
has verified the existing database.

### 20.3 Later upgrade

1. Record image/schema/manifest state and confirm backup policy.
2. Stop rollout of runtime code requiring the candidate schema.
3. Run candidate `status`.
4. Run candidate `upgrade` with migrator/runtime identities.
5. Require fresh runtime verification.
6. Roll out the candidate resident control plane.
7. Observe readiness and PostgreSQL health.

Application rollback never blindly reverses a database migration. Follow the
existing compatibility/restore policy. An `unknown_outcome` blocks automated
rollout until `status` reconciles the database.

## 21. Security review checklist

- [ ] Normal runtime has no bootstrap or migrator credential/profile.
- [x] Normal startup performs no database write or migration.
- [x] Upgrade configuration rejects a bootstrap administrator profile.
- [ ] Bootstrap Job is fixed-command, no-ingress, manual, bounded, and retry
      limit zero.
- [x] Migrator policy has no `CREATEROLE`, `CREATEDB`, superuser, replication, bypass
      RLS, inherited membership, target-database ownership, or ownership outside
      `favn_control`.
- [x] Runtime has no DDL/schema-migration mutation or object ownership.
- [x] Role convergence never issues redundant `NOSUPERUSER`.
- [x] Mapping compares exact Azure object IDs and fails closed on conflicts.
- [x] Password role creation never sends plaintext credential material to
      PostgreSQL. The derived salted verifier remains sensitive database hash
      material; deployment-controlled database/audit logs must protect it.
- [x] Password verifier creation uses the server's effective SCRAM iteration
      policy and accepts only a documented SASLprep-stable password alphabet.
- [x] No arbitrary SQL enters through arguments, environment, or provider
      configuration.
- [x] Results, logs, exceptions, telemetry, and `Inspect` output are redacted.
- [x] Unknown writes are not blindly retried.
- [x] Concurrent writers are serialized with bounded locks.
- [x] Success requires a fresh restricted runtime connection.
- [ ] Deployment removes temporary Azure authorization/resources only after
      verified success.

## 22. Completion checklist

Issue #611 is complete only when:

- [x] `bootstrap`, `status`, and `upgrade` are implemented in the immutable
      control-plane image.
- [ ] One `bootstrap` Job replaces all Favn-specific manual SQL and the current
      multi-Job setup sequence.
- [ ] Fresh, already-ready, partial, conflicting, concurrent, unavailable,
      timeout, and unknown-outcome paths are tested.
- [x] Password and Azure managed-identity deployments use the same common
      workflow.
- [ ] Live Azure PostgreSQL 18 proves exact mapping and supported hardening.
- [ ] Migrator `NOCREATEROLE` is proved in tests and deployment evidence.
- [x] `upgrade` works without bootstrap administrator authority in the PostgreSQL integration path.
- [x] Runtime startup remains least-privileged and fail-closed.
- [x] Structured output and exit codes are stable and fully redacted.
- [x] Canonical documentation reflects only implemented behavior.
- [ ] The Azure reference deployment removes temporary authority after runtime
      verification.
- [x] Issue #612 remains the only UI follow-up and uses this Job contract.
- [ ] This temporary plan is removed or reduced to a durable architecture
      decision before issue closure.

## 23. Implementation proof gates

These are technical proof tasks, not reasons to broaden scope:

1. Confirm Azure PostgreSQL 18 `pgaadauth` result shapes and authority using a
   live disposable target.
2. Confirm whether Azure-created service principals carry any unavoidable role
   membership. If so, stop and review the no-membership invariant.
3. Confirm which non-superuser role attributes the delegated Entra
   administrator may alter. Already-safe unsupported attributes require no SQL;
   unsafe unsupported attributes fail closed.
4. Prove database creation behavior for both Azure and password administrators.
   If unauthorized, return the exact deployment prerequisite and continue to
   support a pre-created database.
5. Prove advisory-lock contention across maintenance and target databases.
6. Prove that missing password roles receive a locally derived salted SCRAM
   verifier, plaintext never reaches SQL, and existing credentials are checked
   before hardening.
7. Determine whether the release operations entrypoint can run with
   distribution disabled. This is a simplification, not permission to weaken
   distribution security.

Any failed gate must produce a concrete issue/plan update with the observed
provider behavior. It must not be resolved by restoring permanent administrator
authority to the normal migrator or runtime.
