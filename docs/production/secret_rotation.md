# Manual secret rotation

Favn v1 reads production secrets from environment variables once at boot. It
does not read mounted secret files, call Azure Key Vault or another cloud SDK,
poll a secret provider, or hot-reload credentials. A platform may resolve a
vault reference into an environment value, but Favn treats the result as an
ordinary boot-time value.

Every rotation is operator-controlled:

1. record the current environment revision without secret values;
2. stop admission and allow the bounded drain to complete;
3. update the platform's environment configuration;
4. restart the affected service or services;
5. require full readiness and execute a smoke run; and
6. remove the old value only after the replacement is proven.

Never pass secrets as command-line arguments or place them in Compose YAML,
manifests, runner identities, image labels, logs, diagnostics, telemetry,
support bundles, or shell history.

## General platform service token

`FAVN_ORCHESTRATOR_API_SERVICE_TOKENS` supports overlapping versioned
identities for general platform roles. It rejects `capacity_reader` and the
reserved `capacity-scaler` identities.

1. Add the new identity/token beside the old value and restart the control
   plane.
2. Move every client to the new identity and prove an authenticated operation.
3. Remove the old entry, restart again, and verify the old token is rejected.

Do not reuse one identity with two secrets. The versioned identity is the
observable, non-secret rotation handle.

## Manifest deployer token

`FAVN_ORCHESTRATOR_MANIFEST_DEPLOYER_TOKENS` is a JSON array whose versioned
identities each carry an exact workspace allowlist. It grants only manifest
archive upload and scoped status reads.

1. Add a new object with a new `-vN` identity, a distinct token, and the same
   intended workspace allowlist, then restart the control plane.
2. Move the upload client and prove one authenticated deployment status read.
3. Remove the old object, restart, and verify the old token is rejected.

Do not reuse a token from the general platform set. Favn rejects that ambiguous
configuration at startup and retains only token hashes.

## Capacity-reader token

`FAVN_ORCHESTRATOR_CAPACITY_READER_TOKEN` is the only primary credential for
elastic infrastructure demand reads. The optional
`FAVN_ORCHESTRATOR_CAPACITY_READER_PREVIOUS_TOKEN` provides bounded overlap;
it is invalid without the primary token. Never add `capacity_reader` to the
general platform-token variable.

1. Create the new secret version without changing the scaler.
2. Configure the new version as the primary and the active old version as the
   previous token, then restart the control plane.
3. Prove both versions can read one exact runner-demand endpoint.
4. Move every scaler to the new version and prove another authenticated demand
   read.
5. Remove the previous variable, restart the control plane, and verify the old
   credential is rejected.

The primary and previous values must differ. The control plane retains only
their hashes and reports only the reserved non-secret identities
`capacity-scaler` and `capacity-scaler-overlap`.

## Runtime-input encryption key

`FAVN_RUNTIME_INPUT_PIN_KEYS` is a versioned key ring and
`FAVN_RUNTIME_INPUT_PIN_KEY_VERSION` selects the current write key.

1. Add the new version while retaining every existing version and switch the
   current write version in one environment revision.
2. Restart the control plane and require readiness.
3. Run `favn_control_plane_ops runtime-input-key-inventory`; verify that the new
   version is current and review reference counts without exposing key material.
4. If required, run the bounded release-safe compaction procedure from the
   PostgreSQL runbook.
5. Remove an old version only after inventory proves PostgreSQL no longer pins
   it, then restart and require readiness again.

Removing a referenced old key makes readiness fail closed and can make durable
runtime inputs unreadable. Never skip the inventory gate.

## Operator command fingerprint key

`FAVN_OPERATOR_COMMAND_HMAC_SECRET` must remain stable so persisted command and
idempotency fingerprints remain comparable. When upgrading from the former
single-BEAM release, set it initially to the previous
`FAVN_VIEW_SECRET_KEY_BASE` value; the domain-separated derived key is then
unchanged. Do not rotate it as routine hygiene. A forced rotation requires a
maintenance window and makes retries of commands fingerprinted with the prior
key conflict rather than replay.

## Browser session signing key

Changing `FAVN_VIEW_SECRET_KEY_BASE` invalidates all existing browser sessions.
Announce the maintenance, replace the View revision, then require every
operator to sign in again. PostgreSQL application state is not removed.

## Distribution cookie and database credentials

The distribution cookie must match on View, Orchestrator, and every runner.
Rotate it only during a full cluster maintenance stop: drain Orchestrator, stop
all roles, update every environment, start Orchestrator, then View and runners,
and require full readiness. There is no cookie-overlap mode.

Database credential rotation follows the database provider's overlap/revocation
procedure. Update the runtime and one-off-operation environments, restart the
control plane, prove readiness and a database-backed smoke run, then revoke the
old credential. Preserve the separate migrator and runtime privilege boundary.

Automatic rotation and provider integrations remain deferred to
[#530](https://github.com/eirhop/favn/issues/530).
