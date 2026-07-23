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

## Service token

`FAVN_ORCHESTRATOR_API_SERVICE_TOKENS` supports overlapping versioned
identities.

1. Add the new identity/token beside the old value and restart the control
   plane.
2. Move every client to the new identity and prove an authenticated operation.
3. Remove the old entry, restart again, and verify the old token is rejected.

Do not reuse one identity with two secrets. The versioned identity is the
observable, non-secret rotation handle.

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

## Browser session signing key

Changing `FAVN_VIEW_SECRET_KEY_BASE` invalidates all existing browser sessions.
Announce the maintenance, drain and restart the control plane, then require every
operator to sign in again. PostgreSQL application state is not removed.

## Distribution cookie and database credentials

The distribution cookie must match on both nodes. Rotate it only during a full
two-node maintenance stop: drain the control plane, stop both services, update
both environments, start the runner, then start the control plane and require
full readiness. There is no cookie-overlap mode.

Database credential rotation follows the database provider's overlap/revocation
procedure. Update the runtime and one-off-operation environments, restart the
control plane, prove readiness and a database-backed smoke run, then revoke the
old credential. Preserve the separate migrator and runtime privilege boundary.

Automatic rotation and provider integrations remain deferred to
[#530](https://github.com/eirhop/favn/issues/530).
