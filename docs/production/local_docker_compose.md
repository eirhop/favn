# Single-host deployment example

`mix favn.init --target deployment` copies a small Compose example to
`deploy/favn/`. It is a deployment starting point, not the normal development
environment.

The copied files contain:

- `compose.yml` for one control plane and one customer runner;
- `runner.Dockerfile` for the consumer project;
- `runner-image-contract.sh` for static image verification;
- `env.example` listing required deployment variables.

Favn never overwrites the directory. The customer owns changes, extra services,
volumes, networks, ingress, observability, and registry policy.

Both example services drop all Linux capabilities, disable privilege
escalation, use a read-only root filesystem, and receive a small writable
`/tmp`. If customer assets intentionally write local files, add a narrowly
scoped volume for those files; do not make the release, DuckDB driver, or
extension directory writable.

PostgreSQL is intentionally absent. Supply a reachable PostgreSQL service and
run migration/provisioning as separate operator steps. The portable template
defaults to `FAVN_DATABASE_SSL_MODE=verify-full` and uses system trust when no
custom CA path is supplied. When the database uses a private CA, mount that CA
certificate using your platform's secret mechanism and set
`FAVN_DATABASE_SSL_CA_FILE` to its in-container path. Required values use
Compose's `:?` form so configuration fails before an invalid deployment starts.

## Example use

```bash
mix favn.init --target deployment
cp deploy/favn/env.example deploy/favn/.env
```

Edit the copied files, build and publish the customer runner, select immutable
image digests, then:

```bash
docker compose \
  --env-file deploy/favn/.env \
  -f deploy/favn/compose.yml \
  config

docker compose \
  --env-file deploy/favn/.env \
  -f deploy/favn/compose.yml \
  up -d
```

Compose's `--env-file` belongs to Compose. It does not restore an application
`.env` loader in Favn.

For ordinary source development, use `mix favn.dev`; it does not invoke these
files or require Docker.

## Repository qualification harness

Maintainers who need to exercise the complete production release boundary on
one Docker host should use
[`deployment/docker-compose/`](../../deployment/docker-compose/README.md).
Unlike the copied customer starting point, that disposable harness supplies a
TLS PostgreSQL container, mutual-TLS distribution, explicit release operations,
manifest publication, and a bounded zero-to-three elastic-runner simulation.
It is intentionally separate so `mix favn.init` does not pretend to provision
customer infrastructure or generate production secrets.
