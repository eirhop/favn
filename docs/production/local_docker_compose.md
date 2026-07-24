# Single-host deployment example

`mix favn.init --target deployment` copies a small Compose example to
`deploy/favn/`. It is a deployment starting point, not the normal development
environment.

The copied files contain:

- `compose.yml` for one control plane and one customer runner;
- `runner.Dockerfile` for the consumer project;
- `env.example` listing required deployment variables.

Favn never overwrites the directory. The customer owns changes, extra services,
volumes, networks, ingress, observability, and registry policy.

PostgreSQL is intentionally absent. Supply a reachable PostgreSQL service and
run migration/provisioning as separate operator steps. The portable template
defaults TLS off because Favn cannot know how your platform mounts secrets.
For production, set `FAVN_DATABASE_SSL_MODE=verify-full`, mount the CA
certificate using your platform's secret mechanism, and set
`FAVN_DATABASE_SSL_CA_FILE` to its in-container path.

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
