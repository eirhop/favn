# favn_local

Purpose: Docker-free source-development lifecycle behind the public
`mix favn.dev`, `mix favn.reload`, `mix favn.stop`, and `mix favn.doctor` tasks.

## Ownership

`favn_local` owns:

- validation of developer-supplied PostgreSQL and environment configuration;
- Orchestrator and View startup in the current BEAM;
- one child runner BEAM using the consumer's compiled code;
- reload sequencing, runner replacement admission, and manifest deployment;
- a small `.favn/local/` locator and owner-only local credentials.

It does not own PostgreSQL lifecycle, migrations, workspace provisioning,
Docker, Compose, deployment images, dotenv parsing, or deployment scaffolding.

Topology-neutral operator clients are owned by `Favn.CLI`. Immutable manifest
artifact construction is owned by `FavnAuthoring.Deployment`.

## Primary code

- `apps/favn_local/lib/favn_local/config.ex`
- `apps/favn_local/lib/favn_local/lifecycle.ex`
- `apps/favn_local/lib/favn_local/runner_child.ex`
- `apps/favn_local/lib/favn_local/publication.ex`
- `apps/favn_local/lib/favn_local/locator.ex`

## Invariants

- development startup never invokes Docker;
- startup verifies but never mutates PostgreSQL schema/workspace setup;
- exactly one child runner OS process is owned at a time;
- reload compiles before replacement and binds a fresh release ID to the new
  manifest;
- environment, dependency, plugin, port, and database changes require a full
  stop/start;
- stop is idempotent and never deletes durable data;
- obsolete Docker-era `.favn/` state is rejected instead of silently reused.

## Verification

```bash
MIX_ENV=test mix do --app favn_local cmd mix test --no-compile \
  --exclude acceptance --exclude container --exclude slow --exclude browser
```
