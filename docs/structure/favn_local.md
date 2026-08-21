# favn_local

Purpose: Docker-free source-development lifecycle behind the public
`mix favn.dev`, `mix favn.reload`, `mix favn.stop`, and `mix favn.doctor` tasks.

## Ownership

`favn_local` owns:

- validation of developer-supplied PostgreSQL and environment configuration;
- Orchestrator and View startup in the current BEAM;
- one child runner BEAM using the consumer's compiled code;
- reload sequencing, maintenance admission, and manifest deployment;
- building the current dependency-owned View assets before source startup;
- a small `.favn/local/` locator, child-BEAM resolver file, and owner-only local
  credentials, including the stable local View key needed to preserve browser
  sessions across restart.

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
- local operator, runner, and client nodes use the reserved `favn-local.test`
  alias; each BEAM maps it to loopback without external DNS or hosts-file changes;
- reload compiles first and derives the runner release ID from the compiled BEAM
  closure;
- an unchanged compiled closure and manifest is a no-op only while the expected
  deployment remains durably active; manifest-only changes deploy without
  replacing the runner; changed compiled code replaces the runner;
- reload registers only execution packages absent from durable storage and emits
  build, package, publication, activation, and total phase timings;
- environment, dependency, plugin, port, and database changes require a full
  stop/start;
- normal stop/start preserves the local View password and browser-session key;
  changing the configured workspace rotates the browser-session key;
- source development explicitly enables passwordless loopback View login;
- source startup resolves Heroicons and JavaScript packages from the consumer's
  active Mix dependency directory, builds CSS and JavaScript from the selected
  Favn checkout, and verifies the generated stylesheet exists and is non-empty
  before starting; it never serves stale ignored files left in
  `favn_view/priv/static`;
  other View boot paths retain normal authentication;
- stop is idempotent and never deletes durable data;
- obsolete Docker-era `.favn/` state is rejected instead of silently reused.

## Verification

```bash
MIX_ENV=test mix do --app favn_local cmd mix test --no-compile \
  --exclude acceptance --exclude container --exclude slow --exclude browser
```
