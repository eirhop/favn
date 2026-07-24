# favn_local

`favn_local` owns the Docker-free source-development lifecycle behind:

```bash
mix favn.dev
mix favn.reload
mix favn.stop
mix favn.doctor
```

It starts the Orchestrator and View in the current BEAM and one child runner
BEAM. It owns only a small locator under `.favn/local/` so later Mix commands can
reach the running process.

It does not:

- install, start, stop, migrate, or delete PostgreSQL;
- parse `.env` files;
- run Docker or Compose;
- build control-plane or runner images;
- scaffold a mutable local stack;
- silently provision workspaces.

The caller supplies `FAVN_DATABASE_URL` and
`FAVN_RUNTIME_INPUT_PIN_KEY`, migrates PostgreSQL, and provisions the selected
workspace explicitly. See the public
[Local Development guide](../favn/guides/local-development.md).

Topology-neutral operator clients live under `Favn.CLI`. Manifest deployment
artifacts live under `FavnAuthoring.Deployment`. Production images and Compose
files are not owned by this application.
