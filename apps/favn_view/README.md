# FavnView

`favn_view` is the thin Phoenix/LiveView UI/API boundary for local Favn
tooling. It must call backend behavior only through the public orchestrator
facade.

## Local Tooling

- Start the development server from the umbrella root. See
  [`docs/contributing/dev-server.md`](../../docs/contributing/dev-server.md) for the
  command, the required environment variables, and why the other roots do not work.
- `/design-system` and `/tidewave/mcp` exist only on that server. The design
  system lives in `apps/favn_view/dev/`, which is outside `elixirc_paths` for
  `:prod`, and Tidewave is a dev-only dependency, so a separate project depending
  on `favn` has neither. Use `mix favn.dev` in a Favn project to check the UI
  against real data.
- The default local URL is `http://127.0.0.1:4173`.
- Tidewave is plugged only in dev. Do not expose it beyond the local interface
  unless you intentionally change local dev networking and understand Tidewave's
  security guidance.
- The design system discovers components by reflection, so a new component needs
  no registration. Add a curated example in
  `apps/favn_view/dev/design_system/examples/`; `/design-system` lists the
  components that have none.
- Run detail pages expose active-run cancellation through the public
  `FavnOrchestrator` facade. Do not call storage, runner, `RunManager`, or
  `RunServer` directly from UI code.

Do not build product UI screens here unless explicitly requested.
