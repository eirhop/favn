# FavnView

`favn_view` is the thin Phoenix/LiveView UI/API boundary for local Favn
tooling. It must call backend behavior only through the public orchestrator
facade.

## Local Tooling

- Use `mix favn.dev` for the Docker-free source-development runtime.
- Maintainers may run
  `FAVN_DATABASE_URL=<local-postgres-url> FAVN_RUNTIME_INPUT_PIN_KEY=<32-byte-key> mix phx.server`
  from the umbrella root for source-level Phoenix and Tidewave inspection. This
  is not a deployment example.
- The default local URL is `http://127.0.0.1:4173`.
- Tidewave is plugged only in dev. Use it from the local Phoenix endpoint; do not enable remote access unless you intentionally change local dev networking and understand Tidewave's security guidance.
- PhoenixStorybook is available at `/storybook` when dev routes are enabled.
- Stories live under `apps/favn_view/storybook/`. Add or update stories when adding reusable UI components.
- Run detail pages expose active-run cancellation through the public
  `FavnOrchestrator` facade. Do not call storage, runner, `RunManager`, or
  `RunServer` directly from UI code.

Do not build product UI screens here unless explicitly requested.
