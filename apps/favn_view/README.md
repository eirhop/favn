# FavnView

`favn_view` is the thin Phoenix/LiveView UI/API boundary for local Favn
tooling. It must call backend behavior only through the public orchestrator
facade.

## Local Tooling

- Start through the root local loop with `mix favn.dev`, or from this app with `mix phx.server`.
- The default local URL is `http://127.0.0.1:4173`.
- Tidewave is plugged only in dev. Use it from the local Phoenix endpoint; do not enable remote access unless you intentionally change local dev networking and understand Tidewave's security guidance.
- PhoenixStorybook is available at `/storybook` when dev routes are enabled.
- Stories live under `apps/favn_view/storybook/`. Add or update stories when adding reusable UI components.

Do not build product UI screens here unless explicitly requested.
