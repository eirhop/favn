<p align="center">
  <img src="docs/images/favn-logo-transparent.png" alt="Favn logo" width="220" />
</p>

<h1 align="center">Favn</h1>

<p align="center">
  Define business-oriented data assets in Elixir, model how they depend on each other, and turn them into predictable runs.
</p>

Favn is a manifest-first Elixir system for authoring data assets and pipelines,
compiling them into a deterministic execution contract, and running them through
a PostgreSQL-backed control plane. Asset logic stays in ordinary Elixir modules;
DuckDB and other data systems remain runner-owned integrations.

Canonical manifests are bound to the exact verified customer runner release;
changing executable user code therefore cannot be deployed as a manifest-only update.

Favn is private pre-v1 software. APIs may change, and the supported production
release artifacts are still being completed.

## Start here

- [Favn package and HexDocs landing page](apps/favn/README.md)
- [Getting started](apps/favn/guides/getting-started.md)
- [Authoring assets and pipelines](apps/favn/guides/authoring-assets.md)
- [Local development](apps/favn/guides/local-development.md)
- [Configuration](apps/favn/guides/configuration.md)
- [AI-assisted development](apps/favn/guides/ai-agents.md)

The complete public guide set lives in [`apps/favn/guides/`](apps/favn/guides/).
Repository contributors should use the [internal documentation map](docs/README.md).
Current capability and production work are tracked in
[`docs/FEATURES.md`](docs/FEATURES.md), [`docs/ROADMAP.md`](docs/ROADMAP.md), and
[`docs/production/`](docs/production/).
