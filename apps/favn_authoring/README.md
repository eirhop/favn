# `apps/favn_authoring`

Purpose:

- internal authoring implementation ownership for the public `favn` package

Visibility:

- internal

Allowed dependencies:

- `favn_core`

Must not depend on:

- `favn_runner`, `favn_orchestrator`
- `favn_storage_postgres`, `favn_storage_sqlite`, `favn_duckdb`

Current status:

- owns authoring/manifest-facing implementation modules
- exports `FavnAuthoring` as the internal facade used by the public wrapper package
