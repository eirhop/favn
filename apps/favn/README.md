# `apps/favn`

Purpose:

- one public package users depend on (`{:favn, ...}`)
- own public facade and public `mix favn.*` entrypoints

Visibility:

- public package

Allowed dependencies:

- `favn_authoring`
- `favn_local`

Must not depend on:

- `favn_core` directly
- `favn_runner`, `favn_orchestrator`
- `favn_storage_postgres`, `favn_storage_sqlite`, `favn_duckdb`

Current status:

- thin wrapper package
- delegates authoring facade calls to `FavnAuthoring`
- delegates lifecycle task behavior to `Favn.Dev` (owned by `favn_local`)
