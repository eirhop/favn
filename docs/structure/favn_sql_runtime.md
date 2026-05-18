# favn_sql_runtime

Purpose: shared SQL connection validation, SQL client/session runtime, admission
control, adapter bootstrap hooks, backend concurrency policy contracts, and safe
read-only relation inspection primitives used by local data preview flows.
SQL asset materialization planning is runner-owned; this app owns the shared
client/session primitives and `%Favn.SQL.WritePlan{}` adapter contract they use.

Code:
- `apps/favn_sql_runtime/lib/favn/connection/`
- `apps/favn_sql_runtime/lib/favn/sql/`
- `apps/favn_sql_runtime/lib/favn_sql_runtime*.ex`

Tests:
- `apps/favn_sql_runtime/test/`

Use when changing connection runtime config validation, SQL client behavior,
session lifecycle, transaction behavior, adapter bootstrap, read-only relation
inspection, write-plan adapter contracts, or SQL admission and concurrency policy
behavior.

Catalog-level admission is driven by materialization write plans whose target
relations include a catalog. Session bootstrap can also acquire catalog permits
when callers pass `required_catalogs: [...]` to the SQL client; this is how
DuckDB/DuckLake attach work is serialized before a session opens. Raw
`Favn.SQLClient.execute/3` and `Favn.SQLClient.query/3` do not parse arbitrary SQL
to infer write catalogs; callers that issue manual raw writes must handle any
needed serialization until the client contract grows an explicit target catalog
option.

Local `mix favn.query` read-only validation is a best-effort operator guardrail,
not a SQL sandbox or security boundary.
