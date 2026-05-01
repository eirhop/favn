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
