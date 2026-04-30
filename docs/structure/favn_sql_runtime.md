# favn_sql_runtime

Purpose: shared SQL connection validation, SQL client/session runtime, admission
control, adapter bootstrap hooks, backend concurrency policy contracts, and safe
read-only relation inspection primitives used by local data preview flows.

Code:
- `apps/favn_sql_runtime/lib/favn/connection/`
- `apps/favn_sql_runtime/lib/favn/sql/`
- `apps/favn_sql_runtime/lib/favn_sql_runtime*.ex`

Tests:
- `apps/favn_sql_runtime/test/`

Use when changing connection runtime config validation, SQL client behavior,
session lifecycle, transaction behavior, adapter bootstrap, read-only relation
inspection, or SQL admission and concurrency policy behavior.
