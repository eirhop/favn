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
session lifecycle, native script planning, transaction behavior, adapter bootstrap, read-only relation
inspection, write-plan adapter contracts, or SQL admission and concurrency policy
behavior.

Adapters may implement `materialize_in_transaction/3` so the runner can execute
a generated write plan inside an already-active checked-materialization
transaction without nesting adapter transactions. Checked transactions are
never retried, retain bounded body diagnostics on commit failure, and remain
mutation paths that are discarded rather than returned to an idle pool.

SQL session pooling is default-on for poolable DuckDB/ADBC adapters and can be
disabled with `pool: [enabled: false]`. Optional tuning is connection-level:
`pool: [enabled: true, max_idle_per_key: 1, idle_timeout_ms: 300_000]`. Pooling
keeps warm sessions only inside the current runner BEAM. Pool reuse must be keyed
by connection identity/config hash, required catalog/resource sets, and adapter fingerprint.
DuckDB adapters extend that fingerprint with selected file content and parameter
fingerprints.
Deferred `Favn.RuntimeValue` session-script parameters resolve immediately
before plan rendering. Secret refs are redacted like secret environment refs,
and the resolved value contributes only a hash to session-pool identity. A
credential refresh therefore selects a different pool key rather than reusing a
session initialized with the old credential. The pool tracks the latest adapter
fingerprint for each stable connection/requirements scope, evicts superseded
idle sessions, and closes superseded active sessions on checkin. This releases
finite admission leases before replacement bootstrap.
Checked-out sessions remain exclusive to one asset execution at a time; the pool
is not a distributed coordinator and does not increase configured write/catalog
concurrency. The SQL client enforces checkout ownership, so copied session structs
cannot be operated on or disconnected by non-owner processes. Raw
execute/materialize/transaction paths discard pooled sessions after mutation
unless explicitly proven pool-safe internally.
When concurrent work misses the same pool key, Favn may create multiple fresh
sessions in parallel up to the selected finite admission/catalog limit. This keeps
fresh connection-per-write paths efficient without making arbitrary raw SQL
session reuse safe. If the relevant policy is unlimited, same-key fresh creation
stays conservative unless a finite catalog policy such as DuckLake
`write_concurrency` is configured.
Idle pooled sessions retain catalog admission until reuse or eviction. That keeps
physical pooled sessions inside the configured catalog budget. Superseded
fingerprints in the same stable pool scope are evicted automatically; unrelated
incompatible scopes can still compete for the same finite catalog capacity.

Catalog-level admission is driven by materialization write plans whose target
relations include a catalog. Session bootstrap can also acquire catalog permits
when callers pass `required_catalogs: [...]` to the SQL client; SQL assets also
pass versioned `required_resources`. The normalized sets are retained on the SQL
session. Raw write operations such as
`Favn.SQLClient.execute/3`, write-style `Favn.SQLClient.query/3`, and
`Favn.SQLClient.transaction/3` use an explicit `admission: [...]` operation
target when one is provided, otherwise they use the session required catalog
scope. Multi-catalog raw writes acquire the configured catalog policies in
deterministic order. Favn still does not parse arbitrary SQL to infer write
catalogs. When
`required_catalogs` is omitted for a connection with configured catalog policies,
session bootstrap is treated as all-catalog bootstrap and must acquire every
configured catalog permit before opening the adapter session.
Elixir assets executed by the runner get a process-local default scope from
their owned relation catalog when they open that same relation connection without
passing `required_catalogs` explicitly. Spawned asset tasks do not inherit that
process-local default; wrap child process bodies with
`Favn.SQLClient.with_required_catalogs/2` or pass `required_catalogs` explicitly.

DuckLake catalogs backed by PostgreSQL metadata can use multiple PostgreSQL
backend connections per admitted DuckLake writer. Observed deployments used about
three PostgreSQL backends per concurrent writer, so operators should size
DuckLake `write_concurrency` with that multiplier and leave headroom for admin
tools, migrations, monitoring, and other traffic.
DuckDB extension settings are native startup/resource SQL and complement, but
do not replace, Favn admission. Total metadata pressure is a product of admitted
Favn work, DuckLake catalog concurrency, DuckDB parallelism, attached
Postgres-backed catalogs, and each catalog's pool limit.

Retry handling must stay operation-aware. Bounded retries are acceptable around
session creation/bootstrap and read-only inspection/query paths. Blind retries of
SQL writes are not safe, and unknown commit state must be surfaced rather than
retried.

Pool checkout, admission waits, and SQL operations use finite default timeouts.
`%Favn.SQL.Deadline{}` carries per-operation budgets through query, execute,
materialize, transaction, and inspection paths. SQL operation timeouts return a
typed `%Favn.SQL.Error{type: :operation_timeout}` with `unknown_outcome?: true`
and discard pooled sessions rather than returning potentially mutated native
state to the idle pool.
`%Favn.SQL.CancelToken{}` is the SQL-owned cancellation context passed toward
adapter operations. Adapters may ignore it when native cancellation is
unsupported, but runner/orchestrator outcomes must then preserve native
uncertainty instead of claiming data-plane cancellation certainty.

Local `mix favn.query` read-only validation is a best-effort operator guardrail,
not a SQL sandbox or security boundary. `mix favn.inspect` and `mix favn.query`
start `:favn_sql_runtime` before connecting so `Favn.SQL.SessionPool` is
available for direct CLI inspection.
