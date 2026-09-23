# Query runtime metadata

Favn automatically publishes runtime metadata for managed SQL tables executed
through the native DuckDB ADBC adapter, including DuckLake tables. No asset list,
schema setting, semantic model or CI publication is required. Views, Elixir
assets, direct SQL client calls and remote transports are outside this contract.

Metadata lives in `favn_runtime` in the table's write catalog. For example,
`mart.sales.daily` publishes into `mart.favn_runtime`. An omitted catalog uses
the actual session write catalog. The schema is reserved for Favn; incompatible
existing objects cause the asset transaction to fail before changing its data.
The runner needs permission to create and write these objects. Unqualified
relations must resolve unambiguously in the session search path. If multiple
catalogs are eligible, qualify the existing relation's catalog and schema;
Favn rejects the ambiguous write before changing data.

## Freshness without extra runs

```sql
SELECT asset_ref, published_at, fresh_until, time_freshness,
       quality_status, coverage_support
FROM mart.favn_runtime.freshness;
```

An asset published at 02:00 with `max_age: 6 hours` has `fresh_until = 08:00`.
The view compares that timestamp with the query time. It becomes expired after
08:00 without a job updating a boolean. Maximum-age equality is fresh; calendar
period boundaries are exclusive. Calendar expiry uses the period pinned when
work was planned, including its timezone, rather than the completion date.

`time_freshness` is `fresh`, `expired`, `unknown` (no policy), or `always`
(an always-run policy). Window-success policies without a refresh cadence have
no time deadline. Time freshness alone does not prove complete coverage or
passing checks. Missing rows mean no supported publication evidence exists.

## Checks and coverage

```sql
SELECT asset_ref, check_name, phase, outcome
FROM mart.favn_runtime.check_result;

SELECT target_id, window_kind, timezone, start_at, end_at, publication_id
FROM mart.favn_runtime.coverage
ORDER BY target_id, start_at;
```

Quality is separate from time freshness. `not_checked` means no checks were
evaluated; skipped checks are not a pass. Check rows expose names, phases and
outcomes, not raw SQL or arbitrary result metrics.

Coverage contains exact successfully published windows. January and March
produce two rows; they do not imply February exists. A full table replacement
clears older window evidence. Group replacement publishes a receipt and checks,
but reports window coverage as `unsupported` and clears prior window evidence.
Check `coverage_support` before interpreting an empty coverage result.

## Publication and failure behavior

The data change, immutable contract snapshot, receipt and current metadata
commit in the same SQL transaction. A metadata or failed-check error rolls back
the data change. A no-op creates no receipt. Existing write admission still
applies.

For ordinary managed DuckLake materialization, Favn retries a commit only when
the native adapter proves that DuckLake rejected the entire transaction and
rollback cleanup is confirmed. It makes at most four total attempts, each on a
fresh session, under one deadline. Delays are 50, 100 and 200 milliseconds plus
up to the same amount of jitter. Data, staging, checks and runtime metadata are
rebuilt together; only the successful attempt publishes results. Exhaustion is
a known rolled-back failure and does not enable another node retry.

Eligibility requires a matching pinned publication and one generated persistent
write destination in a native DuckLake catalog. Additional input catalogs are
allowed. Query and check SQL must be read-only and must not invoke external side
effects; this is a trusted authoring contract, not a SQL parser guarantee.
Session setup must remain idempotent. Raw SQL callbacks, standalone SQL client
calls, generation candidates and activation do not participate. A lost commit
acknowledgement, timeout during a write or uncertain rollback remains unknown
and stops retries. Deploying this behavior does not repair historical unknown
runner tasks.
First introduction of a target or contract uses a shared schema revision guard
because DuckLake has no unique constraints. Different first-time publications
can therefore conflict; established publications use their target revision.
DuckLake can also reject concurrent updates to different rows in a shared
metadata table. Keep the existing catalog write-admission limit appropriate
for the backend; a conflict rolls back the complete publication.

Candidate rebuild metadata stays hidden from the current views until the
existing generation activation transaction switches the table and metadata
together. `publication` retains immutable history and original write-relation
provenance; `contract_snapshot` stores deduplicated contract documents. History
is retained without automatic pruning in this version.

If commit acknowledgement is lost, Favn keeps its existing unknown-outcome
state. A receipt can help an operator diagnose a committed target write, but
does not automatically settle the run or authorize another mutation. These
views describe the last published target data, not active manifest membership,
failed attempts or current orchestrator run status. Removing an asset does not
delete its historical metadata.

Deployments enforce the new execution contract before old tasks can start and
reject an old contract downgrade. Retained native targets cannot be silently
reintroduced through an unsupported writer or moved to another destination.
Resolve existing uncertain writes through the normal recovery workflow before
activating a deployment that requires tracking.

Static definitions and semantic macros remain a separate
[CI publication workflow](sql-catalog-publication.md).
