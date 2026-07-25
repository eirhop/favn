# Generic CRM Example CLI QA

Status: temporary implementation and exploratory QA record. This is not a
product contract.

## Scope

- Test `examples/basic-workflow-tutorial` as a CLI user.
- Use PostgreSQL for the local control plane and one DuckDB physical session for
  the data plane.
- Verify `mix favn.reload`, pipeline and asset submission, repeated runs, exact
  data results, contracts, and positive/negative quality checks.
- UI redesign and UI QA are intentionally out of scope.

## Verified scenarios

| Scenario | Result |
| --- | --- |
| Start and stop the local stack | Passed with the configured `favn_runtime` PostgreSQL role. |
| `mix favn.reload` | Passed; reload published a new runner release and manifest. |
| Forced bootstrap pipeline | Passed after final review as `run_api_efaade2b0341b108bd537650b662df20`. |
| Forced daily pipeline for `2026-07-23` | Passed after final review as `run_api_c5a965166bce74973b4d337683e0f1d4`. |
| Repeat the same forced daily pipeline | Passed; the one-session DuckDB pool serialized concurrent assets and reused the session. |
| Run `Core.ActivitiesDaily` with all forced upstream dependencies | Passed as `run_api_9bb1de3d7fdee29af0e76066cfb8f5c8`. |
| Use documented plain module shorthand for an asset target | Passed. |
| Query final marts with `mix favn.query` | Passed after stopping the long-running stack so the standalone command could acquire DuckDB's file lock. |
| Run the example's ExUnit suite directly | Passed: 6 tests. Test config uses a deterministic source-runner release identity; the suite includes disposable DuckDB output and negative quality-check coverage. |
| Positive contracts and checks | Passed for incremental Source/Core assets and Mart contracts/checks. |
| Negative quality check | Passed: a temporary null `activity_id` made `run_api_d5299d59a49180158872d04a7afd3579` fail with `check_failed` before materialization. The file was restored and `run_api_553f5b5b05ea69d1ba511a6cb570d1ec` passed. |

## Expected data

For the `2026-07-23` daily window:

- `mart.executive_overview`: 3 customers, 2 deals, 53,000 cents.
- `mart.pipeline_daily`: proposal 45,000 cents/1 deal; won 8,000 cents/1 deal.
- `mart.account_health`: 3 engaged accounts, each with one contact.
- `core.activities_daily`: 2 activities.

## Favn defects found and fixed

| Defect | Cause | Fix |
| --- | --- | --- |
| Concurrent DuckDB assets timed out despite a one-session pool. | Pool creation capacity counted creators but not an already checked-out session. | Count active same-key sessions against creation capacity and wait for check-in. |
| Replacing a successful DuckDB write session could race the driver's file-lock release. | The shared SQL client conservatively discarded every successful controlled materialization session, even though DuckDB can reset and safely reuse it. | Let adapters explicitly declare controlled operations safe for pooled reuse; arbitrary SQL still discards, and checked transactions require both an internal request and a DuckDB capability grant. |
| Forced asset refresh lost its selected refs after persistence. | Run snapshot metadata did not decode refresh-policy refs back to canonical tuples. | Normalize refresh policy metadata during snapshot decoding. |
| Asset runs with `dependencies: all` executed only the target. | Manual asset runs always selected the sequential target-only executor. | Use staged graph execution for manual plans with all dependencies. |
| Documented asset module shorthand was rejected. | CLI matching required internal `Elixir.`-prefixed manifest labels. | Match plain default and named module shorthand against canonical asset targets. |
| Executive overview multiplied pipeline totals by account count. | The view cross-joined row-level account health before aggregation. | Aggregate account count in a scalar subquery before joining pipeline metrics. |

## Known CLI constraint

`mix favn.query` is a standalone SQL process. With a file-backed DuckDB database,
it cannot open the file while `mix favn.dev` owns the single physical
connection. Stop the stack before using the standalone query command.
