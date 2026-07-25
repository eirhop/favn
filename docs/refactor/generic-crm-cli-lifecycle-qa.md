# Generic CRM CLI Lifecycle QA

Status: temporary manual QA record, last run 2026-07-25. This document records
implementation evidence and is not a public product contract.

## Scope

- Exercise `examples/basic-workflow-tutorial` as a CLI user; UI is out of scope.
- Use only the local PostgreSQL control plane and the example's one-slot
  DuckDB execution pool.
- Use a fresh workspace and DuckDB path for clean rebuild evidence.
- Treat a negative test as passing only when persisted state and CLI output
  match the expected safety contract.

## Example coverage

| Concern | Example asset or pipeline |
| --- | --- |
| Full refresh | `Pipelines.BootstrapCrmDemo`, landing extracts, Source.Accounts, Core.Customers, Mart.AccountHealth |
| Daily windows and lookback | `Pipelines.DailyCrmAnalytics`, daily Source/Core/Mart assets |
| Data contracts and checks | Explicit Source/Core/Mart contracts and Mart.AccountHealth candidate check |
| Retry | Side-effect-free `Lifecycle.RetryProbe` with an authored three-attempt policy |
| Cancellation | Side-effect-free 30-second `Lifecycle.CancellationProbe` |
| Schedules | Ten-second `Pipelines.LifecycleScheduleProbe`, inactive by default |
| Source schema change | Compile-time `CRM_EXAMPLE_SCHEMA_VERSION=v1|v2` account contract |
| Rebuild | Managed Source.Accounts contract changes from three to four columns |

## Manual results

| Scenario | Status | Evidence |
| --- | --- | --- |
| Reload and bootstrap | Pass | Clean-workspace V1 run `run_api_c86a593a60e4fceef42bb160fc52be20` finished `ok`. |
| Default daily window | Pass | `run_api_98559d93cdfea57c5cee06149a70c14e` selected the latest complete day and finished `ok`. |
| Exact daily windows | Pass | Post-rebuild runs `run_api_ecde0e18e580e8b5a8b3db1050f68b4c` and `run_api_7e83cbc8c7d3dc1c82280d60d4f6d507` materialized 2026-07-22 and 2026-07-23 independently. |
| Backfill dry run and submit | Pass | Dry run created no run; submission expanded the two-day range into two successful exact child windows. |
| Authored retry | Pass | Probe finished `ok` on attempt 3 with policy source `asset`. |
| Operator retry override | Pass | `--retry-max-attempts 2` replaced the authored policy and produced the expected terminal `error` after two attempts. |
| Run cancellation | Pass | Active cancellation persisted intent and acknowledgement; repeated cancellation remained safe and terminal state was `cancelled`. |
| Scheduler disabled | Pass | Starting without `--scheduler` created no scheduled probe occurrences. |
| Schedule activation | Pass | Newly published schedule was inactive, preview showed the intended occurrence, activation created successful previous-complete-day runs, and deactivation stopped new submissions. |
| Additive landing schema | Pass | V2 landing wrote three account rows with non-null `industry`; the active V1 managed generation remained readable before rebuild. |
| Rebuild admission | Pass | Ordinary V2 writes to Source.Accounts were rejected as `rebuild_required`. |
| Stale rebuild plan | Pass | Starting a plan after its manifest input changed was rejected without activating a candidate. |
| Rebuild execution | Pass | Plan `rebuild_api_250e84650040123a5190bc65d890f933` executed all 12 items and finished `succeeded`. |
| Terminal rebuild guards | Pass | Cancel and retry against the successful operation both returned the expected HTTP 409 conflict. |
| Post-rebuild pipelines | Pass | Bootstrap `run_api_2f202f26de28e2e70681cb3d776a395a` and default daily `run_api_d769b7539c5da4547c58652c3ade9419` finished `ok`. |
| DuckDB result inspection | Pass | Source.Accounts, Core.Customers, and Mart.AccountHealth each contained 3 rows; daily facts covered both fixture dates with expected totals. |

## Required schema/rebuild sequence

The schema version is read at compile time. Switching versions without forcing
compilation can publish a manifest that does not match the packaged runner code.

```powershell
$env:CRM_EXAMPLE_SCHEMA_VERSION = 'v2'
mix compile --force
mix favn.reload

mix favn.run FavnReferenceWorkload.Warehouse.Landing.WriteExtracts:accounts_snapshot --dependencies all --refresh force_all
mix favn.rebuild plan FavnReferenceWorkload.Warehouse.Source.Accounts --reason "exercise additive CRM account contract"
mix favn.rebuild start PLAN_ID --plan-hash PLAN_HASH
mix favn.rebuild status OPERATION_ID
```

The V2 landing refresh must happen before rebuild execution because
Source.Accounts now selects `industry`. Rebuilding against a V1 landing file
correctly fails instead of inventing the missing input.

## Data assertions

- 2026-07-22 contains one deal worth 12,000 cents.
- 2026-07-23 contains two deals worth 53,000 cents.
- Source.Accounts and Core.Customers contain three distinct accounts.
- V2 Source.Accounts contains non-null industries `software`, `healthcare`, and
  `retail`.
- Mart.AccountHealth contains three rows after rebuild.

Stop Favn before opening the same database through another DuckDB client.

## Product defects found and fixed

1. Local CLI roles were exact rather than hierarchical, so an admin token could
   not call operator endpoints used by backfill and rebuild.
2. Cancellation intent was not durable and repeatable.
3. Local scheduler configuration used a different workspace option from the
   persistence scheduler.
4. Newly published schedules started active instead of requiring explicit
   activation.
5. Compact CLI lifecycle lists and schedule previews assumed shapes not returned
   by the API.
6. Manual daily pipelines required an explicit window even though the pipeline
   already declared the previous-complete-period anchor.
7. Rebuild plans could appear stale because database ordering and timestamp
   precision differed from their in-memory draft.
8. Compact rebuild execution dropped non-persisted semantic input assets.
9. Safe local setup failures were reported as unknown post-submit outcomes,
   preventing safe rebuild progression.
10. `favn.query` introduced a second DuckDB ownership path and was removed; use
    a DuckDB client after stopping Favn.

## Experience notes

The happy path is now coherent from the CLI: reload, current-window run,
historical exact runs, backfill, retry, cancellation, schedule
activation/deactivation, schema detection, and rebuild all preserve explicit
state and produce inspectable results.

One local restart produced a transient invalid runner-cookie startup failure.
Stopping the partial stack and starting again succeeded immediately. It was not
reproduced and is recorded as an observation rather than a confirmed defect.
