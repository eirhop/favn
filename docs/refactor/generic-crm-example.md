# Temporary CRM Example Implementation Note

Status: temporary documentation for the UI-example work. This page records the
intended graph while the example is being rebuilt; the tutorial README and
executable asset modules remain the source of truth for runnable behavior.

## Purpose

The standalone example should provide a small, deterministic CRM workload that
can be run locally and inspected in the Favn UI. It uses DuckDB for the data
plane and the normal local PostgreSQL control-plane workflow.

## Layer contract

| Layer | Ownership | Relations/files |
| --- | --- | --- |
| Landing | Elixir assets | Compact JSON under `.data/generic_crm/landing/` |
| Source | Favn SQL assets | `source.accounts`, `source.contacts`, `source.deals_daily`, `source.activities_daily` |
| Core | Favn SQL assets | `core.customers`, `core.opportunities_daily`, `core.activities_daily` |
| Mart | Favn SQL assets | `mart.account_health`, `mart.pipeline_daily`, `mart.executive_overview` |

## Feature coupling

- `Landing.GenerateSeed` is an ordinary full-refresh `Favn.Asset`.
- `Landing.WriteExtracts` is a `Favn.MultiAsset`; snapshot children are full
  refresh and deal/activity children use required daily windows.
- Daily Source, Core, and Mart assets use delete-insert incremental tables with
  `occurred_at` or `snapshot_date` as their window column.
- Daily assets declare coverage and window-success freshness. Snapshot assets
  use `freshness :always`.
- `Mart.AccountHealth` and `Mart.PipelineDaily` declare typed output contracts
  with grains, non-null columns, unique keys, and a minimum-row claim.
- `Mart.AccountHealth` has a transactional candidate check.
- `FavnReferenceWorkload.Pipelines.BootstrapCrmDemo` demonstrates a full-refresh
  target. `FavnReferenceWorkload.Pipelines.DailyCrmAnalytics` demonstrates
  scheduled daily windows, lookback, missed occurrence handling, overlap
  protection, execution-pool admission, and UI outputs.
- Automatic retries are intentionally not configured for local file writes or
  DuckDB materializations because an unknown write outcome must be inspected.
- `Favn.Source` is not used for these Source relations: it is reserved for
  relations managed outside Favn, while this example owns JSON loading.

## Temporary limitation

The landing directory is intentionally fixed for local UI work. It is not a
production external-storage contract and should be replaced by a runtime-input
or source-system integration when this example becomes a deployment guide.
