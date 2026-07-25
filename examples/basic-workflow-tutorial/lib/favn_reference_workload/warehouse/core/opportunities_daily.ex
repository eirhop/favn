defmodule FavnReferenceWorkload.Warehouse.Core.OpportunitiesDaily do
  @moduledoc "Daily system-agnostic opportunity model."

  use Favn.SQLAsset

  alias FavnReferenceWorkload.Warehouse.Source.{Accounts, DealsDaily}

  relation(true)
  execution_pool(:local_duckdb)
  depends(Accounts)
  depends(DealsDaily)
  window(Favn.Window.daily(timezone: "Etc/UTC", required: true))
  coverage(from: ~D[2026-07-22], through: :latest_closed, availability_delay: {:hours, 1})
  materialized({:incremental, strategy: :delete_insert, window_column: :occurred_at})
  meta(category: :opportunities, tags: [:core, :daily, :incremental])

  check :required_keys_are_present, at: :before_materialize, on_violation: :fail do
    ~SQL"""
    select
      count(*) filter (where deal_id is null or occurred_at is null) = 0 as passed
    from query()
    """
  end

  query do
    ~SQL"""
    select
      deals.deal_id,
      deals.account_id,
      accounts.segment,
      deals.stage,
      deals.amount_cents,
      deals.occurred_at
    from source.deals_daily as deals
    inner join source.accounts as accounts
      on accounts.account_id = deals.account_id
    where deals.occurred_at >= @window_start
      and deals.occurred_at < @window_end
    """
  end
end
