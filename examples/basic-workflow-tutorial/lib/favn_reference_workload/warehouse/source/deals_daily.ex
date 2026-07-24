defmodule FavnReferenceWorkload.Warehouse.Source.DealsDaily do
  @moduledoc "Daily-window source relation loaded from landing deals JSON."

  use Favn.SQLAsset

  alias FavnReferenceWorkload.Warehouse.Landing.WriteExtracts

  relation(true)
  depends({WriteExtracts, :deals_daily})
  window(Favn.Window.daily(timezone: "Etc/UTC", required: true))
  coverage(from: ~D[2026-07-22], through: :latest_closed, availability_delay: {:hours, 1})
  freshness(window_success: true)
  materialized({:incremental, strategy: :delete_insert, window_column: :occurred_at})
  meta(category: :deals, tags: [:source, :daily, :incremental])

  query do
    ~SQL"""
    select
      deal_id,
      account_id,
      stage,
      cast(amount_cents as bigint) as amount_cents,
      cast(occurred_at as timestamp) as occurred_at
    from read_json('.data/generic_crm/landing/deals.json', auto_detect = true)
    where cast(occurred_at as timestamp) >= @window_start
      and cast(occurred_at as timestamp) < @window_end
    """
  end
end
