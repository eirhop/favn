defmodule FavnReferenceWorkload.Warehouse.Mart.ExecutiveOverview do
  @moduledoc "Compact view joining the demo's most useful UI-facing metrics."

  use Favn.SQLAsset

  alias FavnReferenceWorkload.Warehouse.Mart.{AccountHealth, PipelineDaily}

  relation(true)
  depends(AccountHealth)
  depends(PipelineDaily)
  window(Favn.Window.daily(timezone: "Etc/UTC", required: true))
  materialized(:view)
  meta(category: :executive, tags: [:mart, :daily, :ui])

  query do
    ~SQL"""
    select
      pipeline.snapshot_date,
      sum(pipeline.pipeline_amount_cents) as pipeline_amount_cents,
      sum(pipeline.deal_count) as deal_count,
      health.customer_count
    from mart.pipeline_daily as pipeline
    cross join (
      select count(*) as customer_count
      from mart.account_health
    ) as health
    group by pipeline.snapshot_date, health.customer_count
    """
  end
end
