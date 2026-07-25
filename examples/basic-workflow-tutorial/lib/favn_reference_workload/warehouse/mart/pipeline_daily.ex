defmodule FavnReferenceWorkload.Warehouse.Mart.PipelineDaily do
  @moduledoc "Daily pipeline value by opportunity stage."

  use Favn.SQLAsset

  alias FavnReferenceWorkload.Warehouse.Core.OpportunitiesDaily

  relation(true)
  depends(OpportunitiesDaily)
  window(Favn.Window.daily(timezone: "Etc/UTC", required: true))
  materialized({:incremental, strategy: :delete_insert, window_column: :snapshot_date})
  freshness(window_success: true)
  meta(category: :pipeline, tags: [:mart, :daily, :incremental])

  contract do
    grain(by: [:snapshot_date, :stage], description: "one pipeline row per day and stage")
    column(:snapshot_date, :date, null: false)
    column(:stage, :string, null: false)
    column(:deal_count, :integer, null: false)
    column(:pipeline_amount_cents, :integer, null: false)
    unique([:snapshot_date, :stage])
  end

  query do
    ~SQL"""
    select
      cast(occurred_at as date) as snapshot_date,
      stage,
      count(*) as deal_count,
      sum(amount_cents) as pipeline_amount_cents
    from core.opportunities_daily
    where occurred_at >= @window_start
      and occurred_at < @window_end
    group by cast(occurred_at as date), stage
    """
  end
end
