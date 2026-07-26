defmodule FavnReferenceWorkload.Warehouse.Mart.PipelineDaily do
  @moduledoc "Daily pipeline value by opportunity stage."

  use Favn.SQLAsset

  alias FavnReferenceWorkload.Warehouse.Core.OpportunitiesDaily

  relation(true)
  execution_pool(:local_duckdb)
  depends(OpportunitiesDaily)
  window(Favn.Window.daily(timezone: "Etc/UTC", required: true))
  coverage(from: ~D[2026-07-22], through: :latest_closed, availability_delay: {:hours, 1})
  materialized({:incremental, strategy: :delete_insert, window_column: :snapshot_date})
  freshness(window_success: true)
  meta(category: :pipeline, tags: [:mart, :daily, :incremental])

  contract do
    column(:snapshot_date, :date)
    column(:stage, :string)
    column(:deal_count, :integer)
    column(:pipeline_amount_cents, :integer)
    unique([:snapshot_date, :stage])
  end

  check :required_values_are_present, at: :before_materialize, on_violation: :fail do
    ~SQL"""
    select
      count(*) filter (
        where snapshot_date is null
          or stage is null
          or deal_count is null
          or pipeline_amount_cents is null
      ) = 0 as passed
    from query()
    """
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
