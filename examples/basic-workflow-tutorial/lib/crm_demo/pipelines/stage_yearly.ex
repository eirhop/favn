defmodule CrmDemo.Pipelines.StageYearly do
  @moduledoc """
  The year-grained stage rollup, on its own yearly anchor.

  See `CrmDemo.Pipelines.StageHourly` for why each grain gets its own pipeline.
  """

  use Favn.Pipeline

  alias CrmDemo.Warehouse.Mart.Sales.StageYearly

  pipeline :stage_yearly do
    assets([StageYearly])
    deps(:none)
    window(:yearly, anchor: :previous_complete_period, timezone: "Etc/UTC")
    max_concurrency(1)
    execution_pool(:duckdb)
  end
end
