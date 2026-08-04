defmodule CrmDemo.Pipelines.StageMonthly do
  @moduledoc """
  The month-grained stage rollup, on its own monthly anchor.

  See `CrmDemo.Pipelines.StageHourly` for why each grain gets its own pipeline.
  """

  use Favn.Pipeline

  alias CrmDemo.Warehouse.Mart.Sales.StageMonthly

  pipeline :stage_monthly do
    assets([StageMonthly])
    deps(:none)
    window(:monthly, anchor: :previous_complete_period, timezone: "Etc/UTC")
    max_concurrency(1)
    execution_pool(:duckdb)
  end
end
