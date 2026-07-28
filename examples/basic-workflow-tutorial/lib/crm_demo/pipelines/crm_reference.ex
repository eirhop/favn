defmodule CrmDemo.Pipelines.CrmReference do
  @moduledoc """
  Full refresh of everything that has no time dimension.

  A pipeline names targets; the asset graph decides the order. `deps(:all)`
  means selecting `AccountHealth` is enough to pull in the Core customer model,
  both Source relations, and both landing extractions.
  """

  use Favn.Pipeline

  alias CrmDemo.Warehouse.Mart.Sales.AccountHealth

  pipeline :crm_reference do
    asset(AccountHealth)
    deps(:all)
    max_concurrency(2)
    execution_pool(:duckdb)
  end
end
