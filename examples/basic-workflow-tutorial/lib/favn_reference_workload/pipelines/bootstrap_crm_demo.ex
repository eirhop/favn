defmodule FavnReferenceWorkload.Pipelines.BootstrapCrmDemo do
  @moduledoc "Temporary full-refresh CRM demo pipeline."

  use Favn.Pipeline

  pipeline :bootstrap_crm_demo do
    asset(FavnReferenceWorkload.Warehouse.Mart.AccountHealth)
    deps(:all)
    max_concurrency(2)
    execution_pool(:local_duckdb)
    outputs([:account_health])
    meta(%{purpose: "full_refresh_demo", layer: "mart"})
  end
end
