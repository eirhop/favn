defmodule CrmDemo.Warehouse.Source.Crm.Inputs do
  @moduledoc """
  Selects the landing manifest one Source relation should publish.

  The file list cannot be known when the manifest is compiled - it depends on
  which landing run finished. A runtime-input resolver is the contract for that:
  it runs once per node, after the window is final and before any SQL, and Favn
  pins its result so retries and replays reuse the exact same input.

  It returns bind values only. It cannot generate SQL, table names, or
  identifiers.
  """

  @behaviour Favn.SQLAsset.RuntimeInputs

  alias CrmDemo.Support.Landing.Storage
  alias Favn.SQLAsset.RuntimeInputs.Result

  @impl true
  def resolve(ctx) do
    dataset = ctx.asset.settings.landing_dataset
    manifest = Storage.latest_manifest!(dataset, ctx.window)

    {:ok,
     %Result{
       params: %{
         files_json: Jason.encode!(manifest.files),
         landing_run_id: manifest.landing_run_id,
         extracted_at: manifest.extracted_at,
         expected_row_count: manifest.row_count
       },
       identity: manifest.landing_run_id,
       metadata: %{
         dataset: dataset,
         part_count: length(manifest.files),
         rows_landed: manifest.row_count
       }
     }}
  end
end
