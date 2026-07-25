defmodule FavnReferenceWorkload.Warehouse.Landing.GenerateSeed do
  @moduledoc "Full-refresh Elixir asset that creates deterministic CRM seed data."

  use Favn.Asset

  alias FavnReferenceWorkload.{CRMData, LandingFiles}

  meta(category: :crm_seed, tags: [:landing, :full_refresh])
  freshness(:always)

  def asset(_ctx) do
    seed = CRMData.seed()
    :ok = LandingFiles.write_seed!(seed)

    {:ok,
     %{
       mode: "full_refresh",
       rows_written: Enum.reduce(seed, 0, fn {_entity, rows}, total -> total + length(rows) end),
       landing_path: ".data/generic_crm/landing/crm_seed.json"
     }}
  end
end
