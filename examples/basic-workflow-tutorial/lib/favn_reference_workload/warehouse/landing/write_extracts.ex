defmodule FavnReferenceWorkload.Warehouse.Landing.WriteExtracts do
  @moduledoc "Multiasset that writes entity JSON files to the fixed CRM landing path."

  use Favn.MultiAsset

  alias FavnReferenceWorkload.{CRMData, LandingFiles}

  meta(category: :crm_extracts, tags: [:landing, :multiasset])
  settings(format: "compact_json")
  execution_pool(:local_duckdb)
  depends({FavnReferenceWorkload.Warehouse.Landing.GenerateSeed, :asset})

  asset :accounts_snapshot do
    description("Write the full account snapshot.")
    settings(entity: :accounts, mode: "full_refresh")
    freshness(:always)
    window(nil)
    coverage(nil)
  end

  asset :contacts_snapshot do
    description("Write the full contact snapshot.")
    settings(entity: :contacts, mode: "full_refresh")
    freshness(:always)
    window(nil)
    coverage(nil)
  end

  asset :deals_daily do
    description("Write the deal records for the selected daily window.")
    settings(entity: :deals, mode: "daily_window")
    window(Favn.Window.daily(timezone: "Etc/UTC", required: true))
    coverage(from: ~D[2026-07-22], through: :latest_closed, availability_delay: {:hours, 1})
    freshness(window_success: true)
  end

  asset :activities_daily do
    description("Write the CRM activities for the selected daily window.")
    settings(entity: :activities, mode: "daily_window")
    window(Favn.Window.daily(timezone: "Etc/UTC", required: true))
    coverage(from: ~D[2026-07-22], through: :latest_closed, availability_delay: {:hours, 1})
    freshness(window_success: true)
  end

  def asset(ctx) do
    entity = normalize_entity(ctx.asset.settings.entity)
    mode = ctx.asset.settings.mode
    rows = rows_for(entity, mode, ctx.window)
    :ok = LandingFiles.write_entity!(entity, rows)

    {:ok,
     %{
       entity: to_string(entity),
       mode: mode,
       rows_written: length(rows),
       landing_path: ".data/generic_crm/landing/#{entity}.json"
     }}
  end

  defp normalize_entity("accounts"), do: :accounts
  defp normalize_entity("contacts"), do: :contacts
  defp normalize_entity("deals"), do: :deals
  defp normalize_entity("activities"), do: :activities
  defp normalize_entity(entity) when is_atom(entity), do: entity

  defp rows_for(entity, "full_refresh", _window), do: Map.fetch!(CRMData.seed(), entity)

  defp rows_for(entity, "daily_window", %{start_at: start_at, end_at: end_at}) do
    start_date = DateTime.to_date(start_at)
    end_date = DateTime.to_date(end_at)

    CRMData.seed()
    |> Map.fetch!(entity)
    |> Enum.filter(fn row ->
      {:ok, occurred_at, _offset} = DateTime.from_iso8601(row.occurred_at)
      date = DateTime.to_date(occurred_at)
      Date.compare(date, start_date) != :lt and Date.compare(date, end_date) == :lt
    end)
  end
end
