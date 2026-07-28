defmodule CrmDemo.Landing.Crm.Daily do
  @moduledoc """
  Lands one UTC day of CRM event data per run.

  `window/1` makes the day a required input, so the asset can never run without
  knowing which day it is extracting. `coverage/1` tells Favn which days are
  expected to exist, which is what makes freshness gaps and backfills
  meaningful. `availability_delay` keeps the newest hour out of scope until the
  source system has settled.
  """

  use Favn.MultiAsset

  alias CrmDemo.Landing.Crm.Extractor

  settings(mode: "daily_window", page_size: 3)
  window(Favn.Window.daily(timezone: "Etc/UTC", required: true))
  coverage(from: ~D[2026-07-22], through: :latest_closed, availability_delay: {:hours, 1})
  freshness(window_success: true)
  meta(tags: [:landing, :daily])

  asset :deals do
    description("Land the CRM deals that changed during the selected day.")
    settings(dataset: "deals", endpoint: "Deals", date_field: "OccurredAt")
  end

  asset :activities do
    description("Land the CRM activities that occurred during the selected day.")
    settings(dataset: "activities", endpoint: "Activities", date_field: "OccurredAt")
  end

  @doc "Extracts one daily CRM window into landing storage."
  def asset(ctx), do: Extractor.extract(ctx)
end
