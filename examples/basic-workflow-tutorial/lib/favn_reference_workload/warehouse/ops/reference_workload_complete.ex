defmodule FavnReferenceWorkload.Warehouse.Ops.ReferenceWorkloadComplete do
  @moduledoc """
  Terminal marker asset for the canonical reference workload pipeline.

  This module uses `Favn.Asset` (Elixir function asset) instead of SQL.

  In simple terms:

  - `@depends FavnReferenceWorkload.Warehouse.Gold.ExecutiveOverview` says
    "only run this when the full business summary is ready".
  - `def asset(_ctx), do: :ok` acts as a successful completion marker.

  Why this is useful:

  - gives pipelines one clear top-level target
  - demonstrates mixed SQL + Elixir asset graphs

  Alternative:

  - Replace this with a real operational side effect (notification, handoff,
    export) once you move beyond tutorial mode.
  """

  use Favn.Namespace
  use Favn.Asset

  @meta owner: "reference-workload", category: :ops, tags: [:terminal]
  @depends FavnReferenceWorkload.Warehouse.Gold.ExecutiveOverview
  @relation true
  def asset(_ctx), do: :ok
end
