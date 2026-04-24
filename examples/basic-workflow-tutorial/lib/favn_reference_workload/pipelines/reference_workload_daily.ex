defmodule FavnReferenceWorkload.Pipelines.ReferenceWorkloadDaily do
  @moduledoc """
  Canonical manual reference workload pipeline.

  Pipeline blocks define "what should run".

  In this module:

  - `asset(...)` points to the top target asset.
  - `deps(:all)` means include all upstream dependencies automatically.
  - `config(...)` and `meta(...)` add run intent and operator context.

  Alternative patterns:

  - Use a lower-level target asset for faster development runs.
  - Use a narrower dependency mode if you only want partial graph execution.
  - Add a schedule when you are ready for time-based execution.
  """

  use Favn.Pipeline

  pipeline :reference_workload_daily do
    asset(FavnReferenceWorkload.Warehouse.Ops.ReferenceWorkloadComplete)
    deps(:all)
    config(requested_by: "manual")
    meta(owner: "reference-workload", purpose: :canonical_demo)
  end
end
