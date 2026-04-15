defmodule Favn.Manifest.Catalog do
  @moduledoc """
  Intermediate manifest catalog built from explicit module lists.
  """

  alias Favn.Asset
  alias Favn.Manifest.Pipeline
  alias Favn.Manifest.Schedule

  @type t :: %__MODULE__{
          assets: [Asset.t()],
          assets_by_ref: %{Favn.Ref.t() => Asset.t()},
          pipelines: [Pipeline.t()],
          schedules: [Schedule.t()],
          diagnostics: [Favn.Diagnostic.t()]
        }

  defstruct assets: [],
            assets_by_ref: %{},
            pipelines: [],
            schedules: [],
            diagnostics: []
end
