defmodule Favn.Manifest do
  @moduledoc """
  Canonical manifest generated from authored modules.
  """

  alias Favn.Diagnostic
  alias Favn.Manifest.Asset
  alias Favn.Manifest.Pipeline
  alias Favn.Manifest.Schedule

  @type t :: %__MODULE__{
          version: pos_integer(),
          generated_at: DateTime.t(),
          assets: [Asset.t()],
          pipelines: [Pipeline.t()],
          schedules: [Schedule.t()],
          diagnostics: [Diagnostic.t()]
        }

  defstruct version: 1,
            generated_at: nil,
            assets: [],
            pipelines: [],
            schedules: [],
            diagnostics: []
end
