defmodule Favn.Manifest.Catalog do
  @moduledoc """
  Intermediate compile catalog used to build canonical manifests.
  """

  @type t :: %__MODULE__{
          assets: [map()],
          assets_by_ref: %{{module(), atom()} => map()},
          pipelines: [map()],
          schedules: [{module(), atom(), map()}],
          connection_definitions: %{optional(atom()) => Favn.Connection.Definition.t()},
          diagnostics: [term()]
        }

  defstruct assets: [],
            assets_by_ref: %{},
            pipelines: [],
            schedules: [],
            connection_definitions: %{},
            diagnostics: []
end
