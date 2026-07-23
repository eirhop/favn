defmodule Favn.SQL.GenerationMarkerInitializationResult do
  @moduledoc false

  alias Favn.SQL.GenerationInspection
  alias Favn.SQL.GenerationMarker

  @enforce_keys [:marker, :physical_fingerprint, :inspection]
  defstruct @enforce_keys

  @type t :: %__MODULE__{
          marker: GenerationMarker.t(),
          physical_fingerprint: String.t(),
          inspection: GenerationInspection.t()
        }
end
