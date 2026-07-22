defmodule Favn.SQL.GenerationActivationResult do
  @moduledoc """
  Data-plane evidence returned after an observed activation commit.
  """

  alias Favn.SQL.{GenerationInspection, GenerationMarker}

  @enforce_keys [:marker, :candidate_fingerprint, :physical_fingerprint, :inspection]
  defstruct [:marker, :candidate_fingerprint, :physical_fingerprint, :inspection]

  @type t :: %__MODULE__{
          marker: GenerationMarker.t(),
          candidate_fingerprint: String.t(),
          physical_fingerprint: String.t(),
          inspection: GenerationInspection.t()
        }
end
