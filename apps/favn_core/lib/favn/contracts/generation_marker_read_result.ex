defmodule Favn.Contracts.GenerationMarkerReadResult do
  @moduledoc "Typed result for reading one target-generation marker."

  alias Favn.Contracts.GenerationMarker

  @enforce_keys [:marker]
  defstruct @enforce_keys

  @type t :: %__MODULE__{marker: GenerationMarker.t() | nil}

  @spec validate(t()) :: :ok | {:error, term()}
  def validate(%__MODULE__{marker: nil}), do: :ok

  def validate(%__MODULE__{marker: %GenerationMarker{} = marker}),
    do: GenerationMarker.validate(marker)

  def validate(value), do: {:error, {:invalid_generation_marker_read_result, value}}
end
