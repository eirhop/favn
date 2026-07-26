defmodule Favn.Contracts.GenerationMarkerReadRequest do
  @moduledoc "Typed request for reading one target-generation marker on a runner."

  alias Favn.Manifest.Version

  @enforce_keys [:manifest, :asset_ref]
  defstruct @enforce_keys

  @type t :: %__MODULE__{manifest: Version.t(), asset_ref: Favn.Ref.t()}

  @spec validate(t()) :: :ok | {:error, term()}
  def validate(%__MODULE__{manifest: %Version{}, asset_ref: {module, name}})
      when is_atom(module) and is_atom(name),
      do: :ok

  def validate(value), do: {:error, {:invalid_generation_marker_read_request, value}}
end
