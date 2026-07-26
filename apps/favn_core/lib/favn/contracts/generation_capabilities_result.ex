defmodule Favn.Contracts.GenerationCapabilitiesResult do
  @moduledoc "Typed result for runner target-generation capabilities."

  @max_encoded_bytes 262_144

  @enforce_keys [:capabilities]
  defstruct @enforce_keys

  @type t :: %__MODULE__{capabilities: map()}

  @spec validate(t()) :: :ok | {:error, term()}
  def validate(%__MODULE__{capabilities: capabilities} = result) when is_map(capabilities) do
    if byte_size(:erlang.term_to_binary(result, [:deterministic])) <= @max_encoded_bytes,
      do: :ok,
      else: {:error, :generation_capabilities_result_too_large}
  end

  def validate(value), do: {:error, {:invalid_generation_capabilities_result, value}}
end
