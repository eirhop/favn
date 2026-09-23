defmodule Favn.Contracts.RuntimeInputExpectation do
  @moduledoc "A bounded runtime-input identity and fingerprint, excluding parameter payloads."

  alias Favn.RuntimeInput.Identity
  alias Favn.RuntimeInput.Resolution

  @enforce_keys [:resolver, :input_identity, :payload_fingerprint]
  defstruct @enforce_keys

  @type t :: %__MODULE__{
          resolver: String.t(),
          input_identity: String.t(),
          payload_fingerprint: String.t()
        }

  @doc "Discards parameters and metadata from a successful read-only resolution."
  @spec from_resolution(Resolution.t()) :: t()
  def from_resolution(%Resolution{} = resolution) do
    %__MODULE__{
      resolver: Atom.to_string(resolution.resolver),
      input_identity: resolution.input_identity,
      payload_fingerprint: resolution.payload_fingerprint
    }
  end

  @doc "Validates a bounded expectation received from a runner."
  @spec validate(t()) :: :ok | {:error, term()}
  def validate(%__MODULE__{
        resolver: resolver,
        input_identity: identity,
        payload_fingerprint: hash
      })
      when is_binary(resolver) and byte_size(resolver) in 1..255 and is_binary(hash) do
    with :ok <- Identity.validate(identity),
         true <- Regex.match?(~r/\A[0-9a-f]{64}\z/, hash) do
      :ok
    else
      _ -> {:error, :invalid_runtime_input_expectation}
    end
  end

  def validate(_), do: {:error, :invalid_runtime_input_expectation}
end
