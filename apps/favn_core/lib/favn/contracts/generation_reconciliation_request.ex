defmodule Favn.Contracts.GenerationReconciliationRequest do
  @moduledoc """
  Read-only request to reconcile a possibly committed generation activation.

  Reconciliation uses the original activation identity and token. It observes
  the data-plane marker and relations; it does not perform a new activation.
  """

  alias Favn.Contracts.GenerationActivationRequest

  @enforce_keys [:activation]
  defstruct [:activation]

  @type t :: %__MODULE__{activation: GenerationActivationRequest.t()}

  @doc "Validates the original activation identity used for reconciliation."
  @spec validate(t()) :: :ok | {:error, term()}
  def validate(%__MODULE__{activation: %GenerationActivationRequest{} = activation}),
    do: GenerationActivationRequest.validate(activation)

  def validate(value), do: {:error, {:invalid_generation_reconciliation_request, value}}
end
