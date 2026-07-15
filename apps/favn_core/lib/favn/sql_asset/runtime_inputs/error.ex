defmodule Favn.SQLAsset.RuntimeInputs.Error do
  @moduledoc """
  Safe, typed failure returned by a runtime SQL input resolver.

  `reason` is a stable resolver-owned code, `message` is an operator-safe
  explanation, and `retryable?` is preserved in the runner error contract.
  `metadata` must contain only bounded JSON-safe diagnostic values and must not
  contain resolved parameter values or credentials.
  """

  @enforce_keys [:reason, :message]
  defstruct reason: nil, message: nil, retryable?: false, metadata: %{}

  @type t :: %__MODULE__{
          reason: atom() | String.t(),
          message: String.t(),
          retryable?: boolean(),
          metadata: map()
        }
end
