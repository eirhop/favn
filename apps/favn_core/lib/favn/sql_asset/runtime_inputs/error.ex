defmodule Favn.SQLAsset.RuntimeInputs.Error do
  @moduledoc """
  Safe, typed failure returned by a runtime SQL input resolver.

  `reason` is a stable resolver-owned code, `message` is an operator-safe
  explanation, and `retryable?` is preserved in the runner error contract.
  `metadata` must contain only bounded JSON-safe diagnostic values and must not
  contain resolved parameter values or credentials.

  Read `Favn.AI`, `Favn.SQLAsset`, and `Favn.SQLAsset.RuntimeInputs` first.
  Return this struct for an expected resolver failure; raising is reserved for
  unexpected bugs and is normalized by the runner.
  """

  @enforce_keys [:reason, :message]
  defstruct reason: nil, message: nil, retryable?: false, metadata: %{}

  @type reason :: atom() | String.t()
  @type metadata_value ::
          nil
          | boolean()
          | number()
          | String.t()
          | [metadata_value()]
          | %{optional(atom() | String.t()) => metadata_value()}

  @type t :: %__MODULE__{
          reason: reason(),
          message: String.t(),
          retryable?: boolean(),
          metadata: %{optional(atom() | String.t()) => metadata_value()}
        }
end
