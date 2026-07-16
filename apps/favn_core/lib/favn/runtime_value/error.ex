defmodule Favn.RuntimeValue.Error do
  @moduledoc """
  Redacted failure returned while resolving a deferred runtime value.
  """

  @enforce_keys [:reason, :message, :provider]
  defstruct [:reason, :message, :provider, retryable?: false]

  @type t :: %__MODULE__{
          reason: atom(),
          message: String.t(),
          provider: module(),
          retryable?: boolean()
        }
end
