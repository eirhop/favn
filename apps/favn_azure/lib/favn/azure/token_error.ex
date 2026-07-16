defmodule Favn.Azure.TokenError do
  @moduledoc """
  Redacted Azure token acquisition error.
  """

  @enforce_keys [:type, :message]
  defstruct [:type, :message, retryable?: false, details: %{}]

  @type type :: :invalid_config | :authentication_error | :connection_error | :execution_error

  @type t :: %__MODULE__{
          type: type(),
          message: String.t(),
          retryable?: boolean(),
          details: map()
        }
end

defimpl Inspect, for: Favn.Azure.TokenError do
  import Inspect.Algebra

  def inspect(error, opts) do
    concat([
      "#Favn.Azure.TokenError<",
      to_doc(
        [
          type: error.type,
          message: :redacted,
          retryable?: error.retryable?,
          details: :redacted
        ],
        opts
      ),
      ">"
    ])
  end
end
