defmodule Favn.SQL.Error do
  @moduledoc """
  Normalized SQL adapter error used by runtime-facing SQL orchestration.
  """

  @enforce_keys [:type, :message]
  defstruct [
    :type,
    :message,
    :retryable?,
    :adapter,
    :connection,
    :operation,
    :sqlstate,
    details: %{},
    cause: nil
  ]

  @type type ::
          :invalid_config
          | :authentication_error
          | :connection_error
          | :execution_error
          | :unsupported_capability
          | :introspection_mismatch
          | :missing_relation

  @type t :: %__MODULE__{
          type: type(),
          message: String.t(),
          retryable?: boolean() | nil,
          adapter: module() | nil,
          connection: atom() | nil,
          operation: atom() | nil,
          sqlstate: binary() | nil,
          details: map(),
          cause: term()
        }
end
