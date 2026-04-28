defmodule Favn.RuntimeConfig.Error do
  @moduledoc """
  Structured runtime configuration resolution error.

  The runner and connection loader return this shape when a runtime config ref
  cannot be resolved. User-facing text should prefer `message/1`, for example
  `missing_env SOURCE_SYSTEM_TOKEN`.
  """

  defexception [:type, :provider, :key, :scope, :field, :secret?, :message]

  @type type :: :missing_env | :invalid_ref

  @type t :: %__MODULE__{
          type: type(),
          provider: atom() | nil,
          key: String.t() | nil,
          scope: atom() | nil,
          field: atom() | nil,
          secret?: boolean(),
          message: String.t()
        }

  @impl true
  def message(%__MODULE__{message: message}) when is_binary(message), do: message
  def message(%__MODULE__{type: :missing_env, key: key}), do: "missing_env #{key}"
  def message(%__MODULE__{} = error), do: inspect(error)
end
