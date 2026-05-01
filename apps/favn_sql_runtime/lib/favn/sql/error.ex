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
          | :admission_timeout

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

  @sensitive_key_parts ~w(password passwd token secret credential api_key access_key dsn metadata data_path account_name)

  @doc false
  @spec redact(term()) :: term()
  def redact(%__MODULE__{} = error) do
    %__MODULE__{
      error
      | message: redact_text(error.message),
        details: redact_value(error.details),
        cause: redact_value(error.cause)
    }
  end

  def redact(value), do: redact_value(value)

  defp redact_value(%__MODULE__{} = error), do: redact(error)

  defp redact_value(%_{} = value) do
    value
    |> Map.from_struct()
    |> redact_value()
  end

  defp redact_value(value) when is_map(value) do
    Map.new(value, fn {key, child} ->
      if sensitive_key?(key) do
        {key, :redacted}
      else
        {key, redact_value(child)}
      end
    end)
  end

  defp redact_value(value) when is_list(value), do: Enum.map(value, &redact_value/1)

  defp redact_value(value) when is_tuple(value) do
    value
    |> Tuple.to_list()
    |> Enum.map(&redact_value/1)
    |> List.to_tuple()
  end

  defp redact_value(value) when is_binary(value), do: redact_text(value)

  defp redact_value(value), do: value

  defp sensitive_key?(key) do
    normalized = key |> to_string() |> String.downcase()
    Enum.any?(@sensitive_key_parts, &String.contains?(normalized, &1))
  end

  defp redact_text(value) when is_binary(value) do
    value
    |> String.replace(~r/([a-z][a-z0-9+.-]*:\/\/)[^\s\/]+@/i, "\\1redacted@")
    |> String.replace(
      ~r/(password|passwd|token|secret|credential|api_key|access_key)=([^&\s]+)/i,
      "\\1=redacted"
    )
  end

  defp redact_text(value), do: value
end
