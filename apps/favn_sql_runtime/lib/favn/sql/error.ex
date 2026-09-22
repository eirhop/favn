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
          | :pool_timeout
          | :operation_timeout
          | :transaction_conflict
          | :catalog_conflict
          | :catalog_integrity_failure
          | :catalog_schema_conflict

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

  @doc "True only for an adapter-issued rejected transaction without contradictory uncertainty."
  @spec rejected_transaction?(term()) :: boolean()
  def rejected_transaction?(
        %__MODULE__{
          type: :transaction_conflict,
          operation: :transaction,
          details: %{transaction_outcome: :rolled_back, transaction_stage: :commit}
        } = error
      ),
      do: not uncertain?(error)

  def rejected_transaction?(_), do: false

  defp uncertain?(%__MODULE__{type: :operation_timeout}), do: true
  defp uncertain?(%_{} = value), do: uncertain?(Map.from_struct(value))

  defp uncertain?(value) when is_map(value) do
    Enum.any?(value, fn
      {key, type}
      when key in [:type, "type"] and type in [:operation_timeout, "operation_timeout"] ->
        true

      {key, outcome} when key in [:transaction_outcome, "transaction_outcome"] ->
        outcome in [:unknown, "unknown"]

      {key, true} when key in [:unknown_outcome?, "unknown_outcome?"] ->
        true

      {key, classification}
      when key in [:classification, "classification"] and
             classification in [
               :unknown_commit_state,
               :unknown_outcome_timeout,
               "unknown_commit_state",
               "unknown_outcome_timeout"
             ] ->
        true

      {_, child} ->
        uncertain?(child)
    end)
  end

  defp uncertain?(value) when is_list(value), do: Enum.any?(value, &uncertain?/1)
  defp uncertain?(value) when is_tuple(value), do: value |> Tuple.to_list() |> uncertain?()
  defp uncertain?(_), do: false

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
