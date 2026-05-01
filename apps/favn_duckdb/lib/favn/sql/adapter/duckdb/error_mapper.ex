defmodule Favn.SQL.Adapter.DuckDB.ErrorMapper do
  @moduledoc false

  alias Favn.SQL.Error

  @spec normalize(atom(), atom() | nil, term()) :: Error.t()
  def normalize(operation, connection, reason) do
    %Error{
      type: error_type(operation, reason),
      message: error_message(reason),
      retryable?: retryable_reason?(reason),
      adapter: Favn.SQL.Adapter.DuckDB,
      operation: operation,
      connection: connection,
      details: %{
        classification: classification(operation, reason),
        reason: inspect(reason)
      }
    }
    |> Error.redact()
  end

  @spec rollback_failure(Error.t(), term()) :: Error.t()
  def rollback_failure(%Error{} = previous, rollback_reason) do
    %Error{
      type: :execution_error,
      message: "transaction rollback failed",
      retryable?: false,
      adapter: Favn.SQL.Adapter.DuckDB,
      operation: :transaction,
      connection: previous.connection,
      details: %{
        classification: :execution,
        transaction_stage: :rollback,
        rollback_reason: inspect(rollback_reason),
        original_error: %{
          type: previous.type,
          message: previous.message,
          retryable?: previous.retryable?,
          operation: previous.operation,
          details: previous.details
        }
      },
      cause: previous
    }
    |> Error.redact()
  end

  defp classification(:connect, :invalid_database), do: :invalid_config
  defp classification(_operation, :worker_call_timeout), do: :timeout
  defp classification(_operation, :worker_not_available), do: :worker_unavailable
  defp classification(_operation, :invalid_handle), do: :worker_handle
  defp classification(:connect, _), do: :connection
  defp classification(:ping, _), do: :connection
  defp classification(_operation, {:error, reason}), do: classification(:query, reason)
  defp classification(_operation, reason) when reason in [:busy, :locked], do: :conflict

  defp classification(_operation, reason) when is_binary(reason) do
    if String.contains?(String.downcase(reason), "conflict"), do: :conflict, else: :execution
  end

  defp classification(_operation, _reason), do: :execution

  defp error_type(:connect, :invalid_database), do: :invalid_config
  defp error_type(:connect, _), do: :connection_error
  defp error_type(:ping, _), do: :connection_error
  defp error_type(_operation, _reason), do: :execution_error

  defp error_message(:worker_call_timeout), do: "DuckDB worker call timed out"
  defp error_message(:worker_not_available), do: "DuckDB worker is not available"
  defp error_message(:invalid_handle), do: "DuckDB worker handle is invalid"
  defp error_message(reason) when is_binary(reason), do: reason
  defp error_message({:error, message}) when is_binary(message), do: message
  defp error_message(_), do: "duckdb operation failed"

  defp retryable_reason?(:worker_call_timeout), do: true
  defp retryable_reason?(:worker_not_available), do: true
  defp retryable_reason?(reason) when reason in [:busy, :locked], do: true

  defp retryable_reason?({:error, reason}), do: retryable_reason?(reason)

  defp retryable_reason?(reason) when is_binary(reason),
    do: String.contains?(String.downcase(reason), "conflict")

  defp retryable_reason?(_), do: false
end
