defmodule Favn.SQL.Adapter.DuckDB.ADBC.ErrorMapper do
  @moduledoc false

  alias Favn.SQL.Error

  @spec normalize(atom(), atom() | nil, term()) :: Error.t()
  def normalize(operation, connection, reason) do
    %Error{
      type: error_type(operation, reason),
      message: error_message(reason),
      retryable?: retryable_reason?(reason),
      adapter: Favn.SQL.Adapter.DuckDB.ADBC,
      operation: operation,
      connection: connection,
      details: %{
        classification: classification(operation, reason),
        reason: inspect(reason)
      },
      cause: reason
    }
    |> Error.redact()
  end

  @spec rollback_failure(Error.t(), term()) :: Error.t()
  def rollback_failure(%Error{} = previous, rollback_reason) do
    %Error{
      type: :execution_error,
      message: "transaction rollback failed",
      retryable?: false,
      adapter: Favn.SQL.Adapter.DuckDB.ADBC,
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
  defp classification(:connect, _reason), do: :connection
  defp classification(:ping, _reason), do: :connection
  defp classification(_operation, {:result_row_limit_exceeded, _rows, _limit}), do: :bounded_result
  defp classification(_operation, reason) when reason in [:busy, :locked], do: :conflict

  defp classification(_operation, reason) when is_binary(reason) do
    if String.contains?(String.downcase(reason), "conflict"), do: :conflict, else: :execution
  end

  defp classification(_operation, %_{} = exception), do: exception.__struct__
  defp classification(_operation, _reason), do: :execution

  defp error_type(:connect, :invalid_database), do: :invalid_config
  defp error_type(:connect, _reason), do: :connection_error
  defp error_type(:ping, _reason), do: :connection_error
  defp error_type(_operation, _reason), do: :execution_error

  defp error_message({:result_row_limit_exceeded, rows, limit}) do
    "DuckDB ADBC result has #{rows} rows, exceeding the configured limit of #{limit}; write large results to an explicit external path with DuckDB SQL"
  end

  defp error_message(reason) when is_binary(reason), do: reason
  defp error_message(%{message: message}) when is_binary(message), do: message
  defp error_message(%_{} = exception), do: Exception.message(exception)
  defp error_message({:error, message}) when is_binary(message), do: message
  defp error_message(_reason), do: "DuckDB ADBC operation failed"

  defp retryable_reason?(reason) when reason in [:busy, :locked], do: true
  defp retryable_reason?({:error, reason}), do: retryable_reason?(reason)

  defp retryable_reason?(reason) when is_binary(reason),
    do: String.contains?(String.downcase(reason), "conflict")

  defp retryable_reason?(_reason), do: false
end
