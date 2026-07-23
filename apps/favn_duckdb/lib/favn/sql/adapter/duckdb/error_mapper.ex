defmodule Favn.SQL.Adapter.DuckDB.ErrorMapper do
  @moduledoc false

  alias Favn.SQL.Error

  @capacity_reason_atoms [
    :capacity,
    :capacity_exceeded,
    :metadata_capacity,
    :resource_exhausted,
    :too_many_connections,
    :too_many_requests
  ]

  @capacity_reason_fragments [
    "capacity",
    "metadata capacity",
    "resource exhausted",
    "too many connections",
    "too many requests",
    "rate limit",
    "rate-limit",
    "throttl",
    "out of memory",
    "no space left"
  ]

  @spec classify(term(), keyword()) :: Favn.SQL.Adapter.error_classification()
  def classify(%Error{} = error, _opts) do
    classification =
      get_in(error.details || %{}, [:classification]) || classification(error.operation, error)

    %{
      classification: classification,
      retryable?: error.retryable? == true,
      capacity?: classification == :capacity,
      unknown_outcome?: classification in [:unknown_outcome_timeout, :activation_outcome_unknown]
    }
  end

  def classify(reason, _opts) do
    classification = classification(:query, reason)

    %{
      classification: classification,
      retryable?: retryable_reason?(reason),
      capacity?: classification == :capacity,
      unknown_outcome?: classification == :unknown_outcome_timeout
    }
  end

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
  defp classification(_operation, :worker_call_timeout), do: :unknown_outcome_timeout
  defp classification(_operation, :worker_not_available), do: :worker_unavailable
  defp classification(_operation, :invalid_handle), do: :worker_handle
  defp classification(_operation, reason) when reason in @capacity_reason_atoms, do: :capacity
  defp classification(:connect, _), do: :connection
  defp classification(:ping, _), do: :connection
  defp classification(_operation, {:error, reason}), do: classification(:query, reason)
  defp classification(_operation, reason) when reason in [:busy, :locked], do: :conflict

  defp classification(_operation, reason) when is_binary(reason) do
    downcased = String.downcase(reason)

    cond do
      capacity_reason_text?(downcased) -> :capacity
      String.contains?(downcased, "conflict") -> :conflict
      true -> :execution
    end
  end

  defp classification(_operation, _reason), do: :execution

  defp error_type(:connect, :invalid_database), do: :invalid_config
  defp error_type(:connect, _), do: :connection_error
  defp error_type(:ping, _), do: :connection_error
  defp error_type(_operation, _reason), do: :execution_error

  defp error_message(:worker_call_timeout),
    do: "DuckDB worker call timed out; operation outcome is unknown"

  defp error_message(:worker_not_available), do: "DuckDB worker is not available"
  defp error_message(:invalid_handle), do: "DuckDB worker handle is invalid"
  defp error_message(reason) when is_binary(reason), do: reason
  defp error_message({:error, message}) when is_binary(message), do: message
  defp error_message(_), do: "duckdb operation failed"

  defp retryable_reason?(:worker_not_available), do: true
  defp retryable_reason?(reason) when reason in @capacity_reason_atoms, do: true
  defp retryable_reason?(reason) when reason in [:busy, :locked], do: true

  defp retryable_reason?({:error, reason}), do: retryable_reason?(reason)

  defp retryable_reason?(reason) when is_binary(reason) do
    downcased = String.downcase(reason)
    capacity_reason_text?(downcased) or String.contains?(downcased, "conflict")
  end

  defp retryable_reason?(_), do: false

  defp capacity_reason_text?(downcased_reason) do
    Enum.any?(@capacity_reason_fragments, &String.contains?(downcased_reason, &1))
  end
end
