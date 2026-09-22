defmodule Favn.SQL.Adapter.DuckDB.ADBC.Rejection do
  @moduledoc false
  alias Favn.SQL.Error

  def commit?(
        %Error{
          type: :execution_error,
          operation: :transaction,
          details: %{classification: Adbc.Error, transaction_stage: :commit}
        } = error
      ),
      do: rejected_conflict?(error)

  def commit?(_), do: false

  def cleanup?(:ok), do: true

  def cleanup?(
        {:error,
         %Adbc.Error{
           message: "TransactionContext Error: cannot rollback - no transaction is active"
         }}
      ),
      do: true

  def cleanup?(_), do: false

  def rejected_conflict?(%Error{type: :transaction_conflict} = error),
    do: Error.rejected_transaction?(error)

  def rejected_conflict?(%{
        details: %{
          transaction_stage: :rollback,
          rollback_reason: rollback,
          original_error: original
        }
      })
      when is_binary(rollback) do
    String.contains?(
      rollback,
      "TransactionContext Error: cannot rollback - no transaction is active"
    ) and
      rejected_conflict?(original)
  end

  def rejected_conflict?(%{
        type: :execution_error,
        message: message,
        details: %{classification: Adbc.Error, transaction_stage: stage}
      }) do
    (stage == :body and
       (message == "TransactionContext Error: Conflict on update!" or
          String.starts_with?(
            message,
            "TransactionContext Error: Catalog write-write conflict on create with "
          ))) or
      (stage == :commit and
         String.starts_with?(
           message,
           "TransactionContext Error: Failed to commit: Failed to commit DuckLake transaction.\nTransaction conflict - "
         ))
  end

  def rejected_conflict?(_), do: false
end
