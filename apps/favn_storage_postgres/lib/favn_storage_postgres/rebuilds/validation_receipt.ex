defmodule FavnStoragePostgres.Rebuilds.ValidationReceipt do
  @moduledoc false
  alias Ecto.Adapters.SQL
  alias FavnOrchestrator.Persistence.Error
  alias FavnOrchestrator.Rebuild.Validation
  alias FavnStoragePostgres.Idempotency.Transaction
  alias FavnStoragePostgres.Repo

  # Uses the external command's existing receipt; it never acquires a successor lease.
  def admit!(operation, attempt, mutation) do
    Transaction.execute_with_outcome!(
      operation.workspace_id,
      attempt.receipt,
      mutation,
      fn result -> {:ok, encode(operation.operation_id, result)} end,
      fn %{response: response} -> {:ok, decode_for(response, operation.operation_id)} end
    )
  end

  def read!(operation, %{receipt: nil} = attempt) do
    current = Validation.decode(operation.validation_request)

    cond do
      current && current.attempt_id == attempt.attempt_id ->
        {:ok, current}

      attempt.purpose == :plan && operation.action_count > 0 ->
        {:ok, %{attempt | status: "accepted"}}

      true ->
        {:error,
         Error.new(:unavailable, "Rebuild request outcome is unavailable", retryable?: true)}
    end
  end

  def read!(operation, attempt) do
    receipt = attempt.receipt

    result =
      SQL.query!(
        Repo,
        """
        SELECT response FROM favn_control.idempotency_records
        WHERE workspace_id=$1 AND operation=$2 AND principal_kind=$3 AND principal_id=$4
          AND key_hash=$5 AND request_fingerprint=$6 AND status='committed'
        """,
        [
          operation.workspace_id,
          receipt.operation,
          Atom.to_string(receipt.principal_kind),
          receipt.principal_id,
          receipt.key_hash,
          receipt.request_fingerprint
        ]
      )

    case result.rows do
      [[response]] ->
        decode_for(response, operation.operation_id)

      _ ->
        {:error,
         Error.new(:unavailable, "Rebuild request outcome is unavailable", retryable?: true)}
    end
  end

  def finish!(_operation, %{receipt: nil}, _result), do: :ok

  def finish!(operation, attempt, result) do
    receipt = attempt.receipt
    encoded = encode(operation.operation_id, result)

    update =
      SQL.query!(
        Repo,
        """
        UPDATE favn_control.idempotency_records
        SET response = $8, response_status = $9, updated_at = clock_timestamp()
        WHERE workspace_id = $1 AND operation = $2 AND principal_kind = $3
          AND principal_id = $4 AND key_hash = $5 AND request_fingerprint = $6
          AND status = 'committed' AND response->>'validation_status' = 'active'
          AND response->'validation'->>'attempt_id' = $7
        """,
        [
          operation.workspace_id,
          receipt.operation,
          Atom.to_string(receipt.principal_kind),
          receipt.principal_id,
          receipt.key_hash,
          receipt.request_fingerprint,
          attempt.attempt_id,
          encoded.response,
          encoded.response_status
        ]
      )

    expired_failure =
      match?({:error, _}, result) and
        DateTime.compare(receipt.expires_at, FavnStoragePostgres.Rebuilds.Validation.now!()) !=
          :gt

    if update.num_rows != 1 and not expired_failure,
      do: Repo.rollback(Error.new(:fenced, "Rebuild request is no longer current"))

    :ok
  end

  def encode(operation_id, {:error, %Error{} = error}) do
    %{
      response: %{
        "operation_id" => operation_id,
        "validation_status" => "failed",
        "error" => Validation.encode_error(error)
      },
      response_status: if(error.kind in [:conflict, :fenced], do: 409, else: 422),
      resource_kind: "rebuild",
      resource_id: operation_id
    }
  end

  def encode(operation_id, {:ok, %Validation{} = attempt}) do
    %{
      response: %{
        "operation_id" => operation_id,
        "validation_status" => attempt.status,
        "validation" => Validation.encode(attempt)
      },
      response_status: 202,
      resource_kind: "rebuild",
      resource_id: operation_id
    }
  end

  defp decode_for(%{"operation_id" => id} = response, id), do: decode(response)
  defp decode_for(_, _), do: {:error, Error.new(:internal, "Rebuild receipt identity is invalid")}

  defp decode(%{"validation_status" => "failed", "error" => error}) do
    {:error, Validation.decode_error(error)}
  end

  defp decode(%{"operation_id" => operation_id, "state" => state})
       when state in [
              "queued",
              "building",
              "validating",
              "activating",
              "succeeded",
              "failed",
              "cancelled"
            ],
       do: {:accepted, operation_id}

  defp decode(%{"validation" => validation}), do: {:ok, Validation.decode(validation)}
end
