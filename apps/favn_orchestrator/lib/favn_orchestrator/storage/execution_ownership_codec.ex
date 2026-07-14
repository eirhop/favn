defmodule FavnOrchestrator.Storage.ExecutionOwnershipCodec do
  @moduledoc false

  alias FavnOrchestrator.Redaction
  alias FavnOrchestrator.RunExecutionOwnership
  alias FavnOrchestrator.Storage.PayloadCodec

  @spec normalize(RunExecutionOwnership.t() | map()) ::
          {:ok, RunExecutionOwnership.t()} | {:error, term()}
  def normalize(%RunExecutionOwnership{} = ownership), do: validate(ownership)
  def normalize(%{} = ownership), do: from_map(ownership)
  def normalize(_ownership), do: {:error, :invalid_execution_ownership}

  @spec encode(RunExecutionOwnership.t() | map()) :: {:ok, binary()} | {:error, term()}
  def encode(ownership) do
    with {:ok, normalized} <- normalize(ownership) do
      normalized
      |> to_map()
      |> PayloadCodec.encode()
    end
  rescue
    exception -> {:error, {:invalid_execution_ownership_payload, exception}}
  end

  @spec decode(binary()) :: {:ok, RunExecutionOwnership.t()} | {:error, term()}
  def decode(payload) when is_binary(payload) do
    with {:ok, map} <- PayloadCodec.decode(payload) do
      from_map(map)
    end
  rescue
    exception -> {:error, {:invalid_execution_ownership_payload, exception}}
  end

  def decode(_payload), do: {:error, :invalid_execution_ownership_payload}

  defp from_map(map) do
    with {:ok, ownership_id} <- required_binary(map, :ownership_id),
         {:ok, run_id} <- required_binary(map, :run_id),
         {:ok, asset_step_id} <- required_binary(map, :asset_step_id),
         {:ok, status} <- RunExecutionOwnership.normalize_status(field(map, :status)),
         {:ok, cancel_status} <-
           RunExecutionOwnership.normalize_cancel_status(field(map, :cancel_status)),
         {:ok, stage} <- optional_non_negative_integer(map, :stage),
         {:ok, attempt} <- optional_positive_integer(map, :attempt),
         {:ok, execution_pool} <- optional_execution_pool(map),
         {:ok, runner_execution_id} <- optional_binary(map, :runner_execution_id),
         {:ok, dispatch_id} <- optional_binary(map, :dispatch_id, ownership_id),
         {:ok, deadline_at} <- optional_datetime(map, :deadline_at),
         {:ok, cancel_requested_at} <- optional_datetime(map, :cancel_requested_at),
         {:ok, cancel_outcome} <- optional_map(map, :cancel_outcome),
         {:ok, inserted_at} <- required_datetime(map, :inserted_at),
         {:ok, updated_at} <- required_datetime(map, :updated_at) do
      validate(%RunExecutionOwnership{
        ownership_id: ownership_id,
        run_id: run_id,
        asset_step_id: asset_step_id,
        node_key: field(map, :node_key),
        asset_ref: field(map, :asset_ref),
        stage: stage,
        attempt: attempt,
        execution_pool: execution_pool,
        runner_execution_id: runner_execution_id,
        runner_ref: field(map, :runner_ref),
        dispatch_id: dispatch_id,
        deadline_at: deadline_at,
        cancel_requested_at: cancel_requested_at,
        cancel_outcome: cancel_outcome,
        status: status,
        cancel_status: cancel_status,
        cancel_reason: field(map, :cancel_reason),
        last_error: field(map, :last_error),
        inserted_at: inserted_at,
        updated_at: updated_at
      })
    end
  end

  defp validate(%RunExecutionOwnership{} = ownership) do
    with :ok <- non_empty_binary(ownership.ownership_id, :ownership_id),
         :ok <- non_empty_binary(ownership.run_id, :run_id),
         :ok <- non_empty_binary(ownership.asset_step_id, :asset_step_id),
         true <- RunExecutionOwnership.valid_status?(ownership.status),
         true <- RunExecutionOwnership.valid_cancel_status?(ownership.cancel_status),
         :ok <- valid_optional_non_negative(ownership.stage, :stage),
         :ok <- valid_optional_positive(ownership.attempt, :attempt),
         :ok <- valid_execution_pool(ownership.execution_pool),
         :ok <- valid_optional_binary(ownership.runner_execution_id, :runner_execution_id),
         :ok <- non_empty_binary(ownership.dispatch_id, :dispatch_id),
         :ok <- valid_optional_datetime(ownership.deadline_at, :deadline_at),
         :ok <- valid_optional_datetime(ownership.cancel_requested_at, :cancel_requested_at),
         :ok <- valid_optional_map(ownership.cancel_outcome, :cancel_outcome),
         :ok <- valid_datetime(ownership.inserted_at, :inserted_at),
         :ok <- valid_datetime(ownership.updated_at, :updated_at) do
      {:ok, sanitize(ownership)}
    else
      false -> {:error, :invalid_execution_ownership}
      {:error, _reason} = error -> error
    end
  end

  defp sanitize(%RunExecutionOwnership{} = ownership) do
    %{
      ownership
      | cancel_outcome: safe_diagnostic(:details, ownership.cancel_outcome),
        cancel_reason: safe_diagnostic(:reason, ownership.cancel_reason),
        last_error: safe_diagnostic(:error, ownership.last_error)
    }
  end

  defp to_map(%RunExecutionOwnership{} = ownership) do
    %{
      ownership_id: ownership.ownership_id,
      run_id: ownership.run_id,
      asset_step_id: ownership.asset_step_id,
      node_key: ownership.node_key,
      asset_ref: ownership.asset_ref,
      stage: ownership.stage,
      attempt: ownership.attempt,
      execution_pool: ownership.execution_pool,
      runner_execution_id: ownership.runner_execution_id,
      runner_ref: ownership.runner_ref,
      dispatch_id: ownership.dispatch_id,
      deadline_at: ownership.deadline_at,
      cancel_requested_at: ownership.cancel_requested_at,
      cancel_outcome: ownership.cancel_outcome,
      status: ownership.status,
      cancel_status: ownership.cancel_status,
      cancel_reason: ownership.cancel_reason,
      last_error: ownership.last_error,
      inserted_at: ownership.inserted_at,
      updated_at: ownership.updated_at
    }
  end

  defp required_binary(map, key) do
    case field(map, key) do
      value when is_binary(value) and byte_size(value) > 0 -> {:ok, value}
      _invalid -> field_error(key)
    end
  end

  defp optional_binary(map, key, default \\ nil) do
    case field(map, key) do
      nil -> {:ok, default}
      value when is_binary(value) and byte_size(value) > 0 -> {:ok, value}
      _invalid -> field_error(key)
    end
  end

  defp optional_non_negative_integer(map, key) do
    case field(map, key) do
      nil -> {:ok, nil}
      value when is_integer(value) and value >= 0 -> {:ok, value}
      _invalid -> field_error(key)
    end
  end

  defp optional_positive_integer(map, key) do
    case field(map, key) do
      nil -> {:ok, nil}
      value when is_integer(value) and value > 0 -> {:ok, value}
      _invalid -> field_error(key)
    end
  end

  defp optional_execution_pool(map) do
    case field(map, :execution_pool) do
      nil -> {:ok, nil}
      value when is_atom(value) or is_binary(value) -> {:ok, value}
      _invalid -> field_error(:execution_pool)
    end
  end

  defp optional_map(map, key) do
    case field(map, key) do
      nil -> {:ok, nil}
      value when is_map(value) -> {:ok, value}
      _invalid -> field_error(key)
    end
  end

  defp required_datetime(map, key) do
    case parse_datetime(field(map, key)) do
      {:ok, %DateTime{} = datetime} -> {:ok, datetime}
      :error -> field_error(key)
    end
  end

  defp optional_datetime(map, key) do
    case field(map, key) do
      nil ->
        {:ok, nil}

      value ->
        case parse_datetime(value) do
          {:ok, %DateTime{} = datetime} -> {:ok, datetime}
          :error -> field_error(key)
        end
    end
  end

  defp parse_datetime(%DateTime{} = value), do: {:ok, value}

  defp parse_datetime(value) when is_binary(value) do
    case DateTime.from_iso8601(value) do
      {:ok, datetime, _offset} -> {:ok, datetime}
      _invalid -> :error
    end
  end

  defp parse_datetime(_value), do: :error

  defp non_empty_binary(value, _field) when is_binary(value) and byte_size(value) > 0, do: :ok
  defp non_empty_binary(_value, field), do: field_error(field)

  defp valid_optional_non_negative(nil, _field), do: :ok
  defp valid_optional_non_negative(value, _field) when is_integer(value) and value >= 0, do: :ok
  defp valid_optional_non_negative(_value, field), do: field_error(field)

  defp valid_optional_positive(nil, _field), do: :ok
  defp valid_optional_positive(value, _field) when is_integer(value) and value > 0, do: :ok
  defp valid_optional_positive(_value, field), do: field_error(field)

  defp valid_execution_pool(nil), do: :ok
  defp valid_execution_pool(value) when is_atom(value) or is_binary(value), do: :ok
  defp valid_execution_pool(_value), do: field_error(:execution_pool)

  defp valid_optional_binary(nil, _field), do: :ok

  defp valid_optional_binary(value, _field) when is_binary(value) and byte_size(value) > 0,
    do: :ok

  defp valid_optional_binary(_value, field), do: field_error(field)

  defp valid_optional_datetime(nil, _field), do: :ok
  defp valid_optional_datetime(%DateTime{}, _field), do: :ok
  defp valid_optional_datetime(_value, field), do: field_error(field)

  defp valid_optional_map(nil, _field), do: :ok
  defp valid_optional_map(value, _field) when is_map(value), do: :ok
  defp valid_optional_map(_value, field), do: field_error(field)

  defp valid_datetime(%DateTime{}, _field), do: :ok
  defp valid_datetime(_value, field), do: field_error(field)

  defp field(map, key), do: Map.get(map, key, Map.get(map, Atom.to_string(key)))
  defp field_error(field), do: {:error, {:invalid_execution_ownership_field, field}}

  defp safe_diagnostic(_key, nil), do: nil

  defp safe_diagnostic(key, value) do
    case Redaction.redact_operational_bounded(%{key => value}) do
      %{^key => safe} -> safe
      _other -> "[REDACTED]"
    end
  end
end
