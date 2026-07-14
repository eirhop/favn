defmodule FavnOrchestrator.Storage.Adapter.Memory.Idempotency do
  @moduledoc """
  Pure idempotency-record transitions for the in-memory adapter.

  Input is normalized through a fixed key and status vocabulary. Malformed
  persisted-shaped maps therefore return errors instead of crashing the storage
  process or converting arbitrary strings to atoms.
  """

  alias FavnOrchestrator.Storage.Adapter.Memory.State

  @keys [
    :id,
    :operation,
    :idempotency_key_hash,
    :actor_id,
    :session_id,
    :service_identity,
    :request_fingerprint,
    :status,
    :response_status,
    :response_body,
    :resource_type,
    :resource_id,
    :created_at,
    :updated_at,
    :expires_at,
    :completed_at
  ]
  @keys_by_name Map.new(@keys, &{Atom.to_string(&1), &1})
  @statuses [:in_progress, :completed, :failed]

  @doc false
  @spec reserve(State.t(), map()) :: {{:ok, tuple()} | {:error, term()}, State.t()}
  def reserve(%State{} = state, record) when is_map(record) do
    with {:ok, record} <- normalize(record),
         :ok <- validate_reservation(record) do
      reserve_normalized(state, record)
    else
      {:error, _reason} = error -> {error, state}
    end
  end

  @doc false
  @spec complete(State.t(), String.t(), map()) :: {:ok | {:error, term()}, State.t()}
  def complete(%State{} = state, record_id, attrs)
      when is_binary(record_id) and is_map(attrs) do
    with {:ok, stored} <- fetch(state.idempotency_records, record_id),
         {:ok, attrs} <- normalize_keys(attrs),
         {:ok, updated} <- stored |> Map.merge(attrs) |> normalize() do
      records = Map.put(state.idempotency_records, record_id, updated)
      {:ok, %{state | idempotency_records: records}}
    else
      {:error, _reason} = error -> {error, state}
    end
  end

  @doc false
  @spec get(State.t(), String.t()) :: {:ok, map()} | {:error, :not_found}
  def get(%State{} = state, record_id), do: fetch(state.idempotency_records, record_id)

  defp reserve_normalized(state, record) do
    case Map.fetch(state.idempotency_records, record.id) do
      :error ->
        store_reservation(state, record)

      {:ok, stored} ->
        if expired?(stored) do
          store_reservation(state, record)
        else
          {classify(stored, record.request_fingerprint), state}
        end
    end
  end

  defp store_reservation(state, record) do
    records = Map.put(state.idempotency_records, record.id, record)
    {{:ok, {:reserved, record}}, %{state | idempotency_records: records}}
  end

  defp classify(stored, request_fingerprint) do
    cond do
      stored.request_fingerprint != request_fingerprint ->
        {:error, :idempotency_conflict}

      stored.status == :in_progress ->
        {:error, :operation_in_progress}

      stored.status in [:completed, :failed] ->
        {:ok, {:replay, stored}}

      true ->
        {:error, {:invalid_idempotency_status, stored.status}}
    end
  end

  defp normalize(record) do
    with {:ok, normalized} <- normalize_keys(record),
         {:ok, status} <- normalize_status(Map.get(normalized, :status)) do
      {:ok, Map.put(normalized, :status, status)}
    end
  end

  defp normalize_keys(record) do
    Enum.reduce_while(record, {:ok, %{}}, fn {key, value}, {:ok, acc} ->
      case known_key(key) do
        {:ok, normalized_key} -> {:cont, {:ok, Map.put(acc, normalized_key, value)}}
        :error -> {:halt, {:error, {:invalid_idempotency_record_key, key}}}
      end
    end)
  end

  defp known_key(key) when key in @keys, do: {:ok, key}

  defp known_key(key) when is_binary(key) do
    Map.fetch(@keys_by_name, key)
  end

  defp known_key(_key), do: :error

  defp normalize_status(status) when status in @statuses, do: {:ok, status}

  defp normalize_status(status) when is_binary(status) do
    case Enum.find(@statuses, &(Atom.to_string(&1) == status)) do
      nil -> {:error, {:invalid_idempotency_status, status}}
      normalized -> {:ok, normalized}
    end
  end

  defp normalize_status(status), do: {:error, {:invalid_idempotency_status, status}}

  defp validate_reservation(record) do
    case required_binary(record, :id) do
      :ok -> required_binary(record, :request_fingerprint)
      {:error, _reason} = error -> error
    end
  end

  defp required_binary(record, key) do
    case Map.get(record, key) do
      value when is_binary(value) and value != "" -> :ok
      _other -> {:error, {:invalid_idempotency_record_field, key}}
    end
  end

  defp expired?(%{expires_at: %DateTime{} = expires_at}) do
    DateTime.compare(expires_at, DateTime.utc_now()) != :gt
  end

  defp expired?(_record), do: false

  defp fetch(values, key) do
    case Map.fetch(values, key) do
      {:ok, value} -> {:ok, value}
      :error -> {:error, :not_found}
    end
  end
end
