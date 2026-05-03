defmodule FavnOrchestrator.Idempotency do
  @moduledoc """
  Durable idempotency helpers for mutating orchestrator command APIs.

  Records are scoped by operation, actor id, session id, service identity, and a
  SHA-256 hash of the caller supplied idempotency key. Request bodies are never
  persisted; only a canonical request fingerprint is stored.
  """

  alias FavnOrchestrator.Storage

  @default_retention_seconds 7 * 24 * 60 * 60

  @type scope :: %{
          required(:operation) => String.t(),
          required(:idempotency_key_hash) => String.t(),
          optional(:actor_id) => String.t() | nil,
          optional(:session_id) => String.t() | nil,
          optional(:service_identity) => String.t() | nil
        }

  @spec key_hash(String.t()) :: String.t()
  def key_hash(key) when is_binary(key) do
    key
    |> sha256()
    |> Base.encode16(case: :lower)
  end

  @spec request_fingerprint(term()) :: String.t()
  def request_fingerprint(input) do
    input
    |> canonicalize()
    |> Jason.encode!()
    |> sha256()
    |> Base.encode16(case: :lower)
  end

  @spec new_record(scope(), String.t()) :: map()
  def new_record(scope, request_fingerprint)
      when is_map(scope) and is_binary(request_fingerprint) do
    now = DateTime.utc_now() |> DateTime.truncate(:second)
    operation = Map.fetch!(scope, :operation)
    key_hash = Map.fetch!(scope, :idempotency_key_hash)
    actor_id = Map.get(scope, :actor_id)
    session_id = Map.get(scope, :session_id)
    service_identity = Map.get(scope, :service_identity)

    %{
      id: record_id(operation, actor_id, session_id, service_identity, key_hash),
      operation: operation,
      idempotency_key_hash: key_hash,
      actor_id: actor_id,
      session_id: session_id,
      service_identity: service_identity,
      request_fingerprint: request_fingerprint,
      status: :in_progress,
      response_status: nil,
      response_body: nil,
      resource_type: nil,
      resource_id: nil,
      created_at: now,
      updated_at: now,
      expires_at: DateTime.add(now, retention_seconds(), :second),
      completed_at: nil
    }
  end

  @spec reserve(map()) ::
          {:ok, {:reserved, map()} | {:replay, map()}}
          | {:error, :idempotency_conflict | :operation_in_progress | term()}
  def reserve(record) when is_map(record), do: Storage.reserve_idempotency_record(record)

  @spec complete(String.t(), map()) :: :ok | {:error, term()}
  def complete(record_id, attrs) when is_binary(record_id) and is_map(attrs) do
    now = DateTime.utc_now() |> DateTime.truncate(:second)

    attrs =
      attrs
      |> Map.put(:status, Map.get(attrs, :status, :completed))
      |> Map.put(:updated_at, now)
      |> Map.put(:completed_at, now)

    Storage.complete_idempotency_record(record_id, attrs)
  end

  @spec get(String.t()) :: {:ok, map()} | {:error, term()}
  def get(record_id) when is_binary(record_id), do: Storage.get_idempotency_record(record_id)

  @spec record_id(String.t(), String.t() | nil, String.t() | nil, String.t() | nil, String.t()) ::
          String.t()
  def record_id(operation, actor_id, session_id, service_identity, key_hash)
      when is_binary(operation) and is_binary(key_hash) do
    ["v1", operation, actor_id || "", session_id || "", service_identity || "", key_hash]
    |> Enum.join(<<0>>)
    |> sha256()
    |> Base.url_encode64(padding: false)
  end

  defp retention_seconds do
    Application.get_env(:favn_orchestrator, :idempotency_retention_seconds) ||
      @default_retention_seconds
  end

  defp sha256(value) when is_binary(value), do: :crypto.hash(:sha256, value)

  defp canonicalize(%DateTime{} = value), do: DateTime.to_iso8601(value)

  defp canonicalize(value) when is_map(value) do
    value
    |> Enum.map(fn {key, val} -> [to_string(key), canonicalize(val)] end)
    |> Enum.sort_by(fn [key, _val] -> key end)
  end

  defp canonicalize(value) when is_list(value), do: Enum.map(value, &canonicalize/1)
  defp canonicalize(value) when is_tuple(value), do: value |> Tuple.to_list() |> canonicalize()
  defp canonicalize(value) when is_atom(value), do: Atom.to_string(value)
  defp canonicalize(value), do: value
end
