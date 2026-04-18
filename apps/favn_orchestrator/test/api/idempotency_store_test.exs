defmodule FavnOrchestrator.API.IdempotencyStoreTest do
  use ExUnit.Case, async: false

  alias FavnOrchestrator.API.IdempotencyStore

  setup do
    start_state = ensure_idempotency_store_started()
    :ok = IdempotencyStore.reset()

    on_exit(fn ->
      maybe_stop_idempotency_store(start_state)
    end)

    :ok
  end

  test "stores and replays response entries by scope key" do
    scope_key = "POST:/api/orchestrator/v1/runs:act_1:key_1"

    response_entry = %{
      status: 201,
      body: ~s({"data":{"run":{"id":"run-1"}}}),
      content_type: "application/json"
    }

    assert :not_found = IdempotencyStore.fetch(scope_key)
    assert :ok = IdempotencyStore.put(scope_key, response_entry)
    assert {:ok, ^response_entry} = IdempotencyStore.fetch(scope_key)
  end

  defp ensure_idempotency_store_started do
    case Process.whereis(IdempotencyStore) do
      nil ->
        start_supervised!({IdempotencyStore, []})
        :started

      _pid ->
        :existing
    end
  end

  defp maybe_stop_idempotency_store(:existing), do: :ok

  defp maybe_stop_idempotency_store(:started) do
    case Process.whereis(IdempotencyStore) do
      nil -> :ok
      pid -> GenServer.stop(pid, :normal)
    end
  end
end
