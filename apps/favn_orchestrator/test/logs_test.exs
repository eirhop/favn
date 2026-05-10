defmodule FavnOrchestrator.LogsTest do
  use ExUnit.Case, async: false

  alias Favn.Log.Entry
  alias Favn.Log.Filter
  alias FavnOrchestrator.Storage.Adapter.Memory

  setup do
    previous_adapter = Application.get_env(:favn_orchestrator, :storage_adapter)
    previous_opts = Application.get_env(:favn_orchestrator, :storage_adapter_opts)

    Application.put_env(:favn_orchestrator, :storage_adapter, Memory)
    Application.put_env(:favn_orchestrator, :storage_adapter_opts, [])

    Memory.reset()

    on_exit(fn ->
      restore_env(:favn_orchestrator, :storage_adapter, previous_adapter)
      restore_env(:favn_orchestrator, :storage_adapter_opts, previous_opts)
      Memory.reset()
    end)

    :ok
  end

  test "log writer broadcasts persisted entries" do
    assert {:ok, subscription} = FavnOrchestrator.subscribe_logs(%Filter{run_id: "run_log_live"})

    assert {:ok, [persisted]} =
             FavnOrchestrator.emit_log(%Entry{
               run_id: "run_log_live",
               source: :runner,
               level: :info,
               message: "asset execution started",
               producer_id: "runner:exec-log-live",
               producer_sequence: 1
             })

    assert persisted.global_sequence == 1
    assert {:ok, received} = receive_log_entry("run_log_live")
    assert received.global_sequence == persisted.global_sequence

    assert :ok = FavnOrchestrator.unsubscribe_logs(subscription)
  end

  test "list and replay APIs delegate to storage" do
    assert {:ok, [_first, second]} =
             FavnOrchestrator.emit_logs([
               %Entry{
                 run_id: "run_log_read",
                 message: "first",
                 producer_id: "p",
                 producer_sequence: 1
               },
               %Entry{
                 run_id: "run_log_read",
                 message: "second",
                 producer_id: "p",
                 producer_sequence: 2
               }
             ])

    assert {:ok, page} = FavnOrchestrator.list_logs(%Filter{run_id: "run_log_read"})
    assert Enum.map(page.items, & &1.message) == ["first", "second"]

    assert {:ok, replayed} =
             FavnOrchestrator.replay_logs(second.global_sequence - 1, %Filter{
               run_id: "run_log_read"
             })

    assert Enum.map(replayed, & &1.message) == ["second"]
  end

  defp receive_log_entry(run_id, timeout \\ 1_000) do
    receive do
      {:favn_log_entry, %Entry{run_id: ^run_id} = entry} -> {:ok, entry}
      {:favn_log_entry, %Entry{}} -> receive_log_entry(run_id, timeout)
    after
      timeout -> {:error, :timeout}
    end
  end

  defp restore_env(app, key, nil), do: Application.delete_env(app, key)
  defp restore_env(app, key, value), do: Application.put_env(app, key, value)
end
