defmodule FavnOrchestrator.LogsTest do
  use ExUnit.Case, async: false

  alias Favn.Log.Entry
  alias Favn.Log.Filter
  alias FavnOrchestrator.RunnerLogBridge
  alias FavnOrchestrator.Storage.Adapter.Memory

  defmodule RunnerClientLogStub do
    def subscribe_execution_logs(execution_id, subscriber, opts) do
      opts
      |> Keyword.fetch!(:runner_log_entries)
      |> Map.fetch!(execution_id)
      |> Enum.each(fn entry -> send(subscriber, {:runner_log_entry, execution_id, entry}) end)

      :ok
    end

    def unsubscribe_execution_logs(_execution_id, _subscriber, _opts), do: :ok
  end

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

  test "list logs supports descending order for latest pages" do
    entries =
      for sequence <- 1..5 do
        %Entry{
          run_id: "run_log_latest",
          message: "log #{sequence}",
          producer_id: "latest",
          producer_sequence: sequence
        }
      end

    assert {:ok, _persisted} = FavnOrchestrator.emit_logs(entries)

    assert {:ok, page} =
             FavnOrchestrator.list_logs(%Filter{run_id: "run_log_latest"},
               limit: 2,
               order: :desc
             )

    assert Enum.map(page.items, & &1.message) == ["log 5", "log 4"]
    assert page.has_more?
  end

  test "runner bridge merges step context so runner logs filter by asset step" do
    runner_opts = [
      runner_log_entries: %{
        "exec_step_a" => [
          %{
            level: :info,
            source: :runner,
            message: "from runner step a",
            producer_id: "runner:exec_step_a",
            producer_sequence: 1,
            metadata: %{password: "secret", keep: "visible"}
          }
        ],
        "exec_step_b" => [
          %{
            level: :info,
            source: :runner,
            message: "from runner step b",
            producer_id: "runner:exec_step_b",
            producer_sequence: 1
          }
        ]
      }
    ]

    assert {:ok, bridge_a} =
             RunnerLogBridge.start(RunnerClientLogStub, "exec_step_a", runner_opts, %{
               run_id: "run_bridge_filter",
               asset_step_id: "step_a",
               node_key: {:node, :a},
               asset_ref: {__MODULE__, :asset_a},
               runner_execution_id: "exec_step_a",
               attempt: 2
             })

    assert {:ok, bridge_b} =
             RunnerLogBridge.start(RunnerClientLogStub, "exec_step_b", runner_opts, %{
               run_id: "run_bridge_filter",
               asset_step_id: "step_b",
               node_key: {:node, :b},
               asset_ref: {__MODULE__, :asset_b},
               runner_execution_id: "exec_step_b",
               attempt: 1
             })

    eventually(fn ->
      assert {:ok, page} =
               FavnOrchestrator.list_logs(%Filter{
                 run_id: "run_bridge_filter",
                 asset_step_id: "step_a"
               })

      assert [%Entry{} = entry] = page.items
      assert entry.message == "from runner step a"
      assert entry.run_id == "run_bridge_filter"
      assert entry.asset_step_id == "step_a"
      assert entry.node_key == {:node, :a}
      assert entry.asset_ref == {__MODULE__, :asset_a}
      assert entry.runner_execution_id == "exec_step_a"
      assert entry.attempt == 2
      assert entry.metadata["password"] == "[REDACTED]"
    end)

    assert {:ok, replayed} =
             FavnOrchestrator.replay_logs(0, %Filter{
               run_id: "run_bridge_filter",
               asset_step_id: "step_a"
             })

    assert Enum.map(replayed, & &1.message) == ["from runner step a"]

    RunnerLogBridge.stop(bridge_a, RunnerClientLogStub, "exec_step_a", runner_opts)
    RunnerLogBridge.stop(bridge_b, RunnerClientLogStub, "exec_step_b", runner_opts)
  end

  test "runner bridge ignores malformed runner log entries without crashing" do
    runner_opts = [
      runner_log_entries: %{
        "exec_bad_log" => [
          %{
            level: :fatal,
            source: :alien,
            message: "bad runner log",
            producer_id: "runner:exec_bad_log",
            producer_sequence: 1
          }
        ]
      }
    ]

    assert {:ok, bridge} =
             RunnerLogBridge.start(RunnerClientLogStub, "exec_bad_log", runner_opts, %{
               run_id: "run_bad_log",
               asset_step_id: "step_bad"
             })

    Process.sleep(50)
    assert Process.alive?(bridge)

    assert {:ok, page} = FavnOrchestrator.list_logs(%Filter{run_id: "run_bad_log"})
    assert page.items == []

    RunnerLogBridge.stop(bridge, RunnerClientLogStub, "exec_bad_log", runner_opts)
  end

  test "live run subscriptions deliver only matching run entries" do
    assert {:ok, subscription} = FavnOrchestrator.subscribe_logs(%Filter{run_id: "run_live_a"})

    assert {:ok, _} =
             FavnOrchestrator.emit_log(%Entry{run_id: "run_live_b", message: "other run"})

    assert {:ok, [persisted]} =
             FavnOrchestrator.emit_log(%Entry{run_id: "run_live_a", message: "wanted run"})

    assert {:ok, received} = receive_log_entry("run_live_a")
    assert received.global_sequence == persisted.global_sequence
    refute_receive {:favn_log_entry, %Entry{run_id: "run_live_b"}}, 100

    assert :ok = FavnOrchestrator.unsubscribe_logs(subscription)
  end

  test "unsubscribe requires the returned subscription handle" do
    assert {:error, :invalid_log_subscription} =
             FavnOrchestrator.unsubscribe_logs(%Filter{run_id: "run_unsubscribe_filter"})
  end

  test "live asset-step subscriptions use asset topics and honor remaining filters" do
    assert {:ok, subscription} =
             FavnOrchestrator.subscribe_logs(%Filter{
               run_id: "run_live_asset",
               asset_step_id: "step_a",
               levels: [:error]
             })

    assert {:ok, _} =
             FavnOrchestrator.emit_log(%Entry{
               run_id: "run_live_asset",
               asset_step_id: "step_b",
               level: :error,
               message: "other step"
             })

    assert {:ok, _} =
             FavnOrchestrator.emit_log(%Entry{
               run_id: "run_live_asset",
               asset_step_id: "step_a",
               level: :info,
               message: "filtered level"
             })

    assert {:ok, [persisted]} =
             FavnOrchestrator.emit_log(%Entry{
               run_id: "run_live_asset",
               asset_step_id: "step_a",
               level: :error,
               message: "wanted step"
             })

    assert {:ok, received} = receive_log_entry("run_live_asset")
    assert received.global_sequence == persisted.global_sequence
    assert received.asset_step_id == "step_a"
    assert received.level == :error
    refute_receive {:favn_log_entry, %Entry{}}, 100

    assert :ok = FavnOrchestrator.unsubscribe_logs(subscription)
  end

  defp receive_log_entry(run_id, timeout \\ 1_000) do
    receive do
      {:favn_log_entry, %Entry{run_id: ^run_id} = entry} -> {:ok, entry}
      {:favn_log_entry, %Entry{}} -> receive_log_entry(run_id, timeout)
    after
      timeout -> {:error, :timeout}
    end
  end

  defp eventually(fun, attempts \\ 20)

  defp eventually(fun, attempts) when attempts > 0 do
    fun.()
  rescue
    ExUnit.AssertionError ->
      Process.sleep(25)
      eventually(fun, attempts - 1)
  end

  defp eventually(fun, 0), do: fun.()

  defp restore_env(app, key, nil), do: Application.delete_env(app, key)
  defp restore_env(app, key, value), do: Application.put_env(app, key, value)
end
