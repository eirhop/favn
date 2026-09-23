defmodule FavnOrchestrator.RunManager.StartCapacityTest do
  use ExUnit.Case, async: false
  alias FavnOrchestrator.Persistence.{Runtime, Stores, SystemContext}
  alias FavnOrchestrator.RunManager

  defmodule Store do
    def claim_run(command) do
      send(Process.whereis(:run_capacity_test), {:claim_started, command.run_id})
      receive do: (:finish -> {:error, :test_finished})
    end
  end

  setup do
    Process.register(self(), :run_capacity_test)
    stores = struct(Stores, Map.new(Map.keys(Map.from_struct(struct(Stores))), &{&1, Store}))
    start_supervised!({Runtime, %Runtime{backend: __MODULE__, options: [], stores: stores}})
    start_supervised!({FavnOrchestrator.RunControlSupervisor, []})
    %{context: SystemContext.workspace("capacity", :run_recovery)}
  end

  test "only four preparations may claim while storage is blocked", %{context: context} do
    for id <- 1..4 do
      run = "run-#{id}"
      assert {:ok, ^run} = RunManager.recover_candidate(context, run)
      assert_receive {:claim_started, ^run}, 1_000
    end

    assert {:error, :run_start_capacity} = RunManager.recover_candidate(context, "fifth")
    refute_receive {:claim_started, "fifth"}
    assert map_size(:sys.get_state(RunManager).lifecycles) == 4
    assert {:ok, active} = RunManager.active_runs()
    assert length(active) == 4
  end

  test "claim deadlines reclaim all blocked preparation slots through process DOWN", %{
    context: context
  } do
    for id <- 1..4 do
      assert {:ok, _} = RunManager.recover_candidate(context, "blocked-#{id}")
      assert_receive {:claim_started, _}, 1_000
    end

    manager = Process.whereis(RunManager)
    entries = :sys.get_state(manager).lifecycles

    monitors =
      Enum.map(entries, fn {id, entry} ->
        monitor = Process.monitor(entry.preparer)
        assert entry.claim_deadline - System.monotonic_time(:millisecond) <= 20_000
        send(manager, {:claim_deadline, id})
        {entry.preparer, monitor}
      end)

    for {pid, monitor} <- monitors,
        do: assert_receive({:DOWN, ^monitor, :process, ^pid, _}, 1_000)

    # Ordering this barrier after DOWN guarantees that all manager monitors were drained.
    wait_for_empty(manager, 100)
    assert {:ok, "after-timeout"} = RunManager.recover_candidate(context, "after-timeout")
    assert_receive {:claim_started, "after-timeout"}
    assert Process.whereis(RunManager) == manager
  end

  defp wait_for_empty(manager, attempts) do
    if map_size(:sys.get_state(manager).lifecycles) != 0 do
      assert attempts > 0

      receive do
      after
        5 -> :ok
      end

      wait_for_empty(manager, attempts - 1)
    end
  end

  test "a stopping generation remains active until its last helper is down", %{context: context} do
    helper =
      spawn(fn ->
        receive do
          :stop -> :ok
        end
      end)

    on_exit(fn -> Process.exit(helper, :kill) end)

    :sys.replace_state(RunManager, fn state ->
      %{
        state
        | lifecycles: %{
            make_ref() => %{
              key: {context.workspace_id, "draining"},
              coordinator: nil,
              phase: :stopping,
              pids: MapSet.new([helper])
            }
          }
      }
    end)

    assert {:ok, [%{run_id: "draining"}]} = RunManager.active_runs()
  end

  test "the default 64 unique run slots reject work before acquiring ownership", %{
    context: context
  } do
    :sys.replace_state(RunManager, fn state ->
      entries =
        Map.new(1..64, fn id ->
          {id, %{key: {context.workspace_id, "run-#{id}"}, ownership: nil, phase: :running}}
        end)

      %{state | lifecycles: entries}
    end)

    assert {:error, :run_start_capacity} = RunManager.recover_candidate(context, "extra")
    refute_receive {:claim_started, "extra"}
  end
end
