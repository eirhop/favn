defmodule FavnOrchestrator.RunManager.RecoveryMonitorTest do
  use ExUnit.Case, async: false

  alias FavnOrchestrator.RunManager
  alias FavnOrchestrator.RunManager.PlanCapacity
  alias FavnOrchestrator.RunState

  test "a late monitor message cannot untrack the replacement run server" do
    start_supervised!(RunManager)
    old = spawn(fn -> receive do: (:stop -> :ok) end)
    replacement = spawn(fn -> receive do: (:stop -> :ok) end)

    on_exit(fn ->
      send(old, :stop)
      send(replacement, :stop)
    end)

    key = {"workspace", "recovered-run"}
    run = %RunState{id: elem(key, 1), workspace_id: elem(key, 0), plan: %Favn.Plan{}}

    :sys.replace_state(RunManager, fn state ->
      old_monitor = Process.monitor(old)
      new_monitor = Process.monitor(replacement)
      {:ok, capacity} = PlanCapacity.reserve(state.plan_capacity, key, run)

      %{
        state
        | run_pids: %{key => replacement},
          lifecycles: %{
            :old => entry(key, old, :stopping),
            :new => entry(key, replacement, :running)
          },
          process_monitors: %{
            old_monitor => {:old, :coordinator},
            new_monitor => {:new, :coordinator}
          },
          plan_capacity: capacity
      }
    end)

    # Kill the old owner after a new owner has been tracked, then wait for its
    # monitor notification to be processed before checking manager authority.
    send(old, :stop)
    await_monitor_count!(1)

    assert {:ok, [%{workspace_id: "workspace", run_id: "recovered-run"}]} =
             RunManager.active_runs()

    assert {:ok, %{active_run_count: 1, allocated_bytes: bytes}} =
             RunManager.plan_capacity_diagnostics()

    assert bytes > 0

    send(replacement, :stop)
    await_monitor_count!(0)
    assert {:ok, []} = RunManager.active_runs()
    assert {:ok, %{allocated_bytes: 0}} = RunManager.plan_capacity_diagnostics()
  end

  defp entry(key, coordinator, phase),
    do: %{
      key: key,
      coordinator: coordinator,
      phase: phase,
      ownership: nil,
      pids: MapSet.new([coordinator]),
      waiting: [],
      keeper: nil,
      maintenance_ready?: false
    }

  defp await_monitor_count!(expected, attempts \\ 100)
  defp await_monitor_count!(_expected, 0), do: flunk("monitor notification was not processed")

  defp await_monitor_count!(expected, attempts) do
    if map_size(:sys.get_state(RunManager).process_monitors) == expected do
      :ok
    else
      Process.sleep(5)
      await_monitor_count!(expected, attempts - 1)
    end
  end
end
