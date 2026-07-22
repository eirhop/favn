defmodule FavnOrchestrator.ShutdownTest do
  use ExUnit.Case, async: false

  alias FavnOrchestrator.Lifecycle
  alias FavnOrchestrator.Shutdown

  defmodule IdleRunManager do
    def active_runs, do: {:ok, []}
    def stop_active_for_shutdown, do: {:ok, 0}
  end

  defmodule UnavailableRunManager do
    def active_runs, do: {:error, :unavailable}
    def stop_active_for_shutdown, do: {:error, :unavailable}
  end

  defmodule DisappearingRunManager do
    def active_runs, do: {:ok, Agent.get(__MODULE__, & &1)}

    def cancel_run(_context, _run_id, _reason) do
      Agent.update(__MODULE__, fn _runs -> [] end)
      :ok
    end

    def stop_active_for_shutdown, do: {:ok, 0}
  end

  test "idle drain flips lifecycle before reporting completion" do
    name = unique_name()
    start_supervised!({Lifecycle, name: name, shutdown_drain_timeout_ms: 1_000})
    :ok = Lifecycle.mark_accepting(name)

    opts = [
      lifecycle: name,
      run_manager: IdleRunManager,
      timeout_ms: 20,
      poll_interval_ms: 1,
      settle_timeout_ms: 0
    ]

    assert {:ok, %{status: :drained, active_runs_at_start: 0} = result} =
             Shutdown.drain(opts)

    assert {:ok, ^result} = Shutdown.drain(opts)

    assert %{status: :stopping, ready?: false} = Lifecycle.diagnostics(name)
  end

  test "deadline leaves an unfinished admission visible instead of inventing completion" do
    name = unique_name()
    start_supervised!({Lifecycle, name: name, shutdown_drain_timeout_ms: 1_000})
    :ok = Lifecycle.mark_accepting(name)
    parent = self()

    task =
      Task.async(fn ->
        Lifecycle.with_admission(
          fn ->
            send(parent, :admitted)
            receive do: (:finish -> :ok)
          end,
          name
        )
      end)

    assert_receive :admitted

    assert {:ok,
            %{
              status: :cancelled_at_deadline,
              active_admissions_at_start: 1,
              active_admissions_remaining: 1
            }} =
             Shutdown.drain(
               lifecycle: name,
               run_manager: IdleRunManager,
               timeout_ms: 10,
               poll_interval_ms: 1,
               cancel_timeout_ms: 10,
               settle_timeout_ms: 0
             )

    send(task.pid, :finish)
    assert :ok = Task.await(task)
  end

  test "concurrent drains share the elected coordinator result" do
    name = unique_name()
    start_supervised!({Lifecycle, name: name, shutdown_drain_timeout_ms: 1_000})
    :ok = Lifecycle.mark_accepting(name)
    parent = self()

    admitted =
      Task.async(fn ->
        Lifecycle.with_admission(
          fn ->
            send(parent, :admitted_for_shared_drain)
            receive do: (:finish_shared_drain -> :ok)
          end,
          name
        )
      end)

    assert_receive :admitted_for_shared_drain

    opts = [
      lifecycle: name,
      run_manager: IdleRunManager,
      timeout_ms: 500,
      poll_interval_ms: 1,
      settle_timeout_ms: 0
    ]

    leader = Task.async(fn -> Shutdown.drain(opts) end)
    assert_eventually(fn -> Lifecycle.diagnostics(name).status == :draining end)
    follower = Task.async(fn -> Shutdown.drain(opts) end)

    send(admitted.pid, :finish_shared_drain)
    assert :ok = Task.await(admitted)

    assert {:ok, result} = Task.await(leader)
    assert {:ok, ^result} = Task.await(follower)
  end

  test "multiple followers rejoin one replacement after the elected leader dies" do
    name = unique_name()
    start_supervised!({Lifecycle, name: name, shutdown_drain_timeout_ms: 1_000})
    :ok = Lifecycle.mark_accepting(name)
    parent = self()

    admitted =
      Task.async(fn ->
        Lifecycle.with_admission(
          fn ->
            send(parent, :admitted_for_takeover)
            receive do: (:finish_takeover -> :ok)
          end,
          name
        )
      end)

    assert_receive :admitted_for_takeover

    failed_leader =
      spawn(fn ->
        send(parent, {:failed_leader_elected, Lifecycle.begin_shutdown(name)})
        Process.sleep(:infinity)
      end)

    assert_receive {:failed_leader_elected, :leader}

    opts = [
      lifecycle: name,
      run_manager: IdleRunManager,
      timeout_ms: 500,
      poll_interval_ms: 1,
      settle_timeout_ms: 0
    ]

    follower_one = Task.async(fn -> Shutdown.drain(opts) end)
    follower_two = Task.async(fn -> Shutdown.drain(opts) end)
    assert_eventually(fn -> length(:sys.get_state(name).shutdown_waiters) == 2 end)

    Process.exit(failed_leader, :kill)

    assert_eventually(fn ->
      match?({:in_progress, _owner, _monitor}, :sys.get_state(name).shutdown)
    end)

    send(admitted.pid, :finish_takeover)
    assert :ok = Task.await(admitted)

    assert {:ok, result} = Task.await(follower_one)
    assert {:ok, ^result} = Task.await(follower_two)
  end

  test "unavailable run enumeration is reported as unknown instead of idle" do
    name = unique_name()
    start_supervised!({Lifecycle, name: name, shutdown_drain_timeout_ms: 1_000})
    :ok = Lifecycle.mark_accepting(name)

    assert {:ok,
            %{
              status: :state_unknown,
              active_runs_at_start: :unknown,
              forced_run_server_stops: :unknown
            }} =
             Shutdown.drain(
               lifecycle: name,
               run_manager: UnavailableRunManager,
               timeout_ms: 10,
               poll_interval_ms: 1,
               cancel_timeout_ms: 10,
               settle_timeout_ms: 0
             )
  end

  test "local disappearance after a cancellation request remains an unknown durable outcome" do
    name = unique_name()
    task_supervisor = unique_name()
    run = %{workspace_id: "workspace", run_id: "run"}

    start_supervised!(%{
      id: DisappearingRunManager,
      start: {Agent, :start_link, [fn -> [run] end, [name: DisappearingRunManager]]}
    })

    start_supervised!({Task.Supervisor, name: task_supervisor})
    start_supervised!({Lifecycle, name: name, shutdown_drain_timeout_ms: 1_000})
    :ok = Lifecycle.mark_accepting(name)

    assert {:ok,
            %{
              status: :cancelled_at_deadline,
              active_runs_remaining: 0,
              cancellation: %{
                targeted: 1,
                requested: 1,
                locally_inactive_before_force_stop: 1,
                settled_before_force_stop: 0,
                unknown_outcomes: 1
              }
            }} =
             Shutdown.drain(
               lifecycle: name,
               run_manager: DisappearingRunManager,
               task_supervisor: task_supervisor,
               timeout_ms: 5,
               poll_interval_ms: 1,
               cancel_timeout_ms: 100,
               settle_timeout_ms: 10
             )
  end

  defp unique_name, do: :"shutdown_lifecycle_#{System.unique_integer([:positive, :monotonic])}"

  defp assert_eventually(fun, attempts \\ 100)
  defp assert_eventually(_fun, 0), do: flunk("condition did not become true")

  defp assert_eventually(fun, attempts) do
    if fun.() do
      :ok
    else
      Process.sleep(5)
      assert_eventually(fun, attempts - 1)
    end
  end
end
