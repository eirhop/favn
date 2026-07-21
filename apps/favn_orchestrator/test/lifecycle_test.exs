defmodule FavnOrchestrator.LifecycleTest do
  use ExUnit.Case, async: true

  alias FavnOrchestrator.Lifecycle

  test "admission permits finish across a monotonic drain boundary" do
    name = unique_name()
    start_supervised!({Lifecycle, name: name, shutdown_drain_timeout_ms: 5_000})

    assert %{status: :starting, ready?: false} = Lifecycle.diagnostics(name)
    assert {:error, :runtime_starting} = Lifecycle.ensure_accepting(name)
    assert :ok = Lifecycle.mark_accepting(name)

    parent = self()

    task =
      Task.async(fn ->
        Lifecycle.with_admission(
          fn ->
            send(parent, :admitted)
            receive do: (:finish -> :finished)
          end,
          name
        )
      end)

    assert_receive :admitted
    assert %{active_admissions: 1, status: :accepting} = Lifecycle.diagnostics(name)

    assert :ok = Lifecycle.drain(name)
    assert {:error, :runtime_draining} = Lifecycle.ensure_accepting(name)
    assert {:error, :runtime_draining} = Lifecycle.with_admission(fn -> :never end, name)

    send(task.pid, :finish)
    assert :finished = Task.await(task)
    assert %{active_admissions: 0, status: :draining} = Lifecycle.diagnostics(name)

    assert :ok = Lifecycle.stop(name)
    assert :ok = Lifecycle.drain(name)
    assert %{status: :stopping, ready?: false} = Lifecycle.diagnostics(name)
  end

  test "a failed admission owner cannot leave the runtime busy" do
    name = unique_name()
    start_supervised!({Lifecycle, name: name, shutdown_drain_timeout_ms: 5_000})
    :ok = Lifecycle.mark_accepting(name)
    parent = self()

    pid =
      spawn(fn ->
        Lifecycle.with_admission(
          fn ->
            send(parent, :admitted)
            Process.sleep(:infinity)
          end,
          name
        )
      end)

    assert_receive :admitted
    Process.exit(pid, :kill)

    assert_eventually(fn -> Lifecycle.diagnostics(name).active_admissions == 0 end)
  end

  test "beginning shutdown cannot regress an already stopping lifecycle" do
    name = unique_name()
    start_supervised!({Lifecycle, name: name, shutdown_drain_timeout_ms: 5_000})

    assert :ok = Lifecycle.stop(name)
    assert :leader = Lifecycle.begin_shutdown(name)
    assert %{status: :stopping} = Lifecycle.diagnostics(name)
  end

  test "shutdown election recovers when the elected coordinator exits" do
    name = unique_name()
    start_supervised!({Lifecycle, name: name, shutdown_drain_timeout_ms: 5_000})
    :ok = Lifecycle.mark_accepting(name)
    parent = self()

    owner =
      spawn(fn ->
        send(parent, {:elected, Lifecycle.begin_shutdown(name)})
        Process.sleep(:infinity)
      end)

    assert_receive {:elected, :leader}
    Process.exit(owner, :kill)

    assert {:error, :shutdown_coordinator_failed} = Lifecycle.await_shutdown(1_000, name)
    assert :leader = Lifecycle.begin_shutdown(name)
    assert :ok = Lifecycle.complete_shutdown(%{status: :recovered}, name)
  end

  test "an admitted owner can enter nested boundaries after draining starts" do
    name = unique_name()
    start_supervised!({Lifecycle, name: name, shutdown_drain_timeout_ms: 5_000})
    :ok = Lifecycle.mark_accepting(name)
    parent = self()

    task =
      Task.async(fn ->
        Lifecycle.with_admission(
          fn ->
            send(parent, :outer_admitted)
            receive do: (:continue -> :ok)
            Lifecycle.with_admission(fn -> :nested_finished end, name)
          end,
          name
        )
      end)

    assert_receive :outer_admitted
    :ok = Lifecycle.drain(name)
    send(task.pid, :continue)
    assert :nested_finished = Task.await(task)
    assert Lifecycle.diagnostics(name).active_admissions == 0
  end

  test "lifecycle transitions emit bounded telemetry" do
    name = unique_name()
    handler = "orchestrator-lifecycle-#{System.unique_integer([:positive])}"
    parent = self()

    :ok =
      :telemetry.attach(
        handler,
        [:favn, :orchestrator, :lifecycle_transition],
        fn event, measurements, metadata, _config ->
          send(parent, {event, measurements, metadata})
        end,
        nil
      )

    on_exit(fn -> :telemetry.detach(handler) end)
    start_supervised!({Lifecycle, name: name, shutdown_drain_timeout_ms: 5_000})
    :ok = Lifecycle.mark_accepting(name)

    assert_receive {[:favn, :orchestrator, :lifecycle_transition],
                    %{duration_in_previous_state_ms: duration},
                    %{from: :starting, to: :accepting}}

    assert is_integer(duration) and duration >= 0
  end

  defp assert_eventually(fun, attempts \\ 50)
  defp assert_eventually(_fun, 0), do: flunk("condition did not become true")

  defp assert_eventually(fun, attempts) do
    if fun.() do
      :ok
    else
      Process.sleep(5)
      assert_eventually(fun, attempts - 1)
    end
  end

  defp unique_name, do: :"lifecycle_#{System.unique_integer([:positive, :monotonic])}"
end
