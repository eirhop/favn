defmodule FavnRunner.LifecycleTest do
  use ExUnit.Case, async: true

  alias FavnRunner.Lifecycle

  test "runner admission is rejected after drain while an existing permit can finish" do
    name = unique_name()
    start_supervised!({Lifecycle, name: name, shutdown_drain_timeout_ms: 2_000})
    assert :ok = Lifecycle.mark_accepting(name)
    parent = self()

    task =
      Task.async(fn ->
        Lifecycle.with_admission(
          fn ->
            send(parent, :running)
            receive do: (:finish -> :ok)
          end,
          name
        )
      end)

    assert_receive :running
    assert :ok = Lifecycle.drain(name)
    assert {:error, :runtime_draining} = Lifecycle.with_admission(fn -> :never end, name)
    assert Lifecycle.diagnostics(name).active_admissions == 1

    send(task.pid, :finish)
    assert :ok = Task.await(task)
    assert Lifecycle.diagnostics(name).active_admissions == 0
  end

  test "an admitted owner can enter nested boundaries after draining starts" do
    name = unique_name()
    start_supervised!({Lifecycle, name: name, shutdown_drain_timeout_ms: 2_000})
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

  test "beginning shutdown cannot regress an already stopping lifecycle" do
    name = unique_name()
    start_supervised!({Lifecycle, name: name, shutdown_drain_timeout_ms: 2_000})

    assert :ok = Lifecycle.stop(name)
    assert :leader = Lifecycle.begin_shutdown(name)
    assert %{status: :stopping} = Lifecycle.diagnostics(name)
  end

  test "shutdown election recovers when the elected coordinator exits" do
    name = unique_name()
    start_supervised!({Lifecycle, name: name, shutdown_drain_timeout_ms: 2_000})
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

  test "runner lifecycle transitions emit bounded telemetry" do
    name = unique_name()
    handler = "runner-lifecycle-#{System.unique_integer([:positive])}"
    parent = self()

    :ok =
      :telemetry.attach(
        handler,
        [:favn, :runner, :lifecycle_transition],
        fn event, measurements, metadata, _config ->
          send(parent, {event, measurements, metadata})
        end,
        nil
      )

    on_exit(fn -> :telemetry.detach(handler) end)
    start_supervised!({Lifecycle, name: name, shutdown_drain_timeout_ms: 2_000})
    :ok = Lifecycle.mark_accepting(name)

    assert_receive {[:favn, :runner, :lifecycle_transition],
                    %{duration_in_previous_state_ms: duration},
                    %{from: :starting, to: :accepting}}

    assert is_integer(duration) and duration >= 0
  end

  defp unique_name, do: :"runner_lifecycle_#{System.unique_integer([:positive, :monotonic])}"
end
