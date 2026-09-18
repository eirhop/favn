defmodule FavnOrchestrator.LifecycleTest do
  use ExUnit.Case, async: true

  alias FavnOrchestrator.Lifecycle

  test "deployment delegation rejects other owners and closes with its parent across drain and stop" do
    name = :"delegation_lifecycle_#{System.unique_integer([:positive])}"
    start_supervised!({Lifecycle, name: name, shutdown_drain_timeout_ms: 5_000})
    assert :ok = Lifecycle.mark_accepting(name)
    token = String.duplicate("e", 43)
    assert {:ok, ^token} = Lifecycle.begin_maintenance(:deployment, token, name)
    assert {:ok, permit} = Lifecycle.acquire_maintenance_admission(token, name)

    assert Task.async(fn -> Lifecycle.delegate_deployment(permit, "ws", "op", name) end)
           |> Task.await() ==
             {:error, :invalid_admission_permit}

    assert :ok = Lifecycle.delegate_deployment(permit, "ws", "op", name)
    assert {:ok, second} = Lifecycle.acquire_maintenance_admission(token, name)

    assert {:error, :deployment_admission_conflict} =
             Lifecycle.delegate_deployment(second, "ws", "op", name)

    assert :ok = Lifecycle.release_admission(second, name)
    assert :ok = Lifecycle.drain(name)

    assert {:error, :invalid_admission_permit} =
             Lifecycle.delegate_deployment(permit, "ws", "later", name)

    parent = self()

    child =
      spawn(fn ->
        Lifecycle.with_deployment_admission(
          "ws",
          "op",
          fn ->
            send(parent, :child_admitted)
            receive do: (:finish -> :ok)
          end,
          name
        )
      end)

    assert_receive :child_admitted
    assert :ok = Lifecycle.release_admission(permit, name)
    assert %{active_admissions: 1} = Lifecycle.diagnostics(name)
    Process.exit(child, :kill)
    monitor = Process.monitor(child)
    assert_receive {:DOWN, ^monitor, :process, ^child, _}
    # A same-sender call is a barrier after explicitly delivering the monitored exit.
    eventually_no_admissions(name)

    assert Task.async(fn ->
             Lifecycle.with_deployment_admission("ws", "op", fn -> :wrong end, name)
           end)
           |> Task.await() ==
             {:error, :runtime_draining}

    assert :ok = Lifecycle.stop(name)

    assert Task.async(fn ->
             Lifecycle.with_deployment_admission("ws", "op", fn -> :wrong end, name)
           end)
           |> Task.await() ==
             {:error, :runtime_draining}
  end

  defp eventually_no_admissions(name, remaining \\ 100)
  defp eventually_no_admissions(_name, 0), do: flunk("child admission was not released")

  defp eventually_no_admissions(name, remaining) do
    unless Lifecycle.diagnostics(name).active_admissions == 0 do
      Process.sleep(1)
      eventually_no_admissions(name, remaining - 1)
    end
  end

  test "delegated deployment admission is scoped, monitored and preserves maintenance authorization" do
    name = :"deployment_lifecycle_#{System.unique_integer([:positive])}"
    start_supervised!({Lifecycle, name: name, shutdown_drain_timeout_ms: 5_000})
    assert :ok = Lifecycle.mark_accepting(name)
    token = String.duplicate("d", 43)
    assert {:ok, ^token} = Lifecycle.begin_maintenance(:deployment, token, name)
    assert {:ok, permit} = Lifecycle.acquire_maintenance_admission(token, name)
    assert :ok = Lifecycle.delegate_deployment(permit, "workspace", "operation", name)

    assert Task.async(fn ->
             Lifecycle.with_deployment_admission(
               "workspace",
               "operation",
               fn ->
                 Lifecycle.with_admission(fn -> :activated end, name)
               end,
               name
             )
           end)
           |> Task.await() == :activated

    assert Task.async(fn ->
             Lifecycle.with_deployment_admission("other", "operation", fn -> :wrong end, name)
           end)
           |> Task.await() == {:error, :runtime_maintenance}

    assert :ok = Lifecycle.release_admission(permit, name)

    assert Task.async(fn ->
             Lifecycle.with_deployment_admission("workspace", "operation", fn -> :wrong end, name)
           end)
           |> Task.await() == {:error, :runtime_maintenance}
  end

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

  test "runner maintenance is resumable and admits only the opaque lease owner" do
    name = unique_name()
    start_supervised!({Lifecycle, name: name, shutdown_drain_timeout_ms: 5_000})
    :ok = Lifecycle.mark_accepting(name)
    token = String.duplicate("a", 43)

    assert {:ok, ^token} = Lifecycle.begin_maintenance(:operator_maintenance, token, name)

    assert {:ok, ^token} =
             Lifecycle.begin_maintenance(:operator_maintenance, token, name)

    assert {:error, :maintenance_active} =
             Lifecycle.begin_maintenance(:operator_maintenance, String.duplicate("b", 43), name)

    assert {:error, :invalid_maintenance_token} =
             Lifecycle.begin_maintenance(:operator_maintenance, "short", name)

    assert %{
             status: :accepting,
             ready?: false,
             accepting?: false,
             maintenance?: true,
             maintenance_kind: :operator_maintenance,
             active_admissions: 0
           } = Lifecycle.diagnostics(name)

    assert {:error, :runtime_maintenance} = Lifecycle.acquire_admission(name)
    assert :ok = Lifecycle.ensure_accepting(name)
    assert {:error, :runtime_maintenance} = Lifecycle.ensure_ready(name)

    assert {:error, :runtime_maintenance} =
             Task.async(fn -> Lifecycle.acquire_admission(name) end) |> Task.await()

    assert {:ok, permit} = Lifecycle.acquire_maintenance_admission(token, name)
    assert :ok = Lifecycle.release_admission(permit, name)
    assert {:error, :invalid_maintenance_token} = Lifecycle.end_maintenance("wrong", name)
    assert :ok = Lifecycle.end_maintenance(token, name)
    assert %{ready?: true, accepting?: true, maintenance?: false} = Lifecycle.diagnostics(name)
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
