defmodule FavnRunner.ApplicationTest do
  use ExUnit.Case, async: false

  test "a lifecycle crash restarts runner dependencies and restores admission" do
    lifecycle = Process.whereis(FavnRunner.Lifecycle)
    server = Process.whereis(FavnRunner.Server)
    starter = Process.whereis(FavnRunner.RuntimeStarter)

    assert is_pid(lifecycle)
    assert is_pid(server)
    assert is_pid(starter)
    assert FavnRunner.Lifecycle.diagnostics().status == :accepting

    Process.exit(lifecycle, :kill)

    assert_eventually(fn ->
      restarted_lifecycle = Process.whereis(FavnRunner.Lifecycle)
      restarted_server = Process.whereis(FavnRunner.Server)
      restarted_starter = Process.whereis(FavnRunner.RuntimeStarter)

      is_pid(restarted_lifecycle) and restarted_lifecycle != lifecycle and
        is_pid(restarted_server) and restarted_server != server and
        is_pid(restarted_starter) and restarted_starter != starter and
        FavnRunner.Lifecycle.diagnostics().status == :accepting
    end)
  end

  for critical <- [FavnRunner.Server, FavnRunner.ManifestStore] do
    test "a #{inspect(critical)} crash cannot orphan an active worker" do
      critical = unquote(critical)
      lifecycle = Process.whereis(FavnRunner.Lifecycle)
      worker_supervisor = Process.whereis(FavnRunner.WorkerSupervisor)
      critical_pid = Process.whereis(critical)

      child = %{
        id: make_ref(),
        start: {Task, :start_link, [fn -> Process.sleep(:infinity) end]},
        restart: :temporary
      }

      assert {:ok, worker} =
               DynamicSupervisor.start_child(FavnRunner.WorkerSupervisor, child)

      assert Process.alive?(worker)
      Process.exit(critical_pid, :kill)

      assert_eventually(fn ->
        restarted_lifecycle = Process.whereis(FavnRunner.Lifecycle)
        restarted_worker_supervisor = Process.whereis(FavnRunner.WorkerSupervisor)
        restarted_critical = Process.whereis(critical)

        not Process.alive?(worker) and is_pid(restarted_lifecycle) and
          restarted_lifecycle != lifecycle and is_pid(restarted_worker_supervisor) and
          restarted_worker_supervisor != worker_supervisor and is_pid(restarted_critical) and
          restarted_critical != critical_pid and
          FavnRunner.Lifecycle.diagnostics().status == :accepting
      end)
    end
  end

  defp assert_eventually(fun, attempts \\ 100)
  defp assert_eventually(_fun, 0), do: flunk("condition did not become true")

  defp assert_eventually(fun, attempts) do
    if fun.() do
      :ok
    else
      Process.sleep(10)
      assert_eventually(fun, attempts - 1)
    end
  end
end
