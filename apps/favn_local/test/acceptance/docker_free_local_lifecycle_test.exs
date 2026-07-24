defmodule FavnLocal.DockerFreeLocalLifecycleAcceptanceTest do
  use ExUnit.Case, async: false

  alias Favn.Manifest.Publication
  alias FavnLocal.Lifecycle, as: LocalLifecycle
  alias FavnLocal.Locator
  alias FavnLocal.Publication, as: LocalPublication
  alias FavnOrchestrator.Lifecycle, as: OrchestratorLifecycle
  alias FavnStoragePostgres.Release

  @moduletag :acceptance
  @moduletag timeout: 180_000

  test "source development starts, reloads, and shuts down safely after reload failure" do
    root_dir =
      Path.join(System.tmp_dir!(), "favn_source_dev_#{System.unique_integer([:positive])}")

    workspace_id = "source-dev-#{System.unique_integer([:positive])}"
    File.mkdir_p!(Path.join(root_dir, "config"))
    File.write!(Path.join([root_dir, "config", "config.exs"]), "import Config\n")
    on_exit(fn -> File.rm_rf(root_dir) end)

    assert {:ok, %{status: :ok}} =
             Release.provision_workspace(
               workspace_id: workspace_id,
               slug: workspace_id,
               display_name: "Source Development Acceptance"
             )

    Application.put_env(:favn, :dev,
      workspace_id: workspace_id,
      orchestrator_port: free_port(),
      view_port: free_port()
    )

    assert {:ok, started} = FavnLocal.dev(root_dir: root_dir, startup_timeout_ms: 60_000)
    assert started.status == :ready
    assert Process.alive?(started.supervisor)

    test_process = self()

    admission_holder =
      Task.async(fn ->
        {:ok, permit} = OrchestratorLifecycle.acquire_admission()
        send(test_process, :admission_held)

        receive do
          :release_admission -> :ok
        end

        :ok = OrchestratorLifecycle.release_admission(permit)
      end)

    assert_receive :admission_held

    reload =
      Task.async(fn ->
        FavnLocal.reload(root_dir: root_dir, reload_timeout_ms: 60_000)
      end)

    assert_eventually(fn -> LocalLifecycle.status().status == :reloading end)
    assert nil == Task.yield(reload, 100)

    send(admission_holder.pid, :release_admission)
    assert :ok = Task.await(admission_holder)
    assert {:ok, reloaded} = Task.await(reload, 60_000)
    assert reloaded.runner_release_id != started.runner_release_id

    failed_release_id = FavnTestSupport.runner_release_id(:alternate)
    assert {:ok, %Publication{} = publication} = LocalPublication.build(failed_release_id)
    invalid_publication = %{publication | execution_packages: [%{}]}

    assert {:error, _reason} =
             LocalLifecycle.reload(invalid_publication, failed_release_id, 60_000)

    ref = Process.monitor(started.supervisor)
    assert_receive {:DOWN, ^ref, :process, _pid, _reason}, 10_000
    assert {:error, :not_running} = Locator.read(root_dir)
  end

  defp free_port do
    {:ok, socket} = :gen_tcp.listen(0, [:binary, active: false, ip: {127, 0, 0, 1}])
    {:ok, {_address, port}} = :inet.sockname(socket)
    :ok = :gen_tcp.close(socket)
    port
  end

  defp assert_eventually(fun, attempts \\ 300)

  defp assert_eventually(fun, attempts) when attempts > 0 do
    if fun.() do
      :ok
    else
      Process.sleep(100)
      assert_eventually(fun, attempts - 1)
    end
  end

  defp assert_eventually(_fun, 0), do: flunk("condition did not become true")
end
