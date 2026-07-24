defmodule Favn.DockerFreeLocalLifecycleAcceptanceTest do
  use ExUnit.Case, async: false

  alias Favn.Manifest.Publication
  alias FavnLocal.Lifecycle
  alias FavnLocal.Locator
  alias FavnLocal.Publication, as: LocalPublication
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

    assert {:ok, reloaded} = FavnLocal.reload(root_dir: root_dir, reload_timeout_ms: 60_000)
    assert reloaded.runner_release_id != started.runner_release_id

    failed_release_id = FavnTestSupport.runner_release_id(:alternate)
    assert {:ok, %Publication{} = publication} = LocalPublication.build(failed_release_id)
    invalid_publication = %{publication | execution_packages: [%{}]}

    assert {:error, _reason} =
             Lifecycle.reload(invalid_publication, failed_release_id, 60_000)

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
end
