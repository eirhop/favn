defmodule FavnOrchestrator.RunServerTest do
  use ExUnit.Case, async: false

  alias Favn.Manifest
  alias Favn.Manifest.Version
  alias FavnOrchestrator.RunServer
  alias FavnOrchestrator.RunState
  alias FavnOrchestrator.Storage
  alias FavnOrchestrator.Storage.Adapter.Memory

  defmodule RunnerClientCancelBeforeStepStartedStub do
    @behaviour Favn.Contracts.RunnerClient

    @impl true
    def register_manifest(_version, _opts), do: :ok

    @impl true
    def submit_work(work, _opts) do
      {:ok, running} = Storage.get_run(work.run_id)

      cancelled =
        running
        |> RunState.transition(
          status: :cancelled,
          error: {:cancelled, %{reason: :submit_race}},
          runner_execution_id: nil,
          metadata: Map.put(running.metadata, :cancelled, true)
        )

      :ok = Storage.put_run(cancelled)
      {:ok, "exec_#{work.run_id}"}
    end

    @impl true
    def await_result(_execution_id, _timeout, _opts) do
      raise "await_result/3 should not be called after external cancel wins step_started"
    end

    @impl true
    def cancel_work(_execution_id, _reason, _opts), do: :ok

    @impl true
    def inspect_relation(_request, _opts), do: {:error, :not_supported}
  end

  setup do
    previous_client = Application.get_env(:favn_orchestrator, :runner_client)
    previous_opts = Application.get_env(:favn_orchestrator, :runner_client_opts)

    Application.put_env(:favn_orchestrator, :runner_client, nil)
    Application.put_env(:favn_orchestrator, :runner_client_opts, [])

    Memory.reset()

    on_exit(fn ->
      Application.put_env(:favn_orchestrator, :runner_client, previous_client)
      Application.put_env(:favn_orchestrator, :runner_client_opts, previous_opts)
      Memory.reset()
    end)

    :ok
  end

  test "marks run as failed when runner client is unavailable" do
    version = manifest_version("mv_run_server")

    run_state =
      RunState.new(
        id: "run_server_1",
        manifest_version_id: version.manifest_version_id,
        manifest_content_hash: version.content_hash,
        asset_ref: {MyApp.Assets.Gold, :asset},
        target_refs: [{MyApp.Assets.Gold, :asset}]
      )

    assert :ok = Storage.put_run(run_state)

    assert {:ok, pid} =
             RunServer.start_link(%{
               run_state: run_state,
               version: version
             })

    ref = Process.monitor(pid)
    assert_receive {:DOWN, ^ref, :process, ^pid, :normal}, 1_000

    assert {:ok, run} = Storage.get_run("run_server_1")
    assert run.status == :error
    assert run.error == :runner_client_not_available

    assert {:ok, events} = Storage.list_run_events("run_server_1")
    assert Enum.map(events, & &1.event_type) == [:run_started, :step_failed, :run_failed]
  end

  test "does not crash when run_started persist loses to external cancel" do
    version = manifest_version("mv_run_server_cancelled_before_start")

    run_state =
      RunState.new(
        id: "run_server_cancelled_before_start",
        manifest_version_id: version.manifest_version_id,
        manifest_content_hash: version.content_hash,
        asset_ref: {MyApp.Assets.Gold, :asset},
        target_refs: [{MyApp.Assets.Gold, :asset}]
      )

    cancelled =
      run_state
      |> RunState.transition(
        status: :cancelled,
        error: {:cancelled, %{reason: :pre_start_cancel}},
        metadata: %{cancelled: true}
      )

    assert :ok = Storage.put_run(cancelled)

    assert {:ok, pid} = RunServer.start_link(%{run_state: run_state, version: version})

    ref = Process.monitor(pid)
    assert_receive {:DOWN, ^ref, :process, ^pid, :normal}, 1_000

    assert {:ok, stored} = Storage.get_run(run_state.id)
    assert stored.status == :cancelled
    assert stored.error == {:cancelled, %{reason: :pre_start_cancel}}

    assert {:ok, events} = Storage.list_run_events(run_state.id)
    assert events == []
  end

  test "does not crash when step_started persist loses to external cancel" do
    previous_client = Application.get_env(:favn_orchestrator, :runner_client)
    previous_opts = Application.get_env(:favn_orchestrator, :runner_client_opts)

    on_exit(fn ->
      Application.put_env(:favn_orchestrator, :runner_client, previous_client)
      Application.put_env(:favn_orchestrator, :runner_client_opts, previous_opts)
    end)

    Application.put_env(
      :favn_orchestrator,
      :runner_client,
      RunnerClientCancelBeforeStepStartedStub
    )

    Application.put_env(:favn_orchestrator, :runner_client_opts, [])

    version = manifest_version("mv_run_server_cancelled_before_step_started")

    run_state =
      RunState.new(
        id: "run_server_cancelled_before_step_started",
        manifest_version_id: version.manifest_version_id,
        manifest_content_hash: version.content_hash,
        asset_ref: {MyApp.Assets.Gold, :asset},
        target_refs: [{MyApp.Assets.Gold, :asset}]
      )

    assert :ok = Storage.put_run(run_state)

    assert {:ok, pid} = RunServer.start_link(%{run_state: run_state, version: version})

    ref = Process.monitor(pid)
    assert_receive {:DOWN, ^ref, :process, ^pid, :normal}, 1_000

    assert {:ok, stored} = Storage.get_run(run_state.id)
    assert stored.status == :cancelled
    assert stored.error == {:cancelled, %{reason: :submit_race}}

    assert {:ok, events} = Storage.list_run_events(run_state.id)
    assert Enum.map(events, & &1.event_type) == [:run_started]
  end

  defp manifest_version(manifest_version_id) do
    manifest =
      %Manifest{
        assets: [
          %Favn.Manifest.Asset{
            ref: {MyApp.Assets.Gold, :asset},
            module: MyApp.Assets.Gold,
            name: :asset
          }
        ]
      }

    {:ok, version} = Version.new(manifest, manifest_version_id: manifest_version_id)
    version
  end
end
