defmodule FavnOrchestrator.RunnerIntegrationTest do
  use ExUnit.Case, async: false

  alias Favn.Manifest
  alias Favn.Manifest.Asset
  alias Favn.Manifest.Graph
  alias Favn.Manifest.Pipeline
  alias Favn.Manifest.Version
  alias FavnOrchestrator
  alias FavnOrchestrator.Storage.Adapter.Memory

  setup do
    previous_client = Application.get_env(:favn_orchestrator, :runner_client)
    previous_opts = Application.get_env(:favn_orchestrator, :runner_client_opts)

    Application.put_env(:favn_orchestrator, :runner_client, FavnRunner)
    Application.put_env(:favn_orchestrator, :runner_client_opts, [])
    Memory.reset()
    {:ok, _} = Application.ensure_all_started(:favn_runner)

    on_exit(fn ->
      Application.put_env(:favn_orchestrator, :runner_client, previous_client)
      Application.put_env(:favn_orchestrator, :runner_client_opts, previous_opts)
      Memory.reset()
    end)

    :ok
  end

  test "same-node orchestrator run stays pinned when active manifest changes mid-flight" do
    version_a = manifest_version("mv_runner_a")
    version_b = manifest_version("mv_runner_b")

    assert :ok = FavnOrchestrator.register_manifest(version_a)
    assert :ok = FavnOrchestrator.register_manifest(version_b)
    assert :ok = FavnOrchestrator.activate_manifest(version_a.manifest_version_id)

    assert {:ok, run_id} = FavnOrchestrator.submit_asset_run({__MODULE__.SleepAsset, :asset})
    assert :ok = FavnOrchestrator.activate_manifest(version_b.manifest_version_id)

    assert {:ok, run} = await_terminal_run(run_id)
    assert run.status == :ok
    assert run.manifest_version_id == version_a.manifest_version_id
  end

  test "manual pipeline run resolves from persisted manifest pipeline descriptor" do
    version = manifest_version("mv_runner_pipeline")
    assert :ok = FavnOrchestrator.register_manifest(version)
    assert :ok = FavnOrchestrator.activate_manifest(version.manifest_version_id)

    assert {:ok, run_id} = FavnOrchestrator.submit_pipeline_run(__MODULE__.DailyPipeline)
    assert {:ok, run} = await_terminal_run(run_id)
    assert run.status == :ok
    assert run.submit_kind == :pipeline
    assert run.target_refs == [{__MODULE__.SleepAsset, :asset}]
  end

  defp await_terminal_run(run_id, attempts \\ 60)

  defp await_terminal_run(run_id, attempts) when attempts > 0 do
    case FavnOrchestrator.get_run(run_id) do
      {:ok, run} when run.status in [:ok, :error, :cancelled, :timed_out] ->
        {:ok, run}

      {:ok, _run} ->
        Process.sleep(20)
        await_terminal_run(run_id, attempts - 1)

      error ->
        error
    end
  end

  defp await_terminal_run(_run_id, 0), do: {:error, :timeout_waiting_for_terminal_state}

  defp manifest_version(manifest_version_id) do
    assets = [
      %Asset{
        ref: {__MODULE__.SleepAsset, :asset},
        module: __MODULE__.SleepAsset,
        name: :asset,
        type: :elixir,
        execution: %{entrypoint: :asset, arity: 1},
        depends_on: [],
        config: %{manifest_version_id: manifest_version_id}
      }
    ]

    refs = Enum.map(assets, & &1.ref)

    manifest = %Manifest{
      schema_version: 1,
      runner_contract_version: 1,
      assets: assets,
      pipelines: [
        %Pipeline{
          module: __MODULE__.DailyPipeline,
          name: :daily,
          selectors: [{:asset, {__MODULE__.SleepAsset, :asset}}],
          deps: :all,
          schedule: nil,
          metadata: %{owner: :integration}
        }
      ],
      schedules: [],
      graph: %Graph{nodes: refs, edges: [], topo_order: refs},
      metadata: %{}
    }

    {:ok, version} = Version.new(manifest, manifest_version_id: manifest_version_id)
    version
  end
end

defmodule FavnOrchestrator.RunnerIntegrationTest.SleepAsset do
  alias Favn.Run.Context

  def asset(%Context{} = _ctx) do
    Process.sleep(150)
    :ok
  end
end

defmodule FavnOrchestrator.RunnerIntegrationTest.DailyPipeline do
end
