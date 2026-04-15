defmodule FavnRunner.WorkerTest do
  use ExUnit.Case, async: false

  alias Favn.Contracts.RunnerResult
  alias Favn.Contracts.RunnerWork
  alias Favn.Manifest
  alias Favn.Manifest.Asset
  alias Favn.Manifest.Graph
  alias Favn.Manifest.Version

  test "worker sends runner result for crashing asset invocation" do
    asset =
      %Asset{
        ref: {FavnRunner.WorkerTest.CrashingAsset, :asset},
        module: FavnRunner.WorkerTest.CrashingAsset,
        name: :asset,
        type: :elixir,
        execution: %{entrypoint: :asset, arity: 1}
      }

    manifest =
      %Manifest{
        schema_version: 1,
        runner_contract_version: 1,
        assets: [asset],
        pipelines: [],
        schedules: [],
        graph: %Graph{nodes: [asset.ref], edges: [], topo_order: [asset.ref]},
        metadata: %{}
      }

    {:ok, version} = Version.new(manifest, manifest_version_id: "mv_worker_test")

    work =
      %RunnerWork{
        run_id: "run_worker_test",
        manifest_version_id: version.manifest_version_id,
        manifest_content_hash: version.content_hash,
        asset_ref: asset.ref,
        metadata: %{}
      }

    assert {:ok, _pid} =
             FavnRunner.Worker.start_link(%{
               server: self(),
               execution_id: "rx_worker_test",
               work: work,
               version: version,
               asset: asset
             })

    assert_receive {:runner_result, "rx_worker_test", %RunnerResult{} = result}, 2_000
    assert result.status == :error
    assert [%{status: :error}] = result.asset_results
  end
end

defmodule FavnRunner.WorkerTest.CrashingAsset do
  @spec asset(Favn.Run.Context.t()) :: no_return()
  def asset(_ctx), do: raise("boom")
end
