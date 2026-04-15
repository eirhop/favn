defmodule FavnRunner.ServerTest do
  use ExUnit.Case, async: false

  alias Favn.Contracts.RunnerResult
  alias Favn.Contracts.RunnerWork
  alias Favn.Manifest
  alias Favn.Manifest.Asset
  alias Favn.Manifest.Graph
  alias Favn.Manifest.Version

  test "cancel_work marks running execution as cancelled" do
    {:ok, version} =
      Version.new(build_manifest(FavnRunner.ServerTest.SlowAsset),
        manifest_version_id: unique_id("mv")
      )

    assert :ok = FavnRunner.register_manifest(version)

    work =
      %RunnerWork{
        run_id: unique_id("run"),
        manifest_version_id: version.manifest_version_id,
        manifest_content_hash: version.content_hash,
        asset_ref: {FavnRunner.ServerTest.SlowAsset, :asset}
      }

    assert {:ok, execution_id} = FavnRunner.submit_work(work)
    assert :ok = FavnRunner.cancel_work(execution_id, %{reason: :operator_cancel})
    assert {:ok, result} = FavnRunner.await_result(execution_id, 2_000)
    assert result.status == :cancelled
  end

  test "late runner_result does not overwrite a cancelled terminal result" do
    {:ok, version} =
      Version.new(build_manifest(FavnRunner.ServerTest.SlowAsset),
        manifest_version_id: unique_id("mv")
      )

    assert :ok = FavnRunner.register_manifest(version)

    work =
      %RunnerWork{
        run_id: unique_id("run"),
        manifest_version_id: version.manifest_version_id,
        manifest_content_hash: version.content_hash,
        asset_ref: {FavnRunner.ServerTest.SlowAsset, :asset}
      }

    assert {:ok, execution_id} = FavnRunner.submit_work(work)
    assert :ok = FavnRunner.cancel_work(execution_id, %{reason: :operator_cancel})

    send(FavnRunner.Server, {
      :runner_result,
      execution_id,
      %RunnerResult{
        run_id: work.run_id,
        manifest_version_id: work.manifest_version_id,
        manifest_content_hash: work.manifest_content_hash,
        status: :ok,
        asset_results: [],
        error: nil,
        metadata: %{}
      }
    })

    assert {:ok, result} = FavnRunner.await_result(execution_id, 2_000)
    assert result.status == :cancelled
  end

  test "server returns worker crash result when worker dies unexpectedly" do
    {:ok, version} =
      Version.new(build_manifest(FavnRunner.ServerTest.SlowAsset),
        manifest_version_id: unique_id("mv")
      )

    assert :ok = FavnRunner.register_manifest(version)

    work =
      %RunnerWork{
        run_id: unique_id("run"),
        manifest_version_id: version.manifest_version_id,
        manifest_content_hash: version.content_hash,
        asset_ref: {FavnRunner.ServerTest.SlowAsset, :asset}
      }

    assert {:ok, execution_id} = FavnRunner.submit_work(work)

    state = :sys.get_state(FavnRunner.Server)
    worker_pid = get_in(state, [:executions, execution_id, :pid])
    assert is_pid(worker_pid)
    Process.exit(worker_pid, :kill)

    assert {:ok, result} = FavnRunner.await_result(execution_id, 2_000)
    assert result.status == :error
    assert result.error == {:worker_crash, :killed}
  end

  defp build_manifest(asset_module) do
    ref = {asset_module, :asset}

    %Manifest{
      schema_version: 1,
      runner_contract_version: 1,
      assets: [
        %Asset{
          ref: ref,
          module: asset_module,
          name: :asset,
          type: :elixir,
          execution: %{entrypoint: :asset, arity: 1}
        }
      ],
      pipelines: [],
      schedules: [],
      graph: %Graph{nodes: [ref], edges: [], topo_order: [ref]},
      metadata: %{}
    }
  end

  defp unique_id(prefix),
    do: prefix <> "_" <> Base.encode16(:crypto.strong_rand_bytes(8), case: :lower)
end

defmodule FavnRunner.ServerTest.SlowAsset do
  @spec asset(Favn.Run.Context.t()) :: :ok
  def asset(_ctx) do
    Process.sleep(5_000)
    :ok
  end
end
