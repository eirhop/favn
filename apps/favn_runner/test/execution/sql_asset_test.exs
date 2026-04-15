defmodule FavnRunner.ExecutionSQLAssetTest do
  use ExUnit.Case, async: false

  alias Favn.Contracts.RunnerWork
  alias Favn.Manifest
  alias Favn.Manifest.Asset
  alias Favn.Manifest.Graph
  alias Favn.Manifest.Version

  test "sql assets are rejected until manifest-carried sql payload is supported" do
    ref = {FavnRunner.ExecutionSQLAssetTest.SQLAsset, :asset}

    manifest =
      %Manifest{
        schema_version: 1,
        runner_contract_version: 1,
        assets: [
          %Asset{
            ref: ref,
            module: FavnRunner.ExecutionSQLAssetTest.SQLAsset,
            name: :asset,
            type: :sql,
            execution: %{entrypoint: :asset, arity: 1}
          }
        ],
        pipelines: [],
        schedules: [],
        graph: %Graph{nodes: [ref], edges: [], topo_order: [ref]},
        metadata: %{}
      }

    {:ok, version} =
      Version.new(manifest,
        manifest_version_id:
          "mv_sql_unsupported_" <> Base.encode16(:crypto.strong_rand_bytes(8), case: :lower)
      )

    assert :ok = FavnRunner.register_manifest(version)

    work =
      %RunnerWork{
        run_id: "run_sql_unsupported",
        manifest_version_id: version.manifest_version_id,
        manifest_content_hash: version.content_hash,
        asset_ref: ref
      }

    assert {:ok, result} = FavnRunner.run(work)
    assert result.status == :error

    assert result.error[:reason] == :sql_manifest_execution_not_supported
  end
end

defmodule FavnRunner.ExecutionSQLAssetTest.SQLAsset do
end
