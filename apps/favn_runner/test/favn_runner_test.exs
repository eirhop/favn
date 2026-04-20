defmodule FavnRunnerTest do
  use ExUnit.Case, async: false

  alias Favn.Contracts.RunnerWork
  alias Favn.Manifest
  alias Favn.Manifest.Asset
  alias Favn.Manifest.Graph
  alias Favn.Manifest.Version
  alias FavnTestSupport.Fixtures

  setup do
    Fixtures.compile_fixture!(:runner_assets)

    manifest_version = "mv_" <> Base.encode16(:crypto.strong_rand_bytes(8), case: :lower)

    manifest =
      build_manifest([
        %Asset{
          ref: {FavnRunnerTest.ElixirAsset, :asset},
          module: FavnRunnerTest.ElixirAsset,
          name: :asset,
          type: :elixir,
          execution: %{entrypoint: :asset, arity: 1},
          config: %{hello: "world"}
        },
        %Asset{
          ref: {FavnRunnerTest.SourceAsset, :asset},
          module: FavnRunnerTest.SourceAsset,
          name: :asset,
          type: :source,
          execution: %{entrypoint: nil, arity: nil},
          relation: %{name: "external_source"}
        }
      ])

    {:ok, version} = Version.new(manifest, manifest_version_id: manifest_version)
    :ok = FavnRunner.register_manifest(version)

    %{version: version}
  end

  test "runs a shared fixture asset through runner execution boundary", %{version: version} do
    fixture_ref = {Favn.Test.Fixtures.Assets.Runner.RunnerAssets, :base}

    fixture_manifest =
      build_manifest([
        %Asset{
          ref: fixture_ref,
          module: elem(fixture_ref, 0),
          name: :base,
          type: :elixir,
          execution: %{entrypoint: :base, arity: 1}
        }
      ])

    {:ok, fixture_version} =
      Version.new(fixture_manifest,
        manifest_version_id:
          "mv_fixture_" <> Base.encode16(:crypto.strong_rand_bytes(8), case: :lower)
      )

    assert :ok = FavnRunner.register_manifest(fixture_version)

    work =
      %RunnerWork{
        run_id: "run_fixture",
        manifest_version_id: fixture_version.manifest_version_id,
        manifest_content_hash: fixture_version.content_hash,
        asset_ref: fixture_ref,
        params: %{partition: "2026-03-25"}
      }

    assert {:ok, result} = FavnRunner.run(work)
    assert result.status == :ok
    assert [%{ref: ^fixture_ref, status: :ok}] = result.asset_results

    assert [%{meta: meta}] = result.asset_results
    assert meta == %{partition: "2026-03-25"}

    assert version.manifest_version_id != fixture_version.manifest_version_id
  end

  test "runs one elixir asset through runner boundary", %{version: version} do
    work =
      %RunnerWork{
        run_id: "run_elixir",
        manifest_version_id: version.manifest_version_id,
        manifest_content_hash: version.content_hash,
        asset_ref: {FavnRunnerTest.ElixirAsset, :asset},
        params: %{value: 42},
        metadata: %{attempt: 1}
      }

    assert {:ok, result} = FavnRunner.run(work)
    assert result.status == :ok
    assert result.manifest_version_id == version.manifest_version_id
    assert [%{ref: {FavnRunnerTest.ElixirAsset, :asset}, status: :ok}] = result.asset_results
  end

  test "runs one source asset as observe/no-op", %{version: version} do
    work =
      %RunnerWork{
        run_id: "run_source",
        manifest_version_id: version.manifest_version_id,
        manifest_content_hash: version.content_hash,
        asset_ref: {FavnRunnerTest.SourceAsset, :asset}
      }

    assert {:ok, result} = FavnRunner.run(work)
    assert result.status == :ok

    assert [asset_result] = result.asset_results
    assert asset_result.ref == {FavnRunnerTest.SourceAsset, :asset}
    assert asset_result.meta[:observed] == true
  end

  test "rejects unknown manifest version" do
    work =
      %RunnerWork{
        run_id: "run_missing",
        manifest_version_id: "mv_missing",
        manifest_content_hash: "hash_missing",
        asset_ref: {FavnRunnerTest.ElixirAsset, :asset}
      }

    assert {:error, :manifest_not_found} = FavnRunner.submit_work(work)
  end

  defp build_manifest(assets) do
    refs = Enum.map(assets, & &1.ref)

    %Manifest{
      schema_version: 1,
      runner_contract_version: 1,
      assets: assets,
      pipelines: [],
      schedules: [],
      graph: %Graph{nodes: refs, edges: [], topo_order: refs},
      metadata: %{}
    }
  end
end

defmodule FavnRunnerTest.ElixirAsset do
  alias Favn.Run.Context

  @spec asset(Context.t()) :: :ok | {:ok, map()}
  def asset(%Context{} = ctx) do
    {:ok, %{current_ref: ctx.current_ref, params: ctx.params}}
  end
end

defmodule FavnRunnerTest.SourceAsset do
end
