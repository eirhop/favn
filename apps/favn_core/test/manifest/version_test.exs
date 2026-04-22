defmodule Favn.Manifest.VersionTest do
  use ExUnit.Case, async: true

  alias Favn.Manifest
  alias Favn.Manifest.Asset
  alias Favn.Manifest.Build
  alias Favn.Manifest.Compatibility
  alias Favn.Manifest.Graph
  alias Favn.Manifest.Identity
  alias Favn.Manifest.Index
  alias Favn.Manifest.Pipeline
  alias Favn.Manifest.PipelineResolver
  alias Favn.Manifest.Schedule
  alias Favn.Manifest.Serializer
  alias Favn.Manifest.SQLExecution
  alias Favn.Manifest.Version
  alias Favn.RelationRef
  alias Favn.SQL.Template

  test "builds pinned manifest version with id and content hash" do
    manifest = %{schema_version: 1, runner_contract_version: 1, assets: []}

    assert {:ok, %Version{} = version} =
             Version.new(manifest,
               manifest_version_id: "mv_test_001",
               inserted_at: ~U[2026-01-01 00:00:00Z]
             )

    assert version.manifest_version_id == "mv_test_001"
    assert version.schema_version == Compatibility.current_schema_version()
    assert version.runner_contract_version == Compatibility.current_runner_contract_version()
    assert version.serialization_format == "json-v1"
    assert version.inserted_at == ~U[2026-01-01 00:00:00Z]
    assert is_binary(version.content_hash)
    assert byte_size(version.content_hash) == 64
  end

  test "fails when schema version is unsupported" do
    manifest = %{schema_version: 2, runner_contract_version: 1, assets: []}

    assert {:error, {:unsupported_schema_version, 2, 1}} =
             Version.new(manifest)
  end

  test "pins canonical manifest payload when input is build" do
    canonical_manifest = %{schema_version: 1, runner_contract_version: 1, assets: []}
    build = Build.new(canonical_manifest, diagnostics: [%{message: "warn"}])

    assert {:ok, %Version{} = version} = Version.new(build, manifest_version_id: "mv_test_build")

    assert version.manifest == canonical_manifest
    assert is_map(version.manifest)
    refute Map.has_key?(version.manifest, :manifest)
  end

  test "build input uses canonical payload hash invariant" do
    canonical_manifest = %{schema_version: 1, runner_contract_version: 1, assets: []}
    build = Build.new(canonical_manifest, diagnostics: [%{message: "warn"}])

    assert {:ok, %Version{} = version} =
             Version.new(build, manifest_version_id: "mv_test_build_hash")

    assert {:ok, manifest_hash} = Identity.hash_manifest(build.manifest)
    assert version.content_hash == manifest_hash
  end

  test "envelope versions are derived from manifest payload" do
    manifest = %{schema_version: 1, runner_contract_version: 1, assets: []}

    assert {:ok, %Version{} = version} =
             Version.new(manifest,
               manifest_version_id: "mv_test_versions"
             )

    assert version.schema_version == manifest.schema_version
    assert version.runner_contract_version == manifest.runner_contract_version
  end

  test "rejects version override options" do
    manifest = %{schema_version: 1, runner_contract_version: 1, assets: []}

    assert {:error, {:unknown_opt, :schema_version}} =
             Version.new(manifest, schema_version: 1)
  end

  test "rehydrates decoded manifests into canonical runtime structs" do
    ref = {MyApp.Assets.SalesSummary, :asset}

    template =
      Template.compile!("SELECT 1 AS id",
        file: "test/fixtures/version_test.sql",
        line: 1,
        module: __MODULE__,
        scope: :query,
        enforce_query_root: true
      )

    manifest = %Manifest{
      schema_version: 1,
      runner_contract_version: 1,
      assets: [
        %Asset{
          ref: ref,
          module: elem(ref, 0),
          name: :asset,
          type: :sql,
          execution: %{entrypoint: :asset, arity: 1},
          relation:
            RelationRef.new!(%{connection: :warehouse, schema: "gold", name: "sales_summary"}),
          materialization: {:incremental, strategy: :delete_insert, unique_key: [:id]},
          sql_execution: %SQLExecution{
            sql: "SELECT 1 AS id",
            template: template,
            sql_definitions: []
          },
          metadata: %{category: :sales, tags: [:gold]}
        }
      ],
      pipelines: [
        %Pipeline{
          module: MyApp.Pipelines.Daily,
          name: :daily,
          selectors: [{:asset, ref}, {:tag, :gold}, {:category, :sales}],
          deps: :all,
          schedule:
            {:inline,
             %Schedule{
               module: MyApp.Pipelines.Daily,
               name: :daily,
               ref: {MyApp.Pipelines.Daily, :daily},
               kind: :cron,
               cron: "0 * * * *",
               timezone: "Etc/UTC",
               missed: :skip,
               overlap: :forbid,
               active: true,
               origin: :inline
             }},
          window: :day,
          source: :scheduler,
          outputs: [:default],
          metadata: %{category: :sales}
        }
      ],
      schedules: [],
      graph: %Graph{nodes: [ref], edges: [], topo_order: [ref]},
      metadata: %{}
    }

    assert {:ok, encoded} = Serializer.encode_manifest(manifest)
    assert {:ok, decoded} = Serializer.decode_manifest(encoded)
    assert {:ok, version} = Version.new(decoded, manifest_version_id: "mv_rehydrated")

    assert %Manifest{} = version.manifest
    assert %Asset{} = asset = hd(version.manifest.assets)
    assert %RelationRef{} = asset.relation
    assert %SQLExecution{} = asset.sql_execution
    assert %Template{} = asset.sql_execution.template
    assert asset.metadata.category == :sales
    assert asset.metadata.tags == [:gold]

    assert %Graph{} = version.manifest.graph
    assert version.manifest.graph.nodes == [ref]

    assert %Pipeline{} = pipeline = hd(version.manifest.pipelines)
    assert pipeline.selectors == [{:asset, ref}, {:tag, :gold}, {:category, :sales}]
    assert {:inline, %Schedule{cron: "0 * * * *"}} = pipeline.schedule

    assert {:ok, index} = Index.build_from_version(version)

    assert {:ok, %{target_refs: [^ref]}} =
             PipelineResolver.resolve(index, pipeline,
               trigger: %{kind: :manual},
               params: %{}
             )
  end
end
