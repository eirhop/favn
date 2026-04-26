defmodule FavnOrchestrator.Storage.ManifestCodecTest do
  use ExUnit.Case, async: true

  alias Favn.Manifest
  alias Favn.Manifest.Asset
  alias Favn.Manifest.Graph
  alias Favn.Manifest.Identity
  alias Favn.Manifest.Pipeline
  alias Favn.Manifest.Schedule
  alias Favn.Manifest.Serializer
  alias Favn.Manifest.SQLExecution
  alias Favn.Manifest.Version
  alias Favn.RelationRef
  alias Favn.SQL.Template
  alias FavnOrchestrator.Storage.ManifestCodec

  test "round-trips manifest version records" do
    version = manifest_version("mv_codec")

    assert {:ok, record} = ManifestCodec.to_record(version)
    assert record.manifest_version_id == "mv_codec"

    assert {:ok, decoded} = ManifestCodec.from_record(record)
    assert decoded.manifest_version_id == version.manifest_version_id
    assert decoded.content_hash == version.content_hash
    assert %Manifest{} = decoded.manifest
    assert [%Favn.Manifest.Asset{ref: {MyApp.Asset, :asset}}] = decoded.manifest.assets
  end

  test "rejects content hash mismatch" do
    version = manifest_version("mv_codec_mismatch")
    assert {:ok, record} = ManifestCodec.to_record(version)

    mismatch = %{record | content_hash: String.duplicate("0", 64)}

    assert {:error, {:manifest_content_hash_mismatch, expected, _actual}} =
             ManifestCodec.from_record(mismatch)

    assert expected == String.duplicate("0", 64)
  end

  test "preserves content hash invariant across raw decode and rehydration" do
    version = complex_manifest_version("mv_codec_complex")

    assert {:ok, record} = ManifestCodec.to_record(version)
    assert {:ok, raw_manifest} = Serializer.decode_manifest(record.manifest_json)
    assert {:ok, raw_hash} = Identity.hash_manifest(raw_manifest)
    assert raw_hash == record.content_hash

    assert {:ok, decoded} = ManifestCodec.from_record(record)
    assert decoded.content_hash == record.content_hash
  end

  defp manifest_version(manifest_version_id) do
    manifest = %Manifest{
      assets: [
        %Favn.Manifest.Asset{ref: {MyApp.Asset, :asset}, module: MyApp.Asset, name: :asset}
      ]
    }

    {:ok, version} = Version.new(manifest, manifest_version_id: manifest_version_id)
    version
  end

  defp complex_manifest_version(manifest_version_id) do
    ref = {MyApp.Assets.SalesSummary, :asset}

    template =
      Template.compile!("SELECT 1 AS id",
        file: "test/fixtures/manifest_codec_test.sql",
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

    {:ok, version} = Version.new(manifest, manifest_version_id: manifest_version_id)
    version
  end
end
