defmodule FavnOrchestrator.Storage.ManifestCodecTest do
  use ExUnit.Case, async: true

  alias Favn.Manifest
  alias Favn.Manifest.Asset
  alias Favn.Manifest.Graph
  alias Favn.Manifest.Identity
  alias Favn.Manifest.Pipeline
  alias Favn.Manifest.Schedule
  alias Favn.Manifest.Serializer
  alias Favn.Manifest.Version
  alias Favn.RelationRef
  alias FavnOrchestrator.Storage.ManifestCodec

  test "round-trips manifest version records" do
    version = manifest_version("mv_codec")

    assert {:ok, record} = ManifestCodec.to_record(version)
    assert record.manifest_version_id == "mv_codec"
    assert record.required_runner_release_id == FavnTestSupport.runner_release_id()

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

  test "requires the envelope runner release identity and matches it to the manifest" do
    version = manifest_version("mv_codec_release_binding")
    assert {:ok, record} = ManifestCodec.to_record(version)

    assert {:error, {:invalid_manifest_record_field, :required_runner_release_id, nil}} =
             record
             |> Map.delete(:required_runner_release_id)
             |> ManifestCodec.from_record()

    assert {:error, {:manifest_required_runner_release_id_mismatch, alternate_id, required_id}} =
             record
             |> Map.put(
               :required_runner_release_id,
               FavnTestSupport.runner_release_id(:alternate)
             )
             |> ManifestCodec.from_record()

    assert alternate_id == FavnTestSupport.runner_release_id(:alternate)
    assert required_id == FavnTestSupport.runner_release_id()
  end

  test "preserves content hash invariant across raw decode and rehydration" do
    version = complex_manifest_version("mv_codec_complex")

    assert {:ok, record} = ManifestCodec.to_record(version)
    assert {:ok, raw_manifest} = Serializer.decode_manifest(record.manifest_index_json)
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

    {:ok, version} =
      Version.new(current_manifest(manifest),
        manifest_version_id: manifest_version_id
      )

    version
  end

  defp complex_manifest_version(manifest_version_id) do
    ref = {MyApp.Assets.SalesSummary, :asset}

    manifest = %Manifest{
      assets: [
        FavnTestSupport.with_target_descriptor(%Asset{
          ref: ref,
          module: elem(ref, 0),
          name: :asset,
          type: :sql,
          execution: %{entrypoint: :asset, arity: 1},
          relation:
            RelationRef.new!(%{connection: :warehouse, schema: "gold", name: "sales_summary"}),
          materialization: {:incremental, strategy: :delete_insert, unique_key: [:id]},
          execution_package_hash: String.duplicate("a", 64),
          metadata: %{category: :sales, tags: [:gold]}
        })
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
          window: Favn.Window.Policy.new!(:day, timezone: "Etc/UTC"),
          source: :scheduler,
          outputs: [:default],
          metadata: %{category: :sales}
        }
      ],
      schedules: [],
      graph: %Graph{nodes: [ref], edges: [], topo_order: [ref]},
      metadata: %{}
    }

    {:ok, version} =
      Version.new(current_manifest(manifest),
        manifest_version_id: manifest_version_id
      )

    version
  end

  defp current_manifest(manifest) do
    manifest
    |> FavnTestSupport.with_manifest_contract()
    |> FavnTestSupport.with_manifest_graph()
  end
end
