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

    assert %Manifest{} = version.manifest
    assert version.manifest.schema_version == 1
    assert version.manifest.runner_contract_version == 1
    assert version.manifest.assets == []
    refute Map.has_key?(version.manifest, :manifest)
  end

  test "build input uses canonical payload hash invariant" do
    canonical_manifest = %{schema_version: 1, runner_contract_version: 1, assets: []}
    build = Build.new(canonical_manifest, diagnostics: [%{message: "warn"}])

    assert {:ok, %Version{} = version} =
             Version.new(build, manifest_version_id: "mv_test_build_hash")

    assert {:ok, manifest_hash} = Identity.hash_manifest(version.manifest)
    assert version.content_hash == manifest_hash
  end

  test "verifies published manifest version envelopes without minting a new identity" do
    manifest = %{schema_version: 1, runner_contract_version: 1, assets: []}

    assert {:ok, original} =
             Version.new(manifest,
               manifest_version_id: "mv_published_envelope",
               inserted_at: ~U[2026-01-01 00:00:00Z]
             )

    assert {:ok, verified} =
             Version.from_published(original.manifest,
               manifest_version_id: original.manifest_version_id,
               content_hash: original.content_hash,
               schema_version: original.schema_version,
               runner_contract_version: original.runner_contract_version,
               serialization_format: original.serialization_format,
               inserted_at: original.inserted_at
             )

    assert verified.manifest_version_id == original.manifest_version_id
    assert verified.content_hash == original.content_hash
    assert verified.schema_version == original.schema_version
    assert verified.runner_contract_version == original.runner_contract_version
    assert verified.serialization_format == original.serialization_format
    assert verified.inserted_at == original.inserted_at
  end

  test "rejects published manifest version envelopes with mismatched hashes" do
    manifest = %{schema_version: 1, runner_contract_version: 1, assets: []}

    assert {:error, {:manifest_content_hash_mismatch, expected, computed}} =
             Version.from_published(manifest,
               manifest_version_id: "mv_bad_hash",
               content_hash: String.duplicate("0", 64)
             )

    assert expected == String.duplicate("0", 64)
    assert byte_size(computed) == 64
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

  test "keeps content hash stable across JSON roundtrip" do
    ref = {MyApp.Assets.Roundtrip, :asset}

    manifest = %Manifest{
      schema_version: 1,
      runner_contract_version: 1,
      assets: [
        %Asset{
          ref: ref,
          module: elem(ref, 0),
          name: :asset,
          type: :elixir,
          execution: %{entrypoint: :asset, arity: 1},
          depends_on: [],
          metadata: %{category: :sales, tags: [:gold]}
        }
      ],
      pipelines: [
        %Pipeline{
          module: MyApp.Pipelines.Roundtrip,
          name: :roundtrip,
          selectors: [{:asset, ref}],
          deps: :all,
          schedule:
            {:inline,
             %Schedule{
               module: MyApp.Pipelines.Roundtrip,
               name: :roundtrip,
               ref: {MyApp.Pipelines.Roundtrip, :roundtrip},
               kind: :cron,
               cron: "*/15 * * * * *",
               timezone: "Etc/UTC",
               missed: :skip,
               overlap: :allow,
               active: true,
               origin: :inline
             }}
        }
      ],
      schedules: [],
      graph: %Graph{nodes: [ref], edges: [], topo_order: [ref]},
      metadata: %{}
    }

    assert {:ok, original} = Version.new(manifest, manifest_version_id: "mv_roundtrip")
    assert {:ok, encoded} = Serializer.encode_manifest(manifest)
    assert {:ok, decoded} = Serializer.decode_manifest(encoded)
    assert {:ok, roundtrip} = Version.new(decoded, manifest_version_id: "mv_roundtrip")

    assert roundtrip.content_hash == original.content_hash
  end

  test "rehydrates known manifest module atoms without loading user modules" do
    manifest = %{
      "schema_version" => 1,
      "runner_contract_version" => 1,
      "assets" => [
        %{
          "ref" => %{"module" => "Elixir.ExternalConsumer.UnknownAsset", "name" => "asset"},
          "module" => "Elixir.ExternalConsumer.UnknownAsset",
          "name" => "asset",
          "type" => "elixir",
          "execution" => %{"entrypoint" => "asset", "arity" => 1},
          "depends_on" => [],
          "config" => %{},
          "metadata" => %{"category" => "external category", "tags" => ["external tag"]}
        }
      ],
      "pipelines" => [],
      "schedules" => [],
      "graph" => %{
        "nodes" => [%{"module" => "Elixir.ExternalConsumer.UnknownAsset", "name" => "asset"}],
        "edges" => [],
        "topo_order" => [%{"module" => "Elixir.ExternalConsumer.UnknownAsset", "name" => "asset"}]
      },
      "metadata" => %{}
    }

    assert {:error, :nofile} = Code.ensure_loaded(ExternalConsumer.UnknownAsset)
    assert {:ok, version} = Version.new(manifest, manifest_version_id: "mv_unloaded_module")

    assert hd(version.manifest.assets).module == ExternalConsumer.UnknownAsset
    assert hd(version.manifest.graph.nodes) == {ExternalConsumer.UnknownAsset, :asset}
    assert hd(version.manifest.assets).metadata.category == "external category"
    assert hd(version.manifest.assets).metadata.tags == ["external tag"]
  end

  test "rejects invalid unloaded module references during rehydration" do
    manifest = %{
      "schema_version" => 1,
      "runner_contract_version" => 1,
      "assets" => [
        %{
          "ref" => %{"module" => "Elixir.not-a-module", "name" => "asset"},
          "module" => "Elixir.not-a-module",
          "name" => "asset",
          "type" => "elixir",
          "execution" => %{"entrypoint" => "asset", "arity" => 1}
        }
      ],
      "pipelines" => [],
      "schedules" => [],
      "graph" => %{},
      "metadata" => %{}
    }

    assert {:error, {:invalid_manifest_payload, %ArgumentError{}}} = Version.new(manifest)
  end

  test "rejects manifest module references longer than the atom limit" do
    module = "Elixir." <> String.duplicate("A", 249)

    manifest = %{
      "schema_version" => 1,
      "runner_contract_version" => 1,
      "assets" => [
        %{
          "ref" => %{"module" => module, "name" => "asset"},
          "module" => module,
          "name" => "asset",
          "type" => "elixir",
          "execution" => %{"entrypoint" => "asset", "arity" => 1}
        }
      ],
      "pipelines" => [],
      "schedules" => [],
      "graph" => %{},
      "metadata" => %{}
    }

    assert byte_size(module) == 256

    assert {:error, {:invalid_manifest_payload, %ArgumentError{message: message}}} =
             Version.new(manifest)

    assert message == "invalid module reference #{inspect(module)}"
  end

  test "accepts valid unloaded manifest atom strings" do
    unique = System.unique_integer([:positive])
    module = "Elixir.ExternalConsumer.UnknownAsset#{unique}"
    name = "generated_asset_#{unique}"

    manifest = %{
      "schema_version" => 1,
      "runner_contract_version" => 1,
      "assets" => [
        %{
          "ref" => %{"module" => module, "name" => name},
          "module" => module,
          "name" => name,
          "type" => "elixir",
          "execution" => %{"entrypoint" => name, "arity" => 1}
        }
      ],
      "pipelines" => [],
      "schedules" => [],
      "graph" => %{},
      "metadata" => %{}
    }

    assert :error = existing_atom(module)
    assert :error = existing_atom(name)
    assert {:ok, version} = Version.new(manifest)

    assert Atom.to_string(hd(version.manifest.assets).module) == module
    assert Atom.to_string(hd(version.manifest.assets).name) == name
    assert {:ok, atom} = existing_atom(module)
    assert Atom.to_string(atom) == module
    assert {:ok, name_atom} = existing_atom(name)
    assert Atom.to_string(name_atom) == name
  end

  test "rejects manifests with too many manifest atom references" do
    assets =
      Enum.map(1..100_001, fn index ->
        %{
          "module" => "Elixir.ExternalConsumer.GeneratedAsset#{index}",
          "name" => "asset",
          "type" => "elixir",
          "execution" => %{"entrypoint" => "asset", "arity" => 1}
        }
      end)

    manifest = %{
      "schema_version" => 1,
      "runner_contract_version" => 1,
      "assets" => assets,
      "pipelines" => [],
      "schedules" => [],
      "graph" => %{},
      "metadata" => %{}
    }

    assert {:error, {:manifest_atom_limit_exceeded, atom_ref_count, 100_000}} =
             Version.new(manifest)

    assert atom_ref_count > 100_000
  end

  defp existing_atom(value) do
    {:ok, String.to_existing_atom(value)}
  rescue
    ArgumentError -> :error
  end
end
