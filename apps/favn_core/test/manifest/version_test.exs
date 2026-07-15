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
  alias Favn.RuntimeInputResolver.Ref, as: RuntimeInputResolverRef
  alias Favn.SQL.Check
  alias Favn.SQL.Template

  test "builds pinned manifest version with id and content hash" do
    manifest = %{schema_version: 4, runner_contract_version: 4, assets: []}

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
    manifest = %{schema_version: 0, runner_contract_version: 4, assets: []}

    assert {:error, {:unsupported_schema_version, 0, 4}} =
             Version.new(manifest)
  end

  test "pins canonical manifest payload when input is build" do
    canonical_manifest = %{schema_version: 4, runner_contract_version: 4, assets: []}
    build = Build.new(canonical_manifest, diagnostics: [%{message: "warn"}])

    assert {:ok, %Version{} = version} = Version.new(build, manifest_version_id: "mv_test_build")

    assert %Manifest{} = version.manifest
    assert version.manifest.schema_version == 4
    assert version.manifest.runner_contract_version == 4
    assert version.manifest.assets == []
    refute Map.has_key?(version.manifest, :manifest)
  end

  test "build input uses canonical payload hash invariant" do
    canonical_manifest = %{schema_version: 4, runner_contract_version: 4, assets: []}
    build = Build.new(canonical_manifest, diagnostics: [%{message: "warn"}])

    assert {:ok, %Version{} = version} =
             Version.new(build, manifest_version_id: "mv_test_build_hash")

    assert {:ok, manifest_hash} = Identity.hash_manifest(version.manifest)
    assert version.content_hash == manifest_hash
  end

  test "verifies published manifest version envelopes without minting a new identity" do
    manifest = %{schema_version: 4, runner_contract_version: 4, assets: []}

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
    manifest = %{schema_version: 4, runner_contract_version: 4, assets: []}

    assert {:error, {:manifest_content_hash_mismatch, expected, computed}} =
             Version.from_published(manifest,
               manifest_version_id: "mv_bad_hash",
               content_hash: String.duplicate("0", 64)
             )

    assert expected == String.duplicate("0", 64)
    assert byte_size(computed) == 64
  end

  test "envelope versions are derived from manifest payload" do
    manifest = %{schema_version: 4, runner_contract_version: 4, assets: []}

    assert {:ok, %Version{} = version} =
             Version.new(manifest,
               manifest_version_id: "mv_test_versions"
             )

    assert version.schema_version == manifest.schema_version
    assert version.runner_contract_version == manifest.runner_contract_version
  end

  test "rejects version override options" do
    manifest = %{schema_version: 4, runner_contract_version: 4, assets: []}

    assert {:error, {:unknown_opt, :schema_version}} =
             Version.new(manifest, schema_version: 4)
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

    check_template =
      Template.compile!("SELECT count(*) > 0 AS passed FROM query()",
        file: "test/fixtures/version_check.sql",
        line: 1,
        module: __MODULE__,
        scope: :query,
        enforce_query_root: true
      )

    check =
      Check.new!(%{
        name: :has_rows,
        at: :before_materialize,
        on_false: :fail,
        sql: "SELECT count(*) > 0 AS passed FROM query()",
        template: check_template,
        file: "test/fixtures/version_check.sql",
        line: 1,
        uses_query?: true,
        uses_target?: false
      })

    manifest = %Manifest{
      schema_version: 4,
      runner_contract_version: 4,
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
            runtime_inputs: %RuntimeInputResolverRef{module: MyApp.SalesSummary.Inputs},
            sql_definitions: [],
            checks: [check]
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

    assert asset.sql_execution.runtime_inputs ==
             %RuntimeInputResolverRef{module: MyApp.SalesSummary.Inputs}

    assert [%Check{name: :has_rows, template: %Template{}, uses_query?: true}] =
             asset.sql_execution.checks

    assert asset.metadata.category == "sales"
    assert asset.metadata.tags == ["gold"]

    assert %Graph{} = version.manifest.graph
    assert version.manifest.graph.nodes == [ref]

    assert %Pipeline{} = pipeline = hd(version.manifest.pipelines)
    assert pipeline.selectors == [{:asset, ref}, {:tag, "gold"}, {:category, "sales"}]
    assert {:inline, %Schedule{cron: "0 * * * *"}} = pipeline.schedule

    assert {:ok, index} = Index.build_from_version(version)

    assert {:ok, %{target_refs: [^ref]}} =
             PipelineResolver.resolve(index, pipeline,
               trigger: %{kind: :manual},
               params: %{}
             )

    invalid =
      put_in(
        decoded,
        ["assets", Access.at(0), "sql_execution", "checks", Access.at(0), "on_false"],
        "ignore"
      )

    assert {:error,
            {:invalid_manifest_payload,
             %ArgumentError{
               message:
                 "invalid enum value \"ignore\" expected one of [:fail, :warn, :skip_materialization]"
             }}} =
             Version.new(invalid)

    invalid_query =
      put_in(
        decoded,
        ["assets", Access.at(0), "sql_execution", "template"],
        get_in(decoded, [
          "assets",
          Access.at(0),
          "sql_execution",
          "checks",
          Access.at(0),
          "template"
        ])
      )

    assert {:error,
            {:invalid_manifest_payload,
             %ArgumentError{message: "query() may only be used inside SQL check bodies"}}} =
             Version.new(invalid_query)

    invalid_runtime_flags =
      put_in(
        decoded,
        ["assets", Access.at(0), "sql_execution", "checks", Access.at(0), "uses_query?"],
        false
      )

    assert {:error,
            {:invalid_manifest_payload,
             %ArgumentError{
               message: "SQL check :has_rows runtime relation flags do not match its template"
             }}} = Version.new(invalid_runtime_flags)

    check_payload =
      get_in(decoded, ["assets", Access.at(0), "sql_execution", "checks", Access.at(0)])

    duplicate_checks =
      put_in(
        decoded,
        ["assets", Access.at(0), "sql_execution", "checks"],
        [check_payload, check_payload]
      )

    assert {:error,
            {:invalid_manifest_payload, %ArgumentError{message: "duplicate SQL check :has_rows"}}} =
             Version.new(duplicate_checks)

    too_many_checks =
      put_in(
        decoded,
        ["assets", Access.at(0), "sql_execution", "checks"],
        List.duplicate(check_payload, 51)
      )

    assert {:error,
            {:invalid_manifest_payload,
             %ArgumentError{message: "SQL assets support at most 50 checks"}}} =
             Version.new(too_many_checks)

    invalid_resolver =
      put_in(
        decoded,
        ["assets", Access.at(0), "sql_execution", "runtime_inputs"],
        %{"module" => "not-a-module"}
      )

    assert {:error,
            {:invalid_manifest_payload, %ArgumentError{message: "unknown atom \"not-a-module\""}}} =
             Version.new(invalid_resolver)

    resolver_with_payload =
      put_in(
        decoded,
        ["assets", Access.at(0), "sql_execution", "runtime_inputs"],
        %{
          "module" => "Elixir.MyApp.SalesSummary.Inputs",
          "params" => %{"secret" => "must-not-be-stored"}
        }
      )

    assert {:error,
            {:invalid_manifest_payload,
             %ArgumentError{
               message:
                 "invalid runtime input resolver reference; expected %{module: MyApp.Inputs}"
             }}} = Version.new(resolver_with_payload)
  end

  test "keeps content hash stable across JSON roundtrip" do
    ref = {MyApp.Assets.Roundtrip, :asset}

    manifest = %Manifest{
      schema_version: 4,
      runner_contract_version: 4,
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

  test "rejects a missing manifest graph instead of upconverting legacy payloads" do
    raw = {MyApp.Assets.LegacyRaw, :asset}
    gold = {MyApp.Assets.LegacyGold, :asset}

    manifest = %Manifest{
      schema_version: 4,
      runner_contract_version: 4,
      assets: [
        %Asset{ref: raw, module: elem(raw, 0), name: :asset, depends_on: []},
        %Asset{ref: gold, module: elem(gold, 0), name: :asset, depends_on: [raw]}
      ],
      pipelines: [],
      schedules: [],
      metadata: %{}
    }

    assert {:error,
            {:invalid_manifest_payload,
             %ArgumentError{message: "manifest graph is required for non-empty assets"}}} =
             Version.new(manifest, manifest_version_id: "mv_missing_graph")
  end

  test "rehydrates known manifest module atoms without loading user modules" do
    manifest = %{
      "schema_version" => 4,
      "runner_contract_version" => 4,
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

  test "keeps atom-like metadata and selector strings as strings" do
    suffix = System.unique_integer([:positive])
    category = "category_#{suffix}"
    tag = "tag_#{suffix}"

    assert_raise ArgumentError, fn -> String.to_existing_atom(category) end
    assert_raise ArgumentError, fn -> String.to_existing_atom(tag) end

    manifest = %{
      "schema_version" => 4,
      "runner_contract_version" => 4,
      "assets" => [
        %{
          "ref" => %{"module" => "Elixir.ExternalConsumer.UnknownAsset", "name" => "asset"},
          "module" => "Elixir.ExternalConsumer.UnknownAsset",
          "name" => "asset",
          "type" => "elixir",
          "execution" => %{"entrypoint" => "asset", "arity" => 1},
          "depends_on" => [],
          "config" => %{},
          "metadata" => %{"category" => category, "tags" => [tag]}
        }
      ],
      "pipelines" => [
        %{
          "module" => "Elixir.ExternalConsumer.Pipeline",
          "name" => "pipeline",
          "selectors" => [["tag", tag], ["category", category]],
          "deps" => "none",
          "outputs" => [],
          "config" => %{},
          "metadata" => %{}
        }
      ],
      "schedules" => [],
      "graph" => %{
        "nodes" => [%{"module" => "Elixir.ExternalConsumer.UnknownAsset", "name" => "asset"}],
        "edges" => [],
        "topo_order" => [%{"module" => "Elixir.ExternalConsumer.UnknownAsset", "name" => "asset"}]
      },
      "metadata" => %{}
    }

    assert {:ok, version} = Version.new(manifest, manifest_version_id: "mv_atom_like_values")

    assert_raise ArgumentError, fn -> String.to_existing_atom(category) end
    assert_raise ArgumentError, fn -> String.to_existing_atom(tag) end

    assert hd(version.manifest.assets).metadata.category == category
    assert hd(version.manifest.assets).metadata.tags == [tag]

    assert hd(version.manifest.pipelines).selectors == [
             {:tag, tag},
             {:category, category}
           ]
  end

  test "resolves atom and string labels consistently after JSON persistence" do
    raw_ref = {MyApp.Assets.RawOrders, :asset}
    mart_ref = {MyApp.Assets.MartOrders, :asset}

    assets = [
      %Asset{
        ref: raw_ref,
        module: elem(raw_ref, 0),
        name: :asset,
        depends_on: [],
        metadata: %{category: :orders, tags: [:raw, "daily"]}
      },
      %Asset{
        ref: mart_ref,
        module: elem(mart_ref, 0),
        name: :asset,
        depends_on: [raw_ref],
        metadata: %{category: "orders", tags: ["mart"]}
      }
    ]

    assert {:ok, graph} = Graph.build(assets)

    manifest = %Manifest{
      schema_version: 4,
      runner_contract_version: 4,
      assets: assets,
      pipelines: [
        %Pipeline{
          module: MyApp.Pipelines.Orders,
          name: :orders,
          selectors: [{:tag, :raw}, {:category, "orders"}],
          deps: :none
        }
      ],
      schedules: [],
      graph: graph,
      metadata: %{}
    }

    assert {:ok, encoded} = Serializer.encode_manifest(manifest)
    assert {:ok, decoded} = Serializer.decode_manifest(encoded)
    assert {:ok, version} = Version.new(decoded, manifest_version_id: "mv_label_resolution")

    assert [raw_asset, mart_asset] = version.manifest.assets
    assert raw_asset.metadata == %{category: "orders", tags: ["raw", "daily"]}
    assert mart_asset.metadata == %{category: "orders", tags: ["mart"]}

    assert [%Pipeline{selectors: [{:tag, "raw"}, {:category, "orders"}]} = pipeline] =
             version.manifest.pipelines

    assert {:ok, index} = Index.build_from_version(version)

    assert {:ok, %{target_refs: [^mart_ref, ^raw_ref]}} =
             PipelineResolver.resolve(index, pipeline, [])
  end

  test "rejects invalid unloaded module references during rehydration" do
    manifest = %{
      "schema_version" => 4,
      "runner_contract_version" => 4,
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
      "schema_version" => 4,
      "runner_contract_version" => 4,
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
      "schema_version" => 4,
      "runner_contract_version" => 4,
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
      "graph" => %{
        "nodes" => [%{"module" => module, "name" => name}],
        "edges" => [],
        "topo_order" => [%{"module" => module, "name" => name}]
      },
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
      "schema_version" => 4,
      "runner_contract_version" => 4,
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
