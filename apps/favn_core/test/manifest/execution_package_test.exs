defmodule Favn.Manifest.ExecutionPackageTest do
  use ExUnit.Case, async: true

  alias Favn.Asset.RelationInput
  alias Favn.Manifest
  alias Favn.Manifest.Asset
  alias Favn.Manifest.ExecutionPackage
  alias Favn.Manifest.Graph
  alias Favn.Manifest.Publication
  alias Favn.Manifest.SQLExecution
  alias Favn.Manifest.Version
  alias Favn.RelationRef
  alias Favn.SQL.Definition
  alias Favn.SQL.Template
  alias Favn.SQL.Template.{Call, DefinitionRef, Placeholder, Relation}

  test "old published bytes retain their hash and fail the schema gate before generated checks" do
    # Fixed schema-4 bytes built with the parser/package code from 5f3c163e.
    published =
      __DIR__ |> Path.join("../fixtures/sql_package_v4.json") |> File.read!() |> Jason.decode!()

    original = Favn.Manifest.Serializer.encode_manifest!(published)
    expected_hash = "6a7ad0e6eb1bcfb0600125f753341b2113a000e6899742438ea3ee8a4291c8b3"

    payload =
      published |> Map.delete("content_hash") |> Favn.Manifest.Serializer.encode_manifest!()

    assert Base.encode16(:crypto.hash(:sha256, payload), case: :lower) == expected_hash
    assert published["content_hash"] == expected_hash

    assert {:error, {:unsupported_execution_package_schema, 4, 5}} =
             ExecutionPackage.from_published(published)

    assert Favn.Manifest.Serializer.encode_manifest!(published) == original

    for schema <- [4, 6] do
      malformed = %{schema_version: schema, sql_execution: %{checks: :invalid}}

      assert {:error, {:unsupported_execution_package_schema, ^schema, 5}} =
               ExecutionPackage.from_published(malformed)

      package = %ExecutionPackage{
        schema_version: schema,
        asset_ref: {MyApp.Orders, :asset},
        content_hash: expected_hash,
        sql_execution: nil
      }

      assert {:error, {:unsupported_execution_package_schema, ^schema, 5}} =
               ExecutionPackage.verify(package)

      assert {:error, {:unsupported_execution_package_schema, ^schema, 5}} =
               ExecutionPackage.from_published(package)
    end
  end

  test "compact literal bytes and source positions remain covered by the hash" do
    package = execution_package({MyApp.Orders, :asset}, "SELECT 1 + 2 AS id")
    assert [%Favn.SQL.Template.Text{} = text] = package.sql_execution.template.nodes

    for altered <- [
          %{text | sql: "SELECT 1 + 3 AS id"},
          %{text | span: %{text.span | end_column: text.span.end_column + 1}}
        ] do
      template = %{package.sql_execution.template | nodes: [altered]}
      tampered = %{package | sql_execution: %{package.sql_execution | template: template}}

      assert {:error, {:execution_package_hash_mismatch, _, _}} =
               ExecutionPackage.verify(tampered)
    end
  end

  test "content hash covers the complete execution payload" do
    package = execution_package({MyApp.Orders, :asset}, "SELECT 1 AS id")
    unchanged = execution_package({MyApp.Orders, :asset}, "SELECT 1 AS id")
    changed = execution_package({MyApp.Orders, :asset}, "SELECT 2 AS id")

    assert {:ok, ^package} = ExecutionPackage.verify(package)
    assert unchanged.content_hash == package.content_hash
    assert changed.content_hash != package.content_hash

    tampered_template = %{package.sql_execution.template | source: "SELECT 2 AS id"}

    tampered = %{
      package
      | sql_execution: %{
          package.sql_execution
          | sql: "SELECT 2 AS id",
            template: tampered_template
        }
    }

    assert {:error, {:execution_package_hash_mismatch, expected, computed}} =
             ExecutionPackage.verify(tampered)

    assert expected == package.content_hash
    assert computed != expected
  end

  test "content hash covers resolved relation bindings" do
    ref = {MyApp.OrderSummary, :asset}
    sql = "SELECT * FROM orders"

    raw_orders =
      relation_input(
        {MyApp.RawOrders, :asset},
        RelationRef.new!(%{
          connection: :warehouse,
          catalog: "raw",
          schema: "sales",
          name: "orders"
        })
      )

    curated_orders =
      relation_input(
        {MyApp.CuratedOrders, :asset},
        RelationRef.new!(%{
          connection: :warehouse,
          catalog: "curated",
          schema: "sales",
          name: "orders"
        })
      )

    raw_package = execution_package(ref, sql, [raw_orders])
    curated_package = execution_package(ref, sql, [curated_orders])

    assert raw_package.content_hash != curated_package.content_hash
  end

  test "replacement scope SQL is immutable execution identity" do
    ref = {MyApp.CustomerRows, :asset}
    first = group_execution_package(ref, "SELECT customer_id FROM changed_customers")
    unchanged = group_execution_package(ref, "SELECT customer_id FROM changed_customers")
    changed = group_execution_package(ref, "SELECT customer_id FROM recently_changed_customers")

    assert first.content_hash == unchanged.content_hash
    refute first.content_hash == changed.content_hash
    assert {:ok, ^first} = ExecutionPackage.verify(first)

    assert {:ok, encoded} = Favn.Manifest.Serializer.encode_manifest(first)
    assert {:ok, decoded} = Favn.Manifest.Serializer.decode_manifest(encoded)
    assert {:ok, roundtripped} = ExecutionPackage.from_published(decoded)

    assert roundtripped.sql_execution.incremental_scope_sql ==
             first.sql_execution.incremental_scope_sql

    assert %Template{} = roundtripped.sql_execution.incremental_scope_template
    assert roundtripped.sql_execution.full_scope_sql == first.sql_execution.full_scope_sql
    assert %Template{} = roundtripped.sql_execution.full_scope_template
  end

  test "rejects replacement_scope usage without immutable scope payloads" do
    ref = {MyApp.InvalidCustomerRows, :asset}
    sql = "SELECT customer_id FROM replacement_scope()"

    template =
      Template.compile!(sql,
        file: "test/execution_package_test.sql",
        line: 1,
        module: __MODULE__,
        scope: :query,
        enforce_query_root: true
      )

    assert {:error, :invalid_execution_package} =
             ExecutionPackage.new(ref, %SQLExecution{sql: sql, template: template})
  end

  test "package scopes must match the indexed asset strategy" do
    ref = {MyApp.CustomerRows, :asset}
    package = group_execution_package(ref, "SELECT customer_id FROM changed_customers")

    ordinary = %Asset{
      ref: ref,
      module: elem(ref, 0),
      name: :asset,
      type: :sql,
      materialization: :table,
      execution_package_hash: package.content_hash,
      relation_inputs: []
    }

    assert {:error, :execution_package_materialization_mismatch} =
             ExecutionPackage.verify_for_asset(package, ordinary)

    group = %{
      ordinary
      | materialization:
          {:incremental, strategy: :replace_groups, replacement_key: [:customer_id]}
    }

    assert {:ok, ^package} = ExecutionPackage.verify_for_asset(package, group)
  end

  test "rejects non-current execution-package schemas" do
    package = execution_package({MyApp.Orders, :asset}, "SELECT 1 AS id")

    assert {:error, {:unsupported_execution_package_schema, 1, 5}} =
             ExecutionPackage.verify(%{package | schema_version: 1})
  end

  test "verification rejects relation bindings that differ from the indexed asset" do
    ref = {MyApp.OrderSummary, :asset}

    relation_input =
      relation_input(
        {MyApp.RawOrders, :asset},
        RelationRef.new!(%{
          connection: :warehouse,
          catalog: "raw",
          schema: "sales",
          name: "orders"
        })
      )

    package = execution_package(ref, "SELECT * FROM orders", [relation_input])

    asset = %Asset{
      ref: ref,
      module: elem(ref, 0),
      name: elem(ref, 1),
      type: :sql,
      execution_package_hash: package.content_hash,
      relation_inputs: []
    }

    assert {:error, {:execution_package_relation_inputs_mismatch, ^ref}} =
             ExecutionPackage.verify_for_asset(package, asset)
  end

  test "publication requires exact package coverage" do
    ref = {MyApp.Orders, :asset}
    package = execution_package(ref, "SELECT 1 AS id")
    version = version(ref, package.content_hash)

    assert {:ok, %Publication{} = publication} = Publication.from_parts(version, [package])
    assert Publication.required_package_hashes(version) == [package.content_hash]
    assert Publication.packages_by_hash(publication) == %{package.content_hash => package}

    assert {:error, {:missing_execution_packages, [missing]}} =
             Publication.from_parts(version, [])

    assert missing == package.content_hash

    unexpected = execution_package({MyApp.Customers, :asset}, "SELECT 2 AS id")

    assert {:error, {:unexpected_execution_packages, [unexpected_hash]}} =
             Publication.from_parts(version, [package, unexpected])

    assert unexpected_hash == unexpected.content_hash
  end

  test "publication verifies that a package belongs to the indexed asset" do
    package = execution_package({MyApp.Customers, :asset}, "SELECT 1 AS id")
    version = version({MyApp.Orders, :asset}, package.content_hash)

    assert {:error,
            {:execution_package_asset_mismatch, hash, {MyApp.Orders, :asset},
             {MyApp.Customers, :asset}}} = Publication.from_parts(version, [package])

    assert hash == package.content_hash
  end

  test "verification rejects a correctly hashed package without SQL execution" do
    package = execution_package({MyApp.Orders, :asset}, "SELECT 1 AS id")

    invalid = %{package | sql_execution: nil}
    payload = %{schema_version: 5, asset_ref: invalid.asset_ref, sql_execution: nil}
    {:ok, encoded} = Favn.Manifest.Serializer.encode_manifest(payload)
    hash = :crypto.hash(:sha256, encoded) |> Base.encode16(case: :lower)

    assert {:error, :invalid_execution_package} =
             ExecutionPackage.verify(%{invalid | content_hash: hash})
  end

  test "construction and verification reject invalid nested template structs" do
    ref = {MyApp.Orders, :asset}

    invalid_template = %Template{
      source: nil,
      root_kind: nil,
      nodes: nil,
      span: nil,
      requires: nil
    }

    execution = %SQLExecution{sql: "SELECT 1 AS id", template: invalid_template}

    assert {:error, {:invalid_manifest_payload, %ArgumentError{}}} =
             ExecutionPackage.new(ref, execution)

    payload = %{schema_version: 5, asset_ref: ref, sql_execution: execution}
    {:ok, encoded} = Favn.Manifest.Serializer.encode_manifest(payload)
    hash = :crypto.hash(:sha256, encoded) |> Base.encode16(case: :lower)

    package = %ExecutionPackage{
      schema_version: 5,
      content_hash: hash,
      asset_ref: ref,
      sql_execution: execution
    }

    assert {:error, {:invalid_manifest_payload, %ArgumentError{}}} =
             ExecutionPackage.verify(package)
  end

  test "construction rejects nested template values that rehydration would normalize" do
    ref = {MyApp.Orders, :asset}

    template = %Template{
      source: "SELECT 1 AS id",
      root_kind: :query,
      nodes: nil,
      span: nil,
      requires: nil
    }

    execution = %SQLExecution{sql: "SELECT 1 AS id", template: template}

    assert {:error, :invalid_execution_package} = ExecutionPackage.new(ref, execution)
  end

  test "construction rejects stable but unknown template node shapes" do
    ref = {MyApp.Orders, :asset}
    valid = execution_package(ref, "SELECT 1 AS id")

    template = %{valid.sql_execution.template | nodes: [%{"bogus" => "node"}]}
    execution = %{valid.sql_execution | template: template}

    assert {:error, :invalid_execution_package} = ExecutionPackage.new(ref, execution)
  end

  test "construction rejects cyclic SQL definitions" do
    ref = {MyApp.Orders, :asset}
    valid = execution_package(ref, "SELECT 1 AS id")

    call = %Call{
      definition: %DefinitionRef{
        provider: MyApp.Definitions,
        name: :loop,
        arity: 0,
        kind: :expression
      },
      args: [],
      context: :expression,
      span: valid.sql_execution.template.span
    }

    definition = %Definition{
      module: MyApp.Definitions,
      name: :loop,
      arity: 0,
      params: [],
      shape: :expression,
      sql: "loop()",
      template: %{
        valid.sql_execution.template
        | source: "loop()",
          root_kind: :expression,
          nodes: [call]
      },
      file: "test/cyclic_definition.sql",
      line: 1,
      declared_file: "test/cyclic_definition.ex",
      declared_line: 1,
      relation_defaults: %{}
    }

    execution = %{valid.sql_execution | sql_definitions: [definition]}

    assert {:error, :invalid_execution_package} = ExecutionPackage.new(ref, execution)
  end

  test "construction rejects relation nodes the renderer cannot handle" do
    ref = {MyApp.Orders, :asset}
    valid = execution_package(ref, "SELECT 1 AS id")

    relation = %Relation{
      raw: "server.catalog.schema.table",
      segments: ["server", "catalog", "schema", "table"],
      span: valid.sql_execution.template.span
    }

    sql = "SELECT * FROM server.catalog.schema.table"
    template = %{valid.sql_execution.template | source: sql, nodes: [relation]}
    execution = %{valid.sql_execution | sql: sql, template: template}

    assert {:error, :invalid_execution_package} = ExecutionPackage.new(ref, execution)
  end

  test "published maps must exactly match the content-addressed canonical package" do
    package = execution_package({MyApp.Orders, :asset}, "SELECT 1 AS id")

    raw =
      package
      |> Favn.Manifest.Serializer.encode_manifest!()
      |> JSON.decode!()

    normalizable = put_in(raw, ["sql_execution", "template", "requires"], nil)
    unknown_field = Map.put(raw, "unknown", true)

    assert {:error, :invalid_execution_package} =
             ExecutionPackage.from_published(normalizable)

    assert {:error, :invalid_execution_package} =
             ExecutionPackage.from_published(unknown_field)
  end

  test "construction rejects SQL calls that disagree with their stored definition" do
    ref = {MyApp.Orders, :asset}
    valid = execution_package(ref, "SELECT 1 AS id")

    definition_sql = "1"

    definition_template =
      Template.compile!(definition_sql,
        file: "test/call_contract_definition.sql",
        line: 1,
        module: MyApp.ActualDefinitions,
        scope: :definition
      )

    definition = %Definition{
      module: MyApp.ActualDefinitions,
      name: :value,
      arity: 0,
      params: [],
      shape: :expression,
      sql: definition_sql,
      template: definition_template,
      file: "test/call_contract_definition.sql",
      line: 1,
      declared_file: "test/call_contract_definition.ex",
      declared_line: 1,
      relation_defaults: %{}
    }

    call = %Call{
      definition: %DefinitionRef{
        provider: MyApp.WrongDefinitions,
        name: :value,
        arity: 0,
        kind: :expression
      },
      args: [],
      context: :expression,
      span: valid.sql_execution.template.span
    }

    sql = "SELECT value() AS id"
    template = %{valid.sql_execution.template | source: sql, nodes: [call]}

    execution = %{
      valid.sql_execution
      | sql: sql,
        template: template,
        sql_definitions: [definition]
    }

    assert {:error, :invalid_execution_package} = ExecutionPackage.new(ref, execution)
  end

  test "construction rejects local arguments in a top-level query template" do
    ref = {MyApp.Orders, :asset}
    valid = execution_package(ref, "SELECT 1 AS id")

    placeholder = %Placeholder{
      name: :value,
      source: {:local_arg, 0},
      span: valid.sql_execution.template.span
    }

    sql = "SELECT @value AS id"
    template = %{valid.sql_execution.template | source: sql, nodes: [placeholder]}
    execution = %{valid.sql_execution | sql: sql, template: template}

    assert {:error, :invalid_execution_package} = ExecutionPackage.new(ref, execution)
  end

  test "construction rejects definition local arguments outside its parameter map" do
    ref = {MyApp.Orders, :asset}
    valid = execution_package(ref, "SELECT 1 AS id")

    placeholder = %Placeholder{
      name: :value,
      source: {:local_arg, 1},
      span: valid.sql_execution.template.span
    }

    definition = %Definition{
      module: MyApp.Definitions,
      name: :value,
      arity: 1,
      params: [%Definition.Param{name: :value, index: 0}],
      shape: :expression,
      sql: "@value",
      template: %{
        valid.sql_execution.template
        | source: "@value",
          root_kind: :expression,
          nodes: [placeholder]
      },
      file: "test/definition_local_arg.sql",
      line: 1,
      declared_file: "test/definition_local_arg.ex",
      declared_line: 1,
      relation_defaults: %{}
    }

    execution = %{valid.sql_execution | sql_definitions: [definition]}

    assert {:error, :invalid_execution_package} = ExecutionPackage.new(ref, execution)
  end

  test "manifest versions reject one package hash assigned to multiple assets" do
    package = execution_package({MyApp.Orders, :asset}, "SELECT 1 AS id")
    first_ref = {MyApp.Orders, :asset}
    second_ref = {MyApp.Customers, :asset}

    manifest =
      current_manifest(%{
        assets: [
          sql_asset(first_ref, package.content_hash),
          sql_asset(second_ref, package.content_hash)
        ],
        graph: %Graph{nodes: [first_ref, second_ref], topo_order: [first_ref, second_ref]}
      })

    assert {:error, {:duplicate_execution_package_hash, hash, [^second_ref, ^first_ref]}} =
             Version.new(manifest)

    assert hash == package.content_hash
  end

  defp execution_package(ref, sql, relation_inputs \\ []) do
    template =
      Template.compile!(sql,
        file: "test/execution_package_test.sql",
        line: 1,
        module: __MODULE__,
        scope: :query,
        enforce_query_root: true
      )

    {:ok, package} =
      ExecutionPackage.new(ref, %SQLExecution{
        sql: sql,
        template: template,
        relation_inputs: relation_inputs
      })

    package
  end

  defp group_execution_package(ref, incremental_scope_sql) do
    sql = "SELECT scope.customer_id FROM replacement_scope() AS scope"
    full_scope_sql = "SELECT customer_id FROM all_customers"

    template_opts = [
      file: "test/execution_package_test.sql",
      line: 1,
      module: __MODULE__,
      scope: :query,
      enforce_query_root: true
    ]

    {:ok, package} =
      ExecutionPackage.new(ref, %SQLExecution{
        sql: sql,
        template: Template.compile!(sql, template_opts),
        incremental_scope_sql: incremental_scope_sql,
        incremental_scope_template: Template.compile!(incremental_scope_sql, template_opts),
        full_scope_sql: full_scope_sql,
        full_scope_template: Template.compile!(full_scope_sql, template_opts)
      })

    package
  end

  defp relation_input(asset_ref, relation_ref) do
    %RelationInput{
      kind: :plain_relation,
      raw: relation_ref.name,
      asset_ref: asset_ref,
      relation_ref: relation_ref,
      resolution: :resolved
    }
  end

  defp version(ref, package_hash) do
    asset = sql_asset(ref, package_hash)

    {:ok, version} =
      Version.new(
        current_manifest(%{assets: [asset], graph: %Graph{nodes: [ref], topo_order: [ref]}})
      )

    version
  end

  defp sql_asset(ref, package_hash) do
    %Asset{
      ref: ref,
      module: elem(ref, 0),
      name: elem(ref, 1),
      type: :sql,
      execution_package_hash: package_hash
    }
  end

  defp current_manifest(attrs) do
    attrs
    |> FavnTestSupport.with_manifest_contract()
    |> then(&struct!(Manifest, &1))
  end
end
