defmodule Favn.Manifest.ExecutionPackageTest do
  use ExUnit.Case, async: true

  alias Favn.Manifest
  alias Favn.Manifest.Asset
  alias Favn.Manifest.ExecutionPackage
  alias Favn.Manifest.Graph
  alias Favn.Manifest.Publication
  alias Favn.Manifest.SQLExecution
  alias Favn.Manifest.Version
  alias Favn.SQL.Definition
  alias Favn.SQL.Template
  alias Favn.SQL.Template.{Call, DefinitionRef, Placeholder, Relation}

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
    payload = %{schema_version: 1, asset_ref: invalid.asset_ref, sql_execution: nil}
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

    payload = %{schema_version: 1, asset_ref: ref, sql_execution: execution}
    {:ok, encoded} = Favn.Manifest.Serializer.encode_manifest(payload)
    hash = :crypto.hash(:sha256, encoded) |> Base.encode16(case: :lower)

    package = %ExecutionPackage{
      schema_version: 1,
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

    manifest = %Manifest{
      assets: [
        sql_asset(first_ref, package.content_hash),
        sql_asset(second_ref, package.content_hash)
      ],
      graph: %Graph{nodes: [first_ref, second_ref], topo_order: [first_ref, second_ref]}
    }

    assert {:error, {:duplicate_execution_package_hash, hash, [^second_ref, ^first_ref]}} =
             Version.new(manifest)

    assert hash == package.content_hash
  end

  defp execution_package(ref, sql) do
    template =
      Template.compile!(sql,
        file: "test/execution_package_test.sql",
        line: 1,
        module: __MODULE__,
        scope: :query,
        enforce_query_root: true
      )

    {:ok, package} = ExecutionPackage.new(ref, %SQLExecution{sql: sql, template: template})
    package
  end

  defp version(ref, package_hash) do
    asset = sql_asset(ref, package_hash)

    {:ok, version} =
      Version.new(%Manifest{assets: [asset], graph: %Graph{nodes: [ref], topo_order: [ref]}})

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
end
