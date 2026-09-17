defmodule Favn.SQLRelationshipTest do
  use ExUnit.Case, async: true
  alias Favn.SQL.Contract
  alias Favn.SQL.Template

  defmodule Store do
    use Favn.SQLAsset
    relation(connection: :warehouse, schema: "mart", name: "store")
    materialized(:table)

    contract do
      grain(by: [:id])
      column(:id, :integer, null: false)
    end

    query do
      ~SQL"SELECT 1 AS id"
    end
  end

  defmodule Sales do
    use Favn.SQLAsset
    depends(Store)
    relation(connection: :warehouse, schema: "mart", name: "sales")
    materialized(:table)

    contract do
      column(:store_id, :integer)

      relationship(:store, Store,
        on: [store_id: :id],
        cardinality: :one_to_one,
        on_violation: :warn
      )
    end

    query do
      ~SQL"SELECT 1 AS store_id"
    end
  end

  test "compiles relationship checks with dependency asset references and revalidates without resolution" do
    definition = Sales.__favn_sql_asset_definition__()
    assert [%{name: :store, target: {Store, :asset}}] = definition.contract.relationships
    assert [before, after_check] = definition.checks
    assert before.uses_query?
    assert after_check.uses_target?
    assert Enum.all?(Template.asset_refs(before.template), &(&1.asset_ref == {Store, :asset}))

    assert definition.checks ==
             Contract.validate_generated_checks!(definition.contract, definition.checks)
  end

  test "relationship execution package rehydrates in a fresh VM without authoring modules" do
    alias Favn.Manifest.{ExecutionPackage, Serializer, SQLExecution}
    definition = Sales.__favn_sql_asset_definition__()

    assert {:ok, package} =
             ExecutionPackage.new({Sales, :asset}, SQLExecution.from_definition(definition))

    path =
      Path.join(
        System.tmp_dir!(),
        "relationship-package-#{System.unique_integer([:positive])}.json"
      )

    File.write!(path, Serializer.encode_manifest!(package))
    on_exit(fn -> File.rm(path) end)
    paths = :code.get_path() |> Enum.flat_map(fn path -> ["-pa", to_string(path)] end)

    code = """
    payload = System.argv() |> hd() |> File.read!() |> Jason.decode!()
    {:ok, package} = Favn.Manifest.ExecutionPackage.from_published(payload)
    [relationship] = package.sql_execution.contract.relationships
    {module, :asset} = relationship.target
    false = Code.ensure_loaded?(module)
    [before, after_check] = package.sql_execution.checks
    true = before.uses_query?
    true = after_check.uses_target?
    IO.puts("package recovered without authoring")
    """

    {output, status} =
      System.cmd(System.find_executable("elixir"), paths ++ ["-e", code, path],
        stderr_to_stdout: true,
        env: [{"ERL_FLAGS", "+S 2:2"}]
      )

    assert status == 0, output
    assert output =~ "package recovered without authoring"
  end

  test "requires explicit dependencies and same-connection targets" do
    [raw] = Sales.__favn_assets_raw__()

    assert_raise CompileError, ~r/explicitly declared with depends/, fn ->
      Favn.SQLAsset.finalize_raw_definition(%{raw | depends: []})
    end

    assert_raise CompileError, ~r/same SQL connection/, fn ->
      Favn.SQLAsset.finalize_raw_definition(%{
        raw
        | relation: [[connection: :other, schema: "mart", name: "sales"]]
      })
    end
  end
end
