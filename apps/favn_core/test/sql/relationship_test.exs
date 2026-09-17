defmodule Favn.SQL.Contract.RelationshipTest do
  use ExUnit.Case, async: true
  alias Favn.SQL.{Contract, Template}
  alias Favn.SQL.Contract.Relationship

  defp relationship(opts \\ []) do
    Relationship.new!(
      Keyword.merge(
        [
          name: :store,
          target: {Example.Store, :asset},
          on: [country: :country, store_id: :id],
          cardinality: :many_to_one,
          on_violation: :fail
        ],
        opts
      )
    )
  end

  defp source(nullable \\ true) do
    Contract.new!(
      columns: [
        %{name: :country, type: :string, null: nullable},
        %{name: :store_id, type: :integer, null: nullable}
      ]
    )
  end

  defp target do
    Contract.new!(
      grain: [by: [:country, :id]],
      columns: [
        %{name: :country, type: :string, null: false},
        %{name: :id, type: :integer, null: false}
      ]
    )
  end

  test "requires full grain in order and matching types" do
    assert Relationship.validate_target!(relationship(), source(), target()) == relationship()

    assert_raise ArgumentError, ~r/complete target grain/, fn ->
      Relationship.validate_target!(
        relationship(on: [store_id: :id, country: :country]),
        source(),
        target()
      )
    end

    assert_raise ArgumentError, ~r/key types/, fn ->
      Relationship.validate_target!(
        relationship(on: [store_id: :country, country: :id]),
        source(),
        target()
      )
    end
  end

  test "rejects mixed composite nullability and duplicate roles" do
    columns = [%{name: :country, type: :string, null: false}, %{name: :store_id, type: :integer}]

    assert_raise ArgumentError, ~r/entirely nullable/, fn ->
      Contract.new!(columns: columns, relationships: [relationship()])
    end

    assert_raise ArgumentError, ~r/duplicate relationship role/, fn ->
      Contract.new!(columns: source().columns, relationships: [relationship(), relationship()])
    end
  end

  test "generated reference checks preserve asset refs and after uniqueness reads full target" do
    [before, after_check] =
      Relationship.check_specs(relationship(cardinality: :one_to_one, on_violation: :warn))

    assert before.at == :before_materialize
    assert before.on_violation == :warn

    template =
      Template.compile!(before.sql, file: "relationship.sql", line: 1, resolve_asset_refs: false)

    assert Enum.map(Template.asset_refs(template), & &1.asset_ref) == [
             {Example.Store, :asset},
             {Example.Store, :asset}
           ]

    assert before.sql =~ "NOT EXISTS"
    assert before.sql =~ "HAVING count(*) > 1"
    assert after_check.at == :after_materialize
    assert after_check.sql =~ "FROM target()"
    refute after_check.sql =~ "query()"
  end

  test "populated relationship contracts decode in a fresh VM" do
    alias Favn.Contracts.RunnerTask.PersistenceData

    contract =
      Contract.new!(
        columns: source().columns,
        relationships: [relationship(cardinality: :one_to_one)]
      )

    {:ok, encoded} = PersistenceData.encode(contract, 1_048_576)
    path = Path.join(System.tmp_dir!(), "relationship-#{System.unique_integer([:positive])}.json")
    File.write!(path, Jason.encode!(encoded))
    on_exit(fn -> File.rm(path) end)
    paths = :code.get_path() |> Enum.flat_map(fn path -> ["-pa", to_string(path)] end)

    code = """
    encoded = System.argv() |> hd() |> File.read!() |> Jason.decode!()
    {:ok, contract} = Favn.Contracts.RunnerTask.PersistenceData.decode(encoded, 1_048_576, nil,
      [Example.Store, :store, :country, :store_id, :id])
    [%Favn.SQL.Contract.Relationship{cardinality: :one_to_one, on: [country: :country, store_id: :id]}] = contract.relationships
    Favn.SQL.Contract.validate!(contract)
    IO.puts("relationship recovered")
    """

    {output, status} =
      System.cmd(System.find_executable("elixir"), paths ++ ["-e", code, path],
        stderr_to_stdout: true,
        env: [{"ERL_FLAGS", "+S 2:2"}]
      )

    assert status == 0, output
    assert output =~ "relationship recovered"
  end
end
