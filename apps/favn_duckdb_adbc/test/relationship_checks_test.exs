defmodule FavnDuckdbADBC.RelationshipChecksTest do
  use ExUnit.Case, async: false
  alias Favn.Connection.Resolved
  alias Favn.SQL.Adapter.DuckDB.ADBC
  alias Favn.SQL.Contract.Relationship
  @moduletag :adbc_integration

  test "composite references handle nulls, orphans, duplicate targets and retained source rows" do
    resolved = %Resolved{
      name: :warehouse,
      adapter: ADBC,
      module: __MODULE__,
      config: %{open: [database: ":memory:"]}
    }

    opts =
      case System.get_env("DUCKDB_ADBC_DRIVER") do
        nil -> []
        "" -> []
        driver -> [duckdb_adbc: [driver: driver, entrypoint: "duckdb_adbc_init"]]
      end

    assert {:ok, conn} = ADBC.connect(resolved, opts)

    try do
      execute(conn, "CREATE TABLE stores(country VARCHAR, id INTEGER)")
      execute(conn, "CREATE TABLE candidate(country VARCHAR, store_id INTEGER)")
      execute(conn, "CREATE TABLE sales(country VARCHAR, store_id INTEGER)")
      execute(conn, "INSERT INTO stores VALUES ('NO', 1)")

      relationship =
        Relationship.new!(
          name: :store,
          target: {Example.Store, :asset},
          on: [country: :country, store_id: :id],
          cardinality: :one_to_one,
          on_violation: :fail
        )

      [before, after_check] = Relationship.check_specs(relationship)

      before_sql =
        before.sql
        |> String.replace("Example.Store", "stores")
        |> String.replace("query()", "candidate")

      after_sql = String.replace(after_check.sql, "target()", "sales")

      execute(conn, "INSERT INTO candidate VALUES ('NO', 1), (NULL, NULL)")
      assert passed?(conn, before_sql)
      execute(conn, "INSERT INTO candidate VALUES ('NO', NULL)")
      refute passed?(conn, before_sql)
      execute(conn, "DELETE FROM candidate WHERE country IS NOT NULL AND store_id IS NULL")
      execute(conn, "INSERT INTO candidate VALUES ('NO', 2)")
      refute passed?(conn, before_sql)
      execute(conn, "DELETE FROM candidate WHERE store_id = 2")
      execute(conn, "INSERT INTO stores VALUES ('NO', 1)")
      refute passed?(conn, before_sql)
      execute(conn, "DELETE FROM stores")
      execute(conn, "INSERT INTO stores VALUES ('NO', 1)")
      execute(conn, "INSERT INTO sales VALUES ('NO', 1), (NULL, NULL), (NULL, NULL)")
      assert passed?(conn, after_sql)

      assert {:error, %Favn.SQL.Error{cause: :relationship_collision}} =
               ADBC.transaction(
                 conn,
                 fn tx ->
                   execute(tx, "INSERT INTO sales SELECT * FROM candidate")

                   if passed?(tx, after_sql),
                     do: {:ok, :published},
                     else: {:error, :relationship_collision}
                 end,
                 []
               )

      assert {:ok, %{rows: [%{"n" => 3}]}} =
               ADBC.query(conn, "SELECT count(*) AS n FROM sales", [])
    after
      ADBC.disconnect(conn, [])
    end
  end

  defp execute(conn, sql), do: assert({:ok, _} = ADBC.execute(conn, sql, []))

  defp passed?(conn, sql) do
    assert {:ok, %{rows: [%{"passed" => passed}]}} = ADBC.query(conn, sql, [])
    passed
  end
end
