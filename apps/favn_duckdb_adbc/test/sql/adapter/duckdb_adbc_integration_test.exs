defmodule FavnDuckdbADBC.SQLAdapterDuckDBADBCIntegrationTest do
  use ExUnit.Case, async: false

  alias Favn.Connection.Resolved
  alias Favn.SQL.Adapter.DuckDB.ADBC

  @moduletag :adbc_integration

  test "runs a real in-memory DuckDB ADBC query" do
    resolved = %Resolved{
      name: :warehouse,
      adapter: ADBC,
      module: __MODULE__,
      config: %{database: ":memory:"}
    }

    assert {:ok, conn} = ADBC.connect(resolved, connect_opts())
    assert :ok = ADBC.ping(conn, [])
    assert {:ok, _result} = ADBC.execute(conn, "CREATE TABLE orders AS SELECT 1 AS id", [])
    assert {:ok, result} = ADBC.query(conn, "SELECT id FROM orders", [])
    assert result.rows == [%{"id" => 1}]
    assert :ok = ADBC.disconnect(conn, [])
  end

  defp connect_opts do
    case System.get_env("DUCKDB_ADBC_DRIVER") do
      nil -> []
      "" -> []
      driver -> [duckdb_adbc: [driver: driver, entrypoint: "duckdb_adbc_init"]]
    end
  end
end
