defmodule FavnDuckdbADBC.SQLAdapterDuckDBADBCIntegrationTest do
  use ExUnit.Case, async: false

  alias Favn.Connection.Resolved
  alias Favn.SQL.Adapter.DuckDB.ADBC

  @moduletag :adbc_integration

  test "runs a native multi-statement session script through DuckDB ADBC" do
    script =
      Path.join(
        System.tmp_dir!(),
        "favn_adbc_session_#{System.unique_integer([:positive])}.sql"
      )

    File.write!(
      script,
      "CREATE TEMP TABLE session_ready(value INTEGER);\nINSERT INTO session_ready VALUES (42);"
    )

    on_exit(fn -> File.rm(script) end)

    resolved = %Resolved{
      name: :warehouse,
      adapter: ADBC,
      module: __MODULE__,
      config: %{open: [database: ":memory:"], duckdb: [startup: [file: script]]}
    }

    assert {:ok, conn} = ADBC.connect(resolved, connect_opts())

    try do
      assert :ok = ADBC.bootstrap(conn, resolved, connect_opts())
      assert :ok = ADBC.ping(conn, [])
      assert {:ok, result} = ADBC.query(conn, "SELECT value FROM session_ready", [])
      assert result.rows == [%{"value" => 42}]
    after
      ADBC.disconnect(conn, [])
    end
  end

  defp connect_opts do
    case System.get_env("DUCKDB_ADBC_DRIVER") do
      nil -> []
      "" -> []
      driver -> [duckdb_adbc: [driver: driver, entrypoint: "duckdb_adbc_init"]]
    end
  end
end
