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

  test "binds DateTime parameters through the real DuckDB ADBC client" do
    resolved = %Resolved{
      name: :warehouse,
      adapter: ADBC,
      module: __MODULE__,
      config: %{open: [database: ":memory:"]}
    }

    run_started_at = ~U[2026-01-01 00:00:00.123456Z]
    window_start = ~U[2025-12-01 00:00:00Z]
    window_end = ~U[2026-01-01 00:00:00Z]
    resolver_at = ~U[2026-02-03 04:05:06.654321Z]

    assert {:ok, conn} = ADBC.connect(resolved, connect_opts())

    try do
      assert {:ok, result} =
               ADBC.query(
                 conn,
                 """
                 SELECT
                   epoch_us(CAST(? AS TIMESTAMPTZ)) AS run_started_at,
                   epoch_us(CAST(? AS TIMESTAMPTZ)) AS window_start,
                   epoch_us(CAST(? AS TIMESTAMPTZ)) AS window_end
                 """,
                 params: [run_started_at, window_start, window_end]
               )

      assert result.rows == [
               %{
                 "run_started_at" => DateTime.to_unix(run_started_at, :microsecond),
                 "window_start" => DateTime.to_unix(window_start, :microsecond),
                 "window_end" => DateTime.to_unix(window_end, :microsecond)
               }
             ]

      assert {:ok, _result} =
               ADBC.execute(
                 conn,
                 """
                 CREATE TABLE resolver_parameter AS
                 SELECT epoch_us(CAST(? AS TIMESTAMPTZ)) AS resolver_at
                 """,
                 params: [resolver_at]
               )

      assert {:ok, result} = ADBC.query(conn, "SELECT resolver_at FROM resolver_parameter", [])

      assert result.rows == [
               %{"resolver_at" => DateTime.to_unix(resolver_at, :microsecond)}
             ]
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
