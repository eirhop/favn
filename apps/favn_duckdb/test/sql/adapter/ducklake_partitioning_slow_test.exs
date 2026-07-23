defmodule FavnDuckdb.SQLAdapterDuckLakePartitioningSlowTest do
  use ExUnit.Case, async: false

  alias Favn.Connection.Resolved
  alias Favn.SQL.Adapter.DuckDB
  alias Favn.SQL.PartitionSpec
  alias Favn.SQL.Relation
  alias Favn.SQL.WritePlan

  @moduletag :slow
  @moduletag :tmp_dir

  test "creates partitioned DuckLake data and reapplies the same spec on a later write", %{
    tmp_dir: tmp_dir
  } do
    extension_dir = Path.join(tmp_dir, "extensions")
    metadata_path = Path.join(tmp_dir, "metadata.ducklake")
    data_path = Path.join(tmp_dir, "data")
    File.mkdir_p!(data_path)

    resolved = %Resolved{
      name: :ducklake_partitioning,
      adapter: DuckDB,
      module: __MODULE__,
      config: %{open: [database: ":memory:"]}
    }

    assert {:ok, conn} = DuckDB.connect(resolved, [])
    on_exit(fn -> DuckDB.disconnect(conn, []) end)

    assert {:ok, _result} =
             DuckDB.execute(
               conn,
               "SET extension_directory = #{quote_literal(extension_dir)}",
               []
             )

    assert {:ok, _result} = DuckDB.execute(conn, "INSTALL ducklake", [])

    assert {:ok, _result} =
             DuckDB.execute(
               conn,
               "ATTACH #{quote_literal("ducklake:" <> metadata_path)} AS lake " <>
                 "(DATA_PATH #{quote_literal(data_path)})",
               []
             )

    partition_spec =
      PartitionSpec.normalize!([
        :tenant_id,
        {:year, :occurred_at},
        {:bucket, 8, :account_id}
      ])

    target = %Relation{catalog: "lake", schema: "main", name: "events", type: :table}

    replacement = %WritePlan{
      materialization: :table,
      target: target,
      select_sql:
        "SELECT 1 AS tenant_id, TIMESTAMP '2026-01-01' AS occurred_at, " <>
          "7 AS account_id",
      replace_existing?: true,
      transactional?: true,
      partition_spec: partition_spec
    }

    assert {:ok, _result} = DuckDB.materialize(conn, replacement, [])

    append = %WritePlan{
      materialization: :incremental,
      strategy: :append,
      mode: :incremental,
      target: target,
      select_sql:
        "SELECT 2 AS tenant_id, TIMESTAMP '2026-02-01' AS occurred_at, " <>
          "9 AS account_id",
      transactional?: true,
      partition_spec: partition_spec
    }

    assert {:ok, _result} = DuckDB.materialize(conn, append, [])

    assert {:ok, database} =
             DuckDB.query(
               conn,
               "SELECT type FROM duckdb_databases() WHERE database_name = 'lake'",
               []
             )

    assert database.rows == [%{"type" => "ducklake"}]

    assert {:ok, rows} =
             DuckDB.query(conn, "SELECT count(*) AS row_count FROM lake.main.events", [])

    assert rows.rows == [%{"row_count" => 2}]
  end

  defp quote_literal(value), do: "'" <> String.replace(value, "'", "''") <> "'"
end
