defmodule FavnDuckdbADBC.SQLAdapterDuckDBADBCIntegrationTest do
  use ExUnit.Case, async: false

  alias Favn.Connection.Resolved
  alias Favn.RelationRef
  alias Favn.SQL.Adapter.DuckDB.ADBC

  alias Favn.SQL.{
    Error,
    GenerationActivation,
    GenerationDiscard,
    GenerationReconciliation,
    GenerationRelation
  }

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

  test "atomically activates and reconciles DuckDB generations through ADBC" do
    resolved = %Resolved{
      name: :warehouse,
      adapter: ADBC,
      module: __MODULE__,
      config: %{open: [database: ":memory:"]}
    }

    stable = %RelationRef{connection: :warehouse, schema: "mart", name: "orders"}
    old_generation = "11111111-1111-4111-8111-111111111111"
    new_generation = "22222222-2222-4222-8222-222222222222"
    first_candidate = GenerationRelation.candidate(stable, old_generation, 128)
    first_retired = GenerationRelation.retired(stable, old_generation, 128)
    second_candidate = GenerationRelation.candidate(stable, new_generation, 128)

    assert {:ok, conn} = ADBC.connect(resolved, connect_opts())

    try do
      assert {:ok, _result} = ADBC.execute(conn, "CREATE SCHEMA mart", [])
      create_table(conn, first_candidate, 1)

      first =
        activation(
          conn,
          stable,
          first_candidate,
          first_retired,
          nil,
          old_generation,
          "op-1",
          "token-1"
        )

      assert {:ok, first_result} = ADBC.activate_generation(conn, first, [])

      create_table(conn, second_candidate, 2)

      second =
        activation(
          conn,
          stable,
          second_candidate,
          first_retired,
          old_generation,
          new_generation,
          "op-2",
          "token-2"
        )
        |> Map.put(:expected_active_marker, first_result.marker)

      assert {:ok, result} = ADBC.activate_generation(conn, second, [])
      assert result.marker.activation_operation_id == "op-2"
      assert result.candidate_fingerprint == second.expected_candidate_fingerprint
      assert rows(conn, stable) == [%{"id" => 2}]
      assert rows(conn, first_retired) == [%{"id" => 1}]

      assert {:ok, marker} =
               ADBC.reconcile_generation(
                 conn,
                 %GenerationReconciliation{
                   logical_target_id: second.logical_target_id,
                   stable_relation: stable
                 },
                 []
               )

      assert marker.active_generation_id == new_generation

      assert {:error, %Error{details: %{classification: :active_generation_discard_forbidden}}} =
               ADBC.discard_generation(
                 conn,
                 %GenerationDiscard{
                   logical_target_id: second.logical_target_id,
                   stable_relation: stable,
                   candidate_generation_id: new_generation,
                   candidate_relation: second_candidate
                 },
                 []
               )
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

  defp activation(
         conn,
         stable,
         candidate,
         retired,
         expected_generation,
         candidate_generation,
         operation_id,
         token
       ) do
    assert {:ok, inspection} = ADBC.inspect_generation(conn, candidate, [])

    %GenerationActivation{
      logical_target_id: "MyApp.Assets.orders",
      stable_relation: stable,
      candidate_relation: candidate,
      retired_relation: retired,
      expected_candidate_fingerprint: inspection.physical_fingerprint.fingerprint,
      expected_active_generation_id: expected_generation,
      candidate_generation_id: candidate_generation,
      activation_operation_id: operation_id,
      activation_token: token,
      activated_at: ~U[2026-07-22 10:00:00Z]
    }
  end

  defp create_table(conn, relation, id) do
    assert {:ok, _result} =
             ADBC.execute(
               conn,
               [
                 "CREATE TABLE ",
                 qualified(relation),
                 " AS SELECT ",
                 Integer.to_string(id),
                 "::BIGINT AS id"
               ],
               []
             )
  end

  defp rows(conn, relation) do
    assert {:ok, result} = ADBC.query(conn, ["SELECT id FROM ", qualified(relation)], [])
    result.rows
  end

  defp qualified(ref), do: [quote_ident(ref.schema), ".", quote_ident(ref.name)]
  defp quote_ident(value), do: ["\"", String.replace(value, "\"", "\"\""), "\""]
end
