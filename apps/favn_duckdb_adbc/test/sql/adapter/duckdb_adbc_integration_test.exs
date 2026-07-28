defmodule FavnDuckdbADBC.SQLAdapterDuckDBADBCIntegrationTest do
  use ExUnit.Case, async: false

  alias Favn.Connection.Resolved
  alias Favn.Manifest.Compatibility
  alias Favn.Manifest.TargetDescriptor
  alias Favn.RelationRef
  alias Favn.SQL.Adapter.DuckDB.ADBC
  alias Favn.SQL.Contract
  alias Favn.SQL.ContractValidation
  alias Favn.TargetCompatibility.PhysicalFingerprint
  alias Favn.TargetGenerationRelation

  alias Favn.SQL.{
    Error,
    GenerationActivation,
    GenerationDiscard,
    GenerationMarkerInitialization,
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

  test "validates a strict contract for a DuckDB CTAS table with uncertain nullability" do
    resolved = %Resolved{
      name: :warehouse,
      adapter: ADBC,
      module: __MODULE__,
      config: %{open: [database: ":memory:"]}
    }

    assert {:ok, conn} = ADBC.connect(resolved, connect_opts())

    try do
      assert {:ok, _result} =
               ADBC.execute(
                 conn,
                 """
                 CREATE TABLE measurement_method AS
                 SELECT *
                 FROM (
                   VALUES
                     (1::BIGINT, 'manual'::VARCHAR),
                     (2::BIGINT, 'sensor'::VARCHAR),
                     (3::BIGINT, 'calculated'::VARCHAR)
                 ) AS methods(id, name)
                 """,
                 []
               )

      assert {:ok, columns} =
               ADBC.columns(
                 conn,
                 %RelationRef{connection: :warehouse, schema: "main", name: "measurement_method"},
                 []
               )

      assert Enum.all?(columns, &(&1.metadata.contract_nullability == :unreliable))

      contract =
        Contract.new!(%{
          columns: [
            %{name: :id, type: :integer, null: false},
            %{name: :name, type: :string, null: false}
          ]
        })

      assert %ContractValidation{status: :passed, differences: []} =
               ContractValidation.compare(contract, columns)

      assert {:ok, relation} =
               ADBC.relation(
                 conn,
                 %RelationRef{connection: :warehouse, schema: "main", name: "measurement_method"},
                 []
               )

      assert {:ok, physical} =
               PhysicalFingerprint.new(adapter: ADBC, relation: relation, columns: columns)

      descriptor =
        TargetDescriptor.from_asset(
          %{
            ref: {__MODULE__, :measurement_method},
            type: :sql,
            relation:
              RelationRef.new!(
                connection: :warehouse,
                schema: "main",
                name: "measurement_method"
              ),
            materialization: :table,
            execution_package_hash: String.duplicate("a", 64),
            assurance: %{contract: contract},
            window: nil,
            coverage: nil
          },
          connection_definitions: %{
            warehouse: %{adapter: ADBC, module: __MODULE__}
          },
          manifest_schema_version: Compatibility.current_schema_version(),
          runner_contract_version: Compatibility.current_runner_contract_version()
        )

      assert PhysicalFingerprint.identity_diff(descriptor, physical, contract) == []
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
      assert {:ok, metadata} = ADBC.table_metadata(conn, stable, [])
      assert metadata.relation_instance_id == TargetGenerationRelation.instance_id("token-2")

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

  test "binds an initial marker to the physical table and rejects a replacement" do
    resolved = %Resolved{
      name: :warehouse,
      adapter: ADBC,
      module: __MODULE__,
      config: %{open: [database: ":memory:"]}
    }

    stable = %RelationRef{connection: :warehouse, schema: "mart", name: "orders"}
    generation_id = "11111111-1111-4111-8111-111111111111"
    token = "initial-token"

    assert {:ok, conn} = ADBC.connect(resolved, connect_opts())

    try do
      assert {:ok, _result} = ADBC.execute(conn, "CREATE SCHEMA mart", [])
      create_table(conn, stable, 1)

      assert {:ok, _result} =
               ADBC.execute(
                 conn,
                 ["COMMENT ON TABLE ", qualified(stable), " IS 'customer-owned comment'"],
                 []
               )

      assert {:ok, inspection} = ADBC.inspect_generation(conn, stable, [])

      request = %GenerationMarkerInitialization{
        logical_target_id: "MyApp.Assets.orders",
        stable_relation: stable,
        active_generation_id: generation_id,
        expected_physical_fingerprint: inspection.physical_fingerprint.fingerprint,
        initialization_operation_id: "initial-operation",
        initialization_token: token,
        initialized_at: ~U[2026-07-22 10:00:00Z]
      }

      assert {:ok, initialized} = ADBC.initialize_generation_marker(conn, request, [])
      assert initialized.marker.active_generation_id == generation_id
      assert {:ok, metadata} = ADBC.table_metadata(conn, stable, [])
      assert metadata.relation_instance_id == TargetGenerationRelation.instance_id(token)

      assert {:ok, comment_result} =
               ADBC.query(
                 conn,
                 "SELECT comment FROM duckdb_tables() WHERE schema_name = 'mart' AND table_name = 'orders'",
                 []
               )

      assert [%{"comment" => comment}] = comment_result.rows
      assert String.ends_with?(comment, "\ncustomer-owned comment")

      assert {:ok, _result} = ADBC.execute(conn, ["DROP TABLE ", qualified(stable)], [])
      create_table(conn, stable, 1)
      assert {:ok, replacement_metadata} = ADBC.table_metadata(conn, stable, [])
      assert replacement_metadata.relation_instance_id == nil

      legacy_read = %GenerationReconciliation{
        logical_target_id: request.logical_target_id,
        stable_relation: stable,
        require_relation_instance?: false
      }

      assert {:ok, legacy_marker} = ADBC.reconcile_generation(conn, legacy_read, [])
      assert legacy_marker.active_generation_id == generation_id

      assert {:error,
              %Error{
                details: %{
                  classification: :relation_instance_identity,
                  reason: :relation_instance_mismatch
                }
              }} =
               ADBC.reconcile_generation(
                 conn,
                 %GenerationReconciliation{
                   logical_target_id: request.logical_target_id,
                   stable_relation: stable,
                   require_relation_instance?: true
                 },
                 []
               )

      assert {:error,
              %Error{
                details: %{
                  classification: :relation_instance_identity,
                  reason: :relation_instance_mismatch
                }
              }} = ADBC.initialize_generation_marker(conn, request, [])
    after
      ADBC.disconnect(conn, [])
    end
  end

  test "persists table-bound generation identity through an attached DuckLake lifecycle" do
    root =
      Path.join(
        System.tmp_dir!(),
        "favn_ducklake_relation_identity_#{System.unique_integer([:positive])}"
      )

    data_path = Path.join(root, "data")
    metadata_path = Path.join(root, "metadata.ducklake")
    File.mkdir_p!(data_path)
    on_exit(fn -> File.rm_rf!(root) end)

    resolved = %Resolved{
      name: :warehouse,
      adapter: ADBC,
      module: __MODULE__,
      config: %{open: [database: ":memory:"]}
    }

    stable = %RelationRef{
      connection: :warehouse,
      catalog: "lake",
      schema: "main",
      name: "orders"
    }

    generation_id = "11111111-1111-4111-8111-111111111111"
    candidate = GenerationRelation.candidate(stable, generation_id, 128)
    retired = GenerationRelation.retired(stable, generation_id, 128)

    assert {:ok, conn} = ADBC.connect(resolved, connect_opts())

    try do
      attach_ducklake(conn, metadata_path, data_path)
      create_table(conn, candidate, 1)

      request =
        activation(
          conn,
          stable,
          candidate,
          retired,
          nil,
          generation_id,
          "ducklake-operation",
          "ducklake-token"
        )

      assert {:ok, result} = ADBC.activate_generation(conn, request, [])
      assert result.marker.active_generation_id == generation_id
    after
      ADBC.disconnect(conn, [])
    end

    assert {:ok, reconnected} = ADBC.connect(resolved, connect_opts())

    try do
      attach_ducklake(reconnected, metadata_path)

      assert {:ok, metadata} = ADBC.table_metadata(reconnected, stable, [])

      assert metadata.relation_instance_id ==
               TargetGenerationRelation.instance_id("ducklake-token")

      assert {:ok, marker} =
               ADBC.reconcile_generation(
                 reconnected,
                 %GenerationReconciliation{
                   logical_target_id: "MyApp.Assets.orders",
                   stable_relation: stable,
                   require_relation_instance?: true
                 },
                 []
               )

      assert marker.active_generation_id == generation_id

      assert {:ok, _result} =
               ADBC.execute(reconnected, ["DROP TABLE ", qualified(stable)], [])

      create_table(reconnected, stable, 1)
      assert {:ok, replacement} = ADBC.table_metadata(reconnected, stable, [])
      assert replacement.relation_instance_id == nil

      assert {:error, %Error{details: %{reason: :relation_instance_mismatch}}} =
               ADBC.reconcile_generation(
                 reconnected,
                 %GenerationReconciliation{
                   logical_target_id: "MyApp.Assets.orders",
                   stable_relation: stable,
                   require_relation_instance?: true
                 },
                 []
               )
    after
      ADBC.disconnect(reconnected, [])
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

  defp attach_ducklake(conn, metadata_path, data_path \\ nil) do
    assert {:ok, _result} = ADBC.execute(conn, "INSTALL ducklake", [])
    assert {:ok, _result} = ADBC.execute(conn, "LOAD ducklake", [])

    attach = [
      "ATTACH ",
      quote_literal("ducklake:" <> metadata_path),
      " AS lake"
    ]

    attach =
      if is_binary(data_path),
        do: [attach, " (DATA_PATH ", quote_literal(data_path), ")"],
        else: attach

    assert {:ok, _result} = ADBC.execute(conn, attach, [])
  end

  defp qualified(%RelationRef{catalog: nil} = ref),
    do: [quote_ident(ref.schema), ".", quote_ident(ref.name)]

  defp qualified(%RelationRef{} = ref),
    do: [quote_ident(ref.catalog), ".", quote_ident(ref.schema), ".", quote_ident(ref.name)]

  defp quote_ident(value), do: ["\"", String.replace(value, "\"", "\"\""), "\""]
  defp quote_literal(value), do: ["'", String.replace(value, "'", "''"), "'"]
end
