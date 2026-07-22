defmodule FavnDuckdb.SQLAdapterDuckDBGenerationTest do
  use ExUnit.Case, async: false

  alias Favn.Connection.Resolved
  alias Favn.RelationRef
  alias Favn.SQL.Adapter.DuckDB

  alias Favn.SQL.{
    Error,
    GenerationActivation,
    GenerationDiscard,
    GenerationMarkerInitialization,
    GenerationReconciliation,
    GenerationRelation,
    GenerationTransaction
  }

  @old_generation "11111111-1111-4111-8111-111111111111"
  @new_generation "22222222-2222-4222-8222-222222222222"

  defmodule LostReplyAdapter do
    alias Favn.SQL.Adapter.DuckDB
    alias Favn.SQL.Error

    defdelegate relation(conn, ref, opts), to: DuckDB
    defdelegate columns(conn, ref, opts), to: DuckDB
    defdelegate execute(conn, statement, opts), to: DuckDB
    defdelegate query(conn, statement, opts), to: DuckDB

    def transaction(conn, fun, opts) do
      case DuckDB.transaction(conn, fun, opts) do
        {:ok, result} ->
          {:error,
           %Error{
             type: :execution_error,
             message: "commit reply lost",
             retryable?: false,
             details: %{transaction_stage: :commit, transaction_body_result: result}
           }}

        {:error, %Error{} = error} ->
          {:error, error}
      end
    end
  end

  setup do
    resolved = %Resolved{
      name: :warehouse,
      adapter: DuckDB,
      module: __MODULE__,
      config: %{open: %{database: ":memory:"}}
    }

    assert {:ok, conn} = DuckDB.connect(resolved, [])
    assert :ok = DuckDB.bootstrap(conn, resolved, [])
    assert {:ok, _result} = DuckDB.execute(conn, "CREATE SCHEMA mart", [])

    on_exit(fn -> DuckDB.disconnect(conn, []) end)
    {:ok, conn: conn, resolved: resolved}
  end

  test "reports explicit rebuild capabilities", %{resolved: resolved} do
    assert {:ok, generation} = DuckDB.generation_capabilities(resolved, [])
    assert Favn.SQL.GenerationCapabilities.rebuild_supported?(generation)
    assert generation.snapshots == :unsupported
  end

  test "activates, reconciles and idempotently replays a target generation", %{conn: conn} do
    stable = relation("orders")
    first_candidate = GenerationRelation.candidate(stable, @old_generation, 128)
    first_retired = GenerationRelation.retired(stable, @old_generation, 128)

    create_table(conn, first_candidate, 1)

    first =
      activation(
        conn,
        stable,
        first_candidate,
        first_retired,
        nil,
        @old_generation,
        "op-1",
        "token-1"
      )

    assert {:ok, first_result} = DuckDB.activate_generation(conn, first, [])
    assert first_result.marker.active_generation_id == @old_generation
    assert rows(conn, stable) == [%{"id" => 1}]

    second_candidate = GenerationRelation.candidate(stable, @new_generation, 128)
    second_retired = GenerationRelation.retired(stable, @old_generation, 128)
    create_table(conn, second_candidate, 2)

    second =
      activation(
        conn,
        stable,
        second_candidate,
        second_retired,
        @old_generation,
        @new_generation,
        "op-2",
        "token-2"
      )
      |> Map.put(:expected_active_marker, first_result.marker)

    assert {:ok, second_result} = DuckDB.activate_generation(conn, second, [])
    assert second_result.marker.activation_operation_id == "op-2"
    assert second_result.marker.activation_token == "token-2"
    assert rows(conn, stable) == [%{"id" => 2}]
    assert rows(conn, second_retired) == [%{"id" => 1}]

    reconciliation = %GenerationReconciliation{
      logical_target_id: "MyApp.Assets.orders",
      stable_relation: stable
    }

    assert {:ok, marker} = DuckDB.reconcile_generation(conn, reconciliation, [])
    assert marker.active_generation_id == @new_generation
    assert marker.activation_operation_id == "op-2"
    assert marker.activation_token == "token-2"

    replay = %{second | activated_at: DateTime.add(second.activated_at, 1, :hour)}
    assert {:ok, replayed} = DuckDB.activate_generation(conn, replay, [])

    assert %{replayed.marker | activated_at: nil} ==
             %{second_result.marker | activated_at: nil}

    assert DateTime.compare(replayed.marker.activated_at, second_result.marker.activated_at) ==
             :eq

    assert rows(conn, stable) == [%{"id" => 2}]
  end

  test "marker and candidate fingerprint mismatches roll back before relation changes", %{
    conn: conn
  } do
    stable = relation("payments")
    initial_candidate = GenerationRelation.candidate(stable, @old_generation, 128)
    initial_retired = GenerationRelation.retired(stable, @old_generation, 128)
    create_table(conn, initial_candidate, 10)

    initial =
      activation(
        conn,
        stable,
        initial_candidate,
        initial_retired,
        nil,
        @old_generation,
        "payments-op-1",
        "payments-token-1"
      )

    assert {:ok, initial_result} = DuckDB.activate_generation(conn, initial, [])

    candidate = GenerationRelation.candidate(stable, @new_generation, 128)
    retired = GenerationRelation.retired(stable, @old_generation, 128)
    create_table(conn, candidate, 20)

    wrong_marker =
      activation(
        conn,
        stable,
        candidate,
        retired,
        @old_generation,
        @new_generation,
        "payments-op-2",
        "payments-token-2"
      )
      |> Map.put(
        :expected_active_marker,
        %{initial_result.marker | activation_token: "unexpected-previous-token"}
      )

    assert {:error, %Error{details: %{classification: :generation_marker_mismatch}}} =
             DuckDB.activate_generation(conn, wrong_marker, [])

    assert rows(conn, stable) == [%{"id" => 10}]
    assert rows(conn, candidate) == [%{"id" => 20}]

    wrong_fingerprint = %{
      wrong_marker
      | expected_active_generation_id: @old_generation,
        expected_active_marker: initial_result.marker,
        expected_candidate_fingerprint: String.duplicate("0", 64)
    }

    assert {:error, %Error{details: %{classification: :candidate_fingerprint_mismatch}}} =
             DuckDB.activate_generation(conn, wrong_fingerprint, [])

    assert rows(conn, stable) == [%{"id" => 10}]
    assert rows(conn, candidate) == [%{"id" => 20}]
    assert {:ok, nil} = DuckDB.relation(conn, retired, [])
  end

  test "initializes an exact marker for an ordinary first generation", %{conn: conn} do
    stable = relation("initial_orders")
    create_table(conn, stable, 42)
    assert {:ok, inspection} = DuckDB.inspect_generation(conn, stable, [])

    initialization = %GenerationMarkerInitialization{
      logical_target_id: "MyApp.Assets.initial_orders",
      stable_relation: stable,
      active_generation_id: @old_generation,
      expected_physical_fingerprint: inspection.physical_fingerprint.fingerprint,
      initialization_operation_id: "initial-orders-materialization",
      initialization_token: "initial-orders-token",
      initialized_at: ~U[2026-07-22 09:00:00Z]
    }

    assert {:ok, initialized} = DuckDB.initialize_generation_marker(conn, initialization, [])
    assert initialized.marker.active_generation_id == @old_generation

    replay = %{
      initialization
      | initialized_at: DateTime.add(initialization.initialized_at, 1, :hour)
    }

    assert {:ok, replayed} = DuckDB.initialize_generation_marker(conn, replay, [])

    assert %{replayed.marker | activated_at: nil} ==
             %{initialized.marker | activated_at: nil}

    assert DateTime.compare(replayed.marker.activated_at, initialized.marker.activated_at) == :eq

    assert {:ok, observed_marker} =
             DuckDB.reconcile_generation(
               conn,
               %GenerationReconciliation{
                 logical_target_id: initialization.logical_target_id,
                 stable_relation: stable
               },
               []
             )

    assert %{observed_marker | activated_at: nil} == %{initialized.marker | activated_at: nil}
    assert DateTime.compare(observed_marker.activated_at, initialized.marker.activated_at) == :eq
  end

  test "a lost reply after commit reconciles the candidate as active", %{conn: conn} do
    stable = relation("lost_reply_orders")
    candidate = GenerationRelation.candidate(stable, @new_generation, 128)
    retired = GenerationRelation.retired(stable, @old_generation, 128)
    create_table(conn, candidate, 99)

    request =
      activation(
        conn,
        stable,
        candidate,
        retired,
        nil,
        @new_generation,
        "lost-reply-operation",
        "lost-reply-token"
      )

    assert {:error,
            %Error{
              details: %{classification: :activation_outcome_unknown, unknown_outcome?: true}
            }} =
             GenerationTransaction.activate(LostReplyAdapter, conn, DuckDB, request, [])

    assert rows(conn, stable) == [%{"id" => 99}]
    assert {:ok, :not_found} = DuckDB.inspect_generation(conn, candidate, [])

    assert {:ok, marker} =
             DuckDB.reconcile_generation(
               conn,
               %GenerationReconciliation{
                 logical_target_id: request.logical_target_id,
                 stable_relation: stable
               },
               []
             )

    assert marker.active_generation_id == @new_generation
    assert marker.activation_token == request.activation_token
  end

  test "discard refuses the active generation and is idempotent for an absent candidate", %{
    conn: conn
  } do
    stable = relation("customers")
    candidate = GenerationRelation.candidate(stable, @old_generation, 128)
    retired = GenerationRelation.retired(stable, @old_generation, 128)
    create_table(conn, candidate, 7)

    request =
      activation(
        conn,
        stable,
        candidate,
        retired,
        nil,
        @old_generation,
        "customers-op-1",
        "customers-token-1"
      )

    assert {:ok, _result} = DuckDB.activate_generation(conn, request, [])

    active_discard = %GenerationDiscard{
      logical_target_id: request.logical_target_id,
      stable_relation: stable,
      candidate_generation_id: @old_generation,
      candidate_relation: candidate
    }

    assert {:error, %Error{details: %{classification: :active_generation_discard_forbidden}}} =
             DuckDB.discard_generation(conn, active_discard, [])

    assert rows(conn, stable) == [%{"id" => 7}]

    absent = %GenerationDiscard{
      active_discard
      | candidate_generation_id: @new_generation,
        candidate_relation: GenerationRelation.candidate(stable, @new_generation, 128)
    }

    assert :ok = DuckDB.discard_generation(conn, absent, [])
    assert :ok = DuckDB.discard_generation(conn, absent, [])
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
    assert {:ok, inspection} = DuckDB.inspect_generation(conn, candidate, [])

    %GenerationActivation{
      logical_target_id: "MyApp.Assets.#{stable.name}",
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
             DuckDB.execute(
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
    assert {:ok, result} = DuckDB.query(conn, ["SELECT id FROM ", qualified(relation)], [])
    result.rows
  end

  defp relation(name), do: %RelationRef{connection: :warehouse, schema: "mart", name: name}
  defp qualified(ref), do: [quote_ident(ref.schema), ".", quote_ident(ref.name)]
  defp quote_ident(value), do: ["\"", String.replace(value, "\"", "\"\""), "\""]
end
