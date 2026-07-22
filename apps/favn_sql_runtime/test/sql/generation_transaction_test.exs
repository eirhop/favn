defmodule Favn.SQL.GenerationTransactionTest do
  use ExUnit.Case, async: true

  alias Favn.RelationRef
  alias Favn.SQL.{Error, GenerationActivation, GenerationMarker, GenerationTransaction}

  defmodule OutcomeAdapter do
    def transaction(_conn, _fun, _opts) do
      case Process.get({__MODULE__, :outcome}) do
        :begin_failure ->
          {:error,
           %Error{
             type: :execution_error,
             message: "begin failed",
             retryable?: false,
             details: %{classification: :execution, transaction_stage: :begin}
           }}

        :body_failure ->
          {:error,
           %Error{
             type: :execution_error,
             message: "body failed",
             retryable?: false,
             details: %{classification: :execution, transaction_stage: :body}
           }}

        :lost_commit_reply ->
          {:error,
           %Error{
             type: :execution_error,
             message: "commit reply lost",
             retryable?: false,
             details: %{
               classification: :execution,
               transaction_stage: :commit,
               transaction_body_result: %{candidate_marker: "observed-before-commit"}
             }
           }}

        :rollback_after_commit_failure ->
          {:error,
           %Error{
             type: :execution_error,
             message: "rollback failed",
             retryable?: false,
             details: %{
               classification: :execution,
               transaction_stage: :rollback,
               original_error: %{
                 details: %{
                   transaction_stage: :commit,
                   transaction_body_result: %{candidate_marker: "observed-before-commit"}
                 }
               }
             }
           }}

        :rollback_after_body_failure ->
          {:error,
           %Error{
             type: :execution_error,
             message: "rollback failed",
             retryable?: false,
             details: %{
               classification: :execution,
               transaction_stage: :rollback,
               original_error: %{details: %{transaction_stage: :body}}
             }
           }}
      end
    end
  end

  setup do
    on_exit(fn -> Process.delete({OutcomeAdapter, :outcome}) end)
    :ok
  end

  test "pre-commit failures remain safe failures" do
    Process.put({OutcomeAdapter, :outcome}, :begin_failure)

    assert {:error,
            %Error{
              operation: :activate_generation,
              retryable?: false,
              details: %{classification: :execution, transaction_stage: :begin}
            }} = activate()

    Process.put({OutcomeAdapter, :outcome}, :body_failure)

    assert {:error,
            %Error{
              details: %{classification: :execution, transaction_stage: :body}
            }} = activate()
  end

  test "a lost commit reply is an explicit unknown outcome and is never retryable" do
    Process.put({OutcomeAdapter, :outcome}, :lost_commit_reply)

    assert {:error,
            %Error{
              operation: :activate_generation,
              retryable?: false,
              details: %{
                classification: :activation_outcome_unknown,
                unknown_outcome?: true,
                transaction_stage: :commit
              }
            }} = activate()
  end

  test "rollback failure does not hide an earlier unknown commit outcome" do
    Process.put({OutcomeAdapter, :outcome}, :rollback_after_commit_failure)

    assert {:error,
            %Error{
              retryable?: false,
              details: %{
                classification: :activation_outcome_unknown,
                unknown_outcome?: true,
                transaction_stage: :rollback
              }
            }} = activate()
  end

  test "rollback failure after a safe body error is still an unknown transaction outcome" do
    Process.put({OutcomeAdapter, :outcome}, :rollback_after_body_failure)

    assert {:error,
            %Error{
              retryable?: false,
              details: %{
                classification: :activation_outcome_unknown,
                unknown_outcome?: true,
                transaction_stage: :rollback
              }
            }} = activate()
  end

  defp activate do
    GenerationTransaction.activate(
      OutcomeAdapter,
      :conn,
      OutcomeAdapter,
      activation(),
      []
    )
  end

  defp activation do
    %GenerationActivation{
      logical_target_id: "MyApp.Assets.orders",
      stable_relation: %RelationRef{connection: :warehouse, schema: "mart", name: "orders"},
      candidate_relation: %RelationRef{
        connection: :warehouse,
        schema: "mart",
        name: "orders__candidate"
      },
      retired_relation: %RelationRef{
        connection: :warehouse,
        schema: "mart",
        name: "orders__retired"
      },
      expected_candidate_fingerprint: String.duplicate("a", 64),
      expected_active_generation_id: "11111111-1111-4111-8111-111111111111",
      expected_active_marker: %GenerationMarker{
        logical_target_id: "MyApp.Assets.orders",
        active_relation: %RelationRef{connection: :warehouse, schema: "mart", name: "orders"},
        active_generation_id: "11111111-1111-4111-8111-111111111111",
        activation_operation_id: "previous-operation",
        activation_token: "previous-token",
        activated_at: ~U[2026-07-22 09:00:00Z]
      },
      candidate_generation_id: "22222222-2222-4222-8222-222222222222",
      activation_operation_id: "rebuild-operation-1",
      activation_token: "activation-token",
      activated_at: ~U[2026-07-22 10:00:00Z]
    }
  end
end
