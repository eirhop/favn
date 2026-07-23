defmodule Favn.SQL.GenerationContractTest do
  use ExUnit.Case, async: true

  alias Favn.RelationRef
  alias Favn.Connection.Resolved

  alias Favn.SQL.{
    Client,
    Error,
    GenerationActivation,
    GenerationCapabilities,
    GenerationDiscard,
    GenerationReconciliation,
    GenerationRelation,
    Session
  }

  defmodule GenerationAdapter do
    def generation_capabilities(_resolved, _opts) do
      {:ok,
       %GenerationCapabilities{
         transactional_ddl: :supported,
         isolated_candidates: :supported,
         physical_inspection: :supported,
         atomic_swap: :supported,
         marker_reconciliation: :supported,
         idempotent_discard: :supported
       }}
    end

    def inspect_generation(_conn, _relation, _opts), do: {:ok, :not_found}

    def initialize_generation_marker(_conn, _request, _opts) do
      {:error,
       %Error{
         type: :execution_error,
         message: "initialization outcome unknown",
         retryable?: false,
         details: %{classification: :generation_mutation_outcome_unknown, unknown_outcome?: true}
       }}
    end

    def activate_generation(_conn, _request, _opts) do
      {:error,
       %Error{
         type: :execution_error,
         message: "activation outcome unknown",
         retryable?: false,
         details: %{classification: :activation_outcome_unknown, unknown_outcome?: true}
       }}
    end

    def reconcile_generation(_conn, _request, _opts), do: {:ok, nil}
    def discard_generation(_conn, _request, _opts), do: :ok
  end

  @generation_id "11111111-1111-4111-8111-111111111111"

  test "rebuild support requires every safety capability explicitly" do
    capabilities = %GenerationCapabilities{
      transactional_ddl: :supported,
      isolated_candidates: :supported,
      physical_inspection: :supported,
      atomic_swap: :supported,
      marker_reconciliation: :supported,
      idempotent_discard: :supported,
      snapshots: :unsupported
    }

    assert GenerationCapabilities.rebuild_supported?(capabilities)
    assert GenerationCapabilities.missing_for_rebuild(capabilities) == []

    capabilities = %{capabilities | marker_reconciliation: :unsupported}

    refute GenerationCapabilities.rebuild_supported?(capabilities)
    assert GenerationCapabilities.missing_for_rebuild(capabilities) == [:marker_reconciliation]
  end

  test "candidate and retired names are deterministic and namespace preserving" do
    logical = %RelationRef{
      connection: :warehouse,
      catalog: "lake",
      schema: "mart",
      name: "orders"
    }

    assert GenerationRelation.candidate(logical, @generation_id, 128) ==
             GenerationRelation.candidate(logical, @generation_id, 128)

    candidate = GenerationRelation.candidate(logical, @generation_id, 128)
    retired = GenerationRelation.retired(logical, @generation_id, 128)

    assert {candidate.connection, candidate.catalog, candidate.schema} ==
             {logical.connection, logical.catalog, logical.schema}

    assert candidate.name == "orders__favn_candidate_11111111111141118111111111111111"
    assert retired.name == "orders__favn_retired_11111111111141118111111111111111"
    refute candidate.name == retired.name
  end

  test "long generation names stay inside the adapter byte limit with a stable hash" do
    logical = %RelationRef{name: String.duplicate("måned", 40)}
    other_generation = "22222222-2222-4222-8222-222222222222"

    candidate = GenerationRelation.candidate(logical, @generation_id, 64)
    same = GenerationRelation.candidate(logical, @generation_id, 64)
    other = GenerationRelation.candidate(logical, other_generation, 64)

    assert candidate == same
    assert byte_size(candidate.name) <= 64
    assert String.valid?(candidate.name)
    refute candidate.name == other.name
  end

  test "marker relation is fixed inside the target namespace" do
    logical = %RelationRef{
      connection: :warehouse,
      catalog: "lake",
      schema: "mart",
      name: "orders"
    }

    assert GenerationRelation.marker(logical) == %RelationRef{
             connection: :warehouse,
             catalog: "lake",
             schema: "mart",
             name: "__favn_target_generation_markers"
           }
  end

  test "SQL client keeps generation operations on its owned session boundary" do
    session = %Session{
      adapter: GenerationAdapter,
      resolved: %Resolved{
        name: :warehouse,
        adapter: GenerationAdapter,
        module: __MODULE__,
        config: %{}
      },
      conn: :owned_conn,
      capabilities: %Favn.SQL.Capabilities{}
    }

    stable = %RelationRef{connection: :warehouse, schema: "mart", name: "orders"}
    candidate = %{stable | name: "orders__candidate"}
    retired = %{stable | name: "orders__retired"}

    activation = %GenerationActivation{
      logical_target_id: "MyApp.Assets.orders",
      stable_relation: stable,
      candidate_relation: candidate,
      retired_relation: retired,
      expected_candidate_fingerprint: String.duplicate("a", 64),
      expected_active_generation_id: @generation_id,
      candidate_generation_id: "22222222-2222-4222-8222-222222222222",
      activation_operation_id: "operation-1",
      activation_token: "token-1",
      activated_at: ~U[2026-07-22 10:00:00Z]
    }

    reconciliation = %GenerationReconciliation{
      logical_target_id: activation.logical_target_id,
      stable_relation: stable
    }

    discard = %GenerationDiscard{
      logical_target_id: activation.logical_target_id,
      stable_relation: stable,
      candidate_generation_id: activation.candidate_generation_id,
      candidate_relation: candidate
    }

    assert {:ok, capabilities} = Client.generation_capabilities(session)
    assert GenerationCapabilities.rebuild_supported?(capabilities)
    assert {:ok, :not_found} = Client.inspect_generation(session, candidate)

    assert {:error,
            %Error{
              retryable?: false,
              details: %{
                classification: :activation_outcome_unknown,
                unknown_outcome?: true
              }
            }} = Client.activate_generation(session, activation)

    assert {:ok, nil} = Client.reconcile_generation(session, reconciliation)
    assert {:ok, :discarded} = Client.discard_generation(session, discard)
  end
end
