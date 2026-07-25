defmodule FavnOrchestrator.RebuildDispatcherTest do
  use ExUnit.Case, async: true

  alias Favn.Contracts.RunnerWork
  alias Favn.Manifest.ExecutionPackage
  alias FavnOrchestrator.MaterializationClaims
  alias FavnOrchestrator.RebuildDispatcher

  describe "candidate_item_busy?/2" do
    test "allows an expired item to be reclaimed after a dispatcher restart" do
      now = ~U[2026-07-25 16:00:00Z]

      refute RebuildDispatcher.candidate_item_busy?(
               [%{claim_expires_at: DateTime.add(now, -1, :second)}],
               now
             )
    end

    test "keeps a live item serial" do
      now = ~U[2026-07-25 16:00:00Z]

      assert RebuildDispatcher.candidate_item_busy?(
               [%{claim_expires_at: DateTime.add(now, 1, :second)}],
               now
             )
    end

    test "treats a malformed active item as busy" do
      assert RebuildDispatcher.candidate_item_busy?(
               [%{claim_expires_at: nil}],
               DateTime.utc_now()
             )
    end
  end

  describe "contract_required?/1" do
    test "accepts persisted and in-memory booleans" do
      assert RebuildDispatcher.contract_required?(true)
      assert RebuildDispatcher.contract_required?("true")
      refute RebuildDispatcher.contract_required?(false)
      refute RebuildDispatcher.contract_required?("false")
    end
  end

  describe "table_relation_kind?/1" do
    test "accepts runner and persisted forms" do
      assert RebuildDispatcher.table_relation_kind?(:table)
      assert RebuildDispatcher.table_relation_kind?("table")
      refute RebuildDispatcher.table_relation_kind?(:view)
    end
  end

  describe "candidate_relation_matches?/2" do
    test "allows DuckDB to resolve an unspecified current catalog" do
      candidate = %{catalog: nil, schema: "mart", name: "account_health_candidate"}

      observed = %{
        catalog: "generic_crm",
        schema: "mart",
        name: "account_health_candidate",
        kind: :table
      }

      assert RebuildDispatcher.candidate_relation_matches?(candidate, observed)

      refute RebuildDispatcher.candidate_relation_matches?(candidate, %{observed | schema: "core"})

      refute RebuildDispatcher.candidate_relation_matches?(candidate, %{observed | kind: :view})
    end

    test "keeps an explicitly authored catalog exact" do
      candidate = %{catalog: "expected", schema: "mart", name: "candidate"}
      observed = %{catalog: "other", schema: "mart", name: "candidate", kind: "table"}

      refute RebuildDispatcher.candidate_relation_matches?(candidate, observed)
    end
  end

  describe "materialization claim enrichment" do
    test "freezes the attached execution package into later evidence" do
      package_hash = String.duplicate("a", 64)

      work = %RunnerWork{
        execution_package: struct(ExecutionPackage, content_hash: package_hash),
        metadata: %{runtime_input_lineage: %{mode: :fresh}}
      }

      assert %{
               execution_package_hash: ^package_hash,
               runtime_input_lineage: %{mode: :fresh}
             } = MaterializationClaims.enrich(%{claim_key: "claim"}, work)
    end
  end
end
