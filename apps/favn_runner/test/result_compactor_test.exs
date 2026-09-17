Code.require_file("../../favn_test_support/fixtures/runner_task_persistence.exs", __DIR__)

defmodule FavnRunner.ResultCompactorTest do
  use ExUnit.Case, async: true
  alias Favn.Contracts.RunnerError
  alias Favn.Contracts.RunnerTask.{PersistenceCodec, PersistenceSchema}
  alias FavnRunner.ResultCompactor
  alias FavnTestSupport.RunnerTaskPersistence, as: Fixture

  test "large SQL detail keeps successful completion identity and typed evidence" do
    version = Fixture.version()
    {:asset_attempt, work, result} = hd(Fixture.tasks(version))
    [asset] = result.asset_results
    asset = %{asset | asset_step_id: work.asset_step_id, attempt_count: work.attempt}
    evidence = %{asset.evidence | metrics: %{detail: String.duplicate("x", 800_000)}}
    result = %{result | asset_results: [%{asset | evidence: evidence}]}
    assert :ok = PersistenceSchema.completion(:asset_attempt, work, result, :succeeded)
    {compacted, bytes, true} = ResultCompactor.compact(result, 768 * 1_024)
    assert bytes < 768 * 1_024
    assert :ok = PersistenceSchema.completion(:asset_attempt, work, compacted, :succeeded)
    assert [kept] = compacted.asset_results
    assert kept.ref == asset.ref
    assert kept.evidence.kind == :sql
    assert kept.evidence.metrics == %{}
    assert kept.evidence.runtime_publication == asset.evidence.runtime_publication
    assert {:ok, encoded} = PersistenceCodec.encode_result(:asset_attempt, :succeeded, compacted)
    assert {:ok, _} = PersistenceCodec.decode_result(:asset_attempt, :succeeded, encoded, version)
  end

  test "compaction preserves unknown-write classification and generation identity" do
    {:asset_attempt, _work, result} = hd(Fixture.tasks(Fixture.version()))
    [asset] = result.asset_results
    error = %RunnerError{outcome: :unknown, details: %{detail: String.duplicate("x", 800_000)}}

    asset = %{
      asset
      | status: :error,
        error: error,
        write_outcome: :outcome_unknown,
        target_operation: :normal_materialization,
        logical_target_id: "target",
        target_generation_id: "generation",
        write_relation: asset.evidence.materialized
    }

    {compacted, bytes, true} =
      ResultCompactor.compact(
        %{result | status: :error, error: error, asset_results: [asset]},
        768 * 1_024
      )

    assert bytes < 768 * 1_024
    assert [kept] = compacted.asset_results

    assert Map.take(kept, [
             :target_operation,
             :logical_target_id,
             :target_generation_id,
             :write_relation,
             :write_outcome
           ]) ==
             Map.take(asset, [
               :target_operation,
               :logical_target_id,
               :target_generation_id,
               :write_relation,
               :write_outcome
             ])

    assert kept.error.outcome == :unknown
    assert compacted.error.outcome == :unknown
  end
end
