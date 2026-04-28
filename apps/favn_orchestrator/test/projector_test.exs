defmodule FavnOrchestrator.ProjectorTest do
  use ExUnit.Case, async: true

  alias Favn.Run
  alias Favn.Run.AssetResult
  alias FavnOrchestrator.Projector
  alias FavnOrchestrator.RunState

  test "projects public run with terminal asset results keyed by ref" do
    started_at = DateTime.utc_now()
    finished_at = DateTime.add(started_at, 1, :second)

    run_state =
      RunState.new(
        id: "run_projected",
        manifest_version_id: "mv_projected",
        manifest_content_hash: "hash_projected",
        asset_ref: {MyApp.Assets.Gold, :asset},
        target_refs: [{MyApp.Assets.Raw, :asset}, {MyApp.Assets.Gold, :asset}],
        submit_kind: :pipeline,
        metadata: %{pipeline_submit_ref: MyApp.Pipelines.Daily, pipeline_context: %{id: :daily}}
      )
      |> Map.put(:inserted_at, started_at)
      |> Map.put(:updated_at, finished_at)
      |> RunState.transition(
        status: :ok,
        result: %{
          status: :ok,
          asset_results: [
            %AssetResult{
              ref: {MyApp.Assets.Raw, :asset},
              stage: 0,
              status: :error,
              started_at: started_at,
              finished_at: finished_at,
              duration_ms: 1,
              meta: %{},
              error: :transient,
              attempt_count: 1,
              max_attempts: 2,
              attempts: []
            },
            %AssetResult{
              ref: {MyApp.Assets.Raw, :asset},
              stage: 0,
              status: :ok,
              started_at: started_at,
              finished_at: finished_at,
              duration_ms: 2,
              meta: %{},
              error: nil,
              attempt_count: 2,
              max_attempts: 2,
              attempts: []
            },
            %AssetResult{
              ref: {MyApp.Assets.Gold, :asset},
              stage: 1,
              status: :ok,
              started_at: started_at,
              finished_at: finished_at,
              duration_ms: 2,
              meta: %{},
              error: nil,
              attempt_count: 1,
              max_attempts: 1,
              attempts: []
            }
          ],
          metadata: %{final: true}
        }
      )

    assert %Run{} = run = Projector.project_run(run_state)
    assert run.id == "run_projected"
    assert run.asset_ref == {MyApp.Assets.Gold, :asset}
    assert run.submit_kind == :pipeline
    assert map_size(run.asset_results) == 2
    assert run.asset_results[{MyApp.Assets.Raw, :asset}].status == :ok
    assert run.pipeline[:submit_ref] == MyApp.Pipelines.Daily
    assert run.pipeline_context == %{id: :daily}
  end

  test "projects backfill submit kinds and partial terminal status" do
    run_state =
      RunState.new(
        id: "run_backfill_projected",
        manifest_version_id: "mv_backfill_projected",
        manifest_content_hash: "hash_backfill_projected",
        asset_ref: {MyApp.Assets.Gold, :asset},
        target_refs: [{MyApp.Assets.Gold, :asset}],
        submit_kind: :backfill_pipeline,
        metadata: %{pipeline_submit_ref: MyApp.Pipelines.Daily}
      )
      |> RunState.transition(status: :partial, result: %{status: :partial})

    assert %Run{} = run = Projector.project_run(run_state)
    assert run.status == :partial
    assert run.submit_kind == :backfill_pipeline
    assert run.finished_at == run_state.updated_at
    assert run.terminal_reason == %{status: :partial, error: nil}
    assert run.pipeline[:submit_ref] == MyApp.Pipelines.Daily

    asset_run_state =
      RunState.new(
        id: "run_backfill_asset_projected",
        manifest_version_id: "mv_backfill_projected",
        manifest_content_hash: "hash_backfill_projected",
        asset_ref: {MyApp.Assets.Gold, :asset},
        submit_kind: :backfill_asset
      )

    assert Projector.project_run(asset_run_state).submit_kind == :backfill_asset
  end
end
