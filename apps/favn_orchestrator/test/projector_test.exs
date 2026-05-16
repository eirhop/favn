defmodule FavnOrchestrator.ProjectorTest do
  use ExUnit.Case, async: true

  alias Favn.Run
  alias Favn.Run.AssetResult
  alias Favn.Run.NodeResult
  alias FavnOrchestrator.AssetStepIdentity
  alias FavnOrchestrator.Projector
  alias FavnOrchestrator.RunEvent
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

    raw_ref = {MyApp.Assets.Raw, :asset}

    assert run.asset_results[{MyApp.Assets.Raw, :asset}].asset_step_id ==
             AssetStepIdentity.asset_step_id(run.id, {raw_ref, nil}, raw_ref)

    assert run.node_results[{{MyApp.Assets.Raw, :asset}, nil}].status == :ok

    assert run.node_results[{{MyApp.Assets.Raw, :asset}, nil}].asset_step_id ==
             run.asset_results[{MyApp.Assets.Raw, :asset}].asset_step_id

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

  test "projects explicit node results before asset result fallback" do
    asset_result = %AssetResult{
      ref: {MyApp.Assets.Raw, :asset},
      stage: 0,
      status: :ok,
      started_at: DateTime.utc_now(),
      finished_at: DateTime.utc_now(),
      duration_ms: 1,
      meta: %{},
      error: nil,
      attempt_count: 1,
      max_attempts: 1,
      attempts: []
    }

    node_key = {{MyApp.Assets.Raw, :asset}, %{window: "2026-05-08"}}

    node_result = %NodeResult{
      node_key: node_key,
      ref: {MyApp.Assets.Raw, :asset},
      stage: 0,
      status: :skipped_fresh,
      freshness_key: "raw:2026-05-08"
    }

    run_state =
      RunState.new(
        id: "run_explicit_node_results",
        manifest_version_id: "mv_explicit_node_results",
        manifest_content_hash: "hash_explicit_node_results",
        asset_ref: {MyApp.Assets.Raw, :asset}
      )
      |> RunState.transition(
        status: :ok,
        result: %{asset_results: [asset_result], node_results: [node_result]}
      )

    assert %Run{} = run = Projector.project_run(run_state)

    expected_asset_step_id =
      AssetStepIdentity.asset_step_id(run_state.id, node_key, {MyApp.Assets.Raw, :asset})

    assert run.asset_results[{MyApp.Assets.Raw, :asset}].status == asset_result.status
    assert run.node_results[node_key].status == node_result.status
    assert run.node_results[node_key].asset_step_id == expected_asset_step_id
    refute Map.has_key?(run.node_results, {{MyApp.Assets.Raw, :asset}, nil})
  end

  test "canonical asset step identity is stable and projected into step events" do
    node_key = {{MyApp.Assets.Raw, :asset}, %{window: "2026-05-08"}}
    ref = {MyApp.Assets.Raw, :asset}

    expected_id = Base.url_encode64(:erlang.term_to_binary(node_key), padding: false)

    assert AssetStepIdentity.asset_step_id("run_a", node_key, ref) == expected_id
    assert AssetStepIdentity.asset_step_id("run_b", node_key, ref) == expected_id

    run_state =
      RunState.new(
        id: "run_step_event_identity",
        manifest_version_id: "mv_step_event_identity",
        manifest_content_hash: "hash_step_event_identity",
        asset_ref: ref
      )

    assert %RunEvent{} =
             event =
             Projector.run_event(run_state, :step_started, %{
               asset_ref: ref,
               node_key: node_key,
               stage: 0
             })

    assert event.entity == :step
    assert event.asset_ref == ref
    assert event.data.asset_step_id == expected_id
  end
end
