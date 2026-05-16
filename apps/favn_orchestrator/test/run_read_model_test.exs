defmodule FavnOrchestrator.RunReadModelTest do
  use ExUnit.Case, async: false

  alias Favn.Window.Anchor
  alias Favn.Window.Key, as: WindowKey
  alias Favn.Run.AssetResult
  alias Favn.Run.NodeResult
  alias FavnOrchestrator.Backfill.BackfillWindow
  alias FavnOrchestrator.RunState
  alias FavnOrchestrator.Storage
  alias FavnOrchestrator.Storage.Adapter.Memory

  setup do
    Memory.reset()

    on_exit(fn ->
      Memory.reset()
    end)

    :ok
  end

  test "list_run_summaries classifies public run roles" do
    anchor = anchor(~U[2026-05-01 00:00:00Z])
    parent = run("backfill_parent", submit_kind: :backfill_pipeline)

    child =
      run("backfill_child",
        submit_kind: :pipeline,
        parent_run_id: parent.id,
        root_run_id: parent.id,
        lineage_depth: 1,
        trigger: %{
          kind: :backfill,
          pipeline_module: MyApp.Pipelines.Daily,
          window_key: window_key(anchor)
        },
        metadata: %{pipeline_context: %{anchor_window: anchor}}
      )

    runs = [
      run("asset", submit_kind: :manual),
      run("pipeline", submit_kind: :pipeline),
      parent,
      child,
      run("rerun", submit_kind: :rerun, rerun_of_run_id: "asset")
    ]

    Enum.each(runs, fn run ->
      assert :ok = Storage.put_run(run)
    end)

    assert {:ok, summaries} = FavnOrchestrator.list_run_summaries()

    summaries_by_id = Map.new(summaries, &{&1.id, &1})

    assert summaries_by_id["asset"].kind == :asset
    assert summaries_by_id["pipeline"].kind == :pipeline
    assert summaries_by_id["backfill_parent"].kind == :backfill_parent
    assert summaries_by_id["backfill_parent"].progress_unit == nil
    assert summaries_by_id["backfill_child"].kind == :backfill_child
    assert summaries_by_id["backfill_child"].parent_run_id == parent.id
    assert summaries_by_id["backfill_child"].root_run_id == parent.id
    assert summaries_by_id["backfill_child"].window.key == window_key(anchor)
    assert summaries_by_id["backfill_child"].window.label == "May 1"
    assert summaries_by_id["rerun"].kind == :rerun
    assert summaries_by_id["rerun"].rerun_of_run_id == "asset"
  end

  test "parent backfill summary includes window progress when ledger rows exist" do
    parent = run("backfill_with_progress", submit_kind: :backfill_pipeline)
    ok_anchor = anchor(~U[2026-05-01 00:00:00Z])
    running_anchor = anchor(~U[2026-05-02 00:00:00Z])
    failed_anchor = anchor(~U[2026-05-03 00:00:00Z])

    assert :ok = Storage.put_run(parent)
    assert :ok = Storage.put_backfill_window(backfill_window(parent.id, ok_anchor, :ok))
    assert :ok = Storage.put_backfill_window(backfill_window(parent.id, running_anchor, :running))
    assert :ok = Storage.put_backfill_window(backfill_window(parent.id, failed_anchor, :error))

    assert {:ok, detail} = FavnOrchestrator.get_run_detail(parent.id)

    summary = detail.summary

    assert summary.kind == :backfill_parent
    assert summary.progress_unit == :windows
    assert summary.progress.label == "2/3 windows complete"

    assert summary.progress.counts == %{
             total: 3,
             pending: 0,
             running: 1,
             succeeded: 1,
             failed: 1,
             cancelled: 0,
             timed_out: 0,
             completed: 2
           }
  end

  test "run detail preserves explicit persisted asset step ids" do
    ref = {MyApp.Assets.Gold, :asset}
    started_at = ~U[2026-05-01 00:00:00Z]
    finished_at = DateTime.add(started_at, 1, :second)

    asset_run =
      run("asset_with_persisted_step_id", submit_kind: :manual)
      |> RunState.transition(
        status: :ok,
        result: %{
          asset_results: [
            %AssetResult{
              ref: ref,
              stage: 0,
              status: :ok,
              started_at: started_at,
              finished_at: finished_at,
              duration_ms: 1_000,
              asset_step_id: "persisted-asset-step"
            }
          ]
        }
      )

    node_run =
      run("node_with_persisted_step_id", submit_kind: :pipeline)
      |> RunState.transition(
        status: :ok,
        result: %{
          node_results: [
            NodeResult.new(%{
              node_key: {ref, "window:day:2026-05-01"},
              ref: ref,
              stage: 0,
              status: :ok,
              started_at: started_at,
              finished_at: finished_at,
              duration_ms: 1_000,
              asset_step_id: "persisted-node-step"
            })
          ]
        }
      )

    assert :ok = Storage.put_run(asset_run)
    assert :ok = Storage.put_run(node_run)

    assert {:ok, asset_detail} = FavnOrchestrator.get_run_detail(asset_run.id)
    assert [%{id: "persisted-asset-step"}] = asset_detail.steps

    assert {:ok, node_detail} = FavnOrchestrator.get_run_detail(node_run.id)
    assert [%{id: "persisted-node-step"}] = node_detail.steps
  end

  defp run(run_id, opts) do
    RunState.new(
      id: run_id,
      manifest_version_id: "mv_run_read_model",
      manifest_content_hash: "hash_run_read_model",
      asset_ref: {MyApp.Assets.Gold, :asset},
      target_refs: [{MyApp.Assets.Gold, :asset}],
      submit_kind: Keyword.fetch!(opts, :submit_kind),
      trigger: Keyword.get(opts, :trigger, %{}),
      metadata: Keyword.get(opts, :metadata, %{}),
      parent_run_id: Keyword.get(opts, :parent_run_id),
      root_run_id: Keyword.get(opts, :root_run_id),
      rerun_of_run_id: Keyword.get(opts, :rerun_of_run_id),
      lineage_depth: Keyword.get(opts, :lineage_depth, 0)
    )
  end

  defp anchor(start_at) do
    {:ok, anchor} = Anchor.new(:day, start_at, DateTime.add(start_at, 1, :day))
    anchor
  end

  defp window_key(%Anchor{} = anchor), do: WindowKey.encode(anchor.key)

  defp backfill_window(parent_run_id, %Anchor{} = anchor, status) do
    {:ok, window} =
      BackfillWindow.new(%{
        backfill_run_id: parent_run_id,
        pipeline_module: MyApp.Pipelines.Daily,
        manifest_version_id: "mv_run_read_model",
        window_kind: anchor.kind,
        window_start_at: anchor.start_at,
        window_end_at: anchor.end_at,
        timezone: anchor.timezone,
        window_key: window_key(anchor),
        status: status,
        updated_at: anchor.start_at
      })

    window
  end
end
