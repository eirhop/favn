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

  test "run detail deduplicates Elixir-prefixed public refs" do
    active_run =
      run("elixir_prefixed_ref", submit_kind: :pipeline)
      |> RunState.transition(
        status: :running,
        result: %{
          asset_results: [
            %{
              ref: "Elixir.MyApp.Assets.Gold.asset",
              status: :running,
              asset_step_id: "live-prefixed-step"
            }
          ]
        }
      )

    assert :ok = Storage.put_run(active_run)

    assert {:ok, detail} = FavnOrchestrator.get_run_detail(active_run.id)
    assert [%{asset_ref: "MyApp.Assets.Gold.asset", id: "live-prefixed-step"}] = detail.steps
  end

  test "run detail marks post-root in-flight failures as cascade failures" do
    root_ref = {MyApp.Assets.Gold, :asset}
    cascade_ref = {MyApp.Assets.Silver, :asset}

    run =
      run("cascade_failures", submit_kind: :pipeline)
      |> Map.put(:target_refs, [root_ref, cascade_ref])

    assert :ok = Storage.put_run(run)

    events = [
      %{
        run_id: run.id,
        sequence: 1,
        event_type: :step_started,
        occurred_at: DateTime.add(DateTime.utc_now(), -4, :second),
        status: :running,
        data: %{
          asset_ref: root_ref,
          asset_step_id: "root-step",
          runner_execution_id: "rx_root",
          stage: 0,
          attempt: 1
        }
      },
      %{
        run_id: run.id,
        sequence: 2,
        event_type: :step_started,
        occurred_at: DateTime.add(DateTime.utc_now(), -4, :second),
        status: :running,
        data: %{
          asset_ref: cascade_ref,
          asset_step_id: "cascade-step",
          runner_execution_id: "rx_cascade",
          stage: 0,
          attempt: 1
        }
      },
      %{
        run_id: run.id,
        sequence: 3,
        event_type: :step_failed,
        occurred_at: DateTime.add(DateTime.utc_now(), -3, :second),
        status: :error,
        data: %{
          asset_ref: root_ref,
          asset_step_id: "root-step",
          error: %{message: "Postgres pool exhausted"},
          stage: 0,
          attempt: 1
        }
      },
      %{
        run_id: run.id,
        sequence: 4,
        event_type: :stage_draining_after_failure,
        occurred_at: DateTime.add(DateTime.utc_now(), -2, :second),
        status: :error,
        data: %{
          failed_asset_ref: root_ref,
          pending_execution_ids: ["rx_cascade"],
          stage: 0,
          attempt: 1
        }
      },
      %{
        run_id: run.id,
        sequence: 5,
        event_type: :step_failed,
        occurred_at: DateTime.add(DateTime.utc_now(), -1, :second),
        status: :error,
        data: %{
          asset_ref: cascade_ref,
          asset_step_id: "cascade-step",
          error: %{message: "query failed after pool exhaustion"},
          stage: 0,
          attempt: 1
        }
      }
    ]

    Enum.each(events, fn event ->
      assert :ok = Storage.append_run_event(run.id, event)
    end)

    assert {:ok, detail} = FavnOrchestrator.get_run_detail(run.id)
    steps_by_id = Map.new(detail.steps, &{&1.id, &1})

    assert steps_by_id["root-step"].failure_role == :primary
    assert steps_by_id["cascade-step"].failure_role == :cascade
    assert steps_by_id["cascade-step"].root_failure_asset_ref == "MyApp.Assets.Gold.asset"
    assert steps_by_id["cascade-step"].explanation =~ "draining in-flight work"
  end

  test "run detail ignores string run event types when building step summaries" do
    run =
      run("string_run_event_type", submit_kind: :manual)
      |> RunState.transition(status: :ok)

    assert :ok = Storage.put_run(run)

    assert :ok =
             Storage.append_run_event(run.id, %{
               sequence: 1,
               event_type: "run_started",
               occurred_at: ~U[2026-05-01 00:00:00Z],
               status: "running"
             })

    assert {:ok, detail} = FavnOrchestrator.get_run_detail(run.id)

    assert detail.steps == []
    assert [%{event_type: "run_started"}] = detail.events
  end

  test "run detail builds step summaries from string step event types" do
    run = run("string_step_event_type", submit_kind: :manual)
    started_at = ~U[2026-05-01 00:00:00Z]
    failed_at = DateTime.add(started_at, 1, :second)

    assert :ok = Storage.put_run(run)

    assert :ok =
             Storage.append_run_event(run.id, %{
               sequence: 1,
               event_type: "step_started",
               occurred_at: started_at,
               status: "running",
               asset_ref: {MyApp.Assets.Gold, :asset},
               stage: 0,
               data: %{asset_step_id: "string-step"}
             })

    assert :ok =
             Storage.append_run_event(run.id, %{
               sequence: 2,
               event_type: "step_failed",
               occurred_at: failed_at,
               status: "error",
               asset_ref: {MyApp.Assets.Gold, :asset},
               stage: 0,
               data: %{asset_step_id: "string-step", error: "boom"}
             })

    assert {:ok, detail} = FavnOrchestrator.get_run_detail(run.id)

    assert [step] = detail.steps
    assert step.id == "string-step"
    assert step.status == :error
    assert step.stage == 0
    assert step.started_at == started_at
    assert step.error == "boom"
    assert step.explanation == "Failed while executing this asset."
  end

  test "pipeline success remains active while persisted step results are incomplete" do
    gold_ref = {MyApp.Assets.Gold, :asset}
    silver_ref = {MyApp.Assets.Silver, :asset}
    started_at = ~U[2026-05-01 00:00:00Z]

    run =
      run("pipeline_success_gap", submit_kind: :pipeline)
      |> Map.put(:target_refs, [gold_ref, silver_ref])
      |> RunState.transition(
        status: :ok,
        result: %{
          node_results: [
            NodeResult.new(%{
              node_key: {gold_ref, nil},
              ref: gold_ref,
              stage: 0,
              status: :ok,
              started_at: started_at,
              finished_at: DateTime.add(started_at, 1, :second),
              duration_ms: 1_000,
              asset_step_id: "gold-step"
            })
          ]
        }
      )

    assert :ok = Storage.put_run(run)

    assert :ok =
             Storage.append_run_event(run.id, %{
               run_id: run.id,
               sequence: run.event_seq + 1,
               event_type: :step_started,
               occurred_at: DateTime.add(started_at, 2, :second),
               status: :running,
               asset_ref: silver_ref,
               stage: 0,
               data: %{asset_step_id: "silver-step"}
             })

    assert {:ok, detail} = FavnOrchestrator.get_run_detail(run.id)

    assert detail.summary.status == :running
    assert detail.summary.finished_at == nil

    steps_by_ref = Map.new(detail.steps, &{&1.asset_ref, &1})
    assert steps_by_ref["MyApp.Assets.Gold.asset"].status == :ok
    assert steps_by_ref["MyApp.Assets.Silver.asset"].status == :running
  end

  test "failed pipeline keeps failure status while filling incomplete step rows" do
    gold_ref = {MyApp.Assets.Gold, :asset}
    silver_ref = {MyApp.Assets.Silver, :asset}
    bronze_ref = {MyApp.Assets.Bronze, :asset}
    started_at = ~U[2026-05-01 00:00:00Z]

    run =
      run("pipeline_failed_gap", submit_kind: :pipeline)
      |> Map.put(:target_refs, [bronze_ref])
      |> Map.put(:plan, dependency_plan([gold_ref, silver_ref, bronze_ref]))
      |> RunState.transition(
        status: :error,
        error: :failed,
        result: %{
          node_results: [
            NodeResult.new(%{
              node_key: {gold_ref, nil},
              ref: gold_ref,
              stage: 0,
              status: :error,
              started_at: started_at,
              finished_at: DateTime.add(started_at, 1, :second),
              duration_ms: 1_000,
              error: %{message: "warehouse unavailable"},
              asset_step_id: "gold-step"
            })
          ]
        }
      )

    assert :ok = Storage.put_run(run)

    assert :ok =
             Storage.append_run_event(run.id, %{
               run_id: run.id,
               sequence: run.event_seq + 1,
               event_type: :step_started,
               occurred_at: DateTime.add(started_at, 2, :second),
               status: :running,
               asset_ref: silver_ref,
               stage: 0,
               data: %{asset_step_id: "silver-step"}
             })

    assert {:ok, detail} = FavnOrchestrator.get_run_detail(run.id)

    assert detail.summary.status == :error

    steps_by_ref = Map.new(detail.steps, &{&1.asset_ref, &1})
    assert steps_by_ref["MyApp.Assets.Gold.asset"].status == :error
    assert steps_by_ref["MyApp.Assets.Silver.asset"].status == :running
    assert steps_by_ref["MyApp.Assets.Bronze.asset"].status == :pending
  end

  test "backfill parent detail includes failed child window context" do
    parent =
      run("backfill_parent_failure", submit_kind: :backfill_pipeline)
      |> RunState.transition(status: :error, error: :failed, result: %{status: :error})

    failed_ref = {MyApp.Assets.Inventory, :inventory_by_day}
    started_at = ~U[2026-05-01 00:00:00Z]
    finished_at = DateTime.add(started_at, 2, :second)
    anchor = anchor(started_at)

    child =
      run("backfill_child_failure",
        submit_kind: :pipeline,
        parent_run_id: parent.id,
        root_run_id: parent.id,
        lineage_depth: 1,
        trigger: %{
          kind: :backfill,
          backfill_run_id: parent.id,
          pipeline_module: MyApp.Pipelines.Daily,
          window_key: window_key(anchor)
        },
        metadata: %{pipeline_submit_ref: MyApp.Pipelines.Daily}
      )
      |> Map.put(:target_refs, [failed_ref])
      |> RunState.transition(
        status: :error,
        error: :failed,
        result: %{
          node_results: [
            NodeResult.new(%{
              node_key: {failed_ref, window_key(anchor)},
              ref: failed_ref,
              stage: 0,
              status: :error,
              started_at: started_at,
              finished_at: finished_at,
              duration_ms: 2_000,
              error: %{message: "DuckDB ADBC connection bootstrap failed at attach_mart"},
              asset_step_id: "failed-inventory-step"
            })
          ]
        }
      )

    window =
      parent.id
      |> backfill_window(anchor, :error)
      |> Map.merge(%{
        child_run_id: child.id,
        latest_attempt_run_id: child.id,
        attempt_count: 1,
        last_error: :failed,
        started_at: started_at,
        finished_at: finished_at,
        updated_at: finished_at
      })

    assert :ok = Storage.put_run(parent)
    assert :ok = Storage.put_run(child)
    assert :ok = Storage.put_backfill_window(window)

    assert {:ok, detail} = FavnOrchestrator.get_run_detail(parent.id)

    assert [failure] = detail.backfill_failures
    assert failure.child_run_id == child.id
    assert failure.asset_ref == "MyApp.Assets.Inventory.inventory_by_day"
    assert failure.window.key == window_key(anchor)
    assert failure.duration_ms == 2_000
    assert failure.error == %{message: "DuckDB ADBC connection bootstrap failed at attach_mart"}
  end

  test "backfill parent detail caps enriched failed window context" do
    parent =
      run("backfill_parent_many_failures", submit_kind: :backfill_pipeline)
      |> RunState.transition(status: :error, error: :failed, result: %{status: :error})

    assert :ok = Storage.put_run(parent)

    for day <- 1..12 do
      anchor = anchor(DateTime.add(~U[2026-05-01 00:00:00Z], (day - 1) * 86_400, :second))

      window =
        parent.id
        |> backfill_window(anchor, :error)
        |> Map.put(:last_error, {:failed_window, day})

      assert :ok = Storage.put_backfill_window(window)
    end

    assert {:ok, detail} = FavnOrchestrator.get_run_detail(parent.id)

    assert detail.backfill_failure_count == 12
    assert length(detail.backfill_failures) == 10
    assert detail.summary.progress.counts.failed == 12
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

  defp dependency_plan(refs) do
    node_keys = Enum.map(refs, &{&1, nil})

    nodes =
      refs
      |> Enum.zip(node_keys)
      |> Enum.with_index()
      |> Map.new(fn {{ref, node_key}, stage} ->
        {node_key,
         %{
           ref: ref,
           node_key: node_key,
           window: nil,
           upstream: Enum.take(node_keys, stage),
           downstream: Enum.drop(node_keys, stage + 1),
           stage: stage,
           action: :run
         }}
      end)

    %Favn.Plan{
      target_refs: [List.last(refs)],
      target_node_keys: [List.last(node_keys)],
      nodes: nodes,
      topo_order: refs,
      stages: Enum.map(refs, &[&1]),
      node_stages: Enum.map(node_keys, &[&1])
    }
  end

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
