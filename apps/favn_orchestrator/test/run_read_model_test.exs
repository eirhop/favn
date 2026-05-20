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

  test "run detail derives terminal step start time from finish and duration" do
    ref = {MyApp.Assets.Gold, :asset}
    submitted_at = ~U[2026-05-20 07:57:13Z]
    finished_at = ~U[2026-05-20 07:57:40Z]

    run =
      run("derived_terminal_step_start", submit_kind: :pipeline)
      |> RunState.transition(
        status: :ok,
        result: %{
          node_results: [
            %{
              node_key: {ref, nil},
              ref: ref,
              stage: 0,
              status: :ok,
              finished_at: finished_at,
              duration_ms: 26_800,
              asset_step_id: "derived-step"
            }
          ]
        }
      )

    assert :ok = Storage.put_run(run)

    assert :ok =
             Storage.append_run_event(run.id, %{
               run_id: run.id,
               sequence: 1,
               event_type: :step_started,
               occurred_at: submitted_at,
               status: :running,
               asset_ref: ref,
               stage: 0,
               data: %{asset_step_id: "derived-step"}
             })

    assert {:ok, detail} = FavnOrchestrator.get_run_detail(run.id)
    assert [step] = detail.steps
    assert step.started_at == DateTime.add(finished_at, -26_800, :millisecond)
    assert step.finished_at == finished_at
    assert step.duration_ms == 26_800
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
    assert detail.summary.progress.label == "1/3 steps"

    steps_by_ref = Map.new(detail.steps, &{&1.asset_ref, &1})
    assert steps_by_ref["MyApp.Assets.Gold.asset"].status == :error
    assert steps_by_ref["MyApp.Assets.Silver.asset"].status == :running
    assert steps_by_ref["MyApp.Assets.Bronze.asset"].status == :pending
  end

  test "pending rows keep repeated asset refs for distinct planned nodes" do
    ref = {MyApp.Assets.Gold, :asset}
    window_a = %{key: "window:a", label: "Window A"}
    window_b = %{key: "window:b", label: "Window B"}
    node_a = {ref, "window:a"}
    node_b = {ref, "window:b"}

    run =
      run("pipeline_repeated_asset_gap", submit_kind: :pipeline)
      |> Map.put(:target_refs, [ref])
      |> Map.put(
        :plan,
        %Favn.Plan{
          target_refs: [ref],
          target_node_keys: [node_a, node_b],
          nodes: %{
            node_a => plan_node(ref, node_a, window_a, 0),
            node_b => plan_node(ref, node_b, window_b, 0)
          },
          topo_order: [ref],
          stages: [[ref]],
          node_stages: [[node_a, node_b]]
        }
      )
      |> RunState.transition(
        status: :running,
        result: %{
          node_results: [
            NodeResult.new(%{
              node_key: node_a,
              ref: ref,
              window: window_a,
              stage: 0,
              status: :running,
              started_at: ~U[2026-05-01 00:00:00Z],
              asset_step_id: "window-a-step"
            })
          ]
        }
      )

    assert :ok = Storage.put_run(run)

    assert {:ok, detail} = FavnOrchestrator.get_run_detail(run.id)

    assert length(detail.steps) == 2

    assert Enum.map(detail.steps, & &1.asset_ref) == [
             "MyApp.Assets.Gold.asset",
             "MyApp.Assets.Gold.asset"
           ]

    assert Enum.map(detail.steps, & &1.window.key) == ["window:a", "window:b"]
    assert Enum.map(detail.steps, & &1.status) == [:running, :pending]
  end

  test "no-plan active run creates distinct pending ids for multiple targets" do
    gold_ref = {MyApp.Assets.Gold, :asset}
    silver_ref = {MyApp.Assets.Silver, :asset}

    run =
      run("active_no_plan_multiple_targets", submit_kind: :pipeline)
      |> Map.put(:target_refs, [gold_ref, silver_ref])
      |> RunState.transition(status: :running, result: %{node_results: []})

    assert :ok = Storage.put_run(run)

    assert {:ok, detail} = FavnOrchestrator.get_run_detail(run.id)

    assert Enum.map(detail.steps, & &1.asset_ref) == [
             "MyApp.Assets.Gold.asset",
             "MyApp.Assets.Silver.asset"
           ]

    assert detail.steps |> Enum.map(& &1.id) |> Enum.uniq() |> length() == 2
  end

  test "pending planned steps expose execution pools and queue reasons" do
    gold_ref = {MyApp.Assets.Gold, :asset}
    silver_ref = {MyApp.Assets.Silver, :asset}
    gold_node = {gold_ref, nil}
    silver_node = {silver_ref, nil}

    plan = %Favn.Plan{
      target_refs: [silver_ref],
      target_node_keys: [silver_node],
      nodes: %{
        gold_node =>
          gold_ref
          |> plan_node(gold_node, nil, 0)
          |> Map.merge(%{downstream: [silver_node], execution_pool: :github_api}),
        silver_node =>
          silver_ref
          |> plan_node(silver_node, nil, 1)
          |> Map.merge(%{upstream: [gold_node]})
      },
      topo_order: [gold_ref, silver_ref],
      stages: [[gold_ref], [silver_ref]],
      node_stages: [[gold_node], [silver_node]]
    }

    run =
      run("pending_execution_policy",
        submit_kind: :pipeline,
        metadata: %{pipeline_execution_policy: %{execution_pool: :warehouse_default}}
      )
      |> Map.put(:target_refs, [silver_ref])
      |> Map.put(:plan, plan)
      |> RunState.transition(status: :running, result: %{node_results: []})

    assert :ok = Storage.put_run(run)

    assert {:ok, detail} = FavnOrchestrator.get_run_detail(run.id)

    steps_by_ref = Map.new(detail.steps, &{&1.asset_ref, &1})
    assert steps_by_ref["MyApp.Assets.Gold.asset"].execution_pool == :github_api
    assert steps_by_ref["MyApp.Assets.Gold.asset"].queue_reason == nil
    assert steps_by_ref["MyApp.Assets.Silver.asset"].execution_pool == :warehouse_default
    assert steps_by_ref["MyApp.Assets.Silver.asset"].queue_reason == :waiting_dependencies
  end

  test "queued step events expose execution pool and queue reason" do
    ref = {MyApp.Assets.Gold, :asset}

    run =
      run("queued_step_event", submit_kind: :pipeline)
      |> RunState.transition(status: :running)

    assert :ok = Storage.put_run(run)

    assert :ok =
             Storage.append_run_event(run.id, %{
               run_id: run.id,
               sequence: 1,
               event_type: :step_queued,
               occurred_at: ~U[2026-05-01 00:00:00Z],
               status: :queued,
               asset_ref: ref,
               stage: 0,
               data: %{
                 asset_step_id: "queued-step",
                 execution_pool: :github_api,
                 queue_reason: :execution_pool
               }
             })

    assert {:ok, detail} = FavnOrchestrator.get_run_detail(run.id)
    assert [%{id: "queued-step", status: :queued} = step] = detail.steps
    assert step.execution_pool == "github_api"
    assert step.queue_reason == "execution_pool"
    assert step.explanation == "Execution is queued by orchestrator admission."
  end

  test "backfill parent detail includes failed child window context" do
    parent =
      run("backfill_parent_failure", submit_kind: :backfill_pipeline)
      |> RunState.transition(status: :error, error: :failed, result: %{status: :error})

    failed_ref = {MyApp.Assets.Orders, :orders_by_day}
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
              asset_step_id: "failed-orders-step"
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
    assert failure.asset_ref == "MyApp.Assets.Orders.orders_by_day"
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

  test "execution group detail aggregates a multi-window backfill" do
    parent = run("exec_group_parent", submit_kind: :backfill_pipeline)
    anchors = Enum.map(0..3, &anchor(DateTime.add(~U[2026-05-01 00:00:00Z], &1, :day)))

    completed_child =
      child_run(parent, "exec_group_child_ok", Enum.at(anchors, 0), :ok,
        result_status: :ok,
        asset_ref: {MyApp.Assets.Gold, :asset},
        started_at: ~U[2026-05-01 00:00:10Z],
        finished_at: ~U[2026-05-01 00:00:20Z]
      )

    failed_child =
      child_run(parent, "exec_group_child_failed", Enum.at(anchors, 1), :error,
        result_status: :error,
        asset_ref: {MyApp.Assets.Silver, :asset},
        started_at: ~U[2026-05-01 00:00:30Z],
        finished_at: ~U[2026-05-01 00:00:40Z],
        error: %{message: "boom"}
      )

    running_child =
      child_run(parent, "exec_group_child_running", Enum.at(anchors, 2), :running,
        asset_ref: {MyApp.Assets.Bronze, :asset}
      )

    queued_child =
      child_run(parent, "exec_group_child_queued", Enum.at(anchors, 3), :running,
        asset_ref: {MyApp.Assets.Copper, :asset}
      )

    runs = [parent, completed_child, failed_child, running_child, queued_child]
    Enum.each(runs, &assert(:ok = Storage.put_run(&1)))

    Enum.zip(anchors, [:ok, :error, :running, :pending])
    |> Enum.zip([completed_child, failed_child, running_child, queued_child])
    |> Enum.each(fn {{anchor, status}, child} ->
      window =
        parent.id
        |> backfill_window(anchor, status)
        |> Map.merge(%{child_run_id: child.id, latest_attempt_run_id: child.id})

      assert :ok = Storage.put_backfill_window(window)
    end)

    assert :ok =
             Storage.append_run_event(running_child.id, %{
               run_id: running_child.id,
               sequence: 1,
               event_type: :step_started,
               occurred_at: ~U[2026-05-01 00:00:50Z],
               status: :running,
               data: %{
                 asset_ref: {MyApp.Assets.Bronze, :asset},
                 asset_step_id: "running-step",
                 stage: 0,
                 attempt: 1,
                 window: public_window(Enum.at(anchors, 2))
               }
             })

    assert {:ok, [group]} = FavnOrchestrator.list_execution_groups(trigger_type: :backfill)

    assert group.id == parent.id

    assert group.child_run_ids ==
             Enum.map([completed_child, failed_child, running_child, queued_child], & &1.id)

    assert group.total_windows == 4
    assert group.completed_windows == 2
    assert group.failed_windows == 1
    assert group.total_asset_attempts == 4
    assert group.completed_asset_attempts == 2
    assert group.failed_asset_attempts == 1
    assert group.running_asset_attempts == 1
    assert group.queued_asset_attempts == 1

    assert {:ok, detail} = FavnOrchestrator.get_execution_group_detail(parent.id)
    assert detail.summary.id == parent.id
    assert length(detail.child_runs) == 4
    assert length(detail.windows) == 4
    assert length(detail.asset_attempts) == 4
  end

  test "execution group asset attempts expose persisted window data" do
    parent = run("exec_group_windows_parent", submit_kind: :backfill_pipeline)
    anchor = anchor(~U[2026-06-01 00:00:00Z])

    child =
      child_run(parent, "exec_group_windows_child", anchor, :ok,
        result_status: :ok,
        asset_ref: {MyApp.Assets.Gold, :asset},
        started_at: ~U[2026-06-01 00:00:00Z],
        finished_at: ~U[2026-06-01 00:00:01Z]
      )

    assert :ok = Storage.put_run(parent)
    assert :ok = Storage.put_run(child)
    assert :ok = Storage.put_backfill_window(backfill_window(parent.id, anchor, :ok))

    assert {:ok, [attempt]} = FavnOrchestrator.list_execution_group_asset_attempts(parent.id)
    assert attempt.window.start_at == anchor.start_at
    assert attempt.window.end_at == anchor.end_at
    assert attempt.window_start_at == anchor.start_at
    assert attempt.window_end_at == anchor.end_at
  end

  test "execution group asset attempts expose output metadata" do
    parent = run("exec_group_output_parent", submit_kind: :backfill_pipeline)
    anchor = anchor(~U[2026-06-01 00:00:00Z])

    child =
      child_run(parent, "exec_group_output_child", anchor, :ok,
        result_status: :ok,
        meta: %{
          rows_written: 0,
          relation: "raw.mercatus.reporting_baseline_feeding",
          mode: :monthly_replace,
          source: %{system: :mercatus}
        }
      )

    assert :ok = Storage.put_run(parent)
    assert :ok = Storage.put_run(child)
    assert :ok = Storage.put_backfill_window(backfill_window(parent.id, anchor, :ok))

    assert {:ok, [attempt]} = FavnOrchestrator.list_execution_group_asset_attempts(parent.id)

    assert attempt.output_metadata == %{
             "rows_written" => 0,
             "relation" => "raw.mercatus.reporting_baseline_feeding",
             "mode" => "monthly_replace",
             "source" => %{"system" => "mercatus"}
           }
  end

  test "asset step log context exposes output metadata states" do
    parent = run("asset_log_output_parent", submit_kind: :backfill_pipeline)
    anchor = anchor(~U[2026-06-01 00:00:00Z])

    success =
      child_run(parent, "asset_log_output_success", anchor, :ok,
        result_status: :ok,
        meta: %{rows_written: 0}
      )

    empty_success =
      child_run(parent, "asset_log_output_empty_success", anchor, :ok,
        result_status: :ok,
        meta: %{}
      )

    failed =
      child_run(parent, "asset_log_output_failed", anchor, :error,
        result_status: :error,
        error: %{message: "failed"},
        meta: %{}
      )

    Enum.each([parent, success, empty_success, failed], &assert(:ok = Storage.put_run(&1)))

    assert {:ok, success_context} =
             FavnOrchestrator.get_asset_step_log_context(success.id, "#{success.id}-step")

    assert success_context.step.output_metadata == %{"rows_written" => 0}

    assert {:ok, empty_context} =
             FavnOrchestrator.get_asset_step_log_context(
               empty_success.id,
               "#{empty_success.id}-step"
             )

    assert empty_context.step.status == :ok
    assert empty_context.step.output_metadata == %{}

    assert {:ok, failed_context} =
             FavnOrchestrator.get_asset_step_log_context(failed.id, "#{failed.id}-step")

    assert failed_context.step.status == :error
    assert failed_context.step.output_metadata == %{}
  end

  test "execution group timeline is ordered by execution start time" do
    parent = run("exec_group_timeline_parent", submit_kind: :backfill_pipeline)
    first_anchor = anchor(~U[2026-07-01 00:00:00Z])
    second_anchor = anchor(~U[2026-07-02 00:00:00Z])

    later_child =
      child_run(parent, "exec_group_timeline_later", second_anchor, :ok,
        result_status: :ok,
        started_at: ~U[2026-07-03 00:00:00Z],
        finished_at: ~U[2026-07-03 00:00:01Z]
      )

    earlier_child =
      child_run(parent, "exec_group_timeline_earlier", first_anchor, :ok,
        result_status: :ok,
        started_at: ~U[2026-07-01 00:00:00Z],
        finished_at: ~U[2026-07-01 00:00:01Z]
      )

    Enum.each([parent, later_child, earlier_child], &assert(:ok = Storage.put_run(&1)))
    assert :ok = Storage.put_backfill_window(backfill_window(parent.id, first_anchor, :ok))
    assert :ok = Storage.put_backfill_window(backfill_window(parent.id, second_anchor, :ok))

    assert {:ok, timeline} = FavnOrchestrator.list_execution_group_timeline(parent.id)
    assert Enum.map(timeline, & &1.child_run_id) == [earlier_child.id, later_child.id]
  end

  test "execution group events include child window run events" do
    parent = run("exec_group_events_parent", submit_kind: :backfill_pipeline)
    anchor = anchor(~U[2026-07-01 00:00:00Z])
    child = child_run(parent, "exec_group_events_child", anchor, :running, result_status: nil)

    Enum.each([parent, child], &assert(:ok = Storage.put_run(&1)))

    assert :ok =
             Storage.append_run_event(parent.id, %{
               run_id: parent.id,
               sequence: 1,
               event_type: :backfill_started,
               occurred_at: ~U[2026-07-01 00:00:00Z],
               status: :running
             })

    assert :ok =
             Storage.append_run_event(child.id, %{
               run_id: child.id,
               sequence: 1,
               event_type: :step_started,
               occurred_at: ~U[2026-07-01 00:00:01Z],
               status: :running,
               asset_ref: {MyApp.Assets.Gold, :asset},
               data: %{asset_step_id: "child-step"}
             })

    assert {:ok, events} = FavnOrchestrator.list_execution_group_events(parent.id)
    assert Enum.map(events, & &1.run_id) == [parent.id, child.id]

    assert {:ok, detail} = FavnOrchestrator.get_execution_group_detail(parent.id)
    assert Enum.map(detail.events, & &1.run_id) == [parent.id, child.id]
  end

  test "execution group attempt filters apply on orchestrator read model" do
    parent = run("exec_group_filter_parent", submit_kind: :backfill_pipeline)
    ok_anchor = anchor(~U[2026-08-01 00:00:00Z])
    failed_anchor = anchor(~U[2026-08-02 00:00:00Z])

    ok_child = child_run(parent, "exec_group_filter_ok", ok_anchor, :ok, result_status: :ok)

    failed_child =
      child_run(parent, "exec_group_filter_failed", failed_anchor, :error,
        result_status: :error,
        error: %{message: "failed"}
      )

    Enum.each([parent, ok_child, failed_child], &assert(:ok = Storage.put_run(&1)))
    assert :ok = Storage.put_backfill_window(backfill_window(parent.id, ok_anchor, :ok))
    assert :ok = Storage.put_backfill_window(backfill_window(parent.id, failed_anchor, :error))

    assert {:ok, [failed]} =
             FavnOrchestrator.list_execution_group_asset_attempts(parent.id, only_failed: true)

    assert failed.status == :error

    assert {:ok, [ok]} =
             FavnOrchestrator.list_execution_group_asset_attempts(parent.id, status: :ok)

    assert ok.status == :ok
  end

  test "execution group summary timing spans child window run activity" do
    parent =
      "exec_group_duration_parent"
      |> run(submit_kind: :backfill_pipeline)
      |> RunState.transition(status: :ok, result: %{status: :ok})
      |> Map.put(:inserted_at, ~U[2026-04-30 00:00:00Z])
      |> Map.put(:updated_at, ~U[2026-04-30 00:00:01Z])

    first_anchor = anchor(~U[2026-05-01 00:00:00Z])
    second_anchor = anchor(~U[2026-05-02 00:00:00Z])

    first_child =
      child_run(parent, "exec_group_duration_first", first_anchor, :ok,
        result_status: :ok,
        started_at: ~U[2026-05-01 00:00:10Z],
        finished_at: ~U[2026-05-01 00:00:20Z]
      )
      |> Map.put(:inserted_at, ~U[2026-05-01 00:00:10Z])
      |> Map.put(:updated_at, ~U[2026-05-01 00:00:20Z])

    second_child =
      child_run(parent, "exec_group_duration_second", second_anchor, :ok,
        result_status: :ok,
        started_at: ~U[2026-05-02 00:00:30Z],
        finished_at: ~U[2026-05-02 00:00:45Z]
      )
      |> Map.put(:inserted_at, ~U[2026-05-02 00:00:30Z])
      |> Map.put(:updated_at, ~U[2026-05-02 00:00:45Z])

    Enum.each([parent, first_child, second_child], &assert(:ok = Storage.put_run(&1)))
    assert :ok = Storage.put_backfill_window(backfill_window(parent.id, first_anchor, :ok))
    assert :ok = Storage.put_backfill_window(backfill_window(parent.id, second_anchor, :ok))

    assert {:ok, [group]} = FavnOrchestrator.list_execution_groups(trigger_type: :backfill)

    assert group.started_at == ~U[2026-04-30 00:00:00Z]
    assert group.finished_at == ~U[2026-05-02 00:00:45Z]
    assert group.duration_ms == 172_845_000
  end

  test "execution group overview fetches missing roots and sorts by child activity" do
    parent =
      "exec_group_missing_root_parent"
      |> run(submit_kind: :backfill_pipeline)
      |> Map.put(:inserted_at, ~U[2026-01-01 00:00:00Z])
      |> Map.put(:updated_at, ~U[2026-01-01 00:00:01Z])

    anchor = anchor(~U[2026-05-01 00:00:00Z])

    child =
      child_run(parent, "exec_group_missing_root_child", anchor, :running,
        result_status: nil,
        started_at: ~U[2026-05-20 00:00:00Z],
        finished_at: ~U[2026-05-20 00:00:10Z]
      )
      |> Map.put(:inserted_at, ~U[2026-05-20 00:00:00Z])
      |> Map.put(:updated_at, ~U[2026-05-20 00:00:00Z])

    filler_runs =
      1..6
      |> Enum.map(fn index ->
        "exec_group_missing_root_filler_#{index}"
        |> run(submit_kind: :pipeline)
        |> Map.put(:inserted_at, DateTime.add(~U[2026-05-19 00:00:00Z], index, :second))
        |> Map.put(:updated_at, DateTime.add(~U[2026-05-19 00:00:00Z], index, :second))
      end)

    Enum.each([parent, child | filler_runs], &assert(:ok = Storage.put_run(&1)))
    assert :ok = Storage.put_backfill_window(backfill_window(parent.id, anchor, :running))

    assert {:ok, [group]} = FavnOrchestrator.list_execution_groups(limit: 1)

    assert group.id == parent.id
    assert group.child_run_ids == [child.id]
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

  defp child_run(parent, run_id, %Anchor{} = anchor, status, opts) do
    asset_ref = Keyword.get(opts, :asset_ref, {MyApp.Assets.Gold, :asset})
    result_status = Keyword.get(opts, :result_status)
    started_at = Keyword.get(opts, :started_at, anchor.start_at)
    finished_at = Keyword.get(opts, :finished_at, DateTime.add(started_at, 1, :second))
    error = Keyword.get(opts, :error)
    meta = Keyword.get(opts, :meta, %{})
    window = public_window(anchor)
    node_key = {asset_ref, window.key}

    base =
      run(run_id,
        submit_kind: :pipeline,
        parent_run_id: parent.id,
        root_run_id: parent.id,
        lineage_depth: 1,
        trigger: %{
          kind: :backfill,
          pipeline_module: MyApp.Pipelines.Daily,
          window_key: window.key
        },
        metadata: %{pipeline_context: %{anchor_window: anchor}}
      )
      |> Map.put(:asset_ref, asset_ref)
      |> Map.put(:target_refs, [asset_ref])
      |> Map.put(:plan, single_node_plan(asset_ref, node_key, window))

    case result_status do
      nil ->
        RunState.transition(base, status: status)

      result_status ->
        result =
          NodeResult.new(%{
            node_key: node_key,
            ref: asset_ref,
            window: window,
            stage: 0,
            status: result_status,
            started_at: started_at,
            finished_at: finished_at,
            duration_ms: DateTime.diff(finished_at, started_at, :millisecond),
            error: error,
            meta: meta,
            attempt_count: 1,
            max_attempts: 1,
            asset_step_id: "#{run_id}-step"
          })

        RunState.transition(base,
          status: status,
          error: error,
          result: %{status: status, node_results: [result]}
        )
    end
  end

  defp single_node_plan(asset_ref, node_key, window) do
    %Favn.Plan{
      target_refs: [asset_ref],
      target_node_keys: [node_key],
      nodes: %{node_key => plan_node(asset_ref, node_key, window, 0)},
      topo_order: [asset_ref],
      stages: [[asset_ref]],
      node_stages: [[node_key]]
    }
  end

  defp anchor(start_at) do
    {:ok, anchor} = Anchor.new(:day, start_at, DateTime.add(start_at, 1, :day))
    anchor
  end

  defp public_window(%Anchor{} = anchor) do
    %{
      key: window_key(anchor),
      label: Calendar.strftime(anchor.start_at, "%b %-d"),
      kind: anchor.kind,
      start_at: anchor.start_at,
      end_at: anchor.end_at,
      timezone: anchor.timezone
    }
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

  defp plan_node(ref, node_key, window, stage) do
    %{
      ref: ref,
      node_key: node_key,
      window: window,
      upstream: [],
      downstream: [],
      stage: stage,
      action: :run
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
