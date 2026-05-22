defmodule FavnOrchestrator.Repair.RuntimeStateTest do
  use ExUnit.Case, async: false

  alias Favn.Manifest
  alias Favn.Manifest.Version
  alias Favn.Plan
  alias Favn.Run.NodeResult
  alias FavnOrchestrator.Backfill.BackfillWindow
  alias FavnOrchestrator.Repair.RuntimeState
  alias FavnOrchestrator.RunReadModel
  alias FavnOrchestrator.RunState
  alias FavnOrchestrator.Storage
  alias FavnOrchestrator.Storage.Adapter.Memory
  alias FavnOrchestrator.TransitionWriter

  defmodule NoClaimAdapter do
    @behaviour Favn.Storage.Adapter

    defdelegate child_spec(opts), to: Memory
    defdelegate put_manifest_version(version, opts), to: Memory
    defdelegate get_manifest_version(id, opts), to: Memory
    defdelegate get_manifest_version_by_content_hash(hash, opts), to: Memory
    defdelegate list_manifest_versions(opts), to: Memory
    defdelegate set_active_manifest_version(id, opts), to: Memory
    defdelegate get_active_manifest_version(opts), to: Memory
    defdelegate put_run(run, opts), to: Memory
    defdelegate get_run(id, opts), to: Memory
    defdelegate list_runs(run_opts, opts), to: Memory
    defdelegate persist_run_transition(run, event, opts), to: Memory
    defdelegate append_run_event(run_id, event, opts), to: Memory
    defdelegate list_run_events(run_id, opts), to: Memory
    defdelegate list_global_run_events(filters, opts), to: Memory
    defdelegate try_acquire_execution_lease(lease, opts), to: Memory
    defdelegate release_execution_lease(lease_id, opts), to: Memory
    defdelegate expire_execution_leases(now, opts), to: Memory
    defdelegate list_execution_leases(opts), to: Memory
    defdelegate persist_log_entries(entries, opts), to: Memory
    defdelegate list_logs(filter, opts, adapter_opts), to: Memory
    defdelegate replay_logs_after(cursor, filter, opts, adapter_opts), to: Memory
    defdelegate put_scheduler_state(key, state, opts), to: Memory
    defdelegate get_scheduler_state(key, opts), to: Memory
    defdelegate put_coverage_baseline(baseline, opts), to: Memory
    defdelegate get_coverage_baseline(id, opts), to: Memory
    defdelegate list_coverage_baselines(filters, opts), to: Memory
    defdelegate put_backfill_window(window, opts), to: Memory
    defdelegate get_backfill_window(backfill_id, module, window_key, opts), to: Memory
    defdelegate list_backfill_windows(filters, opts), to: Memory
    defdelegate apply_backfill_child_projection(window, states, opts), to: Memory
    defdelegate get_backfill_progress(backfill_id, opts), to: Memory
    defdelegate rebuild_backfill_progress(backfill_id, opts), to: Memory
    defdelegate put_asset_window_state(state, opts), to: Memory
    defdelegate get_asset_window_state(module, name, freshness_key, opts), to: Memory
    defdelegate list_asset_window_states(filters, opts), to: Memory
    defdelegate get_asset_freshness_states_by_keys(keys, opts), to: Memory

    defdelegate replace_backfill_read_models(filters, baselines, windows, states, opts),
      to: Memory
  end

  setup do
    previous_adapter = Application.get_env(:favn_orchestrator, :storage_adapter)
    previous_opts = Application.get_env(:favn_orchestrator, :storage_adapter_opts)

    Memory.reset()

    on_exit(fn ->
      Memory.reset()
      restore_env(:favn_orchestrator, :storage_adapter, previous_adapter)
      restore_env(:favn_orchestrator, :storage_adapter_opts, previous_opts)
    end)

    :ok
  end

  test "dry-run reports orphaned active runs and steps without writing" do
    run = running_run("repair_dry_run")
    assert :ok = persist_running_run_with_started_step(run)

    assert {:ok, report} = RuntimeState.repair(dry_run: true, run_id: run.id, freshness: false)

    assert report.mode == :dry_run
    assert report.runs_scanned == 1
    assert report.runs_terminalized == 1
    assert report.steps_terminalized == 1

    assert {:ok, persisted} = Storage.get_run(run.id)
    assert persisted.status == :running

    assert {:ok, events} = Storage.list_run_events(run.id)
    refute :step_failed in Enum.map(events, & &1.event_type)
  end

  test "apply terminalizes orphaned active step events before failing run" do
    run = running_run("repair_apply")
    assert :ok = persist_running_run_with_started_step(run)

    assert {:ok, report} = RuntimeState.repair(dry_run: false, run_id: run.id, freshness: false)

    assert report.mode == :apply
    assert report.runs_terminalized == 1
    assert report.steps_terminalized == 1

    assert {:ok, persisted} = Storage.get_run(run.id)
    assert persisted.status == :error
    assert persisted.error.type == :orphaned_run_reconciled

    assert {:ok, events} = Storage.list_run_events(run.id)

    assert Enum.map(events, & &1.event_type) == [
             :run_created,
             :run_started,
             :step_started,
             :step_failed,
             :run_failed
           ]

    step_failed = Enum.find(events, &(&1.event_type == :step_failed))
    assert get_in(step_failed.data, ["error", "type"]) == "orphaned_step_reconciled"

    assert {:ok, detail} = RunReadModel.get_run_detail(run.id)
    assert [%{status: :error}] = detail.steps
  end

  test "apply rebuilds missing freshness for independent successful node results" do
    ref = {MyApp.Assets.RepairRaw, :asset}
    node_key = {ref, nil}
    version = manifest_version("mv_repair_freshness", ref)
    assert :ok = Storage.put_manifest_version(version)

    result =
      NodeResult.new(%{
        node_key: node_key,
        ref: ref,
        status: :ok,
        freshness_key: Favn.Freshness.Key.latest(),
        asset_step_id: "repair_freshness:raw"
      })

    run =
      "repair_freshness"
      |> base_run(ref)
      |> RunState.transition(
        manifest_version_id: version.manifest_version_id,
        manifest_content_hash: version.content_hash,
        status: :ok,
        plan: single_node_plan(ref, node_key),
        result: %{status: :ok, node_results: [result]}
      )

    assert :ok = Storage.put_run(run)

    assert {:ok, report} = RuntimeState.repair(dry_run: false, run_id: run.id)
    assert report.freshness_states_rebuilt == 1
    assert report.freshness_states_skipped == 0

    assert {:ok, state} =
             Storage.get_asset_freshness_state(
               MyApp.Assets.RepairRaw,
               :asset,
               Favn.Freshness.Key.latest()
             )

    assert state.status == :ok
    assert state.latest_success_run_id == run.id
    assert state.latest_success_node_key == node_key
  end

  test "apply ignores adapters without materialization claim callbacks" do
    Application.put_env(:favn_orchestrator, :storage_adapter, NoClaimAdapter)
    Application.put_env(:favn_orchestrator, :storage_adapter_opts, server: Memory)

    assert {:ok, report} = RuntimeState.repair(dry_run: false, freshness: false)
    assert report.materialization_claims_expired == 0
    assert report.errors == []
  end

  test "apply reconciles stale active backfill windows from terminal child runs" do
    now = DateTime.utc_now()

    parent =
      "repair_backfill_parent"
      |> backfill_parent_run()
      |> RunState.transition(status: :error, error: %{message: "orphaned"})

    child = backfill_child_run(parent.id, "repair_backfill_child", "day:2026-05-21")

    terminal_child =
      RunState.transition(child,
        status: :cancelled,
        error: %{message: "cancelled"},
        result: %{status: :cancelled, asset_results: [], metadata: %{}}
      )

    window =
      %{
        backfill_window(parent.id, child.trigger.window_key, now)
        | status: :running,
          child_run_id: child.id,
          latest_attempt_run_id: child.id,
          attempt_count: 1
      }

    assert :ok = Storage.put_run(parent)
    assert :ok = Storage.put_run(terminal_child)
    assert :ok = Storage.put_backfill_window(window)

    assert {:ok, report} =
             RuntimeState.repair(dry_run: false, backfill_id: parent.id, freshness: false)

    assert report.runs_scanned == 0
    assert report.backfill_windows_reconciled == 1

    assert {:ok, repaired_window} =
             Storage.get_backfill_window(
               parent.id,
               MyApp.Pipelines.Repair,
               child.trigger.window_key
             )

    assert repaired_window.status == :cancelled
    assert repaired_window.last_error == %{message: "cancelled"}

    assert {:ok, repaired_parent} = Storage.get_run(parent.id)
    assert repaired_parent.status == :cancelled
  end

  defp persist_running_run_with_started_step(%RunState{} = run) do
    pending = %{run | status: :pending, event_seq: 1}

    with :ok <- TransitionWriter.persist_transition(pending, :run_created, %{status: :pending}),
         :ok <- TransitionWriter.persist_transition(run, :run_started, %{status: :running}) do
      started = RunState.transition(run, status: :running)

      TransitionWriter.persist_transition(started, :step_started, %{
        asset_step_id: asset_step_id(run),
        asset_ref: run.asset_ref,
        stage: 0,
        runner_execution_id: "exec_#{run.id}"
      })
    end
  end

  defp running_run(run_id) do
    run_id
    |> base_run()
    |> RunState.transition(status: :running, runner_execution_id: "exec_#{run_id}")
  end

  defp base_run(run_id) do
    base_run(run_id, {MyApp.Assets.Gold, :asset})
  end

  defp base_run(run_id, asset_ref) do
    RunState.new(
      id: run_id,
      manifest_version_id: "mv_repair_runtime_state",
      manifest_content_hash: "hash_repair_runtime_state",
      asset_ref: asset_ref,
      target_refs: [asset_ref],
      trigger: %{kind: :manual}
    )
  end

  defp manifest_version(manifest_version_id, {module, name} = ref) do
    manifest = %Manifest{assets: [%Favn.Manifest.Asset{ref: ref, module: module, name: name}]}
    {:ok, version} = Version.new(manifest, manifest_version_id: manifest_version_id)
    version
  end

  defp single_node_plan(ref, node_key) do
    %Plan{
      target_refs: [ref],
      target_node_keys: [node_key],
      nodes: %{
        node_key => %{
          node_key: node_key,
          ref: ref,
          upstream: [],
          downstream: [],
          stage: 0
        }
      },
      topo_order: [ref],
      stages: [[ref]],
      node_stages: [[node_key]]
    }
  end

  defp asset_step_id(%RunState{} = run), do: "#{run.id}:gold"

  defp backfill_parent_run(run_id) do
    RunState.new(
      id: run_id,
      manifest_version_id: "mv_repair_runtime_state",
      manifest_content_hash: "hash_repair_runtime_state",
      asset_ref: {MyApp.Assets.Gold, :asset},
      target_refs: [{MyApp.Assets.Gold, :asset}],
      trigger: %{kind: :backfill, pipeline_module: MyApp.Pipelines.Repair},
      metadata: %{pipeline_submit_ref: MyApp.Pipelines.Repair},
      submit_kind: :backfill_pipeline
    )
    |> Map.put(:status, :running)
    |> RunState.with_snapshot_hash()
  end

  defp backfill_child_run(parent_run_id, run_id, window_key) do
    RunState.new(
      id: run_id,
      manifest_version_id: "mv_repair_runtime_state",
      manifest_content_hash: "hash_repair_runtime_state",
      asset_ref: {MyApp.Assets.Gold, :asset},
      target_refs: [{MyApp.Assets.Gold, :asset}],
      trigger: %{kind: :backfill, backfill_run_id: parent_run_id, window_key: window_key},
      metadata: %{pipeline_submit_ref: MyApp.Pipelines.Repair},
      submit_kind: :pipeline,
      parent_run_id: parent_run_id,
      root_run_id: parent_run_id,
      lineage_depth: 1
    )
  end

  defp backfill_window(backfill_run_id, window_key, start_at) do
    {:ok, window} =
      BackfillWindow.new(%{
        backfill_run_id: backfill_run_id,
        pipeline_module: MyApp.Pipelines.Repair,
        manifest_version_id: "mv_repair_runtime_state",
        window_kind: :day,
        window_start_at: start_at,
        window_end_at: DateTime.add(start_at, 1, :day),
        timezone: "Etc/UTC",
        window_key: window_key,
        status: :pending,
        created_at: start_at,
        updated_at: start_at
      })

    window
  end

  defp restore_env(app, key, nil), do: Application.delete_env(app, key)
  defp restore_env(app, key, value), do: Application.put_env(app, key, value)
end
