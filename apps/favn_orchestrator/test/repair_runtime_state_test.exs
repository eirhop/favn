defmodule FavnOrchestrator.Repair.RuntimeStateTest do
  use ExUnit.Case, async: false

  alias Favn.Manifest
  alias Favn.Manifest.Version
  alias Favn.Plan
  alias Favn.Run.NodeResult
  alias FavnOrchestrator.Repair.RuntimeState
  alias FavnOrchestrator.RunReadModel
  alias FavnOrchestrator.RunState
  alias FavnOrchestrator.Storage
  alias FavnOrchestrator.Storage.Adapter.Memory
  alias FavnOrchestrator.TransitionWriter

  setup do
    Memory.reset()

    on_exit(fn ->
      Memory.reset()
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
end
