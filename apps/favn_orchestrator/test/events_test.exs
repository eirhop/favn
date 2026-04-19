defmodule FavnOrchestrator.EventsTest do
  use ExUnit.Case, async: false

  alias FavnOrchestrator
  alias FavnOrchestrator.RunEvent
  alias FavnOrchestrator.RunState
  alias FavnOrchestrator.Storage.Adapter.Memory
  alias FavnOrchestrator.TransitionWriter

  setup do
    previous_adapter = Application.get_env(:favn_orchestrator, :storage_adapter)
    previous_opts = Application.get_env(:favn_orchestrator, :storage_adapter_opts)

    Application.put_env(:favn_orchestrator, :storage_adapter, Memory)
    Application.put_env(:favn_orchestrator, :storage_adapter_opts, [])

    Memory.reset()

    on_exit(fn ->
      restore_env(:favn_orchestrator, :storage_adapter, previous_adapter)
      restore_env(:favn_orchestrator, :storage_adapter_opts, previous_opts)
      Memory.reset()
    end)

    :ok
  end

  test "run and global subscriptions receive persisted run events" do
    run = run_state("run_events_live")

    assert :ok = FavnOrchestrator.subscribe_run(run.id)
    assert :ok = FavnOrchestrator.subscribe_runs()
    assert :ok = TransitionWriter.persist_transition(run, :run_created, %{kind: :test})

    assert_receive {:favn_run_event, %RunEvent{} = event}
    assert_receive {:favn_run_event, %RunEvent{} = event_again}

    assert event.run_id == run.id
    assert event.sequence == 1
    assert event.event_type == :run_created
    assert event_again.run_id == run.id
    assert event_again.sequence == 1
    assert event_again.event_type == :run_created
  end

  test "unsubscribe helpers stop receiving events" do
    run = run_state("run_events_unsubscribe")

    assert :ok = FavnOrchestrator.subscribe_run(run.id)
    assert :ok = FavnOrchestrator.subscribe_runs()
    assert :ok = FavnOrchestrator.unsubscribe_run(run.id)
    assert :ok = FavnOrchestrator.unsubscribe_runs()

    assert :ok = TransitionWriter.persist_transition(run, :run_created, %{kind: :test})

    refute_receive {:favn_run_event, _event}, 100
  end

  test "list_run_events/2 returns typed events and supports filters" do
    run = run_state("run_events_list")

    running = RunState.transition(run, status: :running)
    failed = RunState.transition(running, status: :error, error: :boom)

    assert :ok = TransitionWriter.persist_transition(run, :run_created, %{stage: 0})
    assert :ok = TransitionWriter.persist_transition(running, :step_started, %{stage: 0})
    assert :ok = TransitionWriter.persist_transition(failed, :run_failed, %{})

    assert {:ok, events} = FavnOrchestrator.list_run_events(run.id)
    assert Enum.map(events, & &1.sequence) == [1, 2, 3]
    assert Enum.all?(events, &match?(%RunEvent{}, &1))

    run_created_event = Enum.find(events, &(&1.event_type == :run_created))
    assert run_created_event.entity == :run
    assert run_created_event.asset_ref == nil
    assert run_created_event.stage == nil

    step_event = Enum.find(events, &(&1.event_type == :step_started))
    assert step_event.entity == :step
    assert step_event.stage == 0
    assert step_event.asset_ref == run.asset_ref

    assert {:ok, after_first} = FavnOrchestrator.list_run_events(run.id, after_sequence: 1)
    assert Enum.map(after_first, & &1.sequence) == [2, 3]

    assert {:ok, limited} = FavnOrchestrator.list_run_events(run.id, limit: 2)
    assert Enum.map(limited, & &1.sequence) == [1, 2]

    assert {:error, :invalid_opts} = FavnOrchestrator.list_run_events(run.id, after_sequence: -1)
    assert {:error, :invalid_opts} = FavnOrchestrator.list_run_events(run.id, limit: 0)
  end

  test "does not broadcast when transition persistence fails" do
    run = run_state("run_events_conflict")
    run_id = run.id
    running = RunState.transition(run, status: :running)
    conflicting = RunState.transition(run, status: :error, error: :boom)

    assert :ok = FavnOrchestrator.subscribe_run(run.id)
    assert :ok = FavnOrchestrator.subscribe_runs()

    assert :ok = TransitionWriter.persist_transition(run, :run_created, %{})
    assert_receive {:favn_run_event, %RunEvent{sequence: 1}}
    assert_receive {:favn_run_event, %RunEvent{sequence: 1}}

    assert :ok = TransitionWriter.persist_transition(running, :run_started, %{})
    assert_receive {:favn_run_event, %RunEvent{sequence: 2}}
    assert_receive {:favn_run_event, %RunEvent{sequence: 2}}

    assert {:error, :conflicting_snapshot} =
             TransitionWriter.persist_transition(conflicting, :run_failed, %{})

    refute_receive {:favn_run_event, %RunEvent{run_id: ^run_id}}, 100
  end

  test "idempotent transition writes are not re-broadcast" do
    run = run_state("run_events_idempotent")

    assert :ok = FavnOrchestrator.subscribe_run(run.id)
    assert :ok = TransitionWriter.persist_transition(run, :run_created, %{kind: :test})
    assert_receive {:favn_run_event, %RunEvent{sequence: 1}}

    assert :ok = TransitionWriter.persist_transition(run, :run_created, %{kind: :test})
    refute_receive {:favn_run_event, _event}, 100
  end

  test "snapshot bootstrap can resume from last persisted sequence without gaps" do
    run = run_state("run_events_bootstrap")
    running = RunState.transition(run, status: :running)
    failed = RunState.transition(running, status: :error, error: :boom)

    assert :ok = TransitionWriter.persist_transition(run, :run_created, %{})
    assert :ok = TransitionWriter.persist_transition(running, :run_started, %{})

    assert {:ok, bootstrap_events} = FavnOrchestrator.list_run_events(run.id)
    assert last_sequence = List.last(bootstrap_events).sequence
    assert {:ok, projected_run} = FavnOrchestrator.get_run(run.id)
    assert projected_run.status == :running

    assert :ok = FavnOrchestrator.subscribe_run(run.id)
    assert :ok = TransitionWriter.persist_transition(failed, :run_failed, %{stage: 0})

    assert_receive {:favn_run_event, %RunEvent{} = live_event}
    assert live_event.sequence == 3

    assert {:ok, delta_events} =
             FavnOrchestrator.list_run_events(run.id, after_sequence: last_sequence)

    assert Enum.map(delta_events, & &1.sequence) == [3]
    assert hd(delta_events).event_type == live_event.event_type

    assert {:ok, updated_run} = FavnOrchestrator.get_run(run.id)
    assert updated_run.status == :error
  end

  defp run_state(run_id) do
    RunState.new(
      id: run_id,
      manifest_version_id: "mv_events",
      manifest_content_hash: "hash_events",
      asset_ref: {MyApp.Assets.Gold, :asset}
    )
  end

  defp restore_env(app, key, nil), do: Application.delete_env(app, key)
  defp restore_env(app, key, value), do: Application.put_env(app, key, value)
end
