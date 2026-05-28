defmodule FavnOrchestrator.Scheduler.RuntimeTest do
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias Favn.Manifest
  alias Favn.Manifest.Asset
  alias Favn.Manifest.Pipeline
  alias Favn.Manifest.Schedule
  alias Favn.Manifest.Version
  alias Favn.Scheduler.State
  alias Favn.Window.Policy
  alias FavnOrchestrator
  alias FavnOrchestrator.RunState
  alias FavnOrchestrator.Scheduler.Runtime
  alias FavnOrchestrator.Storage
  alias FavnOrchestrator.Storage.Adapter.Memory

  defmodule RunnerClientStub do
    @behaviour Favn.Contracts.RunnerClient

    @impl true
    def register_manifest(_version, _opts), do: :ok

    @impl true
    def submit_work(work, _opts), do: {:ok, "exec_#{work.run_id}"}

    @impl true
    def await_result(_execution_id, _timeout, _opts) do
      {:ok,
       %Favn.Contracts.RunnerResult{
         status: :ok,
         asset_results: [
           %Favn.Run.AssetResult{
             ref: {MyApp.Assets.Gold, :asset},
             stage: 0,
             status: :ok,
             started_at: DateTime.utc_now(),
             finished_at: DateTime.utc_now(),
             duration_ms: 0,
             meta: %{},
             error: nil,
             attempt_count: 1,
             max_attempts: 1,
             attempts: []
           }
         ],
         metadata: %{}
       }}
    end

    @impl true
    def cancel_work(_execution_id, _reason, _opts), do: :ok

    @impl true
    def inspect_relation(_request, _opts), do: {:error, :not_supported}
  end

  defmodule SchedulerStateFailingStorageAdapter do
    @behaviour Favn.Storage.Adapter

    alias FavnOrchestrator.Storage.Adapter.Memory

    for {name, arity} <- Favn.Storage.Adapter.behaviour_info(:callbacks),
        {name, arity} not in [put_scheduler_state: 3, get_scheduler_state: 2] do
      args = Macro.generate_arguments(arity, __MODULE__)

      @impl true
      def unquote(name)(unquote_splicing(args)) do
        apply(Memory, unquote(name), [unquote_splicing(args)])
      end
    end

    @impl true
    def put_scheduler_state(key, state, opts) do
      case Keyword.fetch(opts, :put_error) do
        {:ok, reason} -> {:error, reason}
        :error -> Memory.put_scheduler_state(key, state, opts)
      end
    end

    @impl true
    def get_scheduler_state(key, opts) do
      case Keyword.fetch(opts, :get_error) do
        {:ok, reason} -> {:error, reason}
        :error -> Memory.get_scheduler_state(key, opts)
      end
    end
  end

  defmodule SlowSchedulerServer do
    use GenServer

    def start_link(opts),
      do: GenServer.start_link(__MODULE__, opts, name: Keyword.fetch!(opts, :name))

    @impl true
    def init(opts), do: {:ok, opts}

    @impl true
    def handle_call(_message, _from, state) do
      Process.sleep(Keyword.fetch!(state, :sleep_ms))
      {:reply, :ok, state}
    end
  end

  setup do
    previous_storage_adapter = Application.get_env(:favn_orchestrator, :storage_adapter)
    previous_storage_opts = Application.get_env(:favn_orchestrator, :storage_adapter_opts)
    previous_client = Application.get_env(:favn_orchestrator, :runner_client)
    previous_opts = Application.get_env(:favn_orchestrator, :runner_client_opts)
    previous_scheduler = Application.get_env(:favn_orchestrator, :scheduler)

    previous_scheduler_call_timeout =
      Application.get_env(:favn_orchestrator, :scheduler_call_timeout_ms)

    Application.put_env(:favn_orchestrator, :storage_adapter, Memory)
    Application.put_env(:favn_orchestrator, :storage_adapter_opts, [])
    Application.put_env(:favn_orchestrator, :runner_client, RunnerClientStub)
    Application.put_env(:favn_orchestrator, :runner_client_opts, [])
    Memory.reset()

    on_exit(fn ->
      restore_env(:favn_orchestrator, :storage_adapter, previous_storage_adapter)
      restore_env(:favn_orchestrator, :storage_adapter_opts, previous_storage_opts)
      restore_env(:favn_orchestrator, :runner_client, previous_client)
      restore_env(:favn_orchestrator, :runner_client_opts, previous_opts)
      restore_env(:favn_orchestrator, :scheduler, previous_scheduler)
      restore_env(:favn_orchestrator, :scheduler_call_timeout_ms, previous_scheduler_call_timeout)
      Memory.reset()
    end)

    :ok
  end

  test "loads scheduled entries from active manifest" do
    version = scheduler_manifest_version("mv_scheduler_entries")
    assert :ok = FavnOrchestrator.register_manifest(version)
    assert :ok = FavnOrchestrator.activate_manifest(version.manifest_version_id)

    name = unique_runtime_name()
    start_runtime(name)

    [entry] = Runtime.scheduled(name)
    assert entry.module == MyApp.Pipelines.Daily
    assert entry.schedule.name == :daily
    assert entry.manifest_version_id == version.manifest_version_id
  end

  test "scheduler public calls are bounded" do
    server = Module.concat(__MODULE__, "SlowScheduler#{System.unique_integer([:positive])}")

    start_supervised!(%{
      id: server,
      start: {SlowSchedulerServer, :start_link, [[name: server, sleep_ms: 100]]}
    })

    Application.put_env(:favn_orchestrator, :scheduler_call_timeout_ms, 10)

    assert {:error, {:scheduler_call_timeout, :tick}} = Runtime.tick(server)
  end

  test "inspect_entries returns stable scheduler entry dto with state" do
    version = scheduler_manifest_version("mv_scheduler_inspect")
    assert :ok = FavnOrchestrator.register_manifest(version)
    assert :ok = FavnOrchestrator.activate_manifest(version.manifest_version_id)

    name = unique_runtime_name()
    start_runtime(name)

    [raw_entry] = Runtime.scheduled(name)

    state = %State{
      pipeline_module: raw_entry.module,
      schedule_id: raw_entry.schedule.name,
      schedule_fingerprint: raw_entry.schedule_fingerprint,
      last_due_at: DateTime.utc_now(),
      in_flight_run_id: "run_inflight",
      activation_state: :enabled,
      version: 2
    }

    assert :ok = Storage.put_scheduler_state({raw_entry.module, raw_entry.schedule.name}, state)
    assert :ok = Runtime.reload(name)

    [entry] = Runtime.inspect_entries(name)
    assert %FavnOrchestrator.SchedulerEntry{} = entry
    assert entry.pipeline_module == MyApp.Pipelines.Daily
    assert entry.schedule_id == :daily
    assert entry.cron == raw_entry.schedule.cron
    assert entry.timezone == raw_entry.schedule.timezone
    assert entry.in_flight_run_id == "run_inflight"
    assert %DateTime{} = entry.last_due_at
  end

  test "new schedules default to pending activation and do not submit" do
    version = scheduler_manifest_version("mv_scheduler_pending_activation")
    assert :ok = FavnOrchestrator.register_manifest(version)
    assert :ok = FavnOrchestrator.activate_manifest(version.manifest_version_id)

    name = unique_runtime_name()
    start_runtime(name)
    [entry] = Runtime.scheduled(name)

    assert {:ok, %State{} = stored_state} =
             Storage.get_scheduler_state({entry.module, entry.schedule.name})

    assert stored_state.activation_state == :pending_activation

    assert :ok = Runtime.tick(name)
    assert {:ok, runs} = Storage.list_runs()
    refute Enum.any?(runs, &(&1.manifest_version_id == version.manifest_version_id))

    [inspection] = Runtime.inspect_entries(name)
    assert inspection.activation_state == :pending_activation
    refute inspection.effective_enabled?
    assert inspection.runtime_state == :inactive
  end

  test "changed schedule fingerprints are marked needs review" do
    version = scheduler_manifest_version("mv_scheduler_review_before", cron: "0 * * * *")
    changed = scheduler_manifest_version("mv_scheduler_review_after", cron: "*/5 * * * *")

    assert :ok = FavnOrchestrator.register_manifest(version)
    assert :ok = FavnOrchestrator.register_manifest(changed)
    assert :ok = FavnOrchestrator.activate_manifest(version.manifest_version_id)

    name = unique_runtime_name()
    start_runtime(name)
    [entry] = Runtime.scheduled(name)

    assert {:ok, %State{} = stored_state} =
             Storage.get_scheduler_state({entry.module, entry.schedule.name})

    assert :ok =
             Storage.put_scheduler_state({entry.module, entry.schedule.name}, %{
               stored_state
               | activation_state: :enabled,
                 version: stored_state.version + 1
             })

    assert :ok = FavnOrchestrator.activate_manifest(changed.manifest_version_id)
    assert :ok = Runtime.reload(name)

    [inspection] = Runtime.inspect_entries(name)
    assert inspection.activation_state == :needs_review
    refute inspection.effective_enabled?
    assert inspection.runtime_state == :inactive
  end

  test "existing schedules preserve activation state across reload" do
    version = scheduler_manifest_version("mv_scheduler_preserve_activation")
    assert :ok = FavnOrchestrator.register_manifest(version)
    assert :ok = FavnOrchestrator.activate_manifest(version.manifest_version_id)

    name = unique_runtime_name()
    start_runtime(name)
    [entry] = Runtime.scheduled(name)

    assert {:ok, %State{} = stored_state} =
             Storage.get_scheduler_state({entry.module, entry.schedule.name})

    assert :ok =
             Storage.put_scheduler_state({entry.module, entry.schedule.name}, %{
               stored_state
               | activation_state: :enabled,
                 version: stored_state.version + 1
             })

    assert :ok = Runtime.reload(name)
    [inspection] = Runtime.inspect_entries(name)

    assert inspection.activation_state == :enabled
    assert inspection.effective_enabled?
  end

  test "scheduler startup propagates scheduler state bootstrap write errors" do
    version = scheduler_manifest_version("mv_scheduler_bootstrap_put_error")
    assert :ok = FavnOrchestrator.register_manifest(version)
    assert :ok = FavnOrchestrator.activate_manifest(version.manifest_version_id)

    Application.put_env(:favn_orchestrator, :storage_adapter, SchedulerStateFailingStorageAdapter)

    Application.put_env(:favn_orchestrator, :storage_adapter_opts,
      put_error: :scheduler_state_put_failed
    )

    previous_flag = Process.flag(:trap_exit, true)

    try do
      assert {:error, :scheduler_state_put_failed} =
               Runtime.start_link(name: unique_runtime_name(), tick_ms: 60_000, auto_tick?: false)
    after
      Process.flag(:trap_exit, previous_flag)
    end
  end

  test "scheduler reload propagates scheduler state read errors" do
    version = scheduler_manifest_version("mv_scheduler_reload_get_error")
    assert :ok = FavnOrchestrator.register_manifest(version)
    assert :ok = FavnOrchestrator.activate_manifest(version.manifest_version_id)

    name = unique_runtime_name()
    start_runtime(name)

    Application.put_env(:favn_orchestrator, :storage_adapter, SchedulerStateFailingStorageAdapter)

    Application.put_env(:favn_orchestrator, :storage_adapter_opts,
      get_error: :scheduler_state_get_failed
    )

    assert {:error, :scheduler_state_get_failed} = Runtime.reload(name)
  end

  test "schedule list fallback propagates scheduler state read errors" do
    version = scheduler_manifest_version("mv_scheduler_list_get_error")
    assert :ok = FavnOrchestrator.register_manifest(version)
    assert :ok = FavnOrchestrator.activate_manifest(version.manifest_version_id)

    Application.put_env(:favn_orchestrator, :storage_adapter, SchedulerStateFailingStorageAdapter)

    Application.put_env(:favn_orchestrator, :storage_adapter_opts,
      get_error: :scheduler_state_get_failed
    )

    assert {:error, :scheduler_state_get_failed} = FavnOrchestrator.list_schedule_entries()
  end

  test "schedule list fallback ignores unscheduled windowed pipelines" do
    version =
      scheduler_manifest_version("mv_scheduler_unscheduled_window",
        schedule: nil,
        schedules: [],
        window: :month
      )

    assert :ok = FavnOrchestrator.register_manifest(version)
    assert :ok = FavnOrchestrator.activate_manifest(version.manifest_version_id)

    assert {:ok, []} = FavnOrchestrator.list_schedule_entries()
  end

  test "schedule list fallback propagates scheduler state bootstrap write errors" do
    version = scheduler_manifest_version("mv_scheduler_list_put_error")
    assert :ok = FavnOrchestrator.register_manifest(version)
    assert :ok = FavnOrchestrator.activate_manifest(version.manifest_version_id)

    Application.put_env(:favn_orchestrator, :storage_adapter, SchedulerStateFailingStorageAdapter)

    Application.put_env(:favn_orchestrator, :storage_adapter_opts,
      put_error: :scheduler_state_put_failed
    )

    assert {:error, :scheduler_state_put_failed} = FavnOrchestrator.list_schedule_entries()
  end

  test "activation commands propagate scheduler state write errors" do
    version = scheduler_manifest_version("mv_scheduler_activation_put_error")
    assert :ok = FavnOrchestrator.register_manifest(version)
    assert :ok = FavnOrchestrator.activate_manifest(version.manifest_version_id)

    name = unique_runtime_name()
    start_runtime(name)
    [entry] = Runtime.scheduled(name)

    assert {:ok, %State{} = stored_state} =
             Storage.get_scheduler_state({entry.module, entry.schedule.name})

    assert :ok =
             Storage.put_scheduler_state({entry.module, entry.schedule.name}, %{
               stored_state
               | activation_state: :disabled,
                 version: stored_state.version + 1
             })

    Application.put_env(:favn_orchestrator, :storage_adapter, SchedulerStateFailingStorageAdapter)

    Application.put_env(:favn_orchestrator, :storage_adapter_opts,
      put_error: :scheduler_state_put_failed
    )

    schedule_entry_id = "schedule:#{entry.module}:#{entry.schedule.name}"

    assert {:error, :scheduler_state_put_failed} =
             FavnOrchestrator.enable_schedule(schedule_entry_id)
  end

  test "tick retries dirty scheduler state persistence before advancing" do
    version = scheduler_manifest_version("mv_scheduler_dirty_retry")
    assert :ok = FavnOrchestrator.register_manifest(version)
    assert :ok = FavnOrchestrator.activate_manifest(version.manifest_version_id)

    name = unique_runtime_name()
    start_runtime(name)
    [entry] = Runtime.scheduled(name)

    state = %State{
      pipeline_module: entry.module,
      schedule_id: entry.schedule.name,
      schedule_fingerprint: entry.schedule_fingerprint,
      last_due_at: DateTime.add(DateTime.utc_now(), -120, :second),
      activation_state: :enabled,
      version: 2
    }

    assert :ok = Storage.put_scheduler_state({entry.module, entry.schedule.name}, state)
    assert :ok = Runtime.reload(name)

    Application.put_env(:favn_orchestrator, :storage_adapter, SchedulerStateFailingStorageAdapter)

    Application.put_env(:favn_orchestrator, :storage_adapter_opts,
      put_error: :scheduler_state_put_failed
    )

    assert {:error, :scheduler_state_put_failed} = Runtime.tick(name)
    assert {:ok, run} = await_run_submission(version.manifest_version_id)

    assert {:ok, diagnostics} = Runtime.diagnostics(name)
    assert diagnostics.dirty_scheduler_state_count == 1
    assert diagnostics.state_summary.dirty_count == 1
    assert diagnostics.last_scheduler_persist_error.reason == ":scheduler_state_put_failed"

    Application.put_env(:favn_orchestrator, :storage_adapter, Memory)
    Application.put_env(:favn_orchestrator, :storage_adapter_opts, [])

    assert {:ok, %State{} = stale_state} =
             Storage.get_scheduler_state({entry.module, entry.schedule.name})

    assert stale_state.last_submitted_due_at == nil

    assert :ok = Runtime.tick(name)

    assert {:ok, %State{} = retried_state} =
             Storage.get_scheduler_state({entry.module, entry.schedule.name})

    assert %DateTime{} = retried_state.last_submitted_due_at
    assert retried_state.last_submitted_due_at == run.trigger.occurrence.due_at

    assert {:ok, diagnostics} = Runtime.diagnostics(name)
    assert diagnostics.dirty_scheduler_state_count == 0
    assert diagnostics.state_summary.dirty_count == 0
    assert diagnostics.last_scheduler_persist_error == nil

    assert {:ok, runs} = Storage.list_runs()
    assert Enum.count(runs, &(&1.manifest_version_id == version.manifest_version_id)) == 1
  end

  test "enable schedule facade changes effective state without catch-up" do
    version = scheduler_manifest_version("mv_scheduler_enable_facade")
    assert :ok = FavnOrchestrator.register_manifest(version)
    assert :ok = FavnOrchestrator.activate_manifest(version.manifest_version_id)

    name = unique_runtime_name()
    start_runtime(name)
    [entry] = Runtime.scheduled(name)
    schedule_entry_id = "schedule:#{entry.module}:#{entry.schedule.name}"

    assert {:ok, enabled} = FavnOrchestrator.enable_schedule(schedule_entry_id)
    assert enabled.activation_state == :enabled
    assert enabled.effective_enabled?

    assert :ok = Runtime.reload(name)
    assert :ok = Runtime.tick(name)

    assert {:ok, runs} = Storage.list_runs()
    refute Enum.any?(runs, &(&1.manifest_version_id == version.manifest_version_id))
  end

  test "disable schedule facade prevents future submissions but preserves running run" do
    version = scheduler_manifest_version("mv_scheduler_disable_facade")
    assert :ok = FavnOrchestrator.register_manifest(version)
    assert :ok = FavnOrchestrator.activate_manifest(version.manifest_version_id)

    name = unique_runtime_name()
    start_runtime(name)
    [entry] = Runtime.scheduled(name)
    running = running_run_state("run_disable_inflight", version, %{kind: :schedule})
    assert :ok = Storage.put_run(running)

    state = %State{
      pipeline_module: entry.module,
      schedule_id: entry.schedule.name,
      schedule_fingerprint: entry.schedule_fingerprint,
      last_due_at: DateTime.add(DateTime.utc_now(), -120, :second),
      in_flight_run_id: running.id,
      activation_state: :enabled,
      version: 2
    }

    assert :ok = Storage.put_scheduler_state({entry.module, entry.schedule.name}, state)
    schedule_entry_id = "schedule:#{entry.module}:#{entry.schedule.name}"

    assert {:ok, disabled} = FavnOrchestrator.disable_schedule(schedule_entry_id)
    assert disabled.activation_state == :disabled
    refute disabled.effective_enabled?
    assert disabled.in_flight_run_id == running.id

    assert :ok = Runtime.reload(name)
    assert :ok = Runtime.tick(name)

    assert {:ok, runs} = Storage.list_runs()

    assert Enum.count(Enum.filter(runs, &(&1.manifest_version_id == version.manifest_version_id))) ==
             1
  end

  test "occurrence preview is computed by orchestrator with window state" do
    version = scheduler_manifest_version("mv_scheduler_occurrence_preview", window: :day)
    assert :ok = FavnOrchestrator.register_manifest(version)
    assert :ok = FavnOrchestrator.activate_manifest(version.manifest_version_id)

    name = unique_runtime_name()
    start_runtime(name)
    [entry] = Runtime.scheduled(name)
    schedule_entry_id = "schedule:#{entry.module}:#{entry.schedule.name}"

    assert {:ok, occurrences} =
             FavnOrchestrator.preview_schedule_occurrences(schedule_entry_id, limit: 3)

    assert length(occurrences) == 3
    assert Enum.all?(occurrences, &match?(%FavnOrchestrator.ScheduleOccurrencePreview{}, &1))
    assert Enum.all?(occurrences, &(&1.schedule_entry_id == schedule_entry_id))
    assert Enum.all?(occurrences, &(&1.status == :disabled))
    assert Enum.all?(occurrences, &is_map(&1.window))
    assert Enum.all?(occurrences, &("Will not submit until enabled" in &1.notes))
  end

  test "tick submits scheduled pipeline run and persists scheduler state" do
    version = scheduler_manifest_version("mv_scheduler_tick")
    assert :ok = FavnOrchestrator.register_manifest(version)
    assert :ok = FavnOrchestrator.activate_manifest(version.manifest_version_id)

    name = unique_runtime_name()
    start_runtime(name)

    [entry] = Runtime.scheduled(name)

    state = %State{
      pipeline_module: entry.module,
      schedule_id: entry.schedule.name,
      schedule_fingerprint: entry.schedule_fingerprint,
      last_due_at: DateTime.add(DateTime.utc_now(), -120, :second),
      activation_state: :enabled,
      version: 2
    }

    assert :ok = Storage.put_scheduler_state({entry.module, entry.schedule.name}, state)
    assert :ok = Runtime.reload(name)
    assert :ok = Runtime.tick(name)

    assert {:ok, run} = await_run_submission(version.manifest_version_id)
    assert run.manifest_version_id == version.manifest_version_id
    assert run.submit_kind == :pipeline
    assert run.trigger[:kind] == :schedule

    assert {:ok, %State{} = stored_state} =
             Storage.get_scheduler_state({entry.module, entry.schedule.name})

    assert %DateTime{} = stored_state.last_submitted_due_at
  end

  test "tick supports six-field cron schedules with seconds" do
    version = scheduler_manifest_version("mv_scheduler_seconds", cron: "*/15 * * * * *")
    assert :ok = FavnOrchestrator.register_manifest(version)
    assert :ok = FavnOrchestrator.activate_manifest(version.manifest_version_id)

    name = unique_runtime_name()
    start_runtime(name)

    [entry] = Runtime.scheduled(name)

    state = %State{
      pipeline_module: entry.module,
      schedule_id: entry.schedule.name,
      schedule_fingerprint: entry.schedule_fingerprint,
      last_due_at: DateTime.add(DateTime.utc_now(), -30, :second),
      activation_state: :enabled,
      version: 2
    }

    assert :ok = Storage.put_scheduler_state({entry.module, entry.schedule.name}, state)
    assert :ok = Runtime.reload(name)
    assert :ok = Runtime.tick(name)

    assert {:ok, run} = await_run_submission(version.manifest_version_id)
    assert run.trigger[:kind] == :schedule
  end

  test "overlap allow submits a new run even when one is in flight" do
    version = scheduler_manifest_version("mv_scheduler_overlap_allow", overlap: :allow)
    assert :ok = FavnOrchestrator.register_manifest(version)
    assert :ok = FavnOrchestrator.activate_manifest(version.manifest_version_id)

    name = unique_runtime_name()
    start_runtime(name, auto_tick?: false)
    [entry] = Runtime.scheduled(name)

    running = running_run_state("run_inflight_allow", version, %{kind: :schedule})
    assert :ok = Storage.put_run(running)

    state = %State{
      pipeline_module: entry.module,
      schedule_id: entry.schedule.name,
      schedule_fingerprint: entry.schedule_fingerprint,
      in_flight_run_id: running.id,
      last_due_at: DateTime.add(DateTime.utc_now(), -120, :second),
      activation_state: :enabled,
      version: 2
    }

    assert :ok = Storage.put_scheduler_state({entry.module, entry.schedule.name}, state)
    assert :ok = Runtime.reload(name)
    assert :ok = Runtime.tick(name)

    assert {:ok, runs} = Storage.list_runs()
    assert Enum.count(runs) >= 2
  end

  test "overlap forbid skips submission while one is in flight" do
    version = scheduler_manifest_version("mv_scheduler_overlap_forbid", overlap: :forbid)
    assert :ok = FavnOrchestrator.register_manifest(version)
    assert :ok = FavnOrchestrator.activate_manifest(version.manifest_version_id)

    name = unique_runtime_name()
    start_runtime(name)
    [entry] = Runtime.scheduled(name)

    running = running_run_state("run_inflight_forbid", version, %{kind: :schedule})
    assert :ok = Storage.put_run(running)

    state = %State{
      pipeline_module: entry.module,
      schedule_id: entry.schedule.name,
      schedule_fingerprint: entry.schedule_fingerprint,
      in_flight_run_id: running.id,
      last_due_at: DateTime.add(DateTime.utc_now(), -120, :second),
      activation_state: :enabled,
      version: 2
    }

    assert :ok = Storage.put_scheduler_state({entry.module, entry.schedule.name}, state)
    assert :ok = Runtime.reload(name)
    assert :ok = Runtime.tick(name)

    assert {:ok, %State{} = stored_state} =
             Storage.get_scheduler_state({entry.module, entry.schedule.name})

    assert stored_state.in_flight_run_id == running.id
    assert is_nil(stored_state.queued_due_at)
  end

  test "overlap queue_one queues then submits after in-flight run finishes" do
    version = scheduler_manifest_version("mv_scheduler_overlap_queue", overlap: :queue_one)
    assert :ok = FavnOrchestrator.register_manifest(version)
    assert :ok = FavnOrchestrator.activate_manifest(version.manifest_version_id)

    name = unique_runtime_name()
    start_runtime(name)
    [entry] = Runtime.scheduled(name)

    running = running_run_state("run_inflight_queue", version, %{kind: :schedule})
    assert :ok = Storage.put_run(running)

    state = %State{
      pipeline_module: entry.module,
      schedule_id: entry.schedule.name,
      schedule_fingerprint: entry.schedule_fingerprint,
      in_flight_run_id: running.id,
      last_due_at: DateTime.add(DateTime.utc_now(), -120, :second),
      activation_state: :enabled,
      version: 2
    }

    assert :ok = Storage.put_scheduler_state({entry.module, entry.schedule.name}, state)
    assert :ok = Runtime.reload(name)
    assert :ok = Runtime.tick(name)

    assert {:ok, %State{} = queued} =
             Storage.get_scheduler_state({entry.module, entry.schedule.name})

    assert %DateTime{} = queued.queued_due_at

    finished =
      %{running | status: :ok, event_seq: running.event_seq + 1} |> RunState.with_snapshot_hash()

    assert :ok = Storage.put_run(finished)

    assert :ok = Runtime.tick(name)
    assert {:ok, runs} = Storage.list_runs()
    assert Enum.count(runs) >= 2
  end

  test "missed policies select different occurrence counts" do
    skip_count = missed_submission_count(:skip)
    one_count = missed_submission_count(:one)
    all_count = missed_submission_count(:all)

    assert skip_count >= 1
    assert one_count >= 1
    assert all_count >= 2
    assert all_count >= skip_count
    assert all_count >= one_count
  end

  test "missed all caps high-frequency six-field catch-up per tick" do
    Application.put_env(:favn_orchestrator, :scheduler, max_missed_all_occurrences: 3)

    version =
      scheduler_manifest_version("mv_scheduler_missed_all_cap",
        cron: "* * * * * *",
        missed: :all,
        overlap: :allow
      )

    assert :ok = FavnOrchestrator.register_manifest(version)
    assert :ok = FavnOrchestrator.activate_manifest(version.manifest_version_id)

    name = unique_runtime_name()
    start_runtime(name)
    [entry] = Runtime.scheduled(name)

    state = %State{
      pipeline_module: entry.module,
      schedule_id: entry.schedule.name,
      schedule_fingerprint: entry.schedule_fingerprint,
      last_due_at: DateTime.add(DateTime.utc_now(), -20, :second),
      activation_state: :enabled,
      version: 2
    }

    assert :ok = Storage.put_scheduler_state({entry.module, entry.schedule.name}, state)
    assert :ok = Runtime.reload(name)

    log = capture_log(fn -> assert :ok = Runtime.tick(name) end)

    assert log =~ "scheduler missed occurrence catch-up capped"
    assert log =~ "pipeline=MyApp.Pipelines.Daily"
    assert log =~ "schedule_id=:daily"
    assert log =~ "schedule_ref={MyApp.Schedules, :daily}"
    assert log =~ "cron=\"* * * * * *\""
    assert log =~ "cap=3"
    assert log =~ "selected=3"
    assert log =~ "observed=4"

    assert {:ok, runs} = Storage.list_runs()

    capped_runs = Enum.filter(runs, &(&1.manifest_version_id == version.manifest_version_id))
    assert length(capped_runs) == 3
  end

  test "missed all honors per-tick submission budget below catch-up cap" do
    Application.put_env(:favn_orchestrator, :scheduler,
      max_missed_all_occurrences: 10,
      submission_budget: 2
    )

    version =
      scheduler_manifest_version("mv_scheduler_submission_budget",
        cron: "* * * * * *",
        missed: :all,
        overlap: :allow
      )

    assert :ok = FavnOrchestrator.register_manifest(version)
    assert :ok = FavnOrchestrator.activate_manifest(version.manifest_version_id)

    name = unique_runtime_name()
    start_runtime(name)
    [entry] = Runtime.scheduled(name)

    state = %State{
      pipeline_module: entry.module,
      schedule_id: entry.schedule.name,
      schedule_fingerprint: entry.schedule_fingerprint,
      last_due_at: DateTime.add(DateTime.utc_now(), -10, :second),
      activation_state: :enabled,
      version: 2
    }

    assert :ok = Storage.put_scheduler_state({entry.module, entry.schedule.name}, state)
    assert :ok = Runtime.reload(name)
    assert :ok = Runtime.tick(name)

    assert {:ok, runs} = Storage.list_runs()

    budgeted_runs = Enum.filter(runs, &(&1.manifest_version_id == version.manifest_version_id))
    assert length(budgeted_runs) == 2

    first_occurrence_keys = occurrence_keys(budgeted_runs)
    assert length(first_occurrence_keys) == 2

    assert :ok = Runtime.tick(name)
    assert {:ok, runs} = Storage.list_runs()

    continued_runs = Enum.filter(runs, &(&1.manifest_version_id == version.manifest_version_id))
    continued_occurrence_keys = occurrence_keys(continued_runs)

    assert length(continued_runs) == 4
    assert length(continued_occurrence_keys) == 4
    assert Enum.uniq(continued_occurrence_keys) == continued_occurrence_keys
  end

  test "windowed scheduled pipelines carry anchor window into run pipeline context" do
    version = scheduler_manifest_version("mv_scheduler_window", window: :hour)
    assert :ok = FavnOrchestrator.register_manifest(version)
    assert :ok = FavnOrchestrator.activate_manifest(version.manifest_version_id)

    name = unique_runtime_name()
    start_supervised!({Runtime, name: name, tick_ms: 60_000, auto_tick?: false})
    [entry] = Runtime.scheduled(name)

    state = %State{
      pipeline_module: entry.module,
      schedule_id: entry.schedule.name,
      schedule_fingerprint: entry.schedule_fingerprint,
      last_due_at: DateTime.add(DateTime.utc_now(), -120, :second),
      activation_state: :enabled,
      version: 2
    }

    assert :ok = Storage.put_scheduler_state({entry.module, entry.schedule.name}, state)
    assert :ok = Runtime.reload(name)
    assert :ok = Runtime.tick(name)

    assert {:ok, run} = await_run_submission(version.manifest_version_id)
    assert run.trigger[:kind] == :schedule
    assert run.metadata[:pipeline_context][:anchor_window].kind == :hour
  end

  test "monthly anchor windows work in IANA timezones without global timezone config" do
    version =
      scheduler_manifest_version("mv_scheduler_window_month_oslo",
        timezone: "Europe/Oslo",
        window: :month
      )

    assert :ok = FavnOrchestrator.register_manifest(version)
    assert :ok = FavnOrchestrator.activate_manifest(version.manifest_version_id)

    name = unique_runtime_name()
    start_supervised!({Runtime, name: name, tick_ms: 60_000, auto_tick?: false})
    [entry] = Runtime.scheduled(name)

    state = %State{
      pipeline_module: entry.module,
      schedule_id: entry.schedule.name,
      schedule_fingerprint: entry.schedule_fingerprint,
      last_due_at: DateTime.add(DateTime.utc_now(), -120, :second),
      activation_state: :enabled,
      version: 2
    }

    assert :ok = Storage.put_scheduler_state({entry.module, entry.schedule.name}, state)
    assert :ok = Runtime.reload(name)
    assert :ok = Runtime.tick(name)

    assert {:ok, run} = await_run_submission(version.manifest_version_id)
    anchor_window = run.metadata[:pipeline_context][:anchor_window]

    assert anchor_window.kind == :month
    assert anchor_window.timezone == "Europe/Oslo"
    assert anchor_window.start_at.time_zone == "Europe/Oslo"
    assert anchor_window.end_at.time_zone == "Europe/Oslo"
  end

  test "invalid scheduled window policy data fails entry without crashing runtime" do
    version = scheduler_manifest_version("mv_scheduler_invalid_window", window: :hour)
    assert :ok = FavnOrchestrator.register_manifest(version)
    assert :ok = FavnOrchestrator.activate_manifest(version.manifest_version_id)

    name = unique_runtime_name()
    pid = start_runtime(name)
    [entry] = Runtime.scheduled(name)

    state = %State{
      pipeline_module: entry.module,
      schedule_id: entry.schedule.name,
      schedule_fingerprint: entry.schedule_fingerprint,
      last_due_at: DateTime.add(DateTime.utc_now(), -120, :second),
      activation_state: :enabled,
      version: 2
    }

    assert :ok = Storage.put_scheduler_state({entry.module, entry.schedule.name}, state)
    assert :ok = Runtime.reload(name)

    :sys.replace_state(pid, fn runtime_state ->
      put_in(runtime_state, [:entries, entry.module, :window], %Policy{
        kind: :hour,
        timezone: "Invalid/Timezone"
      })
    end)

    log = capture_log(fn -> assert :ok = Runtime.tick(name) end)

    assert Process.alive?(pid)
    assert log =~ "scheduler submit failed"
    assert log =~ "invalid_scheduled_window_policy"

    assert {:ok, runs} = Storage.list_runs()
    refute Enum.any?(runs, &(&1.manifest_version_id == version.manifest_version_id))

    assert {:ok, %State{} = stored_state} =
             Storage.get_scheduler_state({entry.module, entry.schedule.name})

    assert is_nil(stored_state.last_submitted_due_at)

    [inspection] = Runtime.inspect_entries(name)
    assert %FavnOrchestrator.SchedulerError{} = inspection.last_scheduler_error
    assert inspection.last_scheduler_error.phase == :submit_run
  end

  defp await_run_submission(manifest_version_id, attempts \\ 40)

  defp await_run_submission(manifest_version_id, attempts) when attempts > 0 do
    case Storage.list_runs() do
      {:ok, runs} when is_list(runs) ->
        case Enum.find(runs, &(&1.manifest_version_id == manifest_version_id)) do
          %RunState{} = run ->
            {:ok, run}

          nil ->
            Process.sleep(25)
            await_run_submission(manifest_version_id, attempts - 1)
        end

      {:ok, _other} ->
        Process.sleep(25)
        await_run_submission(manifest_version_id, attempts - 1)

      {:error, _reason} = error ->
        error
    end
  end

  defp await_run_submission(_manifest_version_id, 0),
    do: {:error, :timeout_waiting_for_scheduled_run}

  defp unique_runtime_name do
    Module.concat(__MODULE__, "Runtime#{System.unique_integer([:positive])}")
  end

  defp start_runtime(name, opts \\ []) do
    runtime_opts = Keyword.merge([name: name, tick_ms: 60_000, auto_tick?: false], opts)

    start_supervised!(%{
      id: {Runtime, name},
      start: {Runtime, :start_link, [runtime_opts]}
    })
  end

  defp missed_submission_count(policy) do
    Memory.reset()

    version =
      scheduler_manifest_version("mv_scheduler_missed_#{policy}", missed: policy, overlap: :allow)

    assert :ok = FavnOrchestrator.register_manifest(version)
    assert :ok = FavnOrchestrator.activate_manifest(version.manifest_version_id)

    name = unique_runtime_name()
    pid = start_runtime(name)
    [entry] = Runtime.scheduled(name)

    state = %State{
      pipeline_module: entry.module,
      schedule_id: entry.schedule.name,
      schedule_fingerprint: entry.schedule_fingerprint,
      last_due_at: DateTime.add(DateTime.utc_now(), -180, :second),
      activation_state: :enabled,
      version: 2
    }

    assert :ok = Storage.put_scheduler_state({entry.module, entry.schedule.name}, state)
    assert :ok = Runtime.reload(name)
    assert :ok = Runtime.tick(name)
    {:ok, runs} = Storage.list_runs()
    GenServer.stop(pid)
    length(runs)
  end

  defp occurrence_keys(runs) do
    runs
    |> Enum.map(& &1.trigger.occurrence.occurrence_key)
    |> Enum.sort()
  end

  defp running_run_state(run_id, version, trigger) do
    RunState.new(
      id: run_id,
      manifest_version_id: version.manifest_version_id,
      manifest_content_hash: version.content_hash,
      asset_ref: {MyApp.Assets.Gold, :asset},
      target_refs: [{MyApp.Assets.Gold, :asset}],
      trigger: trigger
    )
    |> RunState.transition(status: :running)
  end

  defp scheduler_manifest_version(manifest_version_id, opts \\ []) do
    schedules =
      Keyword.get(opts, :schedules, [
        %Schedule{
          module: MyApp.Schedules,
          name: :daily,
          ref: {MyApp.Schedules, :daily},
          cron: Keyword.get(opts, :cron, "* * * * *"),
          timezone: Keyword.get(opts, :timezone, "Etc/UTC"),
          missed: Keyword.get(opts, :missed, :one),
          overlap: Keyword.get(opts, :overlap, :forbid),
          active: true
        }
      ])

    manifest = %Manifest{
      assets: [
        %Asset{ref: {MyApp.Assets.Raw, :asset}, module: MyApp.Assets.Raw, name: :asset},
        %Asset{
          ref: {MyApp.Assets.Gold, :asset},
          module: MyApp.Assets.Gold,
          name: :asset,
          depends_on: [{MyApp.Assets.Raw, :asset}]
        }
      ],
      pipelines: [
        %Pipeline{
          module: MyApp.Pipelines.Daily,
          name: :daily,
          selectors: [{:asset, {MyApp.Assets.Gold, :asset}}],
          deps: :all,
          schedule: Keyword.get(opts, :schedule, {:ref, {MyApp.Schedules, :daily}}),
          window: Keyword.get(opts, :window),
          metadata: %{owner: :scheduler}
        }
      ],
      schedules: schedules
    }

    {:ok, version} = Version.new(manifest, manifest_version_id: manifest_version_id)
    version
  end

  defp restore_env(app, key, nil), do: Application.delete_env(app, key)
  defp restore_env(app, key, value), do: Application.put_env(app, key, value)
end
