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

  setup do
    previous_storage_adapter = Application.get_env(:favn_orchestrator, :storage_adapter)
    previous_storage_opts = Application.get_env(:favn_orchestrator, :storage_adapter_opts)
    previous_client = Application.get_env(:favn_orchestrator, :runner_client)
    previous_opts = Application.get_env(:favn_orchestrator, :runner_client_opts)
    previous_scheduler = Application.get_env(:favn_orchestrator, :scheduler)

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
      version: 1
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
      version: 1
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
      version: 1
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
      version: 1
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
      version: 1
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
      version: 1
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
      version: 1
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
      version: 1
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
      version: 1
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
      version: 1
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
      version: 1
    }

    assert :ok = Storage.put_scheduler_state({entry.module, entry.schedule.name}, state)
    assert :ok = Runtime.reload(name)
    assert :ok = Runtime.tick(name)
    {:ok, runs} = Storage.list_runs()
    GenServer.stop(pid)
    length(runs)
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
          schedule: {:ref, {MyApp.Schedules, :daily}},
          window: Keyword.get(opts, :window),
          metadata: %{owner: :scheduler}
        }
      ],
      schedules: [
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
      ]
    }

    {:ok, version} = Version.new(manifest, manifest_version_id: manifest_version_id)
    version
  end

  defp restore_env(app, key, nil), do: Application.delete_env(app, key)
  defp restore_env(app, key, value), do: Application.put_env(app, key, value)
end
