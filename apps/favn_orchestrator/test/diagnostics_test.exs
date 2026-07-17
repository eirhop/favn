defmodule FavnOrchestrator.DiagnosticsTest do
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias Favn.Contracts.RunnerClient
  alias Favn.Manifest
  alias Favn.Manifest.Pipeline
  alias Favn.Manifest.Schedule
  alias Favn.Manifest.Version
  alias Favn.Scheduler.State
  alias Favn.Window.Policy
  alias FavnOrchestrator.Diagnostics
  alias FavnOrchestrator.OperationalEvents
  alias FavnOrchestrator.ProjectionDiagnostics
  alias FavnOrchestrator.RunState
  alias FavnOrchestrator.Scheduler.Runtime, as: SchedulerRuntime
  alias FavnOrchestrator.Storage
  alias FavnOrchestrator.Storage.Adapter.Memory

  defmodule RunnerClientStub do
    @behaviour RunnerClient

    @impl true
    def register_manifest(_version, _opts), do: :ok

    @impl true
    def submit_work(_work, _opts), do: {:ok, "execution"}

    @impl true
    def await_result(_execution_id, _timeout, _opts), do: {:error, :not_used}

    @impl true
    def cancel_work(_execution_id, _reason, _opts), do: :ok

    @impl true
    def inspect_relation(_request, _opts), do: {:error, :not_used}

    @impl true
    def diagnostics(_opts),
      do: Process.get(:runner_diagnostics_result, {:ok, %{available?: true}})
  end

  defmodule StorageDiagnosticsStub do
    @behaviour Favn.Storage.Adapter

    @impl true
    def child_spec(_opts), do: :none

    @impl true
    def diagnostics(opts), do: Keyword.fetch!(opts, :diagnostics_result)

    @impl true
    def put_manifest_version(_version, _opts), do: :ok

    @impl true
    def put_execution_packages(_packages, _opts), do: :ok

    @impl true
    def missing_execution_package_hashes(hashes, _opts), do: {:ok, hashes}

    @impl true
    def get_execution_package(_hash, _opts), do: {:error, :not_found}

    @impl true
    def get_manifest_version(_manifest_version_id, _opts),
      do: {:error, :manifest_version_not_found}

    @impl true
    def get_manifest_version_by_content_hash(_content_hash, _opts),
      do: {:error, :manifest_version_not_found}

    @impl true
    def list_manifest_versions(_opts), do: {:ok, []}

    @impl true
    def set_active_manifest_version(_manifest_version_id, _opts), do: :ok

    @impl true
    def get_active_manifest_version(_opts), do: {:error, :active_manifest_not_set}

    @impl true
    def put_run(_run_state, _opts), do: :ok

    @impl true
    def get_run(_run_id, _opts), do: {:error, :not_found}

    @impl true
    def list_runs(_opts, _adapter_opts), do: {:ok, []}

    @impl true
    def list_target_runs(
          _manifest_version_id,
          _target_kind,
          _target_ref,
          _run_opts,
          _adapter_opts
        ),
        do: {:ok, []}

    @impl true
    def persist_run_transition(_run_state, _event, _opts), do: :ok

    @impl true
    def append_run_event(_run_id, _event, _opts), do: :ok

    @impl true
    def list_run_events(_run_id, _opts), do: {:ok, []}

    @impl true
    def list_global_run_events(_filters, _opts), do: {:ok, []}

    @impl true
    def put_execution_ownership(_ownership, _opts), do: :ok

    @impl true
    def get_execution_ownership(_ownership_id, _opts), do: {:error, :not_found}

    @impl true
    def list_execution_ownerships(_run_id, _opts), do: {:ok, []}

    @impl true
    def list_active_execution_ownerships(_run_id, _opts), do: {:ok, []}

    @impl true
    def try_acquire_execution_lease(lease, _opts), do: {:ok, lease}

    @impl true
    def release_execution_lease(_lease_id, _opts), do: :ok

    @impl true
    def release_execution_leases_for_run(run_id, _opts) do
      {:ok, FavnOrchestrator.ExecutionAdmission.LeaseRelease.new(run_id, 0, [])}
    end

    @impl true
    def expire_execution_leases(_now, _opts), do: {:ok, 0}

    @impl true
    def list_execution_leases(_opts), do: {:ok, []}

    @impl true
    def upsert_execution_admission_waiter(waiter, _opts), do: {:ok, waiter}

    @impl true
    def delete_execution_admission_waiter(_waiter_id, _opts), do: :ok

    @impl true
    def delete_execution_admission_waiters_for_run(_run_id, _opts), do: {:ok, 0}

    @impl true
    def list_execution_admission_waiters_for_scope(_scope, _waiter_opts, _opts), do: {:ok, []}

    @impl true
    def expire_execution_admission_waiters(_now, _opts), do: {:ok, 0}

    @impl true
    def persist_log_entries(entries, _opts), do: {:ok, entries}

    @impl true
    def list_logs(_filter, _opts, _adapter_opts),
      do:
        {:ok,
         %FavnOrchestrator.Page{
           items: [],
           limit: 100,
           offset: 0,
           has_more?: false,
           next_offset: nil
         }}

    @impl true
    def replay_logs_after(_cursor, _filter, _opts, _adapter_opts), do: {:ok, []}

    @impl true
    def put_scheduler_state(_key, _state, _opts), do: :ok

    @impl true
    def get_scheduler_state(_key, _opts), do: {:ok, nil}

    @impl true
    def put_coverage_baseline(_baseline, _opts), do: :ok

    @impl true
    def get_coverage_baseline(_baseline_id, _opts), do: {:error, :not_found}

    @impl true
    def list_coverage_baselines(_filters, _opts), do: {:ok, []}

    @impl true
    def put_backfill_window(_window, _opts), do: :ok

    @impl true
    def get_backfill_window(_backfill_run_id, _pipeline_module, _window_key, _opts),
      do: {:error, :not_found}

    @impl true
    def list_backfill_windows(_filters, _opts), do: {:ok, []}

    @impl true
    def scan_backfill_windows(_filters, scan_opts, _opts),
      do: {:ok, FavnOrchestrator.CursorPage.from_fetched([], scan_opts, fn _ -> nil end)}

    @impl true
    def apply_backfill_child_projection(_window, _states, _opts), do: {:error, :not_found}

    @impl true
    def get_backfill_progress(_backfill_run_id, _opts), do: {:error, :not_found}

    @impl true
    def rebuild_backfill_progress(_backfill_run_id, _opts), do: {:error, :not_found}

    @impl true
    def put_asset_window_state(_state, _opts), do: :ok

    @impl true
    def get_asset_window_state(_asset_ref_module, _asset_ref_name, _window_key, _opts),
      do: {:error, :not_found}

    @impl true
    def list_asset_window_states(_filters, _opts), do: {:ok, []}

    @impl true
    def put_asset_freshness_state(_state, _opts), do: :ok

    @impl true
    def get_asset_freshness_state(_asset_ref_module, _asset_ref_name, _freshness_key, _opts),
      do: {:error, :not_found}

    @impl true
    def list_asset_freshness_states(_filters, _opts), do: {:ok, []}

    @impl true
    def scan_asset_freshness_states(_filters, scan_opts, _opts),
      do: {:ok, FavnOrchestrator.CursorPage.from_fetched([], scan_opts, fn _ -> nil end)}

    @impl true
    def get_asset_freshness_states_by_keys(_keys, _opts), do: {:ok, %{}}

    @impl true
    def replace_backfill_read_models(_scope, _baselines, _windows, _states, _opts), do: :ok
  end

  setup do
    keys = [
      :storage_adapter,
      :storage_adapter_opts,
      :scheduler,
      :runner_client,
      :runner_client_opts,
      :metrics_hook
    ]

    previous = Map.new(keys, &{&1, Application.get_env(:favn_orchestrator, &1)})

    memory_server = Module.concat(__MODULE__, "Memory#{System.unique_integer([:positive])}")

    start_supervised!(%{
      id: memory_server,
      start: {Memory, :start_link, [[name: memory_server]]}
    })

    Application.put_env(:favn_orchestrator, :storage_adapter, Memory)
    Application.put_env(:favn_orchestrator, :storage_adapter_opts, server: memory_server)
    Application.put_env(:favn_orchestrator, :scheduler, enabled: false)
    Application.put_env(:favn_orchestrator, :runner_client, RunnerClientStub)
    Application.put_env(:favn_orchestrator, :runner_client_opts, [])
    Application.delete_env(:favn_orchestrator, :metrics_hook)
    Process.delete(:runner_diagnostics_result)
    ProjectionDiagnostics.reset()

    on_exit(fn ->
      Enum.each(previous, fn {key, value} -> restore_env(key, value) end)
      Process.delete(:runner_diagnostics_result)
      ProjectionDiagnostics.reset()
    end)

    %{memory_server: memory_server}
  end

  test "reports healthy diagnostics with active manifest and no runs" do
    version = manifest_version("mv_diagnostics_healthy")
    assert :ok = FavnOrchestrator.register_manifest(version)
    assert :ok = FavnOrchestrator.activate_manifest(version.manifest_version_id)

    assert %{status: :ok, checks: checks} = Diagnostics.report()
    assert check(checks, :storage_readiness).status == :ok
    assert check(checks, :active_manifest).details.manifest_version_id == "mv_diagnostics_healthy"
    assert check(checks, :scheduler).details.enabled == false
    assert check(checks, :runner).details.available? == true
    assert check(checks, :in_flight_runs).details.count == 0
    assert check(checks, :recent_failed_runs).details.count == 0
  end

  test "reports missing active manifest as degraded without failing storage" do
    assert %{status: :degraded, checks: checks} = Diagnostics.report()
    active = check(checks, :active_manifest)
    assert active.status == :warning
    assert active.reason == :active_manifest_not_set
  end

  test "reports degraded storage diagnostics and redacts secrets" do
    secret = "storage-token-secret"

    Application.put_env(:favn_orchestrator, :storage_adapter, StorageDiagnosticsStub)

    Application.put_env(:favn_orchestrator, :storage_adapter_opts,
      diagnostics_result: {:ok, %{ready?: false, status: :schema_not_ready, token: secret}}
    )

    assert %{status: :degraded, checks: checks} = Diagnostics.report()
    storage = check(checks, :storage_readiness)
    assert storage.status == :error
    refute inspect(storage) =~ secret
    assert storage.details.token == "[REDACTED]"
  end

  test "reports runner unavailable" do
    Process.put(:runner_diagnostics_result, {:error, {:runner_node_unreachable, :secret_node}})

    assert %{status: :degraded, checks: checks} = Diagnostics.report()
    runner = check(checks, :runner)
    assert runner.status == :error
    assert runner.reason == {:runner_node_unreachable, :secret_node}
  end

  test "reports scheduler disabled, enabled missing, and enabled running" do
    assert check(Diagnostics.report().checks, :scheduler).details.enabled == false

    missing_name = Module.concat(__MODULE__, MissingScheduler)
    Application.put_env(:favn_orchestrator, :scheduler, enabled: true, name: missing_name)

    assert %{status: :error, reason: :not_running} =
             check(Diagnostics.report().checks, :scheduler)

    version = manifest_version("mv_diagnostics_scheduler")
    assert :ok = FavnOrchestrator.register_manifest(version)
    assert :ok = FavnOrchestrator.activate_manifest(version.manifest_version_id)

    running_name = Module.concat(__MODULE__, RunningScheduler)
    Application.put_env(:favn_orchestrator, :scheduler, enabled: true, name: running_name)
    start_supervised!({SchedulerRuntime, name: running_name, tick_ms: 60_000, auto_tick?: false})

    scheduler = check(Diagnostics.report().checks, :scheduler)
    assert scheduler.status == :ok
    assert scheduler.details.running? == true
    assert scheduler.details.manifest_version_id == "mv_diagnostics_scheduler"
  end

  test "reports deterministic scheduler state evidence without runtime module names" do
    version = schedule_manifest_version("mv_diagnostics_scheduler_state")
    assert :ok = FavnOrchestrator.register_manifest(version)
    assert :ok = FavnOrchestrator.activate_manifest(version.manifest_version_id)

    running_name = Module.concat(__MODULE__, RunningSchedulerState)
    Application.put_env(:favn_orchestrator, :scheduler, enabled: true, name: running_name)
    start_supervised!({SchedulerRuntime, name: running_name, tick_ms: 60_000, auto_tick?: false})

    [entry] = SchedulerRuntime.scheduled(running_name)
    now = ~U[2026-05-16 12:00:00Z]

    assert :ok =
             Storage.put_scheduler_state(
               {entry.module, entry.schedule.name},
               %State{
                 pipeline_module: entry.module,
                 schedule_id: entry.schedule.name,
                 schedule_fingerprint: entry.schedule_fingerprint,
                 last_evaluated_at: now,
                 last_due_at: now,
                 last_submitted_due_at: now,
                 in_flight_run_id: "run_in_flight_scheduler_state",
                 queued_due_at: now,
                 updated_at: now,
                 version: 2
               }
             )

    assert :ok = SchedulerRuntime.reload(running_name)

    scheduler = check(Diagnostics.report().checks, :scheduler)
    summary = scheduler.details.state_summary

    assert summary.state_count == 1
    assert summary.evaluated_count == 1
    assert summary.due_cursor_count == 1
    assert summary.submitted_cursor_count == 1
    assert summary.in_flight_count == 1
    assert summary.queued_count == 1
    assert summary.updated_count == 1
    refute Map.has_key?(summary, :truncated?)

    assert [entry_summary] = summary.entries
    assert entry_summary.schedule_id == :daily
    assert entry_summary[:active?] == true
    assert entry_summary[:evaluated?] == true
    assert entry_summary[:due?] == true
    assert entry_summary[:submitted?] == true
    assert entry_summary[:in_flight?] == true
    assert entry_summary[:queued?] == true
    assert entry_summary[:updated?] == true
    refute Map.has_key?(entry_summary, :pipeline_module)
    refute inspect(scheduler) =~ "MyApp.Pipelines"
  end

  test "scheduler state evidence uses distinct opaque ids for pipelines sharing a schedule" do
    version = shared_schedule_manifest_version("mv_diagnostics_scheduler_distinct_ids")
    assert :ok = FavnOrchestrator.register_manifest(version)
    assert :ok = FavnOrchestrator.activate_manifest(version.manifest_version_id)

    running_name = Module.concat(__MODULE__, SharedScheduleRuntime)
    Application.put_env(:favn_orchestrator, :scheduler, enabled: true, name: running_name)
    start_supervised!({SchedulerRuntime, name: running_name, tick_ms: 60_000, auto_tick?: false})

    scheduler = check(Diagnostics.report().checks, :scheduler)
    entries = scheduler.details.state_summary.entries
    ids = Enum.map(entries, & &1.id)

    assert length(entries) == 2
    assert length(Enum.uniq(ids)) == 2
    assert Enum.all?(entries, &(&1.schedule_id == :daily))
    refute inspect(scheduler) =~ "MyApp.Pipelines"
  end

  test "scheduler state evidence ids stay distinct for same-named pipelines sharing a schedule" do
    version =
      same_named_shared_schedule_manifest_version("mv_diagnostics_scheduler_same_name_ids")

    assert :ok = FavnOrchestrator.register_manifest(version)
    assert :ok = FavnOrchestrator.activate_manifest(version.manifest_version_id)

    running_name = Module.concat(__MODULE__, SameNamedSharedScheduleRuntime)
    Application.put_env(:favn_orchestrator, :scheduler, enabled: true, name: running_name)
    start_supervised!({SchedulerRuntime, name: running_name, tick_ms: 60_000, auto_tick?: false})

    scheduler = check(Diagnostics.report().checks, :scheduler)
    entries = scheduler.details.state_summary.entries
    ids = Enum.map(entries, & &1.id)

    assert length(entries) == 2
    assert length(Enum.uniq(ids)) == 2
    assert Enum.all?(entries, &(&1.schedule_id == :daily))
    refute inspect(scheduler) =~ "MyApp.Pipelines"
  end

  test "summarizes in-flight and recent failed runs without raw error payloads", %{
    memory_server: memory_server
  } do
    secret = "raw-error-secret"

    put_run!("run_running", :running, nil)
    put_run!("run_failed", :error, %{token: secret, cause: "boom"})

    Application.put_env(:favn_orchestrator, :storage_adapter, Memory)
    Application.put_env(:favn_orchestrator, :storage_adapter_opts, server: memory_server)

    report = report_with_memory(memory_server)
    in_flight = check(report.checks, :in_flight_runs)
    failed = check(report.checks, :recent_failed_runs)

    assert in_flight.status == :ok, inspect(in_flight)
    assert in_flight.details.count == 1
    assert [%{run_id: "run_running"}] = in_flight.details.runs
    assert failed.details.count == 1

    assert [%{run_id: "run_failed", error_summary: %{kind: :map, keys: keys}}] =
             failed.details.runs

    assert "token" in keys
    refute inspect(report) =~ secret
  end

  test "emits metrics hook events with redacted metadata" do
    parent = self()

    hook = fn event, measurements, metadata ->
      send(parent, {:metrics_hook, event, measurements, metadata})
    end

    Application.put_env(:favn_orchestrator, :metrics_hook, hook)
    _report = Diagnostics.report()

    assert_receive {:metrics_hook, :diagnostics_report_generated, %{check_count: 7}, _metadata}
  end

  test "operational events emit standard telemetry with redacted metadata" do
    parent = self()
    ref = make_ref()

    :telemetry.attach(
      {__MODULE__, ref},
      [:favn, :orchestrator, :storage_failed],
      fn event, measurements, metadata, _config ->
        send(parent, {:telemetry_event, event, measurements, metadata})
      end,
      nil
    )

    on_exit(fn -> :telemetry.detach({__MODULE__, ref}) end)

    OperationalEvents.emit(:storage_failed, %{count: 1}, %{api_key: "secret-token"})

    assert_receive {:telemetry_event, [:favn, :orchestrator, :storage_failed], %{count: 1},
                    metadata}

    refute inspect(metadata) =~ "secret-token"
  end

  test "diagnostics expose projection degradation and repair-needed status" do
    run =
      RunState.new(
        id: "run_projection_failed",
        manifest_version_id: "mv",
        manifest_content_hash: "hash",
        asset_ref: {MyApp.Asset, :asset}
      )

    ProjectionDiagnostics.record_failure(
      FavnOrchestrator.TargetStatus.Projector,
      run,
      :run_finished,
      {:projector_failed, "/var/lib/favn/secret_projection_path"}
    )

    report = Diagnostics.report()
    projections = check(report.checks, :projections)

    assert report.status == :degraded
    assert projections.status == :warning
    assert projections.details.repair_needed? == true

    assert [%{run_id: "run_projection_failed", event_type: :run_finished}] =
             projections.details.failures

    refute inspect(projections) =~ "secret_projection_path"
  end

  test "operational events redact untrusted reasons, paths, URLs, and key material" do
    parent = self()

    hook = fn event, _measurements, metadata ->
      send(parent, {:metrics_hook, event, metadata})
    end

    Application.put_env(:favn_orchestrator, :metrics_hook, hook)

    log =
      capture_log(fn ->
        OperationalEvents.emit(
          :storage_failed,
          %{},
          %{
            reason: "postgres://user:secret@localhost/db?sslmode=require",
            database_path: "/var/lib/favn/secret.sqlite3",
            url: "https://operator:token@example.test",
            api_key: "api-secret",
            nested: %{detail: "failed with password=secret"}
          },
          level: :warning
        )
      end)

    assert_receive {:metrics_hook, :storage_failed, metadata}

    refute inspect(metadata) =~ "postgres://"
    refute inspect(metadata) =~ "/var/lib/favn/secret.sqlite3"
    refute inspect(metadata) =~ "api-secret"
    refute inspect(metadata) =~ "password=secret"
    assert inspect(metadata) =~ "[REDACTED_URL]"
    assert inspect(metadata) =~ "password=[REDACTED]"
    refute log =~ "postgres://"
    refute log =~ "/var/lib/favn/secret.sqlite3"
    refute log =~ "api-secret"
    refute log =~ "password=secret"
  end

  defp put_run!(id, status, error) do
    run =
      RunState.new(
        id: id,
        manifest_version_id: "mv_diagnostics_runs",
        manifest_content_hash: String.duplicate("a", 64),
        asset_ref: {MyApp.Assets.Diagnostics, :asset},
        target_refs: [{MyApp.Assets.Diagnostics, :asset}]
      )
      |> Map.put(:status, status)
      |> Map.put(:error, error)
      |> Map.put(:updated_at, DateTime.utc_now())
      |> RunState.with_snapshot_hash()

    assert :ok = Storage.put_run(run)
  end

  defp manifest_version(manifest_version_id) do
    manifest = %Manifest{
      assets: [
        %Favn.Manifest.Asset{
          ref: {MyApp.Assets.Diagnostics, :asset},
          module: MyApp.Assets.Diagnostics,
          name: :asset
        }
      ]
    }

    {:ok, version} =
      Version.new(FavnTestSupport.with_manifest_graph(manifest),
        manifest_version_id: manifest_version_id
      )

    version
  end

  defp schedule_manifest_version(manifest_version_id) do
    manifest = %Manifest{
      assets: [
        %Favn.Manifest.Asset{
          ref: {MyApp.Assets.DiagnosticsDaily, :asset},
          module: MyApp.Assets.DiagnosticsDaily,
          name: :asset
        }
      ],
      schedules: [
        %Schedule{
          module: MyApp.Schedules,
          name: :daily,
          ref: {MyApp.Schedules, :daily},
          cron: "0 * * * *",
          timezone: "Etc/UTC",
          missed: :skip,
          overlap: :forbid,
          active: true
        }
      ],
      pipelines: [
        %Pipeline{
          module: MyApp.Pipelines.DiagnosticsDaily,
          name: :diagnostics_daily,
          selectors: [{:asset, {MyApp.Assets.DiagnosticsDaily, :asset}}],
          deps: :all,
          schedule: {:ref, {MyApp.Schedules, :daily}},
          window: Policy.new!(:day),
          source: :dsl,
          outputs: [:asset],
          settings: %{},
          metadata: %{}
        }
      ]
    }

    {:ok, version} =
      Version.new(FavnTestSupport.with_manifest_graph(manifest),
        manifest_version_id: manifest_version_id
      )

    version
  end

  defp shared_schedule_manifest_version(manifest_version_id) do
    manifest = %Manifest{
      assets: [
        %Favn.Manifest.Asset{
          ref: {MyApp.Assets.DiagnosticsDailyA, :asset},
          module: MyApp.Assets.DiagnosticsDailyA,
          name: :asset
        },
        %Favn.Manifest.Asset{
          ref: {MyApp.Assets.DiagnosticsDailyB, :asset},
          module: MyApp.Assets.DiagnosticsDailyB,
          name: :asset
        }
      ],
      schedules: [
        %Schedule{
          module: MyApp.Schedules,
          name: :daily,
          ref: {MyApp.Schedules, :daily},
          cron: "0 * * * *",
          timezone: "Etc/UTC",
          missed: :skip,
          overlap: :forbid,
          active: true
        }
      ],
      pipelines: [
        %Pipeline{
          module: MyApp.Pipelines.DiagnosticsDailyA,
          name: :diagnostics_daily_a,
          selectors: [{:asset, {MyApp.Assets.DiagnosticsDailyA, :asset}}],
          deps: :all,
          schedule: {:ref, {MyApp.Schedules, :daily}},
          window: Policy.new!(:day),
          source: :dsl,
          outputs: [:asset],
          settings: %{},
          metadata: %{}
        },
        %Pipeline{
          module: MyApp.Pipelines.DiagnosticsDailyB,
          name: :diagnostics_daily_b,
          selectors: [{:asset, {MyApp.Assets.DiagnosticsDailyB, :asset}}],
          deps: :all,
          schedule: {:ref, {MyApp.Schedules, :daily}},
          window: Policy.new!(:day),
          source: :dsl,
          outputs: [:asset],
          settings: %{},
          metadata: %{}
        }
      ]
    }

    {:ok, version} =
      Version.new(FavnTestSupport.with_manifest_graph(manifest),
        manifest_version_id: manifest_version_id
      )

    version
  end

  defp same_named_shared_schedule_manifest_version(manifest_version_id) do
    manifest = %Manifest{
      assets: [
        %Favn.Manifest.Asset{
          ref: {MyApp.Assets.DiagnosticsSameNamedDailyA, :asset},
          module: MyApp.Assets.DiagnosticsSameNamedDailyA,
          name: :asset
        },
        %Favn.Manifest.Asset{
          ref: {MyApp.Assets.DiagnosticsSameNamedDailyB, :asset},
          module: MyApp.Assets.DiagnosticsSameNamedDailyB,
          name: :asset
        }
      ],
      schedules: [
        %Schedule{
          module: MyApp.Schedules,
          name: :daily,
          ref: {MyApp.Schedules, :daily},
          cron: "0 * * * *",
          timezone: "Etc/UTC",
          missed: :skip,
          overlap: :forbid,
          active: true
        }
      ],
      pipelines: [
        %Pipeline{
          module: MyApp.Pipelines.DiagnosticsSameNamedDailyA,
          name: :diagnostics_daily,
          selectors: [{:asset, {MyApp.Assets.DiagnosticsSameNamedDailyA, :asset}}],
          deps: :all,
          schedule: {:ref, {MyApp.Schedules, :daily}},
          window: Policy.new!(:day),
          source: :dsl,
          outputs: [:asset],
          settings: %{},
          metadata: %{}
        },
        %Pipeline{
          module: MyApp.Pipelines.DiagnosticsSameNamedDailyB,
          name: :diagnostics_daily,
          selectors: [{:asset, {MyApp.Assets.DiagnosticsSameNamedDailyB, :asset}}],
          deps: :all,
          schedule: {:ref, {MyApp.Schedules, :daily}},
          window: Policy.new!(:day),
          source: :dsl,
          outputs: [:asset],
          settings: %{},
          metadata: %{}
        }
      ]
    }

    {:ok, version} =
      Version.new(FavnTestSupport.with_manifest_graph(manifest),
        manifest_version_id: manifest_version_id
      )

    version
  end

  defp check(checks, name), do: Enum.find(checks, &(&1.check == name))

  defp report_with_memory(memory_server, attempts \\ 20) do
    Application.put_env(:favn_orchestrator, :storage_adapter, Memory)
    Application.put_env(:favn_orchestrator, :storage_adapter_opts, server: memory_server)

    report = Diagnostics.report()

    case check(report.checks, :in_flight_runs) do
      %{status: :ok} ->
        report

      _other when attempts > 1 ->
        Process.sleep(10)
        report_with_memory(memory_server, attempts - 1)

      _other ->
        report
    end
  end

  defp restore_env(key, nil), do: Application.delete_env(:favn_orchestrator, key)
  defp restore_env(key, value), do: Application.put_env(:favn_orchestrator, key, value)
end
