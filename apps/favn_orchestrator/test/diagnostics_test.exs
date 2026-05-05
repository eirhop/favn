defmodule FavnOrchestrator.DiagnosticsTest do
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias Favn.Contracts.RunnerClient
  alias Favn.Manifest
  alias Favn.Manifest.Version
  alias FavnOrchestrator.Diagnostics
  alias FavnOrchestrator.OperationalEvents
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
    def persist_run_transition(_run_state, _event, _opts), do: :ok

    @impl true
    def append_run_event(_run_id, _event, _opts), do: :ok

    @impl true
    def list_run_events(_run_id, _opts), do: {:ok, []}

    @impl true
    def list_global_run_events(_filters, _opts), do: {:ok, []}

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
    def put_asset_window_state(_state, _opts), do: :ok

    @impl true
    def get_asset_window_state(_asset_ref_module, _asset_ref_name, _window_key, _opts),
      do: {:error, :not_found}

    @impl true
    def list_asset_window_states(_filters, _opts), do: {:ok, []}

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

    on_exit(fn ->
      Enum.each(previous, fn {key, value} -> restore_env(key, value) end)
      Process.delete(:runner_diagnostics_result)
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

    assert_receive {:metrics_hook, :diagnostics_report_generated, %{check_count: 6}, _metadata}
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

    {:ok, version} = Version.new(manifest, manifest_version_id: manifest_version_id)
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
