defmodule FavnStorageSqlite.SingleNodeBootstrapAcceptanceTest do
  use ExUnit.Case, async: false

  alias Favn.Manifest
  alias Favn.Manifest.Asset
  alias Favn.Manifest.Graph
  alias Favn.Manifest.Version
  alias Favn.Scheduler.State, as: SchedulerState
  alias Favn.Storage.Adapter.SQLite, as: Adapter
  alias FavnOrchestrator.Storage, as: OrchestratorStorage
  alias FavnStorageSqlite.Repo
  alias FavnStorageSqlite.Supervisor, as: SQLiteSupervisor

  setup do
    state = Favn.TestSetup.capture_state()
    previous_runner_client = Application.get_env(:favn_orchestrator, :runner_client)
    previous_runner_client_opts = Application.get_env(:favn_orchestrator, :runner_client_opts)

    maybe_stop_process(Repo)

    unique = System.unique_integer([:positive, :monotonic])
    db_path = Path.join(System.tmp_dir!(), "favn_sqlite_single_node_bootstrap_#{unique}.db")
    supervisor_name = Module.concat([__MODULE__, "SQLiteSupervisor#{unique}"])

    opts = [
      database: db_path,
      name: supervisor_name,
      supervisor_name: supervisor_name,
      pool_size: 1,
      migration_mode: :auto,
      require_absolute_path: true
    ]

    :ok = Favn.TestSetup.configure_storage_adapter(Adapter, opts)
    Application.put_env(:favn_orchestrator, :runner_client, __MODULE__.RunnerClient)
    Application.put_env(:favn_orchestrator, :runner_client_opts, test_pid: self())

    on_exit(fn ->
      maybe_stop_process(supervisor_name)
      maybe_stop_process(Repo)
      Favn.TestSetup.restore_state(state, clear_storage_adapter_env?: true)
      restore_env(:runner_client, previous_runner_client)
      restore_env(:runner_client_opts, previous_runner_client_opts)
      rm_sqlite_files(db_path)
    end)

    {:ok, db_path: db_path, opts: opts, unique: unique, supervisor_name: supervisor_name}
  end

  test "SQLite single-node bootstrap is ready for run submission after storage restart", %{
    db_path: db_path,
    opts: opts,
    unique: unique,
    supervisor_name: supervisor_name
  } do
    assert Path.type(db_path) == :absolute

    {:ok, supervisor_pid} = SQLiteSupervisor.start_link(opts)

    version = manifest_version("mv_single_node_bootstrap_#{unique}")
    scheduler_key = {__MODULE__.Pipeline, :daily}
    scheduler_state = scheduler_state()

    assert :ok = FavnOrchestrator.register_manifest(version)
    assert :ok = FavnOrchestrator.activate_manifest(version.manifest_version_id)

    assert :ok =
             OrchestratorStorage.put_scheduler_state(
               scheduler_key,
               Map.from_struct(scheduler_state)
             )

    assert {:ok, registration} =
             FavnOrchestrator.register_manifest_with_runner(version.manifest_version_id)

    assert registration.status == "accepted"
    assert registration.manifest_version_id == version.manifest_version_id

    assert_receive {:runner_manifest_registered, ^version, [test_pid: test_pid]}
    assert test_pid == self()

    # This acceptance intentionally stops at ready-for-run-submission: submitting a
    # run exercises RunManager/RunServer supervision and runner work callbacks,
    # which are covered outside this SQLite bootstrap persistence boundary. It
    # also intentionally excludes auth/session/audit durability.
    maybe_stop_pid(supervisor_pid)
    {:ok, restarted_pid} = SQLiteSupervisor.start_link(Keyword.put(opts, :name, supervisor_name))

    assert {:ok, stored_version} = FavnOrchestrator.get_manifest(version.manifest_version_id)
    assert stored_version.manifest_version_id == version.manifest_version_id
    assert stored_version.content_hash == version.content_hash
    assert stored_version.manifest.assets == version.manifest.assets

    assert {:ok, version.manifest_version_id} == FavnOrchestrator.active_manifest()

    assert {:ok, %SchedulerState{} = stored_scheduler_state} =
             OrchestratorStorage.get_scheduler_state(scheduler_key)

    assert stored_scheduler_state.pipeline_module == scheduler_state.pipeline_module
    assert stored_scheduler_state.schedule_id == scheduler_state.schedule_id
    assert stored_scheduler_state.schedule_fingerprint == scheduler_state.schedule_fingerprint
    assert stored_scheduler_state.last_submitted_due_at == scheduler_state.last_submitted_due_at

    maybe_stop_pid(restarted_pid)
  end

  defp manifest_version(manifest_version_id) do
    asset = %Asset{
      ref: {__MODULE__.Asset, :orders_daily},
      module: __MODULE__.Asset,
      name: :orders_daily,
      type: :elixir,
      execution: %{entrypoint: :orders_daily, arity: 1},
      depends_on: [],
      config: %{acceptance: :single_node_bootstrap}
    }

    manifest = %Manifest{
      schema_version: 1,
      runner_contract_version: 1,
      assets: [asset],
      pipelines: [],
      schedules: [],
      graph: %Graph{nodes: [asset.ref], edges: [], topo_order: [asset.ref]},
      metadata: %{phase: 1}
    }

    {:ok, version} = Version.new(manifest, manifest_version_id: manifest_version_id)
    version
  end

  defp scheduler_state do
    now = DateTime.utc_now() |> DateTime.truncate(:second)

    %SchedulerState{
      pipeline_module: __MODULE__.Pipeline,
      schedule_id: :daily,
      schedule_fingerprint: "phase-1-daily",
      last_evaluated_at: now,
      last_due_at: now,
      last_submitted_due_at: now,
      in_flight_run_id: nil,
      queued_due_at: nil,
      version: 1
    }
  end

  defp restore_env(key, nil), do: Application.delete_env(:favn_orchestrator, key)
  defp restore_env(key, value), do: Application.put_env(:favn_orchestrator, key, value)

  defp maybe_stop_process(name) do
    case Process.whereis(name) do
      nil -> :ok
      pid -> maybe_stop_pid(pid)
    end
  end

  defp maybe_stop_pid(nil), do: :ok

  defp maybe_stop_pid(pid) when is_pid(pid) do
    if Process.alive?(pid) do
      Process.unlink(pid)
      Supervisor.stop(pid, :normal, 5_000)
    else
      :ok
    end
  catch
    :exit, _reason -> :ok
  end

  defp rm_sqlite_files(path) do
    File.rm(path)
    File.rm("#{path}-shm")
    File.rm("#{path}-wal")
  end
end

defmodule FavnStorageSqlite.SingleNodeBootstrapAcceptanceTest.RunnerClient do
  @behaviour Favn.Contracts.RunnerClient

  alias Favn.Contracts.RelationInspectionRequest
  alias Favn.Contracts.RelationInspectionResult
  alias Favn.Contracts.RunnerWork
  alias Favn.Manifest.Version

  @impl true
  def register_manifest(%Version{} = version, opts) do
    send(Keyword.fetch!(opts, :test_pid), {:runner_manifest_registered, version, opts})
    :ok
  end

  @impl true
  def submit_work(%RunnerWork{}, _opts), do: {:error, :not_used_by_bootstrap_acceptance}

  @impl true
  def await_result(_execution_id, _timeout, _opts),
    do: {:error, :not_used_by_bootstrap_acceptance}

  @impl true
  def cancel_work(_execution_id, _reason, _opts), do: {:error, :not_used_by_bootstrap_acceptance}

  @impl true
  def inspect_relation(%RelationInspectionRequest{}, _opts) do
    {:ok, %RelationInspectionResult{columns: [], sample: %{rows: []}}}
  end
end

defmodule FavnStorageSqlite.SingleNodeBootstrapAcceptanceTest.Asset do
  def orders_daily(_ctx), do: :ok
end

defmodule FavnStorageSqlite.SingleNodeBootstrapAcceptanceTest.Pipeline do
end
