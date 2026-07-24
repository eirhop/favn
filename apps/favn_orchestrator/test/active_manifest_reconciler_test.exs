defmodule FavnOrchestrator.ActiveManifestReconcilerTest do
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias Favn.Manifest.Version
  alias FavnOrchestrator.ActiveManifestReconciler
  alias FavnOrchestrator.Lifecycle
  alias FavnOrchestrator.Persistence.Error

  setup do
    primary_level = :logger.get_primary_config().level
    :ok = Logger.configure(level: :debug)
    on_exit(fn -> Logger.configure(level: primary_level) end)
    :ok
  end

  defmodule RunnerClient do
    def ensure_manifest(version, opts) do
      cache = Keyword.fetch!(opts, :cache)

      if Agent.get(cache, &MapSet.member?(&1, version.manifest_version_id)),
        do: :ok,
        else: :missing
    end

    def register_manifest(version, opts) do
      cache = Keyword.fetch!(opts, :cache)
      Agent.update(cache, &MapSet.put(&1, version.manifest_version_id))
      send(Keyword.fetch!(opts, :test_pid), {:registered, version.manifest_version_id})
      :ok
    end
  end

  test "periodically restores active manifests after the runner cache is cleared" do
    lifecycle = unique_name(:lifecycle)
    supervisor = unique_name(:tasks)
    cache = start_supervised!({Agent, fn -> MapSet.new() end})
    start_supervised!({Task.Supervisor, name: supervisor})
    start_supervised!({Lifecycle, name: lifecycle, shutdown_drain_timeout_ms: 1_000})
    :ok = Lifecycle.mark_accepting(lifecycle)

    version = %Version{manifest_version_id: "mv_reconciled"}

    load_manifest = fn
      "active" -> {:ok, version}
      "empty" -> {:error, Error.new(:not_found, "workspace has no active deployment")}
    end

    reconciler = unique_name(:reconciler)

    start_supervised!(
      {ActiveManifestReconciler,
       name: reconciler,
       lifecycle: lifecycle,
       task_supervisor: supervisor,
       workspace_ids: ["active", "empty"],
       runner_client: RunnerClient,
       runner_opts: [cache: cache, test_pid: self()],
       load_manifest: load_manifest,
       interval_ms: 20,
       timeout_ms: 500}
    )

    assert_receive {:registered, "mv_reconciled"}, 500

    assert_eventually(fn ->
      match?(
        {:ok, %{checked: 2, aligned: 1, inactive: 1, failed: 0, manifests: [_]}},
        ActiveManifestReconciler.snapshot(reconciler)
      )
    end)

    Agent.update(cache, fn _cache -> MapSet.new() end)
    assert_receive {:registered, "mv_reconciled"}, 500
  end

  test "repeated refresh calls coalesce into one periodic reconciliation loop" do
    lifecycle = unique_name(:lifecycle)
    supervisor = unique_name(:tasks)
    reconciler = unique_name(:reconciler)
    start_supervised!({Task.Supervisor, name: supervisor})
    start_supervised!({Lifecycle, name: lifecycle, shutdown_drain_timeout_ms: 1_000})
    :ok = Lifecycle.mark_accepting(lifecycle)
    test_pid = self()

    load_manifest = fn workspace_id ->
      send(test_pid, {:reconciled, workspace_id})
      {:error, Error.new(:not_found, "workspace has no active deployment")}
    end

    start_supervised!(
      {ActiveManifestReconciler,
       name: reconciler,
       lifecycle: lifecycle,
       task_supervisor: supervisor,
       workspace_ids: ["empty"],
       runner_client: RunnerClient,
       runner_opts: [cache: self(), test_pid: self()],
       load_manifest: load_manifest,
       interval_ms: 200,
       timeout_ms: 500}
    )

    assert_receive {:reconciled, "empty"}, 500

    assert :ok = ActiveManifestReconciler.refresh(reconciler)
    assert :ok = ActiveManifestReconciler.refresh(reconciler)
    assert :ok = ActiveManifestReconciler.refresh(reconciler)

    assert_receive {:reconciled, "empty"}, 500
    refute_receive {:reconciled, "empty"}, 100
    assert_receive {:reconciled, "empty"}, 250
    refute_receive {:reconciled, "empty"}, 100
  end

  test "successful reconciliation logs at debug while failures log at warning" do
    success_log =
      capture_reconciliation_log(fn _workspace_id ->
        {:error, Error.new(:not_found, "workspace has no active deployment")}
      end)

    assert success_log =~ "[debug] favn.operator.active_manifest_reconciliation_completed"
    refute success_log =~ "[info] favn.operator.active_manifest_reconciliation_completed"

    failure_log =
      capture_reconciliation_log(fn _workspace_id ->
        {:error, Error.new(:unavailable, "storage unavailable")}
      end)

    assert failure_log =~ "[warning] favn.operator.active_manifest_reconciliation_completed"
    assert failure_log =~ "status: :error"
  end

  defp capture_reconciliation_log(load_manifest) do
    lifecycle = unique_name(:lifecycle)
    supervisor = unique_name(:tasks)
    reconciler = unique_name(:reconciler)

    capture_log([level: :debug], fn ->
      start_supervised!(
        Supervisor.child_spec({Task.Supervisor, name: supervisor}, id: supervisor)
      )

      start_supervised!(
        Supervisor.child_spec(
          {Lifecycle, name: lifecycle, shutdown_drain_timeout_ms: 1_000},
          id: lifecycle
        )
      )

      :ok = Lifecycle.mark_accepting(lifecycle)

      start_supervised!(
        Supervisor.child_spec(
          {ActiveManifestReconciler,
           name: reconciler,
           lifecycle: lifecycle,
           task_supervisor: supervisor,
           workspace_ids: ["workspace"],
           runner_client: RunnerClient,
           runner_opts: [cache: self(), test_pid: self()],
           load_manifest: load_manifest,
           interval_ms: 1_000,
           timeout_ms: 500},
          id: reconciler
        )
      )

      assert_eventually(fn ->
        not match?(
          {:error, :active_manifest_reconciliation_pending},
          ActiveManifestReconciler.snapshot(reconciler)
        )
      end)
    end)
  end

  defp assert_eventually(fun, attempts \\ 50)
  defp assert_eventually(_fun, 0), do: flunk("condition did not become true")

  defp assert_eventually(fun, attempts) do
    if fun.() do
      :ok
    else
      Process.sleep(5)
      assert_eventually(fun, attempts - 1)
    end
  end

  defp unique_name(prefix),
    do: :"#{prefix}_#{System.unique_integer([:positive, :monotonic])}"
end
