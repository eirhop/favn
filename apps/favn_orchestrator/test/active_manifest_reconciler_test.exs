defmodule FavnOrchestrator.ActiveManifestReconcilerTest do
  use ExUnit.Case, async: true

  alias Favn.Manifest.Version
  alias FavnOrchestrator.ActiveManifestReconciler
  alias FavnOrchestrator.Lifecycle
  alias FavnOrchestrator.Persistence.Error

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
