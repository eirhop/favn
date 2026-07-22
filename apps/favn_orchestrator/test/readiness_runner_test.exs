defmodule FavnOrchestrator.ReadinessRunnerTest do
  use ExUnit.Case, async: false

  alias FavnOrchestrator.Readiness

  defmodule RunnerClient do
    def register_manifest(_version, _opts), do: :ok
    def ensure_manifest(_version, _opts), do: :ok
    def acquire_manifest(_version, _lease_id, _expires_at, _refs, _opts), do: :ok
    def renew_manifest(_lease_id, _expires_at, _opts), do: :ok
    def release_manifest(_lease_id, _opts), do: :ok
    def submit_work(_work, _opts), do: {:ok, "exec"}
    def await_result(_execution_id, _timeout, _opts), do: {:error, :not_used}
    def cancel_work(_execution_id, _reason, _opts), do: {:ok, %{status: :not_found}}
    def inspect_relation(_request, _opts), do: {:error, :not_used}

    def diagnostics(_opts),
      do: Application.fetch_env!(:favn_orchestrator, :readiness_test_runner_diagnostics)
  end

  setup do
    keys = [
      :runner_client,
      :runner_client_opts,
      :readiness_test_runner_diagnostics,
      :workspace_ids
    ]

    previous = Map.new(keys, &{&1, Application.get_env(:favn_orchestrator, &1)})

    Application.put_env(:favn_orchestrator, :runner_client, RunnerClient)
    Application.put_env(:favn_orchestrator, :runner_client_opts, [])

    on_exit(fn ->
      Enum.each(previous, fn
        {key, nil} -> Application.delete_env(:favn_orchestrator, key)
        {key, value} -> Application.put_env(:favn_orchestrator, key, value)
      end)
    end)

    :ok
  end

  test "readiness accepts only ready diagnostics with a valid verified release id" do
    Application.put_env(
      :favn_orchestrator,
      :readiness_test_runner_diagnostics,
      {:ok,
       %{
         available?: true,
         ready?: true,
         status: :ready,
         runner_release_id: FavnTestSupport.runner_release_id(),
         favn_version: Favn.RunnerRelease.current_favn_version(),
         runner_contract_version: Favn.Manifest.Compatibility.current_runner_contract_version(),
         self_verified?: true,
         node_name: "runner@runner.internal"
       }}
    )

    assert %{status: :ok, details: details} = runner_check(:runner_release)
    assert details.runner_release_id == FavnTestSupport.runner_release_id()

    assert %{status: :ok, details: connection} = runner_check(:runner_connection)
    assert connection.client == Atom.to_string(RunnerClient)

    Application.put_env(
      :favn_orchestrator,
      :readiness_test_runner_diagnostics,
      {:ok,
       %{
         available?: true,
         ready?: true,
         status: :ready,
         runner_release_id: "invalid",
         favn_version: Favn.RunnerRelease.current_favn_version(),
         runner_contract_version: Favn.Manifest.Compatibility.current_runner_contract_version(),
         self_verified?: true,
         node_name: "runner@runner.internal"
       }}
    )

    assert %{status: :error, error: :runner_release_info_unavailable} =
             runner_check(:runner_release)
  end

  test "readiness exposes the stable production check names" do
    Application.put_env(
      :favn_orchestrator,
      :readiness_test_runner_diagnostics,
      {:error, :runner_node_unreachable}
    )

    assert Readiness.readiness(
             storage_snapshot: {:error, :not_used},
             runner_snapshot: {:error, :runner_node_unreachable},
             active_manifest_snapshot: {:error, :not_used}
           ).checks
           |> Enum.map(& &1.name) == [
             :config,
             :api,
             :view,
             :storage,
             :schema,
             :scheduler,
             :lifecycle,
             :runner_connection,
             :runner_release,
             :active_manifests
           ]
  end

  test "readiness reports a disconnected or draining runner as unavailable" do
    Application.put_env(
      :favn_orchestrator,
      :readiness_test_runner_diagnostics,
      {:error, :runner_node_unreachable}
    )

    assert %{status: :error, error: :runner_node_unreachable} =
             runner_check(:runner_connection)

    Application.put_env(
      :favn_orchestrator,
      :readiness_test_runner_diagnostics,
      {:ok,
       %{
         available?: true,
         ready?: false,
         status: :draining,
         runner_release_id: FavnTestSupport.runner_release_id(),
         favn_version: Favn.RunnerRelease.current_favn_version(),
         runner_contract_version: Favn.Manifest.Compatibility.current_runner_contract_version(),
         self_verified?: true,
         node_name: "runner@runner.internal"
       }}
    )

    assert %{status: :error, error: :runner_not_ready} = runner_check(:runner_release)
  end

  test "active-manifest readiness accepts undeployed workspaces and rejects malformed snapshots" do
    Application.put_env(:favn_orchestrator, :workspace_ids, ["empty"])

    runner_snapshot =
      {:ok,
       %{
         available?: true,
         ready?: true,
         status: :ready,
         runner_release_id: FavnTestSupport.runner_release_id(),
         favn_version: Favn.RunnerRelease.current_favn_version(),
         runner_contract_version: Favn.Manifest.Compatibility.current_runner_contract_version(),
         self_verified?: true,
         node_name: "runner@runner.internal"
       }}

    valid = %{checked: 1, aligned: 0, inactive: 1, failed: 0, manifests: []}

    assert %{status: :ok, details: %{active_manifest_count: 0}} =
             active_manifest_check(runner_snapshot, {:ok, valid})

    assert %{status: :error, error: :invalid_active_manifest_reconciliation} =
             active_manifest_check(runner_snapshot, {:ok, %{valid | checked: 0}})
  end

  defp runner_check(name) do
    Readiness.readiness(
      runner_snapshot:
        Application.fetch_env!(:favn_orchestrator, :readiness_test_runner_diagnostics),
      active_manifest_snapshot: {:ok, %{manifests: []}},
      storage_snapshot: {:error, :not_used}
    )
    |> Map.fetch!(:checks)
    |> Enum.find(&(&1.name == name))
  end

  defp active_manifest_check(runner_snapshot, active_manifest_snapshot) do
    Readiness.readiness(
      runner_snapshot: runner_snapshot,
      active_manifest_snapshot: active_manifest_snapshot,
      storage_snapshot: {:error, :not_used}
    )
    |> Map.fetch!(:checks)
    |> Enum.find(&(&1.name == :active_manifests))
  end
end
