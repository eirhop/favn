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
    keys = [:runner_client, :runner_client_opts, :readiness_test_runner_diagnostics]
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
         node_name: "runner@runner.internal"
       }}
    )

    assert %{status: :ok, details: details} = runner_check()
    assert details.runner_release_id == FavnTestSupport.runner_release_id()
    assert details.client == Atom.to_string(RunnerClient)

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
         node_name: "runner@runner.internal"
       }}
    )

    assert %{status: :error, error: :runner_release_info_unavailable} = runner_check()
  end

  test "readiness reports a disconnected or draining runner as unavailable" do
    Application.put_env(
      :favn_orchestrator,
      :readiness_test_runner_diagnostics,
      {:error, :runner_node_unreachable}
    )

    assert %{status: :error, error: :runner_node_unreachable} = runner_check()

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
         node_name: "runner@runner.internal"
       }}
    )

    assert %{status: :error, error: :runner_not_ready} = runner_check()
  end

  defp runner_check do
    Readiness.readiness()
    |> Map.fetch!(:checks)
    |> Enum.find(&(&1.name == :runner))
  end
end
