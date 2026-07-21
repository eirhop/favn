defmodule FavnOrchestrator.RunnerHealthTest do
  use ExUnit.Case, async: true

  alias FavnOrchestrator.Lifecycle
  alias FavnOrchestrator.RunnerHealth

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

    def diagnostics(opts) do
      send(Keyword.fetch!(opts, :test_pid), :probed)
      {:ok, %{available?: true}}
    end
  end

  test "publishes one reusable result from a supervised bounded probe" do
    lifecycle = unique_name(:lifecycle)
    supervisor = unique_name(:tasks)
    health = unique_name(:health)
    start_supervised!({Lifecycle, name: lifecycle, shutdown_drain_timeout_ms: 1_000})
    start_supervised!({Task.Supervisor, name: supervisor})
    :ok = Lifecycle.mark_accepting(lifecycle)

    start_supervised!(
      {RunnerHealth,
       name: health,
       lifecycle: lifecycle,
       task_supervisor: supervisor,
       runner_client: RunnerClient,
       runner_opts: [test_pid: self()],
       timeout_ms: 200,
       interval_ms: 1_000}
    )

    assert_receive :probed
    assert_eventually(fn -> RunnerHealth.snapshot(health) == {:ok, %{available?: true}} end)
    assert {:ok, %{available?: true}} = RunnerHealth.snapshot(health)
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
