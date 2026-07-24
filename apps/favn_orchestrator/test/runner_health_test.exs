defmodule FavnOrchestrator.RunnerHealthTest do
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

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

  defmodule FailingRunnerClient do
    def register_manifest(_version, _opts), do: :ok
    def ensure_manifest(_version, _opts), do: :ok
    def acquire_manifest(_version, _lease_id, _expires_at, _refs, _opts), do: :ok
    def renew_manifest(_lease_id, _expires_at, _opts), do: :ok
    def release_manifest(_lease_id, _opts), do: :ok
    def submit_work(_work, _opts), do: {:ok, "exec"}
    def await_result(_execution_id, _timeout, _opts), do: {:error, :not_used}
    def cancel_work(_execution_id, _reason, _opts), do: {:ok, %{status: :not_found}}
    def inspect_relation(_request, _opts), do: {:error, :not_used}
    def diagnostics(_opts), do: {:error, :runner_unreachable}
  end

  setup do
    primary_level = :logger.get_primary_config().level
    :ok = Logger.configure(level: :debug)
    on_exit(fn -> Logger.configure(level: primary_level) end)
    :ok
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

  test "successful probes log at debug and continue emitting telemetry" do
    lifecycle = unique_name(:lifecycle)
    supervisor = unique_name(:tasks)
    health = unique_name(:health)
    telemetry_id = unique_name(:telemetry)
    test_pid = self()

    :ok =
      :telemetry.attach(
        telemetry_id,
        [:favn, :orchestrator, :runner_diagnostic_completed],
        fn event, measurements, metadata, pid ->
          send(pid, {:telemetry, event, measurements, metadata})
        end,
        test_pid
      )

    on_exit(fn -> :telemetry.detach(telemetry_id) end)

    log =
      capture_log([level: :debug], fn ->
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

        assert_receive {:telemetry, [:favn, :orchestrator, :runner_diagnostic_completed],
                        %{duration_ms: duration_ms}, %{status: :ok, result: :ready_snapshot}},
                       500

        assert duration_ms >= 0
      end)

    assert log =~ "[debug] favn.operator.runner_diagnostic_completed"
    refute log =~ "[info] favn.operator.runner_diagnostic_completed"
  end

  test "failed probes remain visible at warning" do
    lifecycle = unique_name(:lifecycle)
    supervisor = unique_name(:tasks)
    health = unique_name(:health)

    log =
      capture_log([level: :debug], fn ->
        start_supervised!({Lifecycle, name: lifecycle, shutdown_drain_timeout_ms: 1_000})
        start_supervised!({Task.Supervisor, name: supervisor})
        :ok = Lifecycle.mark_accepting(lifecycle)

        start_supervised!(
          {RunnerHealth,
           name: health,
           lifecycle: lifecycle,
           task_supervisor: supervisor,
           runner_client: FailingRunnerClient,
           runner_opts: [],
           timeout_ms: 200,
           interval_ms: 1_000}
        )

        assert_eventually(fn ->
          RunnerHealth.snapshot(health) == {:error, :runner_unreachable}
        end)
      end)

    assert log =~ "[warning] favn.operator.runner_diagnostic_completed"
    assert log =~ "status: :error"
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
