defmodule FavnRunner.ShutdownTest do
  use ExUnit.Case, async: true

  alias FavnRunner.Lifecycle
  alias FavnRunner.Shutdown

  defmodule IdleServer do
    def active_execution_count(_opts), do: {:ok, 0}
    def cancel_active(_reason, _opts), do: {:ok, 0}
  end

  defmodule FlakyServer do
    def active_execution_count(opts) do
      Agent.get_and_update(Keyword.fetch!(opts, :probe), fn count ->
        result = if count < 2, do: {:ok, 0}, else: {:error, :runner_not_available}
        {result, count + 1}
      end)
    end

    def cancel_active(_reason, _opts), do: {:ok, 0}
  end

  test "idle runner drain is bounded and monotonic" do
    name = unique_name()
    start_supervised!({Lifecycle, name: name, shutdown_drain_timeout_ms: 1_000})
    :ok = Lifecycle.mark_accepting(name)

    opts = [lifecycle: name, timeout_ms: 20, poll_interval_ms: 1, server: IdleServer]

    assert {:ok,
            %{
              status: :drained,
              active_admissions_at_start: 0,
              active_executions_at_start: 0,
              cancelled_executions: 0
            } = result} = Shutdown.drain(opts)

    assert {:ok, ^result} = Shutdown.drain(opts)

    assert Lifecycle.diagnostics(name).status == :stopping
  end

  test "an unavailable runner server is reported as unknown instead of idle" do
    name = unique_name()
    start_supervised!({Lifecycle, name: name, shutdown_drain_timeout_ms: 1_000})
    :ok = Lifecycle.mark_accepting(name)

    assert {:ok,
            %{
              status: :state_unknown,
              active_executions_at_start: :unknown,
              cancelled_executions: :unknown
            }} =
             Shutdown.drain(
               lifecycle: name,
               timeout_ms: 20,
               poll_interval_ms: 1,
               server_opts: [server: unique_name()]
             )
  end

  test "a final snapshot failure cannot turn an earlier idle observation into drained" do
    name = unique_name()
    probe = start_supervised!({Agent, fn -> 0 end})
    start_supervised!({Lifecycle, name: name, shutdown_drain_timeout_ms: 1_000})
    :ok = Lifecycle.mark_accepting(name)

    assert {:ok, %{status: :state_unknown, active_executions_remaining: :unknown}} =
             Shutdown.drain(
               lifecycle: name,
               timeout_ms: 20,
               poll_interval_ms: 1,
               server: FlakyServer,
               server_opts: [probe: probe]
             )
  end

  defp unique_name, do: :"runner_shutdown_#{System.unique_integer([:positive, :monotonic])}"
end
