defmodule FavnRunner.DrainTest do
  use ExUnit.Case, async: true

  alias FavnRunner.Lifecycle
  alias FavnRunner.Drain

  test "idle runner drain is bounded and monotonic" do
    name = unique_name()
    start_supervised!({Lifecycle, name: name, shutdown_drain_timeout_ms: 1_000})
    :ok = Lifecycle.mark_accepting(name)

    opts = [
      lifecycle: name,
      timeout_ms: 20,
      poll_interval_ms: 1,
      agent: unique_name(),
      executor_supervisor: unique_name()
    ]

    assert {:ok,
            %{
              status: :drained,
              active_admissions_at_start: 0,
              active_executions_at_start: 0,
              cancelled_executions: 0
            } = result} = Drain.drain(opts)

    assert {:ok, ^result} = Drain.drain(opts)

    assert Lifecycle.diagnostics(name).status == :stopping
  end

  defp unique_name, do: :"runner_shutdown_#{System.unique_integer([:positive, :monotonic])}"
end
