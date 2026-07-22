defmodule FavnOrchestrator.RunnerDispatchTest do
  use ExUnit.Case, async: true

  alias FavnOrchestrator.Lifecycle
  alias FavnOrchestrator.RunnerDispatch

  defmodule RunnerClient do
    def submit_work(work, opts), do: notify(:submit_work, work, opts)
    def resolve_runtime_inputs(work, opts), do: notify(:resolve_runtime_inputs, work, opts)
    def inspect_relation(request, opts), do: notify(:inspect_relation, request, opts)

    defp notify(operation, value, opts) do
      send(Keyword.fetch!(opts, :test_pid), {operation, value})
      {:ok, operation}
    end
  end

  test "runner work admitted before drain finishes and later dispatch is rejected" do
    name = unique_name()
    start_supervised!({Lifecycle, name: name, shutdown_drain_timeout_ms: 1_000})
    :ok = Lifecycle.mark_accepting(name)

    assert {:ok, :submit_work} =
             RunnerDispatch.submit_work(RunnerClient, :work, [test_pid: self()], name)

    assert_receive {:submit_work, :work}
    :ok = Lifecycle.drain(name)

    assert {:error, :runtime_draining} =
             RunnerDispatch.submit_work(RunnerClient, :later_work, [test_pid: self()], name)

    assert {:error, :runtime_draining} =
             RunnerDispatch.resolve_runtime_inputs(
               RunnerClient,
               :later_resolution,
               [test_pid: self()],
               name
             )

    assert {:error, :runtime_draining} =
             RunnerDispatch.inspect_relation(
               RunnerClient,
               :later_inspection,
               [test_pid: self()],
               name
             )

    refute_receive {:submit_work, :later_work}
    refute_receive {:resolve_runtime_inputs, :later_resolution}
    refute_receive {:inspect_relation, :later_inspection}
  end

  defp unique_name,
    do: :"runner_dispatch_lifecycle_#{System.unique_integer([:positive, :monotonic])}"
end
