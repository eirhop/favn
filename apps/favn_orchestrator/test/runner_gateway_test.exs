defmodule FavnOrchestrator.RunnerGatewayTest do
  use ExUnit.Case, async: true

  alias FavnOrchestrator.RunnerGateway

  defmodule Worker do
    def request({:block, owner, result}) do
      send(owner, {:gateway_worker_started, self()})

      receive do
        :release -> result
      end
    end

    def request({:reply, result}), do: result
  end

  test "rejects overload without starting an unbounded worker backlog" do
    supervisor = start_supervised!({Task.Supervisor, name: unique_name(:supervisor)})
    gateway_name = unique_name(:gateway)

    start_supervised!(
      {RunnerGateway,
       name: gateway_name, task_supervisor: supervisor, worker: Worker, max_concurrency: 1}
    )

    owner = self()

    first =
      Task.async(fn ->
        RunnerGateway.request(gateway_name, {:block, owner, :first_complete})
      end)

    assert_receive {:gateway_worker_started, worker}

    assert {:error, :runner_gateway_overloaded} =
             RunnerGateway.request(gateway_name, {:reply, :must_not_start})

    send(worker, :release)
    assert Task.await(first) == :first_complete
    assert RunnerGateway.request(gateway_name, {:reply, :capacity_released}) == :capacity_released
  end

  defp unique_name(label),
    do: Module.concat(__MODULE__, "#{label}_#{System.unique_integer([:positive])}")
end
