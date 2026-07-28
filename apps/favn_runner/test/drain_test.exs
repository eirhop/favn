defmodule FavnRunner.DrainTest do
  use ExUnit.Case, async: true

  alias FavnRunner.Lifecycle
  alias FavnRunner.Drain

  defmodule ReportingAgent do
    use GenServer

    def start_link(owner), do: GenServer.start_link(__MODULE__, owner)

    @impl true
    def init(owner), do: {:ok, %{owner: owner, drained?: false}}

    @impl true
    def handle_cast(:drain, state) do
      send(state.owner, :drain_requested)
      Process.send_after(self(), :result_acknowledged, 25)
      {:noreply, state}
    end

    @impl true
    def handle_call(:drained?, _from, state), do: {:reply, state.drained?, state}

    @impl true
    def handle_info(:result_acknowledged, state) do
      send(state.owner, :result_acknowledged)
      {:noreply, %{state | drained?: true}}
    end
  end

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

  test "drain waits for the agent's durable result acknowledgement after executors stop" do
    name = unique_name()
    start_supervised!({Lifecycle, name: name, shutdown_drain_timeout_ms: 1_000})
    :ok = Lifecycle.mark_accepting(name)
    agent = start_supervised!({ReportingAgent, self()})

    assert {:ok, %{status: :drained}} =
             Drain.drain(
               lifecycle: name,
               timeout_ms: 250,
               poll_interval_ms: 1,
               agent: agent,
               executor_supervisor: unique_name()
             )

    assert_received :drain_requested
    assert_received :result_acknowledged
  end

  defp unique_name, do: :"runner_shutdown_#{System.unique_integer([:positive, :monotonic])}"
end
