defmodule FavnOrchestrator.RunnerGateway do
  @moduledoc """
  Stable distributed-BEAM endpoint for protocol-13 runner messages.

  Calls that may touch PostgreSQL execute under a bounded task supervisor so a
  slow claim or result write cannot block registration and presence handling.
  """

  use GenServer

  alias Favn.Contracts.RunnerTask
  alias FavnOrchestrator.RunnerRegistry
  alias FavnOrchestrator.RunnerTasks

  def start_link(opts),
    do: GenServer.start_link(__MODULE__, %{}, name: Keyword.get(opts, :name, __MODULE__))

  def register(server \\ __MODULE__, registration, agent_pid \\ self()),
    do: GenServer.call(server, {:register, registration, agent_pid}, 15_000)

  def request(server \\ __MODULE__, message),
    do: GenServer.call(server, {:request, message}, 60_000)

  def fetch_manifest(server \\ __MODULE__, assignment),
    do: GenServer.call(server, {:fetch_manifest, assignment}, 60_000)

  @impl true
  def init(state), do: {:ok, state}

  @impl true
  def handle_call({:register, registration, agent_pid}, from, state) do
    task =
      Task.Supervisor.async_nolink(FavnOrchestrator.RunnerClaimSupervisor, fn ->
        case RunnerTasks.verify_registration_resume(registration) do
          :ok ->
            RunnerRegistry.register_verified(registration, agent_pid)

          {:error, :stale_runner_task_resume = reason} ->
            {:ok,
             %RunnerTask.RegistrationAck{
               runner_instance_id: registration.runner_instance_id,
               runner_session_generation: registration.runner_session_generation,
               status: :rejected,
               reason: reason
             }}

          {:error, reason} ->
            {:error, reason}
        end
      end)

    {:noreply, Map.put(state, task.ref, from)}
  end

  def handle_call({:request, message}, from, state) do
    task =
      Task.Supervisor.async_nolink(FavnOrchestrator.RunnerClaimSupervisor, fn ->
        dispatch(message)
      end)

    {:noreply, Map.put(state, task.ref, from)}
  end

  def handle_call({:fetch_manifest, assignment}, from, state) do
    task =
      Task.Supervisor.async_nolink(FavnOrchestrator.RunnerClaimSupervisor, fn ->
        RunnerTasks.fetch_manifest(assignment)
      end)

    {:noreply, Map.put(state, task.ref, from)}
  end

  @impl true
  def handle_info({ref, result}, state) when is_reference(ref) do
    Process.demonitor(ref, [:flush])

    case Map.pop(state, ref) do
      {nil, state} ->
        {:noreply, state}

      {from, state} ->
        GenServer.reply(from, result)
        {:noreply, state}
    end
  end

  def handle_info({:DOWN, ref, :process, _pid, reason}, state) do
    case Map.pop(state, ref) do
      {nil, state} ->
        {:noreply, state}

      {from, state} ->
        GenServer.reply(from, {:error, {:runner_gateway_worker_down, reason}})
        {:noreply, state}
    end
  end

  defp dispatch(%RunnerTask.ClaimRequest{} = message), do: RunnerTasks.claim(message)
  defp dispatch(%RunnerTask.Started{} = message), do: RunnerTasks.started(message)
  defp dispatch(%RunnerTask.LeaseRenewal{} = message), do: RunnerTasks.renew(message)

  defp dispatch(%RunnerTask.RuntimeInputsResolved{} = message),
    do: RunnerTasks.runtime_inputs_resolved(message)

  defp dispatch(%RunnerTask.LogBatch{} = message), do: RunnerTasks.append_logs(message)

  defp dispatch(%RunnerTask.CancellationAck{} = message),
    do: RunnerTasks.acknowledge_cancellation(message)

  defp dispatch(%RunnerTask.Result{} = message), do: RunnerTasks.complete(message)
  defp dispatch(message), do: {:error, {:unsupported_runner_task_message, message.__struct__}}
end
