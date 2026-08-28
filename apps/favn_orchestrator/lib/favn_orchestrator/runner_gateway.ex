defmodule FavnOrchestrator.RunnerGateway do
  @moduledoc """
  Stable distributed-BEAM endpoint for protocol-13 runner messages.

  Calls that may touch PostgreSQL pass an explicit concurrency ceiling before
  executing under a task supervisor, so a slow claim or result write cannot
  block registration and presence handling or create an unbounded worker queue.
  """

  use GenServer

  @default_max_concurrency 64

  defmodule Worker do
    @moduledoc false

    alias Favn.Contracts.RunnerTask
    alias FavnOrchestrator.RunnerRegistry
    alias FavnOrchestrator.RunnerTasks

    def register(registration, agent_pid) do
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
    end

    def request(%RunnerTask.ClaimRequest{} = message), do: RunnerTasks.claim(message)
    def request(%RunnerTask.Started{} = message), do: RunnerTasks.started(message)
    def request(%RunnerTask.LeaseRenewal{} = message), do: RunnerTasks.renew(message)

    def request(%RunnerTask.RuntimeInputsResolved{} = message),
      do: RunnerTasks.runtime_inputs_resolved(message)

    def request(%RunnerTask.LogBatch{} = message), do: RunnerTasks.append_logs(message)

    def request(%RunnerTask.CancellationAck{} = message),
      do: RunnerTasks.acknowledge_cancellation(message)

    def request(%RunnerTask.Result{} = message), do: RunnerTasks.complete(message)

    def request(message),
      do: {:error, {:unsupported_runner_task_message, message.__struct__}}

    def fetch_manifest(assignment, owner), do: RunnerTasks.checkout_manifest(assignment, owner)
  end

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: Keyword.get(opts, :name, __MODULE__))
  end

  def register(server \\ __MODULE__, registration, agent_pid \\ self()),
    do: GenServer.call(server, {:register, registration, agent_pid}, 15_000)

  def request(server \\ __MODULE__, message),
    do: GenServer.call(server, {:request, message}, 60_000)

  def fetch_manifest(server \\ __MODULE__, assignment),
    do: GenServer.call(server, {:fetch_manifest, assignment}, 60_000)

  @impl true
  def init(opts) do
    max_concurrency = Keyword.get(opts, :max_concurrency, @default_max_concurrency)

    if is_integer(max_concurrency) and max_concurrency > 0 do
      {:ok,
       %{
         max_concurrency: max_concurrency,
         pending: %{},
         task_supervisor:
           Keyword.get(opts, :task_supervisor, FavnOrchestrator.RunnerClaimSupervisor),
         worker: Keyword.get(opts, :worker, Worker)
       }}
    else
      {:stop, {:invalid_runner_gateway_max_concurrency, max_concurrency}}
    end
  end

  @impl true
  def handle_call({:register, registration, agent_pid}, from, state) do
    admit(state, from, fn -> state.worker.register(registration, agent_pid) end)
  end

  def handle_call({:request, message}, from, state) do
    admit(state, from, fn -> state.worker.request(message) end)
  end

  def handle_call({:fetch_manifest, assignment}, from, state) do
    owner = self()

    admit(state, from, fn ->
      state.worker.fetch_manifest(assignment, owner)
    end)
  end

  @impl true
  def handle_info({ref, result}, state) when is_reference(ref) do
    Process.demonitor(ref, [:flush])

    case Map.pop(state.pending, ref) do
      {nil, _pending} ->
        {:noreply, state}

      {from, pending} ->
        reply(from, result)
        {:noreply, %{state | pending: pending}}
    end
  end

  def handle_info({:DOWN, ref, :process, _pid, reason}, state) do
    case Map.pop(state.pending, ref) do
      {nil, _pending} ->
        {:noreply, state}

      {from, pending} ->
        GenServer.reply(from, {:error, {:runner_gateway_worker_down, reason}})
        {:noreply, %{state | pending: pending}}
    end
  end

  defp admit(state, from, fun) do
    if map_size(state.pending) < state.max_concurrency do
      task = Task.Supervisor.async_nolink(state.task_supervisor, fun)
      {:noreply, %{state | pending: Map.put(state.pending, task.ref, from)}}
    else
      {:reply, {:error, :runner_gateway_overloaded}, state}
    end
  end

  defp reply(from, {:ok, %FavnOrchestrator.ManifestStore.Lease{} = lease}) do
    GenServer.reply(from, {:ok, lease.version})
    FavnOrchestrator.ManifestStore.release_manifest(lease)
  end

  defp reply(from, result), do: GenServer.reply(from, result)
end
