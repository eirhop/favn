defmodule FavnOrchestrator.RebuildExecutionWorker do
  @moduledoc """
  Owns one claimed rebuild continuation while renewing every durable fence.

  The dispatcher starts this worker after claiming the operation and its target
  locks. The continuation runs in a linked task so lease renewal remains
  responsive during runner cold starts and other bounded waits.
  """

  use GenServer

  require Logger

  alias FavnOrchestrator.Persistence
  alias FavnOrchestrator.Persistence.Commands.RenewRebuildOperationLease
  alias FavnOrchestrator.Persistence.Commands.RenewTargetOperationLocks
  alias FavnOrchestrator.Persistence.Results.RebuildOperation
  alias FavnOrchestrator.Persistence.WorkspaceContext
  alias FavnOrchestrator.RebuildDispatcher

  def child_spec(opts) do
    operation = Keyword.fetch!(opts, :operation)

    %{
      id: {__MODULE__, operation.workspace_id, operation.operation_id},
      start: {__MODULE__, :start_link, [opts]},
      restart: :temporary,
      shutdown: 5_000,
      type: :worker
    }
  end

  def start_link(opts) do
    context = Keyword.fetch!(opts, :context)
    operation = Keyword.fetch!(opts, :operation)

    GenServer.start_link(__MODULE__, opts,
      name: via(context.workspace_id, operation.operation_id)
    )
  end

  @doc "Starts one unique execution continuation, or returns the live worker."
  def ensure(
        %WorkspaceContext{} = context,
        %RebuildOperation{} = operation,
        locks,
        opts \\ []
      )
      when is_list(locks) do
    child =
      {__MODULE__,
       [
         context: context,
         operation: operation,
         locks: locks,
         owner_id: Keyword.fetch!(opts, :owner_id),
         lease_duration_ms: Keyword.fetch!(opts, :lease_duration_ms),
         execute:
           Keyword.get(
             opts,
             :execute,
             &RebuildDispatcher.process_claimed_operation/4
           )
       ]}

    case DynamicSupervisor.start_child(FavnOrchestrator.RebuildExecutionSupervisor, child) do
      {:ok, pid} -> {:ok, pid}
      {:error, {:already_started, pid}} -> {:ok, pid}
      {:error, reason} -> {:error, {:rebuild_execution_worker_start_failed, reason}}
    end
  end

  @doc "Returns process-local continuations that the generic dispatcher must not reclaim."
  def active_operation_ids(workspace_id) when is_binary(workspace_id) do
    Registry.select(FavnOrchestrator.RebuildExecutionRegistry, [
      {{{workspace_id, :"$1"}, :_, :_}, [], [:"$1"]}
    ])
  end

  @impl true
  def init(opts) do
    Process.flag(:trap_exit, true)

    state = %{
      context: Keyword.fetch!(opts, :context),
      operation: Keyword.fetch!(opts, :operation),
      locks: Keyword.fetch!(opts, :locks),
      owner_id: Keyword.fetch!(opts, :owner_id),
      lease_ms: Keyword.fetch!(opts, :lease_duration_ms),
      execute: Keyword.fetch!(opts, :execute),
      task: nil,
      renew_timer: nil
    }

    {:ok, state, {:continue, :execute}}
  end

  @impl true
  def handle_continue(:execute, state) do
    task =
      Task.async(fn ->
        dispatcher_state = %{owner_id: state.owner_id, lease_ms: state.lease_ms}
        state.execute.(state.context, state.operation, state.locks, dispatcher_state)
      end)

    {:noreply, schedule_renewal(%{state | task: task})}
  end

  @impl true
  def handle_info({ref, result}, %{task: %Task{ref: ref}} = state) do
    Process.demonitor(ref, [:flush])
    maybe_log_failure(result, state)
    {:stop, :normal, %{state | task: nil}}
  end

  def handle_info({:DOWN, ref, :process, _pid, reason}, %{task: %Task{ref: ref}} = state) do
    log_failure({:rebuild_execution_task_stopped, reason}, state)
    {:stop, :normal, %{state | task: nil}}
  end

  def handle_info(:renew, state) do
    with :ok <- renew_operation(state),
         {:ok, locks} <- renew_locks(state) do
      {:noreply, schedule_renewal(%{state | locks: locks, renew_timer: nil})}
    else
      {:error, reason} ->
        stop_task(state.task)
        log_failure({:rebuild_execution_fenced, reason}, state)
        {:stop, :normal, %{state | task: nil, renew_timer: nil}}
    end
  end

  def handle_info({:EXIT, pid, _reason}, %{task: %Task{pid: pid}} = state),
    do: {:noreply, state}

  @impl true
  def terminate(_reason, state) do
    if state.renew_timer, do: Process.cancel_timer(state.renew_timer)
    stop_task(state.task)
    :ok
  end

  defp renew_operation(state) do
    Persistence.stores().rebuilds.renew_operation_lease(%RenewRebuildOperationLease{
      workspace_context: state.context,
      command_id: command_id("renew-operation", state.operation.operation_id),
      operation_id: state.operation.operation_id,
      owner_id: state.owner_id,
      fencing_token: state.operation.dispatcher.fencing_token,
      lease_duration_ms: state.lease_ms,
      occurred_at: DateTime.utc_now()
    })
  end

  defp renew_locks(%{locks: []}), do: {:ok, []}

  defp renew_locks(state) do
    Persistence.stores().target_operation_locks.renew_many(%RenewTargetOperationLocks{
      workspace_context: state.context,
      command_id: command_id("renew-locks", state.operation.operation_id),
      operation_id: state.operation.operation_id,
      lease_owner: state.operation.operation_id,
      locks: Enum.map(state.locks, &%{target_id: &1.target_id, fencing_token: &1.fencing_token}),
      lease_duration_ms: state.lease_ms,
      occurred_at: DateTime.utc_now()
    })
  end

  defp schedule_renewal(state) do
    if state.renew_timer, do: Process.cancel_timer(state.renew_timer)
    delay = max(div(state.lease_ms, 3), 100)
    %{state | renew_timer: Process.send_after(self(), :renew, delay)}
  end

  defp maybe_log_failure({:error, reason}, state), do: log_failure(reason, state)
  defp maybe_log_failure(_result, _state), do: :ok

  defp log_failure(reason, state) do
    Logger.warning("rebuild execution continuation stopped",
      workspace_id: state.context.workspace_id,
      operation_id: state.operation.operation_id,
      reason: inspect(reason)
    )
  end

  defp stop_task(nil), do: :ok
  defp stop_task(%Task{} = task), do: Task.shutdown(task, :brutal_kill)

  defp via(workspace_id, operation_id),
    do:
      {:via, Registry, {FavnOrchestrator.RebuildExecutionRegistry, {workspace_id, operation_id}}}

  defp command_id(prefix, operation_id) do
    nonce = System.unique_integer([:positive, :monotonic])

    digest =
      :crypto.hash(:sha256, "#{operation_id}:#{nonce}") |> Base.url_encode64(padding: false)

    "#{prefix}:#{String.slice(digest, 0, 40)}"
  end
end
