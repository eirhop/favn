defmodule FavnOrchestrator.RebuildPlanningWorker do
  @moduledoc """
  Owns one durable rebuild-planning continuation and its dispatcher lease.

  The registry identity is process-local advisory state. PostgreSQL remains the
  authority, and the dispatcher fence prevents a stale worker from finalizing.
  """

  use GenServer

  alias FavnOrchestrator.Persistence
  alias FavnOrchestrator.Persistence.Commands.ClaimRebuildOperation
  alias FavnOrchestrator.Persistence.Commands.TransitionRebuildOperation
  alias FavnOrchestrator.Persistence.Error
  alias FavnOrchestrator.Persistence.Results.RebuildOperation
  alias FavnOrchestrator.Persistence.WorkspaceContext
  alias FavnOrchestrator.Rebuilds
  alias FavnOrchestrator.RuntimeConfig

  @default_lease_ms 30_000
  @default_timeout_ms 300_000

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

  @doc "Starts one unique planning worker, or returns the already-running worker."
  def ensure(%WorkspaceContext{} = context, %RebuildOperation{} = operation, opts \\ []) do
    child =
      {__MODULE__,
       [
         context: context,
         operation: operation,
         owner_id: Keyword.get(opts, :owner_id),
         lease_duration_ms: Keyword.get(opts, :lease_duration_ms, @default_lease_ms),
         resume: Keyword.get(opts, :resume, &Rebuilds.resume_planning/2)
       ]}

    case DynamicSupervisor.start_child(FavnOrchestrator.RebuildPlanningSupervisor, child) do
      {:ok, pid} -> {:ok, pid}
      {:error, {:already_started, pid}} -> {:ok, pid}
      {:error, reason} -> {:error, {:rebuild_planning_worker_start_failed, reason}}
    end
  end

  @doc "Ensures the unique worker and waits for its durable planning outcome."
  def ensure_and_await(
        %WorkspaceContext{} = context,
        %RebuildOperation{} = operation,
        opts \\ []
      ) do
    timeout = Keyword.get(opts, :timeout, @default_timeout_ms)

    with {:ok, pid} <- ensure(context, operation, opts) do
      GenServer.call(pid, :await, timeout)
    end
  catch
    :exit, reason -> {:error, {:rebuild_planning_worker_stopped, reason}}
  end

  @doc "Returns process-local planning continuations to exclude from generic claims."
  def active_operation_ids(workspace_id) when is_binary(workspace_id) do
    Registry.select(FavnOrchestrator.RebuildPlanningRegistry, [
      {{{workspace_id, :"$1"}, :_, :_}, [], [:"$1"]}
    ])
  end

  @impl true
  def init(opts) do
    Process.flag(:trap_exit, true)
    operation = Keyword.fetch!(opts, :operation)

    owner_id =
      Keyword.get(opts, :owner_id) || operation.dispatcher.owner || planning_owner_id()

    state = %{
      context: Keyword.fetch!(opts, :context),
      operation: operation,
      owner_id: owner_id,
      lease_ms: Keyword.fetch!(opts, :lease_duration_ms),
      resume: Keyword.fetch!(opts, :resume),
      task: nil,
      waiters: [],
      renew_timer: nil
    }

    {:ok, state, {:continue, :claim_and_resume}}
  end

  @impl true
  def handle_continue(:claim_and_resume, state) do
    case claim(state) do
      {:ok, %RebuildOperation{state: :planning} = operation} ->
        task = Task.async(fn -> state.resume.(state.context, operation) end)
        {:noreply, schedule_renewal(%{state | operation: operation, task: task})}

      {:ok, %RebuildOperation{} = operation} ->
        finish({:error, {:rebuild_planning_not_runnable, operation.state}}, state)

      {:ok, nil} ->
        finish({:error, :rebuild_planning_not_found}, state)

      {:error, reason} ->
        finish({:error, reason}, state)
    end
  end

  @impl true
  def handle_call(:await, from, state), do: {:noreply, %{state | waiters: [from | state.waiters]}}

  @impl true
  def handle_info({ref, result}, %{task: %Task{ref: ref}} = state) do
    Process.demonitor(ref, [:flush])
    result = maybe_terminalize_permanent_failure(result, state)
    finish(result, %{state | task: nil})
  end

  def handle_info({:DOWN, ref, :process, _pid, reason}, %{task: %Task{ref: ref}} = state) do
    finish({:error, {:rebuild_planning_task_stopped, reason}}, %{state | task: nil})
  end

  def handle_info(:renew, state) do
    case claim(state) do
      {:ok,
       %RebuildOperation{
         state: :planning,
         dispatcher: %{fencing_token: fencing_token}
       } = operation}
      when fencing_token == state.operation.dispatcher.fencing_token ->
        {:noreply, schedule_renewal(%{state | operation: operation, renew_timer: nil})}

      {:ok, %RebuildOperation{state: state_name}} when state_name in [:cancelling, :cancelled] ->
        {:noreply, %{state | renew_timer: nil}}

      {:ok, nil} ->
        {:noreply, %{state | renew_timer: nil}}

      _fenced_or_unavailable ->
        stop_task(state.task)
        finish({:error, :rebuild_planning_fenced}, %{state | task: nil, renew_timer: nil})
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

  defp claim(state) do
    Persistence.stores().rebuilds.claim_operation(%ClaimRebuildOperation{
      workspace_context: state.context,
      command_id:
        command_id(
          "planning-claim",
          state.operation.operation_id <>
            ":" <> Integer.to_string(System.unique_integer([:positive, :monotonic]))
        ),
      owner_id: state.owner_id,
      lease_duration_ms: state.lease_ms,
      operation_id: state.operation.operation_id
    })
  end

  defp schedule_renewal(state) do
    if state.renew_timer, do: Process.cancel_timer(state.renew_timer)
    delay = max(div(state.lease_ms, 3), 100)
    %{state | renew_timer: Process.send_after(self(), :renew, delay)}
  end

  defp maybe_terminalize_permanent_failure({:error, %Error{kind: kind} = error}, state)
       when kind in [:conflict, :invalid, :not_found] do
    operation = refresh_claim(state)

    case operation do
      %RebuildOperation{state: :planning} = operation ->
        case Persistence.stores().rebuilds.transition_operation(%TransitionRebuildOperation{
               workspace_context: state.context,
               command_id: command_id("planning-failed", operation.operation_id),
               operation_id: operation.operation_id,
               owner_id: state.owner_id,
               fencing_token: operation.dispatcher.fencing_token,
               expected_version: operation.version,
               expected_states: [:planning],
               state: :failed,
               phase: :terminal,
               terminal_error: %{
                 outcome: "safe_failure",
                 reason: "rebuild_planning_snapshot_unavailable",
                 detail: error.message
               },
               occurred_at: DateTime.utc_now()
             }) do
          {:ok, _failed} -> {:error, error}
          {:error, transition_error} -> {:error, transition_error}
        end

      _other ->
        {:error, error}
    end
  end

  defp maybe_terminalize_permanent_failure(result, _state), do: result

  defp refresh_claim(state) do
    case claim(state) do
      {:ok, operation} -> operation
      _other -> nil
    end
  end

  defp finish(result, state) do
    Enum.each(state.waiters, &GenServer.reply(&1, result))
    {:stop, :normal, state}
  end

  defp stop_task(nil), do: :ok
  defp stop_task(%Task{} = task), do: Task.shutdown(task, :brutal_kill)

  defp via(workspace_id, operation_id),
    do: {:via, Registry, {FavnOrchestrator.RebuildPlanningRegistry, {workspace_id, operation_id}}}

  defp planning_owner_id do
    instance = RuntimeConfig.instance_id() |> String.slice(0, 160)
    suffix = System.unique_integer([:positive, :monotonic])
    "#{instance}:rebuild-planning:#{suffix}"
  end

  defp command_id(prefix, operation_id) do
    digest = :crypto.hash(:sha256, operation_id) |> Base.url_encode64(padding: false)
    "#{prefix}:#{String.slice(digest, 0, 40)}"
  end
end
