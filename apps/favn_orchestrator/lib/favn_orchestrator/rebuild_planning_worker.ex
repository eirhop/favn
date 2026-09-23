defmodule FavnOrchestrator.RebuildPlanningWorker do
  @moduledoc "Owns one live rebuild validation attempt. Process or lease loss requires manual retry."
  use GenServer

  alias FavnOrchestrator.Persistence
  alias FavnOrchestrator.Persistence.Commands.CloseRebuildValidation
  alias FavnOrchestrator.Persistence.Commands.RenewRebuildOperationLease
  alias FavnOrchestrator.Persistence.Error
  alias FavnOrchestrator.Persistence.Results.RebuildOperation
  alias FavnOrchestrator.Persistence.WorkspaceContext
  alias FavnOrchestrator.Rebuilds

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
      name: via(context.workspace_id, operation.operation_id, operation.validation.attempt_id)
    )
  end

  @doc "Starts the worker only for a newly admitted attempt."
  def ensure(%WorkspaceContext{} = context, %RebuildOperation{} = operation, opts \\ []) do
    DynamicSupervisor.start_child(
      FavnOrchestrator.RebuildPlanningSupervisor,
      {__MODULE__,
       [
         context: context,
         operation: operation,
         resume: Keyword.get(opts, :resume, &Rebuilds.resume_planning/2)
       ]}
    )
    |> case do
      {:error, {:already_started, pid}} -> {:ok, pid}
      result -> result
    end
  end

  @doc "Starts a newly admitted attempt and waits within its fixed deadline."
  def ensure_and_await(context, operation, opts \\ []) do
    with {:ok, pid} <- ensure(context, operation, opts), do: await(pid, context, operation)
  end

  @doc "Observes a replay without ever starting a replacement worker."
  def await_existing(_context, %{validation: nil} = operation),
    do: {:error, FavnOrchestrator.Rebuild.Validation.failure(operation.operation_id, :plan)}

  def await_existing(context, operation) do
    case Registry.lookup(
           FavnOrchestrator.RebuildPlanningRegistry,
           {context.workspace_id, operation.operation_id, operation.validation.attempt_id}
         ) do
      [{pid, _}] -> await(pid, context, operation)
      [] -> settled(context, operation)
    end
  end

  @doc "Lists process-local workers; durable leases remain authoritative."
  def active_operation_ids(workspace_id),
    do:
      Registry.select(FavnOrchestrator.RebuildPlanningRegistry, [
        {{{workspace_id, :"$1", :_}, :_, :_}, [], [:"$1"]}
      ])

  @impl true
  def init(opts) do
    Process.flag(:trap_exit, true)

    state = %{
      context: Keyword.fetch!(opts, :context),
      operation: Keyword.fetch!(opts, :operation),
      execute: Keyword.fetch!(opts, :resume),
      task: nil,
      waiters: [],
      timer: nil
    }

    {:ok, state, {:continue, :start}}
  end

  @impl true
  def handle_continue(:start, state) do
    case renew(state) do
      :ok ->
        task = Task.async(fn -> state.execute.(state.context, state.operation) end)
        {:noreply, schedule(%{state | task: task})}

      _ ->
        finish({:error, failure(state.operation)}, state)
    end
  end

  @impl true
  def handle_call(:await, from, state), do: {:noreply, %{state | waiters: [from | state.waiters]}}

  @impl true
  def handle_info({ref, result}, %{task: %Task{ref: ref}} = state) do
    Process.demonitor(ref, [:flush])
    finish(result, %{state | task: nil})
  end

  def handle_info({:DOWN, ref, :process, _pid, _reason}, %{task: %Task{ref: ref}} = state),
    do: finish({:error, failure(state.operation)}, %{state | task: nil})

  def handle_info({:EXIT, _, _}, state), do: {:noreply, state}

  def handle_info(:renew, state) do
    if renew(state) == :ok,
      do: {:noreply, schedule(state)},
      else: finish({:error, failure(state.operation)}, state)
  end

  @impl true
  def terminate(_, state) do
    if state.timer, do: Process.cancel_timer(state.timer)
    if state.task, do: Task.shutdown(state.task, :brutal_kill)
    :ok
  end

  defp renew(%{operation: %{validation: nil}}), do: {:error, :validation_required}

  defp renew(state) do
    v = state.operation.validation

    if v.status == "active" and DateTime.compare(v.deadline_at, DateTime.utc_now()) == :gt do
      Persistence.stores().rebuilds.renew_operation_lease(%RenewRebuildOperationLease{
        workspace_context: state.context,
        command_id:
          "validation-renew:" <>
            v.attempt_id <> ":" <> Integer.to_string(System.unique_integer([:positive])),
        operation_id: v.operation_id,
        owner_id: v.owner_id,
        fencing_token: v.fencing_token,
        lease_duration_ms: 30_000,
        occurred_at: DateTime.utc_now()
      })
    else
      {:error, :validation_interrupted}
    end
  end

  defp schedule(state), do: %{state | timer: Process.send_after(self(), :renew, 10_000)}

  defp finish(result, state) do
    result =
      case result do
        {:error, %Error{details: %{reason_code: "rebuild_input_resolution_unsupported"}}} ->
          {:error,
           %{
             failure(state.operation)
             | details: %{
                 reason_code: "rebuild_input_resolution_unsupported",
                 operation_id: state.operation.operation_id
               },
               message:
                 "The pinned runner release does not support rebuild input checks. Activate an upgraded release and create a new plan."
           }}

        {:error, %Error{kind: kind} = error} when kind in [:conflict, :fenced] ->
          {:error,
           Error.new(kind, "Rebuild checks are no longer current. Retry manually.",
             details: %{
               reason_code:
                 if(error.details[:reason_code] == "rebuild_plan_stale",
                   do: "rebuild_plan_stale",
                   else: "rebuild_validation_interrupted"
                 ),
               operation_id: state.operation.operation_id
             }
           )}

        {:error, _} ->
          {:error, failure(state.operation)}

        _ ->
          result
      end

    result =
      if match?({:error, _}, result) and state.operation.validation do
        case Persistence.stores().rebuilds.close_validation(%CloseRebuildValidation{
               workspace_context: state.context,
               validation: state.operation.validation,
               reason: elem(result, 1)
             }) do
          {:ok, operation} -> terminal_result(operation)
          _ -> {:error, unavailable()}
        end
      else
        result
      end

    Enum.each(state.waiters, &GenServer.reply(&1, result))
    {:stop, :normal, state}
  end

  defp await(pid, context, operation) do
    GenServer.call(pid, :await, 305_000)
  catch
    :exit, _ -> settled(context, operation)
  end

  defp settled(context, operation) do
    case Persistence.stores().rebuilds.get_validation(
           %FavnOrchestrator.Persistence.Queries.GetRebuildValidation{
             workspace_context: context,
             validation: operation.validation
           }
         ) do
      {:ok, %{validation: %{status: "active"}} = current} ->
        now = DateTime.utc_now()

        expired =
          DateTime.compare(current.validation.deadline_at, now) != :gt or
            (current.dispatcher.expires_at &&
               DateTime.compare(current.dispatcher.expires_at, now) != :gt)

        if expired do
          case Persistence.stores().rebuilds.close_validation(%CloseRebuildValidation{
                 workspace_context: context,
                 validation: operation.validation,
                 expired_only: true
               }) do
            {:ok, closed} -> terminal_result(closed)
            _ -> {:error, unavailable()}
          end
        else
          Process.sleep(100)
          settled(context, operation)
        end

      {:ok, current} ->
        terminal_result(current)

      {:error, %Error{kind: kind}} = error when kind in [:invalid, :conflict, :fenced] ->
        error

      _ ->
        {:error, unavailable()}
    end
  end

  defp terminal_result(%{validation: %{status: "accepted"}} = operation), do: {:ok, operation}

  defp terminal_result(%{validation: %{status: "failed", failure: failure}}) when is_map(failure),
    do: {:error, FavnOrchestrator.Rebuild.Validation.decode_error(failure)}

  defp terminal_result(_), do: {:error, unavailable()}

  defp unavailable,
    do:
      Error.new(:unavailable, "Rebuild request outcome is not yet known. Retry the same request.",
        retryable?: true
      )

  defp failure(operation),
    do:
      FavnOrchestrator.Rebuild.Validation.failure(
        operation.operation_id,
        operation.validation.purpose
      )

  defp via(workspace, operation, attempt),
    do:
      {:via, Registry,
       {FavnOrchestrator.RebuildPlanningRegistry, {workspace, operation, attempt}}}
end
