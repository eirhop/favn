defmodule FavnOrchestrator.OperationCancellation do
  @moduledoc """
  Cancels the submitted operation from any operator run detail. Automatic
  backfill/recovery members share an owner; separately requested reruns do not.
  Acceptance is durable intent, not an acknowledgement that external work stopped.
  Existing dispatch/recovery workers reconcile bounded pages without admission.
  """
  alias FavnOrchestrator.{OperationalEvents, Persistence, RunManager, RunOwnership, RunnerTasks}
  alias FavnOrchestrator.Persistence.Error
  alias FavnOrchestrator.Persistence.Commands.RequestRunCancellation
  alias FavnOrchestrator.Persistence.Queries.{GetRun, PageCancellingOperations}
  alias FavnOrchestrator.Persistence.Results.CancellationScope
  alias FavnOrchestrator.Persistence.{SystemContext, WorkspaceContext}

  @doc "Resolves the authoritative scope and cancellation presentation for one detail."
  @spec scope(WorkspaceContext.t(), String.t()) :: {:ok, CancellationScope.t()} | {:error, term()}
  def scope(context, run_id),
    do: store().cancellation_scope(%GetRun{workspace_context: context, run_id: run_id})

  @doc "Records operation cancellation; repeated requests converge on the saved intent."
  @spec request(WorkspaceContext.t(), String.t(), map(), keyword()) :: :ok | {:error, term()}
  def request(context, run_id, reason, opts \\ []) do
    store().request_operation_cancellation(%RequestRunCancellation{
      workspace_context: context,
      run_id: run_id,
      reason: reason,
      command_id: "cancel-operation:" <> short_hash(run_id),
      occurred_at: DateTime.utc_now(),
      idempotency: Keyword.get(opts, :idempotency)
    })
  end

  @doc false
  def start_cleanup(%{cancellation_task: nil} = state, discover_workspaces) do
    task =
      Task.Supervisor.async_nolink(
        FavnOrchestrator.RunManagerTaskSupervisor,
        fn ->
          workspace_ids = discover_workspaces.()

          Map.new([:run, :backfill], fn kind ->
            {kind, reconcile(workspace_ids, kind, Map.get(state.cancellation_cursors, kind, %{}))}
          end)
        end
      )

    %{state | cancellation_task: task.ref}
  end

  def start_cleanup(state, _workspace_ids), do: state

  @doc false
  def reconcile(workspace_ids, kind, cursors \\ %{}) do
    Enum.reduce(workspace_ids, cursors, fn workspace, cursors ->
      context = SystemContext.workspace(workspace, :operation_cancellation)

      case store().page_cancelling_operations(%PageCancellingOperations{
             workspace_context: context,
             kind: kind,
             after: Map.get(cursors, workspace),
             limit: 10
           }) do
        {:ok, page} ->
          Enum.each(page.items, &reconcile_owner(context, &1))
          Map.put(cursors, workspace, page.next_cursor)

        {:error, reason} ->
          report(workspace, nil, reason)
          cursors
      end
    end)
  end

  defp reconcile_owner(context, owner) do
    case store().reconcile_cancellation(%GetRun{workspace_context: context, run_id: owner}) do
      {:ok, work} ->
        reason = %{requested_by: :operator, reason: :operation_cancelled}

        Enum.each(
          work.task_ids,
          &RunnerTasks.request_cancellation(context.workspace_id, &1, reason, wait_for_ack: false)
        )

        Enum.each(work.run_ids, fn run_id ->
          case RunManager.cancel_run(context, run_id, reason, wait_for_ack: false) do
            :ok ->
              :ok

            {:error, :run_already_terminal} ->
              :ok

            {:error, {:runner_cancel_already_completed, _}} ->
              recover_cancelled_run(context, run_id)

            {:error, reason} ->
              report(context.workspace_id, run_id, reason)
          end
        end)

      {:error, reason} ->
        report(context.workspace_id, owner, reason)
    end
  end

  defp recover_cancelled_run(context, run_id) do
    with {:ok, ownership} <- RunOwnership.claim(context, run_id, RunOwnership.owner_id(run_id)),
         {:ok, _run_id} <- RunManager.recover_claimed_run(context, ownership) do
      :ok
    else
      {:error, %Error{kind: :conflict, retryable?: true}} -> :ok
      {:error, {:run_plan_capacity_exhausted, _}} -> :ok
      {:error, :run_not_recoverable} -> :ok
      {:error, reason} -> report(context.workspace_id, run_id, reason)
    end
  end

  defp report(workspace, run_id, reason),
    do:
      OperationalEvents.emit(
        :run_reconciliation_failed,
        %{},
        %{workspace_id: workspace, run_id: run_id, errors: {:operation_cancellation, reason}},
        level: :error
      )

  defp store, do: Persistence.stores().runs
  defp short_hash(id), do: :crypto.hash(:sha256, id) |> Base.url_encode64(padding: false)
end
