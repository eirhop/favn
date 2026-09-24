defmodule FavnOrchestrator.RunRecovery do
  @moduledoc false

  use GenServer

  alias FavnOrchestrator.OperationCancellation
  alias FavnOrchestrator.Lifecycle
  alias FavnOrchestrator.OperationalEvents
  alias FavnOrchestrator.ManifestStore
  alias FavnOrchestrator.Persistence.SystemContext
  alias FavnOrchestrator.RunManager
  alias FavnOrchestrator.Persistence

  @default_interval_ms 5_000
  @default_batch_size 64

  @spec child_spec(keyword()) :: Supervisor.child_spec()
  def child_spec(opts) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [opts]},
      restart: :permanent,
      type: :worker
    }
  end

  @spec start_link(keyword()) :: GenServer.on_start() | :ignore
  def start_link(opts \\ []) when is_list(opts) do
    if Keyword.get(opts, :enabled, true),
      do: GenServer.start_link(__MODULE__, opts, name: __MODULE__),
      else: :ignore
  end

  @spec reconcile_orphaned_runs() :: :ok
  def reconcile_orphaned_runs, do: reconcile_workspaces()

  @impl true
  def init(opts) do
    state = %{
      cancellation_task: nil,
      cancellation_cursors: %{},
      interval_ms: Keyword.get(opts, :interval_ms, @default_interval_ms),
      batch_size: Keyword.get(opts, :batch_size, @default_batch_size)
    }

    send(self(), :reconcile)
    {:ok, state}
  end

  @impl true
  def handle_info({ref, cursors}, %{cancellation_task: ref} = state) do
    Process.demonitor(ref, [:flush])
    {:noreply, %{state | cancellation_task: nil, cancellation_cursors: cursors}}
  end

  def handle_info({:DOWN, ref, :process, _pid, _reason}, %{cancellation_task: ref} = state),
    do: {:noreply, %{state | cancellation_task: nil}}

  @impl true
  def handle_info(:reconcile, state) do
    state = OperationCancellation.start_cleanup(state, &authoritative_workspace_ids/0)
    _ = reconcile_workspaces(state.batch_size)
    Process.send_after(self(), :reconcile, state.interval_ms)
    {:noreply, state}
  end

  defp reconcile_workspaces(batch_size \\ @default_batch_size) do
    authoritative_workspace_ids()
    |> Enum.each(&recover_workspace(&1, batch_size))

    :ok
  end

  defp recover_workspace(workspace_id, batch_size) do
    context = SystemContext.workspace(workspace_id, :run_recovery)

    Lifecycle.with_admission(fn ->
      case Persistence.stores().run_ownership.recovery_candidates(context, min(batch_size, 64)) do
        {:ok, ids} -> Enum.each(ids, &RunManager.recover_candidate(context, &1))
        {:error, reason} -> emit_failure(nil, {:recovery_candidates_failed, workspace_id, reason})
      end
    end)

    case Persistence.stores().run_ownership.cleanup_candidates(context, min(batch_size, 2)) do
      {:ok, ids} -> Enum.each(ids, &RunManager.recover_cleanup(context, &1))
      {:error, reason} -> emit_failure(nil, {:cleanup_candidates_failed, workspace_id, reason})
    end
  end

  defp authoritative_workspace_ids do
    context = SystemContext.platform(:run_recovery_workspace_discovery)

    case page_workspace_ids(context, nil, []) do
      {:ok, workspace_ids} ->
        workspace_ids

      {:error, reason} ->
        emit_failure(nil, {:workspace_discovery_failed, reason})
        []
    end
  end

  defp page_workspace_ids(context, cursor, acc) do
    case ManifestStore.page_workspaces(context, after: cursor, limit: 500) do
      {:ok, page} when page.has_more? ->
        page_workspace_ids(context, page.next_cursor, [page.items | acc])

      {:ok, page} ->
        {:ok, acc |> Enum.reverse() |> List.flatten() |> Kernel.++(page.items)}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp emit_failure(run_id, errors) do
    OperationalEvents.emit(
      :run_reconciliation_failed,
      %{},
      %{run_id: run_id, errors: errors},
      level: :error
    )
  end
end
