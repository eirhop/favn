defmodule FavnOrchestrator.RunRecovery do
  @moduledoc false

  use GenServer

  alias FavnOrchestrator.OperationalEvents
  alias FavnOrchestrator.Persistence.SystemContext
  alias FavnOrchestrator.Persistence.WorkspaceContext
  alias FavnOrchestrator.RunManager
  alias FavnOrchestrator.RunOwnership

  @default_interval_ms 5_000
  @default_batch_size 100

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
      interval_ms: Keyword.get(opts, :interval_ms, @default_interval_ms),
      batch_size: Keyword.get(opts, :batch_size, @default_batch_size)
    }

    send(self(), :reconcile)
    {:ok, state}
  end

  @impl true
  def handle_info(:reconcile, state) do
    reconcile_workspaces(state.batch_size)
    Process.send_after(self(), :reconcile, state.interval_ms)
    {:noreply, state}
  end

  defp reconcile_workspaces(batch_size \\ @default_batch_size) do
    workspace_ids()
    |> Enum.each(&recover_workspace(&1, batch_size))

    :ok
  end

  defp recover_workspace(workspace_id, batch_size) do
    context = SystemContext.workspace(workspace_id, :run_recovery)
    owner_id = recovery_owner_id(workspace_id)

    case RunOwnership.claim_recovery_batch(context, owner_id,
           batch_id: recovery_batch_id(workspace_id),
           limit: batch_size
         ) do
      {:ok, ownerships} ->
        Enum.each(ownerships, &recover_claimed(context, &1))

      {:error, reason} ->
        emit_failure(nil, {:recovery_claim_failed, workspace_id, reason})
    end
  end

  defp recover_claimed(%WorkspaceContext{} = context, ownership) do
    case RunManager.recover_claimed_run(context, ownership) do
      {:ok, _run_id} -> :ok
      {:error, reason} -> emit_failure(ownership.run_id, {:run_recovery_failed, reason})
    end
  end

  defp workspace_ids do
    :favn_orchestrator
    |> Application.get_env(:workspace_ids, [])
    |> Enum.filter(&(is_binary(&1) and &1 != ""))
    |> Enum.uniq()
  end

  defp recovery_owner_id(workspace_id) do
    instance = System.get_env("FAVN_INSTANCE_ID", Atom.to_string(node()))
    "#{String.slice(instance, 0, 96)}:run-recovery:#{short_hash(workspace_id)}"
  end

  defp recovery_batch_id(workspace_id) do
    "recovery:#{short_hash({workspace_id, System.monotonic_time(:millisecond)})}"
  end

  defp short_hash(value) do
    :crypto.hash(:sha256, :erlang.term_to_binary(value))
    |> Base.url_encode64(padding: false)
    |> binary_part(0, 24)
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
