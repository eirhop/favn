defmodule FavnOrchestrator.RunnerSessions do
  @moduledoc """
  Durable runner session lifecycle writes.

  Session history is observability: every write here logs on failure and
  returns, and never blocks registration, claiming, recovery, or shutdown.
  A lost close is repaired by the runner's next registration (the open write
  closes orphaned rows) or by boot reconciliation.
  """

  require Logger

  alias FavnOrchestrator.Persistence
  alias FavnOrchestrator.Persistence.Commands, as: C
  alias FavnOrchestrator.Persistence.SystemContext
  alias FavnOrchestrator.RunnerRegistry.Session

  @boot_id_key {__MODULE__, :control_plane_boot_id}
  @prune_watermark_key {__MODULE__, :prune_watermark}
  @prune_interval_ms :timer.hours(24)
  @retention_days 90

  @doc "Returns the stable identity of this control-plane boot."
  @spec control_plane_boot_id() :: String.t()
  def control_plane_boot_id do
    case :persistent_term.get(@boot_id_key, nil) do
      nil ->
        candidate = "cpb_" <> Base.encode16(:crypto.strong_rand_bytes(16), case: :lower)
        :persistent_term.put(@boot_id_key, candidate)
        :persistent_term.get(@boot_id_key)

      boot_id ->
        boot_id
    end
  end

  @doc """
  Opens the durable session row for a newly accepted registry session.

  Idempotent by the session row id minted at acceptance. Also prunes old
  closed sessions opportunistically, at most once per day per boot.
  """
  @spec open(Session.t()) :: :ok
  def open(%Session{session_row_id: session_row_id} = session)
      when is_binary(session_row_id) do
    command = %C.OpenRunnerSession{
      platform_context: platform_context(),
      session_id: session_row_id,
      runner_instance_id: session.runner_instance_id,
      runner_boot_id: session.boot_id,
      session_generation: session.session_generation,
      control_plane_boot_id: control_plane_boot_id(),
      runner_pool: session.runner_pool,
      required_runner_release_id: session.required_runner_release_id,
      beam_node: session.beam_node,
      protocol_version: session.protocol_version,
      lifecycle_mode: session.lifecycle_mode,
      registered_at: session.registered_at
    }

    case store_call(:open_session, command) do
      {:ok, _session} -> :ok
      {:error, error} -> log_failure(:open, session.runner_instance_id, error)
    end

    maybe_prune()
    :ok
  end

  def open(%Session{}), do: :ok

  @doc """
  Closes the durable session row for an exited registry session.

  Classification: a normal or shutdown exit reason closes as shut down; any
  other reason closes as crashed. A session that was busy at exit records the
  interrupted task independently of the reason.
  """
  @spec close(Session.t(), term(), DateTime.t()) :: :ok
  def close(session, exit_reason, ended_at \\ DateTime.utc_now())

  def close(%Session{session_row_id: session_row_id} = session, exit_reason, ended_at)
      when is_binary(session_row_id) do
    {busy_at_exit, interrupted} = exit_activity(session)

    command = %C.CloseRunnerSession{
      platform_context: platform_context(),
      session_id: session_row_id,
      ended_at: ended_at,
      end_reason: classify_exit(exit_reason),
      busy_at_exit: busy_at_exit,
      interrupted_task_workspace_id: interrupted && interrupted.workspace_id,
      interrupted_task_id: interrupted && interrupted.task_id
    }

    case store_call(:close_session, command) do
      {:ok, outcome} when outcome in [:closed, :already_closed] -> :ok
      {:ok, :not_found} -> log_failure(:close, session.runner_instance_id, :session_row_missing)
      {:error, error} -> log_failure(:close, session.runner_instance_id, error)
    end

    :ok
  end

  def close(%Session{}, _exit_reason, _ended_at), do: :ok

  @doc """
  Closes rows opened by earlier control-plane boots as presumed dead.

  Runs once at orchestrator startup; idempotent and safe to re-run. Rows
  opened by the current boot are never touched.
  """
  @spec reconcile_boot() :: :ok
  def reconcile_boot do
    command = %C.ReconcileRunnerSessions{
      platform_context: platform_context(),
      control_plane_boot_id: control_plane_boot_id(),
      ended_at: DateTime.utc_now()
    }

    case store_call(:reconcile_sessions, command) do
      {:ok, 0} -> :ok
      {:ok, count} -> Logger.info("runner_sessions.reconcile_boot closed #{count} sessions")
      {:error, error} -> log_failure(:reconcile_boot, "all", error)
    end

    :ok
  end

  defp maybe_prune do
    now = System.monotonic_time(:millisecond)

    case :persistent_term.get(@prune_watermark_key, nil) do
      last when is_integer(last) and now - last < @prune_interval_ms ->
        :ok

      _stale ->
        :persistent_term.put(@prune_watermark_key, now)

        command = %C.PruneRunnerSessions{
          platform_context: platform_context(),
          older_than: DateTime.add(DateTime.utc_now(), -@retention_days, :day)
        }

        case store_call(:prune_sessions, command) do
          {:ok, _count} -> :ok
          {:error, error} -> log_failure(:prune, "all", error)
        end
    end
  end

  defp store_call(operation, command) do
    apply(Persistence.stores().runner_tasks, operation, [command])
  rescue
    error -> {:error, error.__struct__}
  catch
    :exit, _reason -> {:error, :persistence_unavailable}
  end

  defp classify_exit(reason) when reason in [:normal, :shutdown], do: :shut_down
  defp classify_exit({:shutdown, _term}), do: :shut_down
  defp classify_exit(_reason), do: :crashed

  defp exit_activity(%Session{status: status, active_assignment: assignment})
       when status in [:busy, :reserved] do
    case assignment do
      %{workspace_id: workspace_id, task_id: task_id} ->
        {true, %{workspace_id: workspace_id, task_id: task_id}}

      _other ->
        {true, nil}
    end
  end

  defp exit_activity(%Session{}), do: {false, nil}

  defp log_failure(operation, runner_instance_id, error) do
    Logger.warning(
      "runner_sessions.#{operation} failed runner=#{runner_instance_id} error=#{error_class(error)}"
    )
  end

  defp error_class(%{kind: kind}) when is_atom(kind), do: kind
  defp error_class(reason) when is_atom(reason), do: reason
  defp error_class(_reason), do: :unknown

  defp platform_context,
    do: SystemContext.platform(:runner_sessions, roles: [:platform_operator])
end
