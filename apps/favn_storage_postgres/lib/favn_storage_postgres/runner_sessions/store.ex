defmodule FavnStoragePostgres.RunnerSessions.Store do
  @moduledoc """
  Durable runner session history.

  Sessions are diagnostics, not control-plane authority. Opens are idempotent
  by session id and repair a lost close by closing any other open row for the
  same runner instance as presumed dead. Closes never overwrite an already
  recorded end reason.
  """

  import Ecto.Query

  alias FavnOrchestrator.Persistence.Commands, as: C
  alias FavnOrchestrator.Persistence.Error
  alias FavnOrchestrator.Persistence.Queries, as: Q
  alias FavnOrchestrator.Persistence.Results.RunnerSession, as: SessionResult
  alias FavnOrchestrator.Persistence.Results.RunnerSessionWindowTotals
  alias FavnStoragePostgres.ErrorMapper
  alias FavnStoragePostgres.Repo
  alias FavnStoragePostgres.Schemas.RunnerSession, as: Session
  alias FavnStoragePostgres.Schemas.RunnerTask

  @end_reasons ~w(shut_down crashed presumed_dead)
  @states [:connected, :shut_down, :crashed, :presumed_dead]

  @spec open(C.OpenRunnerSession.t()) :: {:ok, SessionResult.t()} | {:error, Error.t()}
  def open(%C.OpenRunnerSession{} = command) do
    transact(fn ->
      validate_platform_context!(command.platform_context)

      unless valid_open?(command) do
        Repo.rollback(Error.new(:invalid, "invalid runner session open command"))
      end

      now = command.registered_at

      {_orphans, _} =
        Repo.update_all(
          from(session in Session,
            where:
              session.runner_instance_id == ^command.runner_instance_id and
                is_nil(session.ended_at) and session.session_id != ^command.session_id
          ),
          set: [ended_at: now, end_reason: "presumed_dead", updated_at: now]
        )

      row = %{
        session_id: command.session_id,
        runner_instance_id: command.runner_instance_id,
        runner_boot_id: command.runner_boot_id,
        session_generation: command.session_generation,
        control_plane_boot_id: command.control_plane_boot_id,
        runner_pool: command.runner_pool,
        required_runner_release_id: command.required_runner_release_id,
        beam_node: command.beam_node,
        protocol_version: command.protocol_version,
        lifecycle_mode: to_string(command.lifecycle_mode),
        registered_at: command.registered_at,
        busy_at_exit: false,
        inserted_at: now,
        updated_at: now
      }

      {_count, _} =
        Repo.insert_all(Session, [row], on_conflict: :nothing, conflict_target: [:session_id])

      Repo.get_by!(Session, session_id: command.session_id) |> to_result()
    end)
  end

  @spec close(C.CloseRunnerSession.t()) ::
          {:ok, :closed | :already_closed | :not_found} | {:error, Error.t()}
  def close(%C.CloseRunnerSession{} = command) do
    transact(fn ->
      validate_platform_context!(command.platform_context)

      unless command.end_reason in [:shut_down, :crashed] and
               match?(%DateTime{}, command.ended_at) do
        Repo.rollback(Error.new(:invalid, "invalid runner session close command"))
      end

      updates = [
        ended_at: command.ended_at,
        end_reason: Atom.to_string(command.end_reason),
        busy_at_exit: command.busy_at_exit == true,
        interrupted_task_workspace_id: command.interrupted_task_workspace_id,
        interrupted_task_id: command.interrupted_task_id,
        updated_at: command.ended_at
      ]

      closed =
        Repo.update_all(
          from(session in Session,
            where: session.session_id == ^command.session_id and is_nil(session.ended_at)
          ),
          set: updates
        )

      case closed do
        {1, _} ->
          :closed

        {0, _} ->
          if Repo.exists?(
               from(session in Session, where: session.session_id == ^command.session_id)
             ),
             do: :already_closed,
             else: :not_found
      end
    end)
  end

  @spec reconcile_boot(C.ReconcileRunnerSessions.t()) ::
          {:ok, non_neg_integer()} | {:error, Error.t()}
  def reconcile_boot(%C.ReconcileRunnerSessions{} = command) do
    transact(fn ->
      validate_platform_context!(command.platform_context)

      {count, _} =
        Repo.update_all(
          from(session in Session,
            where:
              session.control_plane_boot_id != ^command.control_plane_boot_id and
                is_nil(session.ended_at)
          ),
          set: [
            ended_at: command.ended_at,
            end_reason: "presumed_dead",
            updated_at: command.ended_at
          ]
        )

      count
    end)
  end

  @spec prune(C.PruneRunnerSessions.t()) :: {:ok, non_neg_integer()} | {:error, Error.t()}
  def prune(%C.PruneRunnerSessions{} = command) do
    transact(fn ->
      validate_platform_context!(command.platform_context)

      unless is_integer(command.limit) and command.limit > 0 do
        Repo.rollback(Error.new(:invalid, "invalid runner session prune limit"))
      end

      prunable =
        from(session in Session,
          where: not is_nil(session.ended_at) and session.ended_at < ^command.older_than,
          select: session.session_id,
          limit: ^command.limit
        )

      {count, _} =
        Repo.delete_all(from(session in Session, where: session.session_id in subquery(prunable)))

      count
    end)
  end

  @spec page(Q.PageRunnerSessions.t()) :: {:ok, [SessionResult.t()]} | {:error, Error.t()}
  def page(%Q.PageRunnerSessions{} = query) do
    read(fn ->
      if valid_page_query?(query) do
        sessions =
          Session
          |> filter_overlap(query.overlapping_after)
          |> filter_states(query.states)
          |> order_by([session], desc: session.registered_at, asc: session.session_id)
          |> limit(^query.limit)
          |> Repo.all()

        counts =
          if query.include_task_counts and sessions != [],
            do: task_counts(sessions),
            else: %{}

        Enum.map(sessions, fn session ->
          session |> to_result() |> struct!(task_counts: Map.get(counts, session.session_id, %{}))
        end)
      else
        {:error, Error.new(:invalid, "invalid runner session page query")}
      end
    end)
  end

  @spec window_totals(Q.GetRunnerSessionWindowTotals.t()) ::
          {:ok, RunnerSessionWindowTotals.t()} | {:error, Error.t()}
  def window_totals(%Q.GetRunnerSessionWindowTotals{} = query) do
    read(fn ->
      with true <- valid_platform_context?(query.platform_context),
           %DateTime{} = window_start <- query.window_start,
           %DateTime{} = window_end <- query.window_end,
           :lt <- DateTime.compare(window_start, window_end) do
        {session_count, awake_ms} =
          Repo.one(
            from(session in Session,
              where:
                session.registered_at <= ^window_end and
                  (is_nil(session.ended_at) or session.ended_at >= ^window_start),
              select: {
                count(session.session_id),
                fragment(
                  "COALESCE(CAST(SUM(EXTRACT(EPOCH FROM (LEAST(COALESCE(?, ?), ?) - GREATEST(?, ?))) * 1000) AS BIGINT), 0)",
                  session.ended_at,
                  ^window_end,
                  ^window_end,
                  session.registered_at,
                  ^window_start
                )
              }
            )
          )

        busy_ms =
          Repo.one(
            from(task in RunnerTask,
              where:
                not is_nil(task.assigned_at) and not is_nil(task.terminal_at) and
                  task.assigned_runner_session_generation > 0 and
                  task.assigned_at <= ^window_end and task.terminal_at >= ^window_start,
              select:
                fragment(
                  "COALESCE(CAST(SUM(EXTRACT(EPOCH FROM (LEAST(?, ?) - GREATEST(?, ?))) * 1000) AS BIGINT), 0)",
                  task.terminal_at,
                  ^window_end,
                  task.assigned_at,
                  ^window_start
                )
            )
          )

        %RunnerSessionWindowTotals{
          session_count: session_count,
          awake_ms: max(awake_ms, 0),
          busy_ms: max(busy_ms, 0)
        }
      else
        _invalid -> {:error, Error.new(:invalid, "invalid runner session totals query")}
      end
    end)
  end

  defp filter_overlap(queryable, nil), do: queryable

  defp filter_overlap(queryable, %DateTime{} = after_at) do
    from(session in queryable,
      where: is_nil(session.ended_at) or session.ended_at >= ^after_at
    )
  end

  defp filter_states(queryable, :all), do: queryable

  defp filter_states(queryable, states) when is_list(states) do
    reasons =
      states
      |> Enum.filter(&(&1 in [:shut_down, :crashed, :presumed_dead]))
      |> Enum.map(&Atom.to_string/1)

    case {:connected in states, reasons} do
      {true, []} ->
        from(session in queryable, where: is_nil(session.ended_at))

      {true, reasons} ->
        from(session in queryable,
          where: is_nil(session.ended_at) or session.end_reason in ^reasons
        )

      {false, reasons} ->
        from(session in queryable, where: session.end_reason in ^reasons)
    end
  end

  defp task_counts(sessions) do
    ids = Enum.map(sessions, & &1.session_id)

    from(task in RunnerTask,
      join: session in Session,
      on:
        task.assigned_runner_instance_id == session.runner_instance_id and
          task.assigned_runner_session_generation == session.session_generation and
          task.assigned_at >= session.registered_at and
          (is_nil(session.ended_at) or task.assigned_at <= session.ended_at),
      where: session.session_id in ^ids and not is_nil(task.terminal_at),
      group_by: [session.session_id, task.status],
      select: {session.session_id, task.status, count(task.task_id)}
    )
    |> Repo.all()
    |> Enum.reduce(%{}, fn {session_id, status, count}, acc ->
      Map.update(acc, session_id, %{String.to_existing_atom(status) => count}, fn counts ->
        Map.put(counts, String.to_existing_atom(status), count)
      end)
    end)
  end

  defp valid_open?(command) do
    is_binary(command.session_id) and String.match?(command.session_id, ~r/^rs_[0-9a-f]{32}$/) and
      is_binary(command.runner_instance_id) and command.runner_instance_id != "" and
      is_binary(command.runner_boot_id) and command.runner_boot_id != "" and
      is_integer(command.session_generation) and command.session_generation > 0 and
      is_binary(command.control_plane_boot_id) and command.control_plane_boot_id != "" and
      match?(%DateTime{}, command.registered_at)
  end

  defp valid_page_query?(query) do
    valid_platform_context?(query.platform_context) and is_integer(query.limit) and
      query.limit in 1..200 and
      (query.states == :all or
         (is_list(query.states) and query.states != [] and
            Enum.all?(query.states, &(&1 in @states)))) and
      (is_nil(query.overlapping_after) or match?(%DateTime{}, query.overlapping_after))
  end

  defp validate_platform_context!(context) do
    if valid_platform_context?(context),
      do: :ok,
      else: Repo.rollback(Error.new(:invalid, "invalid platform runner session authority"))
  end

  defp valid_platform_context?(
         %FavnOrchestrator.Persistence.PlatformContext{roles: roles} = context
       ) do
    FavnOrchestrator.Persistence.PlatformContext.valid?(context) and
      Enum.any?(roles, &(&1 in [:platform_operator, :platform_admin]))
  end

  defp valid_platform_context?(_context), do: false

  defp transact(fun) do
    case Repo.transaction(fun) do
      {:ok, result} -> {:ok, result}
      {:error, %Error{} = error} -> {:error, error}
      {:error, reason} -> {:error, ErrorMapper.map(reason)}
    end
  rescue
    error -> {:error, ErrorMapper.map(error)}
  end

  defp read(fun) do
    case fun.() do
      {:error, %Error{} = error} -> {:error, error}
      value -> {:ok, value}
    end
  rescue
    error -> {:error, ErrorMapper.map(error)}
  catch
    :exit, reason -> {:error, ErrorMapper.map(reason)}
  end

  defp to_result(%Session{} = session) do
    %SessionResult{
      session_id: session.session_id,
      runner_instance_id: session.runner_instance_id,
      runner_boot_id: session.runner_boot_id,
      session_generation: session.session_generation,
      control_plane_boot_id: session.control_plane_boot_id,
      runner_pool: session.runner_pool,
      required_runner_release_id: session.required_runner_release_id,
      beam_node: session.beam_node,
      protocol_version: session.protocol_version,
      lifecycle_mode: session.lifecycle_mode,
      registered_at: session.registered_at,
      ended_at: session.ended_at,
      end_reason: decode_end_reason(session.end_reason),
      busy_at_exit: session.busy_at_exit,
      interrupted_task_workspace_id: session.interrupted_task_workspace_id,
      interrupted_task_id: session.interrupted_task_id,
      inserted_at: session.inserted_at
    }
  end

  defp decode_end_reason(nil), do: nil
  defp decode_end_reason(reason) when reason in @end_reasons, do: String.to_existing_atom(reason)
end
