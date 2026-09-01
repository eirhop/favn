defmodule FavnOrchestrator.RunnerOverview do
  @moduledoc """
  Bounded operator read model for runner health.

  Live presence is a process-local observation. Capacity demand, session
  history, and task attribution come from PostgreSQL and survive runner and
  control-plane restarts. Reads are platform-global after operator
  reauthorization (runners serve every workspace); task-level detail stays
  scoped to the operator's workspace.
  """

  alias FavnOrchestrator.Persistence
  alias FavnOrchestrator.Persistence.Queries, as: Q
  alias FavnOrchestrator.Persistence.Results.RunnerSession
  alias FavnOrchestrator.Persistence.Results.RunnerTask
  alias FavnOrchestrator.Persistence.SystemContext
  alias FavnOrchestrator.Persistence.WorkspaceContext
  alias FavnOrchestrator.RunnerRegistry

  @default_window_days 30
  @default_failed_window_hours 24
  @struggle_max_duration_ms 60_000
  @struggle_min_count 3
  @struggle_gap_ms :timer.minutes(15)
  @busy_statuses [:claiming, :reserved, :busy]

  @doc """
  Returns live presence, capacity, workspace task stats, and session history.

  Options:

    * `:overlapping_after` — keep sessions whose lifetime overlaps the window
      starting there; `nil` means no window (still bounded by `:limit`).
    * `:states` — `:all` or a subset of `:connected`, `:shut_down`,
      `:crashed`, `:presumed_dead`, `:struggling`, applied to the derived
      session entries.
    * `:limit` — page size for the session history, `1..200`.
  """
  @spec get(WorkspaceContext.t(), keyword()) :: {:ok, map()} | {:error, term()}
  def get(%WorkspaceContext{} = context, opts \\ []) when is_list(opts) do
    limit = Keyword.get(opts, :limit, 50)
    overlapping_after = Keyword.get(opts, :overlapping_after)
    states = Keyword.get(opts, :states, :all)
    now = DateTime.utc_now()

    with :ok <- validate_options(limit, overlapping_after, states),
         window_start = overlapping_after || DateTime.add(now, -@default_window_days, :day),
         failed_since =
           overlapping_after || DateTime.add(now, -@default_failed_window_hours, :hour),
         {:ok, stats} <-
           Persistence.stores().runner_tasks.workspace_task_stats(%Q.GetWorkspaceRunnerTaskStats{
             workspace_context: context,
             failed_since: failed_since
           }),
         {:ok, session_rows} <-
           Persistence.stores().runner_tasks.page_sessions(%Q.PageRunnerSessions{
             platform_context: platform_context(),
             overlapping_after: overlapping_after,
             states: :all,
             limit: limit
           }),
         {:ok, totals} <-
           Persistence.stores().runner_tasks.session_window_totals(
             %Q.GetRunnerSessionWindowTotals{
               platform_context: platform_context(),
               window_start: window_start,
               window_end: now
             }
           ),
         {:ok, demands} <-
           Persistence.stores().runner_tasks.list_demands(%Q.ListRunnerCapacityDemands{
             platform_context: platform_context(),
             limit: 256
           }) do
      {registry_status, runners} = live_runners(context.workspace_id)

      sessions =
        session_rows
        |> merge_rows()
        |> Enum.map(&scrub_cross_workspace(&1, context.workspace_id))
        |> derive_struggling_groups()
        |> filter_states(states)

      {:ok,
       %{
         registry_status: registry_status,
         runners: runners,
         runner_count: length(runners),
         busy_runner_count: Enum.count(runners, &(&1.status in @busy_statuses)),
         capacity: capacity(demands, runners),
         workspace_tasks: %{
           queued_count: stats.queued_count,
           active_count: stats.active_count,
           failed_count: stats.failed_count,
           failed_since: failed_since,
           oldest_queued_at: stats.oldest_queued_at
         },
         sessions: sessions,
         totals: %{
           window_start: window_start,
           window_end: now,
           session_count: totals.session_count,
           awake_ms: totals.awake_ms,
           busy_ms: totals.busy_ms,
           idle_ms: max(totals.awake_ms - totals.busy_ms, 0)
         },
         observed_at: now
       }}
    end
  end

  @doc """
  Pages one displayed session's attributed tasks in the operator's workspace.

  Tasks from other workspaces stay hidden; they appear only in the session's
  aggregate counts.
  """
  @spec session_tasks(WorkspaceContext.t(), keyword()) ::
          {:ok, [map()]} | {:error, term()}
  def session_tasks(%WorkspaceContext{} = context, opts) when is_list(opts) do
    query = %Q.PageRunnerSessionTasks{
      workspace_context: context,
      runner_instance_id: Keyword.fetch!(opts, :runner_instance_id),
      session_generation: Keyword.fetch!(opts, :session_generation),
      registered_at: Keyword.fetch!(opts, :registered_at),
      ended_at: Keyword.get(opts, :ended_at),
      statuses: Keyword.get(opts, :statuses, [:failed, :unknown]),
      limit: Keyword.get(opts, :limit, 20)
    }

    with {:ok, tasks} <- Persistence.stores().runner_tasks.page_session_tasks(query) do
      {:ok, Enum.map(tasks, &task/1)}
    end
  end

  defp validate_options(limit, overlapping_after, states) do
    valid_states? =
      states == :all or
        (is_list(states) and states != [] and
           Enum.all?(
             states,
             &(&1 in [:connected, :shut_down, :crashed, :presumed_dead, :struggling])
           ))

    if is_integer(limit) and limit in 1..200 and valid_states? and
         (is_nil(overlapping_after) or match?(%DateTime{}, overlapping_after)),
       do: :ok,
       else: {:error, :invalid_runner_overview_options}
  end

  # Demand partitions are durable and never pruned, so every historical
  # release keeps a row; only partitions with demand, a connected runner, or
  # a health problem say anything about current capacity.
  defp capacity(demands, runners) do
    connected =
      Enum.frequencies_by(runners, &{&1.runner_pool, &1.required_runner_release_id})

    demands
    |> Enum.map(fn demand ->
      %{
        runner_pool: demand.runner_pool,
        required_runner_release_id: demand.required_runner_release_id,
        queued_count: demand.queued_count,
        active_count: demand.active_count,
        oldest_queued_at: demand.oldest_queued_at,
        connected_runner_count:
          Map.get(connected, {demand.runner_pool, demand.required_runner_release_id}, 0),
        healthy?: demand.healthy?
      }
    end)
    |> Enum.filter(&relevant_partition?/1)
    |> Enum.sort_by(&{&1.runner_pool, &1.required_runner_release_id})
  end

  defp relevant_partition?(row) do
    row.queued_count > 0 or row.active_count > 0 or row.connected_runner_count > 0 or
      not row.healthy?
  end

  defp live_runners(workspace_id) do
    case Process.whereis(RunnerRegistry) do
      nil ->
        {:unavailable, []}

      _pid ->
        runners =
          RunnerRegistry.list()
          |> Enum.map(&project_runner(&1, workspace_id))
          |> Enum.sort_by(& &1.runner_instance_id)

        {:available, runners}
    end
  catch
    :exit, _reason -> {:unavailable, []}
  end

  @doc false
  def project_runner(session, workspace_id) do
    %{
      runner_instance_id: session.runner_instance_id,
      beam_node: session.beam_node,
      runner_pool: session.runner_pool,
      required_runner_release_id: session.required_runner_release_id,
      protocol_version: session.protocol_version,
      supported_task_kinds: session.supported_task_kinds,
      capabilities: session.capabilities,
      lifecycle_mode: session.lifecycle_mode,
      status: session.status,
      registered_at: session.registered_at,
      active_task_id: assignment_task_id(session.active_assignment, workspace_id)
    }
  end

  defp assignment_task_id(%{workspace_id: workspace_id, task_id: task_id}, workspace_id),
    do: task_id

  defp assignment_task_id(_assignment, _workspace_id), do: nil

  defp merge_rows(rows) do
    rows
    |> Enum.group_by(&{&1.runner_instance_id, &1.runner_boot_id, &1.session_generation})
    |> Enum.map(fn {_key, grouped} -> merge_group(Enum.sort_by(grouped, & &1.registered_at)) end)
    |> Enum.sort_by(& &1.registered_at, {:desc, DateTime})
  end

  defp merge_group([%RunnerSession{} | _rest] = rows) do
    first = List.first(rows)
    last = List.last(rows)
    open? = Enum.any?(rows, &is_nil(&1.ended_at))

    %{
      kind: :session,
      runner_instance_id: first.runner_instance_id,
      runner_boot_id: first.runner_boot_id,
      session_generation: last.session_generation,
      runner_pool: first.runner_pool,
      required_runner_release_id: first.required_runner_release_id,
      beam_node: first.beam_node,
      protocol_version: first.protocol_version,
      lifecycle_mode: first.lifecycle_mode,
      registered_at: first.registered_at,
      ended_at: if(open?, do: nil, else: last.ended_at),
      state: if(open?, do: :connected, else: last.end_reason),
      busy_at_exit: last.busy_at_exit == true,
      interrupted_task_workspace_id: last.interrupted_task_workspace_id,
      interrupted_task_id: last.interrupted_task_id,
      task_counts: merge_counts(rows),
      row_count: length(rows)
    }
  end

  defp scrub_cross_workspace(entry, workspace_id) do
    scope =
      case entry.interrupted_task_workspace_id do
        nil -> :unknown
        ^workspace_id -> :own
        _other_workspace -> :foreign
      end

    entry
    |> Map.delete(:interrupted_task_workspace_id)
    |> Map.put(:interrupted_scope, scope)
    |> then(fn scrubbed ->
      if scope == :own, do: scrubbed, else: Map.put(scrubbed, :interrupted_task_id, nil)
    end)
  end

  defp merge_counts(rows) do
    Enum.reduce(rows, %{}, fn row, acc ->
      Map.merge(acc, row.task_counts, fn _status, left, right -> left + right end)
    end)
  end

  defp derive_struggling_groups(entries) do
    {candidates, rest} = Enum.split_with(entries, &struggle_candidate?/1)

    groups =
      candidates
      |> Enum.group_by(&{&1.runner_pool, &1.required_runner_release_id})
      |> Enum.flat_map(fn {{pool, release}, sessions} ->
        sessions
        |> Enum.sort_by(& &1.registered_at, DateTime)
        |> cluster_by_gap()
        |> Enum.map(&struggle_entry(pool, release, &1))
      end)

    clustered_ids =
      for %{sessions: sessions} <- groups,
          session <- sessions,
          into: MapSet.new(),
          do: {session.runner_instance_id, session.registered_at}

    kept_candidates =
      Enum.reject(
        candidates,
        &MapSet.member?(clustered_ids, {&1.runner_instance_id, &1.registered_at})
      )

    (rest ++ kept_candidates ++ groups)
    |> Enum.sort_by(&entry_sort_key/1, {:desc, DateTime})
  end

  defp struggle_candidate?(%{kind: :session} = entry) do
    entry.state != :connected and entry.task_counts == %{} and
      DateTime.diff(entry.ended_at, entry.registered_at, :millisecond) <=
        @struggle_max_duration_ms
  end

  defp struggle_candidate?(_entry), do: false

  defp cluster_by_gap(sessions) do
    sessions
    |> Enum.chunk_while(
      [],
      fn session, acc ->
        case acc do
          [] ->
            {:cont, [session]}

          [previous | _rest] ->
            if DateTime.diff(session.registered_at, previous.registered_at, :millisecond) <=
                 @struggle_gap_ms,
               do: {:cont, [session | acc]},
               else: {:cont, Enum.reverse(acc), [session]}
        end
      end,
      fn acc -> {:cont, Enum.reverse(acc), []} end
    )
    |> Enum.filter(&(length(&1) >= @struggle_min_count))
  end

  defp struggle_entry(pool, release, sessions) do
    %{
      kind: :struggling_group,
      runner_pool: pool,
      required_runner_release_id: release,
      session_count: length(sessions),
      first_registered_at: List.first(sessions).registered_at,
      last_registered_at: List.last(sessions).registered_at,
      sessions: Enum.sort_by(sessions, & &1.registered_at, {:desc, DateTime})
    }
  end

  defp entry_sort_key(%{kind: :session} = entry), do: entry.registered_at
  defp entry_sort_key(%{kind: :struggling_group} = entry), do: entry.last_registered_at

  defp filter_states(entries, :all), do: entries

  defp filter_states(entries, states) do
    Enum.filter(entries, fn
      %{kind: :struggling_group} -> :struggling in states
      %{kind: :session, state: state} -> state in states
    end)
  end

  @doc false
  def task(%RunnerTask{} = task) do
    %{
      task_id: task.task_id,
      task_kind: task.task_kind,
      run_id: task.run_id,
      operation_id: task.operation_id,
      status: task.status,
      runner_pool: task.runner_pool,
      required_runner_release_id: task.required_runner_release_id,
      required_capability: task.required_capability,
      assigned_runner_instance_id: task.assigned_runner_instance_id,
      retry_class: task.retry_class,
      enqueued_at: task.enqueued_at,
      assigned_at: task.assigned_at,
      terminal_at: task.terminal_at,
      failure: task_failure(task)
    }
  end

  defp task_failure(%RunnerTask{status: status, error: error})
       when status in [:failed, :unknown] and is_map(error) do
    message =
      error
      |> map_value(:message)
      |> fallback(map_value(error, :reason))
      |> printable("Runner task failed")

    type =
      error
      |> map_value(:type)
      |> fallback(map_value(error, :kind))
      |> printable("runner_task_failed")

    %{
      title: failure_title(type),
      code: to_string(type),
      message: to_string(message),
      phase: optional_string(map_value(error, :phase)),
      outcome: optional_string(map_value(error, :outcome)),
      retryable?: map_value(error, :retryable?),
      remediation: remediation(type, message)
    }
  end

  defp task_failure(_task), do: nil

  defp map_value(map, key) do
    Map.get(map, key) || Map.get(map, Atom.to_string(key))
  end

  defp optional_string(nil), do: nil
  defp optional_string(value), do: to_string(value)

  defp fallback(nil, value), do: value
  defp fallback(value, _fallback), do: value

  defp printable(value, _fallback) when is_binary(value), do: value
  defp printable(value, _fallback) when is_atom(value), do: Atom.to_string(value)
  defp printable(_value, fallback), do: fallback

  defp failure_title(type) do
    type
    |> to_string()
    |> String.replace("_", " ")
    |> String.capitalize()
  end

  defp remediation(type, message) do
    diagnostic = String.downcase("#{type} #{message}")

    if String.contains?(diagnostic, "adbc") or
         (String.contains?(diagnostic, "duckdb") and String.contains?(diagnostic, "driver")) do
      "Install or configure a loadable DuckDB ADBC driver and restart the runner. For local development, set DUCKDB_ADBC_DRIVER."
    else
      "Correct the runner configuration or connection problem, restart the runner if needed, and retry the work."
    end
  end

  defp platform_context,
    do: SystemContext.platform(:runner_overview, roles: [:platform_operator])
end
