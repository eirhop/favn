defmodule FavnOrchestrator.RunnerOverview do
  @moduledoc """
  Bounded operator read model for live runner presence and durable task history.

  Live sessions are process-local observations. Recent task failures come from
  PostgreSQL and remain available after a runner disconnects or restarts.
  """

  alias FavnOrchestrator.Persistence
  alias FavnOrchestrator.Persistence.Queries.PageWorkspaceRunnerTasks
  alias FavnOrchestrator.Persistence.Results.RunnerTask
  alias FavnOrchestrator.Persistence.WorkspaceContext
  alias FavnOrchestrator.RunnerRegistry

  @doc "Returns sanitized live sessions and recent workspace-scoped runner tasks."
  @spec get(WorkspaceContext.t(), keyword()) :: {:ok, map()} | {:error, term()}
  def get(%WorkspaceContext{} = context, opts \\ []) when is_list(opts) do
    limit = Keyword.get(opts, :limit, 50)
    failure_limit = min(limit, 20)

    with true <- is_integer(limit) and limit in 1..200,
         {:ok, tasks} <-
           Persistence.stores().runner_tasks.page_workspace(%PageWorkspaceRunnerTasks{
             workspace_context: context,
             limit: limit
           }),
         {:ok, failures} <-
           Persistence.stores().runner_tasks.page_workspace(%PageWorkspaceRunnerTasks{
             workspace_context: context,
             statuses: [:failed, :unknown],
             limit: failure_limit
           }) do
      {registry_status, runners} = live_runners(context.workspace_id)

      {:ok,
       %{
         runners: runners,
         runner_count: length(runners),
         registry_status: registry_status,
         tasks: Enum.map(tasks, &task/1),
         failures: Enum.map(failures, &task/1),
         observed_at: DateTime.utc_now()
       }}
    else
      false -> {:error, :invalid_runner_overview_limit}
      {:error, _reason} = error -> error
    end
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

  defp task(%RunnerTask{} = task) do
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
end
