defmodule FavnOrchestrator.Diagnostics do
  @moduledoc """
  Operator-facing diagnostics for the orchestrator inside the control-plane
  runtime.
  """

  alias FavnOrchestrator.ManifestStore
  alias FavnOrchestrator.ConnectionCircuitPolicy
  alias FavnOrchestrator.MemoryCapacity
  alias FavnOrchestrator.ExecutionPoolPolicy
  alias FavnOrchestrator.OperationalEvents
  alias FavnOrchestrator.Persistence
  alias FavnOrchestrator.Persistence.SystemContext
  alias FavnOrchestrator.ProjectionDiagnostics
  alias FavnOrchestrator.Redaction
  alias FavnOrchestrator.RunManager
  alias FavnOrchestrator.RunnerRegistry
  alias FavnOrchestrator.RunState
  alias FavnOrchestrator.Runs
  alias FavnOrchestrator.RuntimeConfig
  alias FavnOrchestrator.Scheduler.Runtime, as: SchedulerRuntime
  alias FavnOrchestrator.WorkspaceConfiguration

  @default_recent_limit 5
  @max_recent_limit 100
  @in_flight_statuses [:pending, :running]
  @failed_statuses [:error, :timed_out, :partial]

  @type check :: %{
          required(:check) => atom(),
          required(:status) => :ok | :warning | :error,
          required(:summary) => String.t(),
          required(:details) => map(),
          optional(:reason) => term()
        }

  @doc """
  Returns redacted, stable operator diagnostics.
  """
  @spec report(keyword()) :: %{
          status: :ok | :degraded,
          generated_at: DateTime.t(),
          checks: [check()]
        }
  def report(opts \\ []) when is_list(opts) do
    recent_limit = normalize_recent_limit(Keyword.get(opts, :recent_limit, @default_recent_limit))

    checks = [
      safe_check(:storage_readiness, &storage_check/0),
      safe_check(:active_manifest, &active_manifest_check/0),
      safe_check(:memory_capacity, &memory_capacity_check/0),
      safe_check(:active_run_plan_capacity, &active_run_plan_capacity_check/0),
      safe_check(:scheduler, &scheduler_check/0),
      safe_check(:runner, &runner_check/0),
      safe_check(:projections, &projection_check/0),
      safe_check(:in_flight_runs, &in_flight_runs_check/0),
      safe_check(:recent_failed_runs, fn -> recent_failed_runs_check(recent_limit) end)
    ]

    status = if Enum.all?(checks, &(&1.status == :ok)), do: :ok, else: :degraded
    report = %{status: status, generated_at: DateTime.utc_now(), checks: checks}

    OperationalEvents.emit(:diagnostics_report_generated, %{check_count: length(checks)}, %{
      status: status,
      failing_checks: Enum.map(Enum.reject(checks, &(&1.status == :ok)), & &1.check)
    })

    report
  end

  defp memory_capacity_check do
    details = MemoryCapacity.diagnostics()

    if details.status == :open do
      ok(:memory_capacity, "Memory capacity is measurable", details)
    else
      warning(
        :memory_capacity,
        "Memory capacity cannot admit manifest-heavy work",
        details,
        Map.get(details, :reason, :memory_capacity_unknown)
      )
    end
  end

  defp active_run_plan_capacity_check do
    case RunManager.plan_capacity_diagnostics() do
      {:ok, details} ->
        if details.available_bytes == 0 do
          warning(
            :active_run_plan_capacity,
            "Active run plan capacity is exhausted",
            details,
            :capacity_exhausted
          )
        else
          ok(:active_run_plan_capacity, "Active run plan capacity is available", details)
        end

      {:error, reason} ->
        warning(
          :active_run_plan_capacity,
          "Active run plan capacity is unavailable",
          %{},
          normalize_error(reason)
        )
    end
  end

  defp storage_check do
    case Persistence.diagnostics() do
      {:ok, details} ->
        ok(:storage_readiness, "Storage is ready", details)

      {:error, reason} ->
        error(:storage_readiness, "Storage diagnostics failed", %{}, normalize_error(reason))
    end
  end

  defp active_manifest_check do
    case active_workspace_manifests() do
      {:ok, manifests} ->
        ok(:active_manifest, "Workspace manifests are active", %{
          workspace_count: length(manifests),
          manifests: manifests
        })

      {:error, reason} ->
        error(
          :active_manifest,
          "Workspace manifests cannot be loaded",
          %{},
          normalize_error(reason)
        )
    end
  end

  defp active_workspace_manifests do
    workspace_ids()
    |> Enum.reduce_while({:ok, []}, fn workspace_id, {:ok, acc} ->
      context = SystemContext.workspace(workspace_id, :diagnostics)

      case active_manifest_diagnostics(context) do
        {:ok, runtime, manifest, execution_pools, connection_circuits, environment} ->
          item = %{
            workspace_id: workspace_id,
            deployment_id: runtime.deployment_id,
            manifest_version_id: manifest.manifest_version_id,
            content_hash: manifest.content_hash,
            runner_releases: manifest.runner_releases,
            asset_count: manifest.asset_count,
            pipeline_count: manifest.pipeline_count,
            schedule_count: manifest.schedule_count,
            default_timezone: environment.default_timezone,
            default_timezone_source: environment.default_timezone_source,
            execution_pools: execution_pools,
            connection_circuits: connection_circuits
          }

          {:cont, {:ok, [item | acc]}}

        {:error, reason} ->
          {:halt, {:error, {workspace_id, reason}}}
      end
    end)
    |> case do
      {:ok, []} -> {:error, :workspace_ids_not_configured}
      {:ok, manifests} -> {:ok, Enum.reverse(manifests)}
      {:error, reason} -> {:error, reason}
    end
  end

  defp active_manifest_diagnostics(context) do
    with {:ok, runtime} <- ManifestStore.get_runtime_state(context) do
      ManifestStore.with_deployment_manifest(
        context,
        runtime.deployment_id,
        runtime.manifest_version_id,
        fn version ->
          with {:ok, configuration} <-
                 ManifestStore.get_deployment_configuration(context, runtime.deployment_id),
               {:ok, execution_pools} <- ExecutionPoolPolicy.diagnostics(configuration),
               {:ok, connection_circuits} <- ConnectionCircuitPolicy.diagnostics(configuration),
               {:ok, environment} <- WorkspaceConfiguration.from_configuration(configuration) do
            manifest = %{
              manifest_version_id: version.manifest_version_id,
              content_hash: version.content_hash,
              runner_releases: version.runner_releases,
              asset_count: length(version.manifest.assets),
              pipeline_count: length(version.manifest.pipelines),
              schedule_count: length(version.manifest.schedules)
            }

            {:ok, runtime, manifest, execution_pools, connection_circuits, environment}
          end
        end
      )
    end
  end

  defp scheduler_check do
    scheduler_opts = RuntimeConfig.scheduler()
    enabled? = Keyword.get(scheduler_opts, :enabled, false)
    name = Keyword.get(scheduler_opts, :name, SchedulerRuntime)

    cond do
      not enabled? ->
        ok(:scheduler, "Scheduler is disabled", %{enabled: false, running?: false})

      Process.whereis(name) == nil ->
        error(
          :scheduler,
          "Scheduler is enabled but not running",
          %{enabled: true, running?: false},
          :not_running
        )

      true ->
        case SchedulerRuntime.diagnostics(name) do
          {:ok, details} ->
            ok(:scheduler, "Scheduler is running", Map.put(details, :enabled, true))

          {:error, reason} ->
            error(:scheduler, "Scheduler diagnostics failed", %{enabled: true}, reason)
        end
    end
  end

  defp runner_check do
    case Process.whereis(RunnerRegistry) do
      nil ->
        error(:runner, "Runner registry is unavailable", %{}, :not_running)

      _pid ->
        ok(:runner, "Runner registry is available", RunnerRegistry.snapshot())
    end
  end

  defp projection_check do
    case ProjectionDiagnostics.diagnostics() do
      %{status: :ok} = details ->
        ok(:projections, "Derived projections are healthy", details)

      %{status: :degraded} = details ->
        warning(:projections, "Derived projections are degraded", details, :repair_needed)
    end
  end

  defp in_flight_runs_check do
    case list_runs_by_status(@in_flight_statuses, 50) do
      {:ok, runs} ->
        ok(:in_flight_runs, "In-flight run summary is available", %{
          count: length(runs),
          runs: Enum.map(runs, &run_summary/1)
        })

      {:error, reason} ->
        error(:in_flight_runs, "In-flight runs could not be listed", %{}, normalize_error(reason))
    end
  end

  defp recent_failed_runs_check(limit) do
    case list_runs_by_status(@failed_statuses, limit) do
      {:ok, runs} ->
        ok(:recent_failed_runs, "Recent failed run summary is available", %{
          count: length(runs),
          limit: limit,
          runs: Enum.map(runs, &failed_run_summary/1)
        })

      {:error, reason} ->
        error(
          :recent_failed_runs,
          "Recent failed runs could not be listed",
          %{},
          normalize_error(reason)
        )
    end
  end

  defp list_runs_by_status(statuses, limit) when is_list(statuses) and is_integer(limit) do
    statuses
    |> Enum.reduce_while({:ok, []}, fn status, {:ok, acc} ->
      case list_workspace_runs(status, limit) do
        {:ok, runs} -> {:cont, {:ok, runs ++ acc}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
    |> case do
      {:ok, runs} ->
        {:ok, runs |> Enum.sort_by(&run_sort_key/1, :desc) |> Enum.take(limit)}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp list_workspace_runs(status, limit) do
    workspace_ids()
    |> Enum.reduce_while({:ok, []}, fn workspace_id, {:ok, acc} ->
      context = SystemContext.workspace(workspace_id, :diagnostics)

      case Runs.page_summaries(context, status: status, limit: limit) do
        {:ok, page} -> {:cont, {:ok, page.items ++ acc}}
        {:error, reason} -> {:halt, {:error, {workspace_id, reason}}}
      end
    end)
  end

  defp workspace_ids do
    RuntimeConfig.workspace_ids()
  end

  defp run_summary(%FavnOrchestrator.Persistence.Results.RunSummary{} = run) do
    %{
      run_id: run.run_id,
      status: run.status,
      manifest_version_id: run.manifest_version_id,
      runner_releases: run.runner_releases,
      submit_kind: run.submit_kind,
      updated_at: run.updated_at,
      runner_task_id: nil
    }
  end

  defp run_summary(%RunState{} = run) do
    %{
      run_id: run.id,
      status: run.status,
      manifest_version_id: run.manifest_version_id,
      runner_releases: run.runner_releases,
      submit_kind: run.submit_kind,
      updated_at: run.updated_at,
      runner_task_id: run.runner_task_id
    }
  end

  defp run_sort_key(%{updated_at: %DateTime{} = updated_at}),
    do: DateTime.to_unix(updated_at, :microsecond)

  defp run_sort_key(_run), do: 0

  defp failed_run_summary(%RunState{} = run) do
    run
    |> run_summary()
    |> Map.put(:error_summary, error_summary(run.error))
  end

  defp failed_run_summary(%FavnOrchestrator.Persistence.Results.RunSummary{} = run) do
    run
    |> run_summary()
    |> Map.put(:error_summary, %{kind: :unavailable_in_compact_summary})
  end

  defp error_summary(nil), do: nil
  defp error_summary(reason) when is_atom(reason), do: %{kind: :atom, reason: reason}

  defp error_summary({reason, _details}) when is_atom(reason), do: %{kind: :tuple, reason: reason}

  defp error_summary(%{__struct__: module}) when is_atom(module),
    do: %{kind: :struct, module: module_name(module)}

  defp error_summary(reason) when is_map(reason),
    do: %{kind: :map, keys: reason |> Map.keys() |> Enum.map(&to_string/1) |> Enum.sort()}

  defp error_summary(_reason), do: %{kind: :term}

  defp safe_check(check, fun) when is_atom(check) and is_function(fun, 0) do
    fun.()
  rescue
    exception ->
      error(check, "Check raised", %{}, %{
        kind: :raised,
        exception: module_name(exception.__struct__)
      })
  catch
    :exit, reason ->
      error(check, "Check exited", %{}, %{
        kind: :exited,
        reason: Redaction.redact_untrusted(reason)
      })

    kind, reason ->
      error(check, "Check failed", %{}, %{kind: kind, reason: Redaction.redact_untrusted(reason)})
  end

  defp ok(check, summary, details), do: build(check, :ok, summary, details, nil)

  defp warning(check, summary, details, reason),
    do: build(check, :warning, summary, details, reason)

  defp error(check, summary, details, reason), do: build(check, :error, summary, details, reason)

  defp build(check, status, summary, details, nil) do
    %{
      check: check,
      status: status,
      summary: summary,
      details: Redaction.redact_operational_bounded(details)
    }
  end

  defp build(check, status, summary, details, reason) do
    check
    |> build(status, summary, details, nil)
    |> Map.put(:reason, Redaction.redact_operational_bounded(reason))
  end

  defp normalize_error({:raised, %{__exception__: true, __struct__: module}}),
    do: %{kind: :raised, exception: module_name(module)}

  defp normalize_error({:thrown, reason}),
    do: %{kind: :thrown, reason: Redaction.redact_untrusted(reason)}

  defp normalize_error({:exited, reason}),
    do: %{kind: :exited, reason: Redaction.redact_untrusted(reason)}

  defp normalize_error(reason), do: reason

  defp module_name(nil), do: nil
  defp module_name(module) when is_atom(module), do: Atom.to_string(module)

  defp normalize_recent_limit(limit) when is_integer(limit) and limit in 1..@max_recent_limit,
    do: limit

  defp normalize_recent_limit(_limit), do: @default_recent_limit
end
