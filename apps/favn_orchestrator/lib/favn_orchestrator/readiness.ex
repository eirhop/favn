defmodule FavnOrchestrator.Readiness do
  @moduledoc """
  Aggregates bounded control-plane liveness and production readiness checks.

  Liveness is process-local. Readiness includes boot configuration, lifecycle,
  PostgreSQL, scheduler, and the process-local runner registry.
  Manifest installation is assignment-local and therefore never a readiness
  prerequisite.
  """

  alias FavnOrchestrator.ControlPlaneRuntimeConfig
  alias FavnOrchestrator.Lifecycle
  alias FavnOrchestrator.Persistence
  alias FavnOrchestrator.Persistence.PlatformContext
  alias FavnOrchestrator.Persistence.Queries.GetRunnerCapacityHealth
  alias FavnOrchestrator.Redaction
  alias FavnOrchestrator.RunnerRegistry
  alias FavnOrchestrator.RuntimeConfig
  alias FavnOrchestrator.Scheduler.Readiness, as: SchedulerReadiness
  alias FavnOrchestrator.Scheduler.Runtime, as: SchedulerRuntime

  @spec liveness() :: map()
  def liveness do
    %{status: :ok, checks: [%{name: :process, status: :ok}]}
  end

  @spec readiness(keyword()) :: map()
  def readiness(opts \\ []) when is_list(opts) do
    storage_snapshot = Keyword.get_lazy(opts, :storage_snapshot, &Persistence.readiness/0)
    runner_snapshot = Keyword.get_lazy(opts, :runner_snapshot, &runner_registry_snapshot/0)
    capacity_snapshot = Keyword.get_lazy(opts, :capacity_snapshot, &runner_capacity_snapshot/0)

    checks = [
      safe_check(:config, &config_check/0),
      safe_check(:api, &api_check/0),
      safe_check(:view, &view_check/0),
      safe_check(:storage, fn -> storage_check(storage_snapshot) end),
      safe_check(:schema, fn -> schema_check(storage_snapshot) end),
      safe_check(:scheduler, &scheduler_check/0),
      safe_check(:lifecycle, &lifecycle_check/0),
      safe_check(:runner_capacity, fn -> runner_capacity_check(capacity_snapshot) end),
      safe_check(:runner_registry, fn -> runner_registry_check(runner_snapshot) end)
    ]

    status = if Enum.all?(checks, &(&1.status == :ok)), do: :ready, else: :not_ready
    %{status: status, checks: checks}
  end

  defp safe_check(name, fun) when is_function(fun, 0) do
    fun.()
  rescue
    exception -> error(name, %{kind: :raised, exception: module_name(exception.__struct__)})
  catch
    :exit, reason -> error(name, %{kind: :exited, reason: Redaction.redact_untrusted(reason)})
    kind, reason -> error(name, %{kind: kind, reason: Redaction.redact_untrusted(reason)})
  end

  defp config_check do
    case ControlPlaneRuntimeConfig.ensure_applied() do
      :ok -> ok(:config, %{validated?: true, frozen?: true})
      {:error, reason} -> error(:config, reason)
    end
  end

  defp api_check do
    api_opts = RuntimeConfig.current().api_server
    ok(:api, %{enabled: Keyword.get(api_opts, :enabled, false), frozen?: true})
  end

  defp view_check do
    case ControlPlaneRuntimeConfig.diagnostics() do
      %{view: %{status: :ok} = diagnostics} -> ok(:view, diagnostics)
      %{view: diagnostics} -> error(:view, diagnostics)
      nil -> ok(:view, %{unified_runtime_config?: false})
    end
  end

  defp storage_check(storage_snapshot) do
    case storage_snapshot do
      {:ok, diagnostics} ->
        ok(:storage, %{backend: diagnostics.backend, connected?: true})

      {:error, reason} ->
        error(:storage, normalize_storage_error(reason))
    end
  end

  defp schema_check(storage_snapshot) do
    case storage_snapshot do
      {:ok, %{ready?: true} = diagnostics} ->
        ok(:schema, %{status: diagnostics.status, checks: diagnostics.checks})

      {:ok, diagnostics} ->
        error(:schema, %{status: diagnostics.status, checks: diagnostics.checks})

      {:error, reason} ->
        error(:schema, normalize_storage_error(reason))
    end
  end

  defp scheduler_check do
    scheduler_opts = RuntimeConfig.current().scheduler

    if Keyword.get(scheduler_opts, :enabled, false) do
      name = Keyword.get(scheduler_opts, :name, SchedulerRuntime)

      case Process.whereis(name) do
        nil ->
          error(:scheduler, :not_running)

        _pid ->
          case SchedulerRuntime.diagnostics(name) do
            {:ok, diagnostics} ->
              case SchedulerReadiness.check(diagnostics) do
                :ok -> ok(:scheduler, Map.put(diagnostics, :enabled, true))
                {:error, reason} -> error(:scheduler, reason)
              end

            {:error, reason} ->
              error(:scheduler, reason)
          end
      end
    else
      ok(:scheduler, %{enabled: false})
    end
  end

  defp lifecycle_check do
    case Lifecycle.diagnostics() do
      %{status: :accepting, ready?: true} = diagnostics -> ok(:lifecycle, diagnostics)
      diagnostics -> error(:lifecycle, diagnostics)
    end
  end

  defp runner_registry_check({:ok, %{available?: true} = snapshot}),
    do: ok(:runner_registry, snapshot)

  defp runner_registry_check({:error, reason}), do: error(:runner_registry, reason)
  defp runner_registry_check(snapshot), do: error(:runner_registry, snapshot)

  defp runner_registry_snapshot do
    case Process.whereis(RunnerRegistry) do
      nil -> {:error, :runner_registry_not_running}
      _pid -> {:ok, RunnerRegistry.snapshot()}
    end
  end

  defp runner_capacity_check(
         {:ok,
          %{
            partition_count: partition_count,
            unhealthy_partition_count: unhealthy_partition_count
          }}
       )
       when is_integer(partition_count) and partition_count >= 0 and
              is_integer(unhealthy_partition_count) and unhealthy_partition_count >= 0 do
    details = %{
      partitions: partition_count,
      unhealthy_partitions: unhealthy_partition_count
    }

    if unhealthy_partition_count == 0,
      do: ok(:runner_capacity, details),
      else: error(:runner_capacity, details)
  end

  defp runner_capacity_check({:error, reason}), do: error(:runner_capacity, reason)
  defp runner_capacity_check(snapshot), do: error(:runner_capacity, snapshot)

  defp runner_capacity_snapshot do
    {:ok, context} =
      PlatformContext.new("readiness", "runner-capacity-readiness", [:platform_reader])

    Persistence.stores().runner_tasks.capacity_health(%GetRunnerCapacityHealth{
      platform_context: context
    })
  end

  defp normalize_storage_error({:raised, %{__exception__: true, __struct__: exception_module}}) do
    %{kind: :raised, exception: module_name(exception_module)}
  end

  defp normalize_storage_error({:thrown, reason}),
    do: %{kind: :thrown, reason: Redaction.redact_untrusted(reason)}

  defp normalize_storage_error({:exited, reason}),
    do: %{kind: :exited, reason: Redaction.redact_untrusted(reason)}

  defp normalize_storage_error(reason), do: reason

  defp ok(name, details),
    do: %{name: name, status: :ok, details: Redaction.redact_operational_bounded(details)}

  defp error(name, reason),
    do: %{name: name, status: :error, error: Redaction.redact_operational_bounded(reason)}

  defp module_name(nil), do: nil
  defp module_name(module) when is_atom(module), do: Atom.to_string(module)
end
