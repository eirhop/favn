defmodule FavnOrchestrator.Readiness do
  @moduledoc """
  Aggregates bounded control-plane liveness and production readiness checks.

  Liveness is process-local. Readiness includes boot configuration, lifecycle,
  PostgreSQL, scheduler, remote runner identity, and alignment for each active
  manifest in the configured workspace set. A workspace without a deployment
  is valid and does not make a clean installation unready.
  """

  alias FavnOrchestrator.ControlPlaneRuntimeConfig
  alias FavnOrchestrator.ActiveManifestReconciler
  alias FavnOrchestrator.Lifecycle
  alias FavnOrchestrator.Persistence
  alias FavnOrchestrator.Redaction
  alias FavnOrchestrator.RunnerDiagnostics
  alias FavnOrchestrator.RunnerHealth
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
    runner_snapshot = Keyword.get_lazy(opts, :runner_snapshot, &RunnerHealth.snapshot/0)

    active_manifest_snapshot =
      Keyword.get_lazy(opts, :active_manifest_snapshot, &ActiveManifestReconciler.snapshot/0)

    checks = [
      safe_check(:config, &config_check/0),
      safe_check(:api, &api_check/0),
      safe_check(:view, &view_check/0),
      safe_check(:storage, fn -> storage_check(storage_snapshot) end),
      safe_check(:schema, fn -> schema_check(storage_snapshot) end),
      safe_check(:scheduler, &scheduler_check/0),
      safe_check(:lifecycle, &lifecycle_check/0),
      safe_check(:runner_connection, fn -> runner_connection_check(runner_snapshot) end),
      safe_check(:runner_release, fn -> runner_release_check(runner_snapshot) end),
      safe_check(:active_manifests, fn ->
        active_manifests_check(runner_snapshot, active_manifest_snapshot)
      end)
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
      %{status: :accepting} = diagnostics -> ok(:lifecycle, diagnostics)
      diagnostics -> error(:lifecycle, diagnostics)
    end
  end

  defp runner_connection_check(runner_snapshot) do
    with {:ok, diagnostics} <- runner_snapshot,
         true <- Map.get(diagnostics, :available?, false) do
      ok(:runner_connection, %{
        available?: true,
        node_name: Map.get(diagnostics, :node_name),
        client: module_name(RuntimeConfig.current().runner_client)
      })
    else
      false -> error(:runner_connection, :runner_node_unreachable)
      {:error, reason} -> error(:runner_connection, reason)
    end
  end

  defp runner_release_check(runner_snapshot) do
    runtime_config = RuntimeConfig.current()

    with {:ok, diagnostics} <- runner_snapshot,
         {:ok, release_id} <-
           RunnerDiagnostics.validate_ready(diagnostics, runtime_config.runner_client_opts) do
      ok(:runner_release, %{
        runner_release_id: release_id,
        favn_version: Map.get(diagnostics, :favn_version),
        runner_contract_version: Map.get(diagnostics, :runner_contract_version),
        self_verified?: Map.fetch!(diagnostics, :self_verified?)
      })
    else
      {:error, reason} -> error(:runner_release, reason)
    end
  end

  defp active_manifests_check(runner_snapshot, active_manifest_snapshot) do
    runtime_config = RuntimeConfig.current()

    with {:ok, diagnostics} <- runner_snapshot,
         {:ok, runner_release_id} <-
           RunnerDiagnostics.validate_ready(diagnostics, runtime_config.runner_client_opts),
         {:ok, reconciliation} <-
           validate_reconciliation_snapshot(
             active_manifest_snapshot,
             length(runtime_config.workspace_ids)
           ),
         manifests <- reconciliation.manifests,
         :ok <- validate_manifest_releases(manifests, runner_release_id) do
      ok(:active_manifests, %{
        configured_workspace_count: length(runtime_config.workspace_ids),
        active_manifest_count: length(manifests),
        runner_release_id: runner_release_id,
        manifests: manifests
      })
    else
      {:error, reason} -> error(:active_manifests, reason)
    end
  end

  defp validate_reconciliation_snapshot(
         {:ok,
          %{
            checked: checked,
            aligned: aligned,
            inactive: inactive,
            failed: 0,
            manifests: manifests
          } = reconciliation},
         configured_workspace_count
       )
       when is_integer(checked) and is_integer(aligned) and is_integer(inactive) and
              is_list(manifests) do
    valid_manifests? =
      Enum.all?(manifests, fn manifest ->
        is_binary(Map.get(manifest, :workspace_id)) and
          is_binary(Map.get(manifest, :manifest_version_id)) and
          is_binary(Map.get(manifest, :required_runner_release_id)) and
          Map.get(manifest, :runner_cache) == :registered
      end)

    if checked == configured_workspace_count and aligned == length(manifests) and
         inactive + aligned == checked and valid_manifests? do
      {:ok, reconciliation}
    else
      {:error, :invalid_active_manifest_reconciliation}
    end
  end

  defp validate_reconciliation_snapshot({:error, reason}, _configured_workspace_count),
    do: {:error, reason}

  defp validate_reconciliation_snapshot(_invalid, _configured_workspace_count),
    do: {:error, :invalid_active_manifest_reconciliation}

  defp validate_manifest_releases(manifests, runner_release_id) do
    case Enum.find(manifests, &(&1.required_runner_release_id != runner_release_id)) do
      nil ->
        :ok

      manifest ->
        {:error,
         {:runner_release_mismatch, manifest.required_runner_release_id, runner_release_id}}
    end
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
