defmodule FavnOrchestrator.Readiness do
  @moduledoc """
  Aggregates liveness and readiness checks for the orchestrator runtime.
  """

  alias FavnOrchestrator.API.Config, as: APIConfig
  alias FavnOrchestrator.Redaction
  alias FavnOrchestrator.Persistence
  alias FavnOrchestrator.RunnerClientValidator
  alias FavnOrchestrator.RunnerDiagnostics
  alias FavnOrchestrator.RuntimeConfig
  alias FavnOrchestrator.Scheduler.Runtime, as: SchedulerRuntime

  @spec liveness() :: map()
  def liveness do
    %{status: :ok, checks: [%{name: :process, status: :ok}]}
  end

  @spec readiness() :: map()
  def readiness do
    checks = [
      safe_check(:api, &api_check/0),
      safe_check(:storage, &storage_check/0),
      safe_check(:scheduler, &scheduler_check/0),
      safe_check(:runner, &runner_check/0)
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

  defp api_check do
    case APIConfig.validate() do
      :ok -> ok(:api, %{enabled: Keyword.get(api_opts(), :enabled, false)})
      {:error, reason} -> error(:api, reason)
    end
  end

  defp storage_check do
    case Persistence.readiness() do
      {:ok, %{ready?: false} = diagnostics} ->
        error(:storage, diagnostics)

      {:ok, diagnostics} ->
        ok(:storage, diagnostics)

      {:error, reason} ->
        error(:storage, normalize_storage_error(reason))
    end
  end

  defp scheduler_check do
    scheduler_opts = Application.get_env(:favn_orchestrator, :scheduler, [])

    if Keyword.get(scheduler_opts, :enabled, false) do
      case Process.whereis(Keyword.get(scheduler_opts, :name, SchedulerRuntime)) do
        nil -> error(:scheduler, :not_running)
        _pid -> ok(:scheduler, %{enabled: true})
      end
    else
      ok(:scheduler, %{enabled: false})
    end
  end

  defp runner_check do
    runtime_config = RuntimeConfig.current()
    module = runtime_config.runner_client
    opts = runtime_config.runner_client_opts

    with :ok <- RunnerClientValidator.validate(module),
         true <- function_exported?(module, :diagnostics, 1),
         {:ok, diagnostics} when is_map(diagnostics) <- module.diagnostics(opts),
         {:ok, _release_id} <- RunnerDiagnostics.validate_ready(diagnostics, opts) do
      ok(:runner, Map.put(diagnostics, :client, module_name(module)))
    else
      false -> error(:runner, :runner_diagnostics_not_supported)
      {:error, reason} -> error(:runner, reason)
      _other -> error(:runner, :runner_client_not_available)
    end
  end

  defp api_opts, do: Application.get_env(:favn_orchestrator, :api_server, [])

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
