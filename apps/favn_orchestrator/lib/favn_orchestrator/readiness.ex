defmodule FavnOrchestrator.Readiness do
  @moduledoc """
  Aggregates liveness and readiness checks for the orchestrator runtime.
  """

  alias Favn.Contracts.RunnerClient
  alias FavnOrchestrator.API.Config, as: APIConfig
  alias FavnOrchestrator.Redaction
  alias FavnOrchestrator.Scheduler.Runtime, as: SchedulerRuntime
  alias FavnOrchestrator.Storage

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
    case Storage.readiness() do
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
    module = Application.get_env(:favn_orchestrator, :runner_client, nil)

    with true <- is_atom(module),
         {:module, ^module} <- Code.ensure_loaded(module),
         callbacks <-
           RunnerClient.behaviour_info(:callbacks) --
             RunnerClient.behaviour_info(:optional_callbacks),
         true <-
           Enum.all?(callbacks, fn {name, arity} -> function_exported?(module, name, arity) end),
         :ok <- runner_runtime_check(module) do
      ok(:runner, %{module: module_name(module)})
    else
      {:error, reason} -> error(:runner, reason)
      _other -> error(:runner, :runner_client_not_available)
    end
  end

  defp runner_runtime_check(FavnOrchestrator.RunnerClient.LocalNode) do
    runner_opts = Application.get_env(:favn_orchestrator, :runner_client_opts, [])
    runner_module = Keyword.get(runner_opts, :runner_module, Module.concat([FavnRunner]))

    with {:module, ^runner_module} <- Code.ensure_loaded(runner_module),
         true <- function_exported?(runner_module, :readiness, 0) do
      runner_module.readiness()
    else
      _other -> {:error, :runner_runtime_not_available}
    end
  end

  defp runner_runtime_check(_module), do: :ok

  defp api_opts, do: Application.get_env(:favn_orchestrator, :api_server, [])

  defp normalize_storage_error({:raised, %{__exception__: true, __struct__: exception_module}}) do
    %{kind: :raised, exception: module_name(exception_module)}
  end

  defp normalize_storage_error({:thrown, reason}),
    do: %{kind: :thrown, reason: Redaction.redact_untrusted(reason)}

  defp normalize_storage_error({:exited, reason}),
    do: %{kind: :exited, reason: Redaction.redact_untrusted(reason)}

  defp normalize_storage_error(reason), do: reason

  defp ok(name, details), do: %{name: name, status: :ok, details: Redaction.redact(details)}
  defp error(name, reason), do: %{name: name, status: :error, error: Redaction.redact(reason)}

  defp module_name(nil), do: nil
  defp module_name(module) when is_atom(module), do: Atom.to_string(module)
end
