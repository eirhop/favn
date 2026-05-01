defmodule FavnOrchestrator.Readiness do
  @moduledoc """
  Aggregates liveness and readiness checks for the orchestrator runtime.
  """

  alias Favn.Contracts.RunnerClient
  alias FavnOrchestrator.API.Config, as: APIConfig
  alias FavnOrchestrator.ProductionRuntimeConfig
  alias FavnOrchestrator.Scheduler.Runtime, as: SchedulerRuntime
  alias FavnOrchestrator.Storage

  @spec liveness() :: map()
  def liveness do
    %{status: :ok, checks: [%{name: :process, status: :ok}]}
  end

  @spec readiness() :: map()
  def readiness do
    checks = [api_check(), storage_check(), scheduler_check(), runner_check()]
    status = if Enum.all?(checks, &(&1.status == :ok)), do: :ready, else: :not_ready

    %{status: status, checks: checks}
  end

  defp api_check do
    case APIConfig.validate() do
      :ok -> ok(:api, %{enabled: Keyword.get(api_opts(), :enabled, false)})
      {:error, reason} -> error(:api, reason)
    end
  end

  defp storage_check do
    adapter = Storage.adapter_module()

    with :ok <- Storage.validate_adapter(adapter),
         :ok <- sqlite_schema_check(adapter) do
      ok(:storage, storage_details(adapter))
    else
      {:error, reason} -> error(:storage, reason)
    end
  end

  defp sqlite_schema_check(adapter) do
    if adapter == ProductionRuntimeConfig.sqlite_adapter() do
      migrations = Module.concat([FavnStorageSqlite, Migrations])
      repo = Module.concat([FavnStorageSqlite, Repo])

      with {:module, ^migrations} <- Code.ensure_loaded(migrations),
           true <- function_exported?(migrations, :schema_diagnostics, 1),
           {:ok, %{status: :ready}} <- migrations.schema_diagnostics(repo) do
        :ok
      else
        {:ok, diagnostics} ->
          {:error, {:sqlite_schema_not_ready, redact_diagnostics(diagnostics)}}

        {:error, reason} ->
          {:error, {:sqlite_schema_diagnostics_failed, redact_diagnostics(reason)}}

        _other ->
          {:error, :sqlite_schema_diagnostics_unavailable}
      end
    else
      :ok
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
         callbacks <- RunnerClient.behaviour_info(:callbacks),
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

  defp storage_details(adapter) do
    details = %{adapter: module_name(adapter)}

    if adapter == ProductionRuntimeConfig.sqlite_adapter() do
      Keyword.get(Storage.adapter_opts(), :database)
      |> then(fn
        nil -> Map.put(details, :database, %{configured?: false})
        _path -> Map.put(details, :database, %{configured?: true, path: :redacted})
      end)
    else
      details
    end
  end

  defp ok(name, details), do: %{name: name, status: :ok, details: redact_diagnostics(details)}
  defp error(name, reason), do: %{name: name, status: :error, error: redact_diagnostics(reason)}

  defp redact_diagnostics(value) when is_map(value) do
    value
    |> Enum.map(fn {key, val} -> {key, redact_diagnostics(key, val)} end)
    |> Map.new()
  end

  defp redact_diagnostics(value) when is_list(value), do: Enum.map(value, &redact_diagnostics/1)
  defp redact_diagnostics(value) when is_atom(value), do: value
  defp redact_diagnostics(value) when is_integer(value), do: value
  defp redact_diagnostics(value) when is_boolean(value), do: value
  defp redact_diagnostics(value) when is_binary(value), do: value
  defp redact_diagnostics(value), do: inspect(value)

  defp redact_diagnostics(key, _value) when key in [:token, :tokens, :password, :secret],
    do: "[REDACTED]"

  defp redact_diagnostics(_key, value), do: redact_diagnostics(value)

  defp module_name(nil), do: nil
  defp module_name(module) when is_atom(module), do: Atom.to_string(module)
end
