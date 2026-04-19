defmodule FavnOrchestrator.RunnerClient.LocalNode do
  @moduledoc """
  Local runner client that dispatches to a configured runner module at runtime.

  This keeps the orchestrator side free from compile-time dependencies on
  `favn_runner` while still allowing local-node execution in development.
  """

  @behaviour Favn.Contracts.RunnerClient

  alias Favn.Contracts.RunnerResult
  alias Favn.Contracts.RunnerWork
  alias Favn.Manifest.Version

  @type opt :: {:runner_module, module()}

  @impl true
  @spec register_manifest(Version.t(), [opt()]) :: :ok | {:error, term()}
  def register_manifest(%Version{} = version, opts \\ []) when is_list(opts) do
    dispatch(opts, :register_manifest, [version, opts])
  end

  @impl true
  @spec submit_work(RunnerWork.t(), [opt()]) :: {:ok, String.t()} | {:error, term()}
  def submit_work(%RunnerWork{} = work, opts \\ []) when is_list(opts) do
    dispatch(opts, :submit_work, [work, opts])
  end

  @impl true
  @spec await_result(String.t(), timeout(), [opt()]) :: {:ok, RunnerResult.t()} | {:error, term()}
  def await_result(execution_id, timeout, opts \\ [])
      when is_binary(execution_id) and is_integer(timeout) and timeout > 0 and is_list(opts) do
    dispatch(opts, :await_result, [execution_id, timeout, opts])
  end

  @impl true
  @spec cancel_work(String.t(), map(), [opt()]) :: :ok | {:error, term()}
  def cancel_work(execution_id, reason \\ %{}, opts \\ [])
      when is_binary(execution_id) and is_map(reason) and is_list(opts) do
    dispatch(opts, :cancel_work, [execution_id, reason, opts])
  end

  defp dispatch(opts, function, args)
       when is_list(opts) and is_atom(function) and is_list(args) do
    runner_module = Keyword.get(opts, :runner_module, FavnRunner)

    with {:ok, module} <- ensure_module(runner_module),
         true <- function_exported?(module, function, length(args)) do
      apply(module, function, args)
    else
      false -> {:error, {:runner_function_undefined, runner_module, function, length(args)}}
      {:error, _reason} = error -> error
    end
  end

  defp ensure_module(module) when is_atom(module) do
    case Code.ensure_loaded(module) do
      {:module, ^module} -> {:ok, module}
      {:error, reason} -> {:error, {:runner_module_unavailable, module, reason}}
    end
  end
end
