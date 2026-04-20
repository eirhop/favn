defmodule FavnOrchestrator.RunnerClient.LocalNode do
  @moduledoc """
  Local runner client that dispatches to a live runner node via RPC.

  This keeps the orchestrator side free from compile-time dependencies on
  `favn_runner` while preserving a real process boundary in local development.
  """

  @behaviour Favn.Contracts.RunnerClient

  alias Favn.Contracts.RunnerResult
  alias Favn.Contracts.RunnerWork
  alias Favn.Manifest.Version

  @type opt :: {:runner_node, node()} | {:runner_module, module()}

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

    case fetch_runner_node(opts) do
      {:ok, nil} ->
        dispatch_local(runner_module, function, args)

      {:ok, runner_node} ->
        dispatch_remote(runner_node, runner_module, function, args)
    end
  end

  defp fetch_runner_node(opts) when is_list(opts) do
    case Keyword.get(opts, :runner_node) do
      value when is_atom(value) -> {:ok, value}
      _ -> {:ok, nil}
    end
  end

  defp dispatch_local(runner_module, function, args) do
    if is_atom(runner_module) and function_exported?(runner_module, function, length(args)) do
      apply(runner_module, function, args)
    else
      {:error, {:runner_function_undefined, runner_module, function, length(args)}}
    end
  end

  defp dispatch_remote(runner_node, runner_module, function, args) when is_atom(runner_node) do
    with :ok <- ensure_connected(runner_node) do
      :erpc.call(runner_node, runner_module, function, args, 15_000)
    end
  end

  defp ensure_connected(runner_node) when is_atom(runner_node) do
    case Node.connect(runner_node) do
      true -> :ok
      false -> {:error, {:runner_node_unreachable, runner_node}}
      :ignored -> {:error, {:runner_node_ignored, runner_node}}
    end
  end
end
