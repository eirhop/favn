defmodule FavnOrchestrator.RunnerClient.LocalNode do
  @moduledoc """
  Local runner client that dispatches to a live runner node via RPC.

  This keeps the orchestrator side free from compile-time dependencies on
  `favn_runner` while preserving a real process boundary in local development.
  """

  @behaviour Favn.Contracts.RunnerClient

  alias Favn.Contracts.RelationInspectionRequest
  alias Favn.Contracts.RelationInspectionResult
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

  @impl true
  @spec inspect_relation(RelationInspectionRequest.t(), [opt()]) ::
          {:ok, RelationInspectionResult.t()} | {:error, term()}
  def inspect_relation(%RelationInspectionRequest{} = request, opts \\ []) when is_list(opts) do
    dispatch(opts, :inspect_relation, [request, opts])
  end

  @doc """
  Reports runner availability through the same local/remote dispatch boundary.
  """
  @impl true
  @spec diagnostics([opt()]) :: {:ok, map()} | {:error, term()}
  def diagnostics(opts \\ []) when is_list(opts) do
    runner_module = Keyword.get(opts, :runner_module, FavnRunner)

    case fetch_runner_node(opts) do
      {:ok, nil} ->
        runner_module_diagnostics(runner_module, opts)

      {:ok, runner_node} ->
        with :ok <- ensure_connected(runner_node) do
          dispatch_safely(runner_module, :diagnostics, [opts], fn ->
            result =
              if function_exported?(runner_module, :diagnostics, 1) do
                :erpc.call(runner_node, runner_module, :diagnostics, [opts], 15_000)
              else
                :erpc.call(runner_node, runner_module, :readiness, [], 15_000)
              end

            normalize_diagnostics_result(result, runner_module)
          end)
        end
    end
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
      dispatch_safely(runner_module, function, args, fn ->
        apply(runner_module, function, args)
      end)
    else
      {:error, {:runner_function_undefined, runner_module, function, length(args)}}
    end
  end

  defp dispatch_remote(runner_node, runner_module, function, args) when is_atom(runner_node) do
    with :ok <- ensure_connected(runner_node) do
      dispatch_safely(runner_module, function, args, fn ->
        :erpc.call(runner_node, runner_module, function, args, 15_000)
      end)
    end
  end

  defp runner_module_diagnostics(runner_module, opts) when is_atom(runner_module) do
    cond do
      function_exported?(runner_module, :diagnostics, 1) ->
        dispatch_safely(runner_module, :diagnostics, [opts], fn ->
          runner_module.diagnostics(opts)
        end)

      function_exported?(runner_module, :readiness, 0) ->
        dispatch_safely(runner_module, :readiness, [], fn ->
          runner_module.readiness()
          |> normalize_diagnostics_result(runner_module)
        end)

      true ->
        {:error, {:runner_function_undefined, runner_module, :diagnostics, 1}}
    end
  end

  defp normalize_diagnostics_result({:ok, diagnostics}, _runner_module) when is_map(diagnostics),
    do: {:ok, diagnostics}

  defp normalize_diagnostics_result(:ok, runner_module),
    do: {:ok, %{available?: true, runner_module: runner_module}}

  defp normalize_diagnostics_result({:error, reason}, _runner_module), do: {:error, reason}

  defp normalize_diagnostics_result(other, _runner_module),
    do: {:error, {:invalid_runner_diagnostics, other}}

  defp dispatch_safely(runner_module, function, args, callback) do
    callback.()
  rescue
    exception ->
      {:error, dispatch_error(:error, exception, runner_module, function, args)}
  catch
    kind, reason ->
      {:error, dispatch_error(kind, reason, runner_module, function, args)}
  end

  defp dispatch_error(kind, reason, runner_module, function, args) do
    {:runner_dispatch_failed,
     %{
       runner_module: runner_module,
       function: function,
       arity: length(args),
       kind: kind,
       reason: normalize_dispatch_reason(kind, reason)
     }}
  end

  defp normalize_dispatch_reason(
         :error,
         %ErlangError{original: {:exception, exception, _stacktrace}}
       ) do
    normalize_dispatch_reason(:error, exception)
  end

  defp normalize_dispatch_reason(:error, %ErlangError{original: original})
       when not is_nil(original) do
    original
  end

  defp normalize_dispatch_reason(
         :error,
         %{__exception__: true, __struct__: exception_module} = exception
       ) do
    {exception_module, Exception.message(exception)}
  end

  defp normalize_dispatch_reason(_kind, reason), do: reason

  defp ensure_connected(runner_node) when is_atom(runner_node) do
    case Node.connect(runner_node) do
      true -> :ok
      false -> {:error, {:runner_node_unreachable, runner_node}}
      :ignored -> {:error, {:runner_node_ignored, runner_node}}
    end
  end
end
