defmodule FavnOrchestrator.RunnerClient.BeamNode do
  @moduledoc """
  Production runner client for one statically configured distributed BEAM node.

  Every operation performs a bounded connectivity check and an `:erpc` call.
  There is deliberately no local function-call fallback: local development uses
  a separate runner node too, while tests may explicitly target `Node.self/0`.

  Client-owned options are removed before options cross the runner boundary.
  Failures are normalized into redaction-safe `Favn.Contracts.RunnerError`
  values with explicit retry and outcome semantics.
  """

  @behaviour Favn.Contracts.RunnerClient

  alias Favn.Contracts.RelationInspectionRequest
  alias Favn.Contracts.RelationInspectionResult
  alias Favn.Contracts.RunnerCancellation
  alias Favn.Contracts.RunnerError
  alias Favn.Contracts.RunnerResult
  alias Favn.Contracts.RunnerWork
  alias Favn.Manifest.Version
  alias Favn.RuntimeInput.Resolution

  @default_rpc_timeout_ms 15_000
  @default_diagnostics_timeout_ms 5_000
  @default_await_timeout_buffer_ms 2_000
  @runner_module Module.concat(["FavnRunner"])
  @client_option_keys [
    :runner_node,
    :runner_module,
    :runner_rpc_timeout_ms,
    :runner_dispatch_timeout_ms,
    :runner_diagnostics_timeout_ms,
    :runner_await_timeout_buffer_ms
  ]
  @safe_read_operations [
    :diagnostics,
    :ensure_manifest,
    :await_result,
    :resolve_runtime_inputs,
    :inspect_relation
  ]

  @type opt ::
          {:runner_node, node()}
          | {:runner_module, module()}
          | {:runner_rpc_timeout_ms, pos_integer()}
          | {:runner_diagnostics_timeout_ms, pos_integer()}
          | {:runner_await_timeout_buffer_ms, non_neg_integer()}

  @impl true
  @spec register_manifest(Version.t(), [opt()]) :: :ok | {:error, term()}
  def register_manifest(%Version{} = version, opts \\ []) when is_list(opts),
    do: dispatch(opts, :register_manifest, [version, opts])

  @impl true
  @spec ensure_manifest(Version.t(), [opt()]) :: :ok | :missing | {:error, term()}
  def ensure_manifest(%Version{} = version, opts \\ []) when is_list(opts),
    do: dispatch(opts, :ensure_manifest, [version, opts])

  @impl true
  @spec acquire_manifest(Version.t(), String.t(), DateTime.t(), [Favn.Ref.t()], [opt()]) ::
          :ok | {:error, term()}
  def acquire_manifest(
        %Version{} = version,
        lease_id,
        %DateTime{} = expires_at,
        planned_asset_refs,
        opts \\ []
      )
      when is_binary(lease_id) and is_list(planned_asset_refs) and is_list(opts),
      do:
        dispatch(opts, :acquire_manifest, [
          version,
          lease_id,
          expires_at,
          planned_asset_refs,
          opts
        ])

  @impl true
  @spec renew_manifest(String.t(), DateTime.t(), [opt()]) :: :ok | {:error, term()}
  def renew_manifest(lease_id, %DateTime{} = expires_at, opts \\ [])
      when is_binary(lease_id) and is_list(opts),
      do: dispatch(opts, :renew_manifest, [lease_id, expires_at, opts])

  @impl true
  @spec release_manifest(String.t(), [opt()]) :: :ok
  def release_manifest(lease_id, opts \\ []) when is_binary(lease_id) and is_list(opts) do
    case dispatch(opts, :release_manifest, [lease_id, opts]) do
      :ok -> :ok
      {:error, _reason} -> :ok
    end
  end

  @impl true
  @spec submit_work(RunnerWork.t(), [opt()]) :: {:ok, String.t()} | {:error, term()}
  def submit_work(%RunnerWork{} = work, opts \\ []) when is_list(opts),
    do: dispatch(opts, :submit_work, [work, opts])

  @impl true
  @spec resolve_runtime_inputs(RunnerWork.t(), [opt()]) ::
          {:ok, Resolution.t() | nil} | {:error, term()}
  def resolve_runtime_inputs(%RunnerWork{} = work, opts \\ []) when is_list(opts),
    do: dispatch(opts, :resolve_runtime_inputs, [work, opts])

  @impl true
  @spec await_result(String.t(), timeout(), [opt()]) ::
          {:ok, RunnerResult.t()} | {:error, term()}
  def await_result(execution_id, timeout, opts \\ [])
      when is_binary(execution_id) and is_integer(timeout) and timeout > 0 and is_list(opts),
      do: dispatch(opts, :await_result, [execution_id, timeout, opts])

  @impl true
  @spec cancel_work(String.t(), RunnerCancellation.t(), [opt()]) ::
          {:ok, RunnerCancellation.outcome()} | {:error, RunnerError.t()}
  def cancel_work(execution_id, reason \\ %{}, opts \\ [])
      when is_binary(execution_id) and is_map(reason) and is_list(opts),
      do: dispatch(opts, :cancel_work, [execution_id, RunnerCancellation.from_map(reason), opts])

  @impl true
  @spec subscribe_execution_logs(String.t(), pid(), [opt()]) :: :ok | {:error, term()}
  def subscribe_execution_logs(execution_id, subscriber, opts \\ [])
      when is_binary(execution_id) and is_pid(subscriber) and is_list(opts),
      do: dispatch(opts, :subscribe_execution_logs, [execution_id, subscriber, opts])

  @impl true
  @spec unsubscribe_execution_logs(String.t(), pid(), [opt()]) :: :ok
  def unsubscribe_execution_logs(execution_id, subscriber, opts \\ [])
      when is_binary(execution_id) and is_pid(subscriber) and is_list(opts) do
    case dispatch(opts, :unsubscribe_execution_logs, [execution_id, subscriber, opts]) do
      :ok -> :ok
      {:error, _reason} -> :ok
    end
  end

  @impl true
  @spec inspect_relation(RelationInspectionRequest.t(), [opt()]) ::
          {:ok, RelationInspectionResult.t()} | {:error, term()}
  def inspect_relation(%RelationInspectionRequest{} = request, opts \\ []) when is_list(opts),
    do: dispatch(opts, :inspect_relation, [request, opts])

  @doc "Probes the configured runner node through its bounded diagnostics callback."
  @impl true
  @spec diagnostics([opt()]) :: {:ok, map()} | {:error, RunnerError.t()}
  def diagnostics(opts \\ []) when is_list(opts) do
    with {:ok, runner_node, runner_module} <- destination(opts),
         {:ok, result} <-
           call_with_deadline(
             runner_node,
             runner_module,
             :diagnostics,
             [remote_opts(opts)],
             diagnostics_timeout(opts)
           ) do
      normalize_diagnostics(result)
    end
  end

  defp normalize_diagnostics({:ok, diagnostics}) when is_map(diagnostics),
    do: {:ok, diagnostics}

  defp normalize_diagnostics({:error, %RunnerError{} = error}), do: {:error, error}

  defp normalize_diagnostics({:error, _reason}) do
    {:error,
     boundary_error(
       :runner_diagnostics_failed,
       :diagnostics_failed,
       :diagnostics,
       false,
       :safe_failure,
       "Runner diagnostics failed"
     )}
  end

  defp normalize_diagnostics(_invalid) do
    {:error,
     boundary_error(
       :runner_invalid_diagnostics,
       :invalid_response,
       :diagnostics,
       false,
       :safe_failure,
       "Runner returned invalid diagnostics"
     )}
  end

  defp dispatch(opts, function, args) do
    with {:ok, runner_node, runner_module} <- destination(opts),
         {:ok, result} <-
           call_with_deadline(
             runner_node,
             runner_module,
             function,
             remote_args(args),
             rpc_timeout(function, args, opts)
           ) do
      result
    end
  end

  defp call_with_deadline(runner_node, runner_module, function, args, timeout) do
    deadline = System.monotonic_time(:millisecond) + timeout

    with {:ok, connect_timeout} <- remaining_timeout(deadline, function),
         :ok <- ensure_connected(runner_node, connect_timeout, function),
         {:ok, rpc_timeout} <- remaining_timeout(deadline, function),
         {:ok, result} <-
           rpc_call(runner_node, runner_module, function, args, rpc_timeout) do
      {:ok, result}
    end
  end

  defp remaining_timeout(deadline, function) do
    case deadline - System.monotonic_time(:millisecond) do
      remaining when remaining > 0 -> {:ok, remaining}
      _expired -> {:error, rpc_outcome_error(:runner_rpc_timeout, :timeout, function)}
    end
  end

  defp destination(opts) do
    runner_node = Keyword.get(opts, :runner_node)
    runner_module = Keyword.get(opts, :runner_module, @runner_module)

    cond do
      not is_atom(runner_node) or is_nil(runner_node) ->
        {:error,
         boundary_error(
           :runner_node_not_configured,
           :missing_runner_node,
           :connect,
           false,
           :safe_failure,
           "Runner node is not configured"
         )}

      not is_atom(runner_module) or is_nil(runner_module) ->
        {:error,
         boundary_error(
           :runner_module_not_configured,
           :missing_runner_module,
           :connect,
           false,
           :safe_failure,
           "Runner module is not configured"
         )}

      true ->
        {:ok, runner_node, runner_module}
    end
  end

  defp ensure_connected(runner_node, _timeout, _function) when runner_node == node(), do: :ok

  defp ensure_connected(runner_node, timeout, function) do
    case bounded_connect(runner_node, timeout) do
      true ->
        :ok

      false ->
        {:error, connect_error(:runner_node_unreachable, :nodedown, function)}

      :ignored ->
        {:error, connect_error(:runner_distribution_unavailable, :not_distributed, function)}

      :timeout ->
        {:error, connect_error(:runner_connect_timeout, :timeout, function)}

      :failed ->
        {:error, connect_error(:runner_connect_failed, :connect_failed, function)}
    end
  end

  defp bounded_connect(runner_node, timeout) do
    parent = self()
    result_ref = make_ref()

    {pid, monitor_ref} =
      spawn_monitor(fn -> send(parent, {result_ref, Node.connect(runner_node)}) end)

    receive do
      {^result_ref, result} when result in [true, false, :ignored] ->
        Process.demonitor(monitor_ref, [:flush])
        result

      {:DOWN, ^monitor_ref, :process, ^pid, _reason} ->
        :failed
    after
      timeout ->
        Process.exit(pid, :kill)

        receive do
          {:DOWN, ^monitor_ref, :process, ^pid, _reason} -> :ok
        end

        receive do
          {^result_ref, _late_result} -> :ok
        after
          0 -> :ok
        end

        :timeout
    end
  end

  defp rpc_call(runner_node, runner_module, function, args, timeout) do
    {:ok, :erpc.call(runner_node, runner_module, function, args, timeout)}
  rescue
    exception ->
      {:error,
       normalize_rpc_failure(
         :error,
         exception,
         runner_module,
         function,
         length(args)
       )}
  catch
    kind, reason ->
      {:error, normalize_rpc_failure(kind, reason, runner_module, function, length(args))}
  end

  defp normalize_rpc_failure(kind, reason, runner_module, function, arity) do
    cond do
      requested_function_undefined?(reason, runner_module, function, arity) ->
        boundary_error(
          :runner_function_undefined,
          :unsupported_runner_function,
          function,
          false,
          :safe_failure,
          "Runner function is unavailable"
        )

      timeout_failure?(reason) ->
        rpc_outcome_error(:runner_rpc_timeout, :timeout, function)

      connection_failure?(reason) ->
        rpc_outcome_error(:runner_node_disconnected, :nodedown, function)

      true ->
        rpc_outcome_error(:runner_remote_failure, remote_failure_reason(kind), function)
    end
  end

  defp connect_error(type, reason, function) do
    boundary_error(
      type,
      reason,
      function,
      true,
      :safe_failure,
      "Runner node is unavailable"
    )
  end

  defp rpc_outcome_error(type, reason, function) do
    safe? = function in @safe_read_operations
    retryable? = safe? and type in [:runner_rpc_timeout, :runner_node_disconnected]

    boundary_error(
      type,
      reason,
      function,
      retryable?,
      if(safe?, do: :safe_failure, else: :unknown),
      "Runner RPC failed"
    )
  end

  defp boundary_error(type, reason, function, retryable?, outcome, message) do
    RunnerError.new(
      kind: :boundary,
      type: type,
      phase: :runner_rpc,
      message: message,
      reason: reason,
      details: %{operation: function},
      retryable?: retryable?,
      outcome: outcome
    )
  end

  defp requested_function_undefined?(
         %UndefinedFunctionError{module: module, function: function, arity: arity},
         module,
         function,
         arity
       ),
       do: true

  defp requested_function_undefined?(%ErlangError{original: original}, module, function, arity),
    do: requested_function_undefined?(original, module, function, arity)

  defp requested_function_undefined?(
         {:exception, :undef, [frame | _stacktrace]},
         module,
         function,
         arity
       ),
       do: requested_frame?(frame, module, function, arity)

  defp requested_function_undefined?(
         {:exception, {:undef, [frame | _stacktrace]}},
         module,
         function,
         arity
       ),
       do: requested_frame?(frame, module, function, arity)

  defp requested_function_undefined?({:undef, [frame | _stacktrace]}, module, function, arity),
    do: requested_frame?(frame, module, function, arity)

  defp requested_function_undefined?(_reason, _module, _function, _arity), do: false

  defp requested_frame?({module, function, args, _location}, module, function, arity)
       when is_list(args),
       do: length(args) == arity

  defp requested_frame?({module, function, arity, _location}, module, function, arity)
       when is_integer(arity),
       do: true

  defp requested_frame?(_frame, _module, _function, _arity), do: false

  defp timeout_failure?(%ErlangError{original: original}), do: timeout_failure?(original)
  defp timeout_failure?({:exception, reason, _stacktrace}), do: timeout_failure?(reason)
  defp timeout_failure?({:erpc, :timeout}), do: true
  defp timeout_failure?({:timeout, _details}), do: true
  defp timeout_failure?(:timeout), do: true
  defp timeout_failure?(_reason), do: false

  defp connection_failure?(%ErlangError{original: original}), do: connection_failure?(original)
  defp connection_failure?({:exception, reason, _stacktrace}), do: connection_failure?(reason)
  defp connection_failure?({:erpc, reason}) when reason in [:noconnection, :nodedown], do: true
  defp connection_failure?({:nodedown, _node}), do: true
  defp connection_failure?(:noconnection), do: true
  defp connection_failure?(_reason), do: false

  defp remote_failure_reason(:exit), do: :remote_exit
  defp remote_failure_reason(:throw), do: :remote_throw
  defp remote_failure_reason(_kind), do: :remote_exception

  defp rpc_timeout(:await_result, [_execution_id, timeout, _remote_opts], opts) do
    buffer =
      non_neg_int_opt(opts, :runner_await_timeout_buffer_ms, @default_await_timeout_buffer_ms)

    max(rpc_timeout_opt(opts), timeout + buffer)
  end

  defp rpc_timeout(_function, _args, opts), do: rpc_timeout_opt(opts)

  defp rpc_timeout_opt(opts) do
    opts
    |> Keyword.get(:runner_rpc_timeout_ms, Keyword.get(opts, :runner_dispatch_timeout_ms))
    |> positive_or_default(@default_rpc_timeout_ms)
  end

  defp diagnostics_timeout(opts) do
    opts
    |> Keyword.get(:runner_diagnostics_timeout_ms)
    |> positive_or_default(@default_diagnostics_timeout_ms)
  end

  defp positive_or_default(value, _default) when is_integer(value) and value > 0, do: value
  defp positive_or_default(_value, default), do: default

  defp non_neg_int_opt(opts, key, default) do
    case Keyword.get(opts, key, default) do
      value when is_integer(value) and value >= 0 -> value
      _other -> default
    end
  end

  defp remote_args(args) do
    case List.last(args) do
      opts when is_list(opts) -> List.replace_at(args, -1, remote_opts(opts))
      _other -> args
    end
  end

  defp remote_opts(opts), do: Keyword.drop(opts, @client_option_keys)
end
