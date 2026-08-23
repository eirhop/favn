defmodule FavnView.Orchestrator do
  @moduledoc """
  Location adapter for the public `FavnOrchestrator` facade.

  Local development calls the facade in the current BEAM. The production View
  release calls that same facade on the configured Orchestrator node through
  bounded distributed Erlang calls. Calls are never retried: a failed command
  transport is reported as an unknown outcome so its idempotency key is kept.
  """

  @default_call_timeout_ms 30_000
  @max_call_timeout_ms 120_000
  @persistent_key {__MODULE__, :config}

  @read_calls [
    active_workspace_configuration: 1,
    active_asset_catalogue: 1,
    active_asset_coverage_windows: 3,
    active_asset_detail: 3,
    active_asset_documentation: 2,
    active_asset_run_detail: 3,
    active_pipeline_catalogue: 1,
    active_pipeline_detail: 2,
    authorize_logs_subscription: 2,
    authorize_run_subscription: 2,
    authorize_runs_subscription: 1,
    count_execution_groups: 2,
    get_asset_step_log_context: 3,
    get_operator_lineage_asset: 3,
    get_operator_lineage_edge: 3,
    get_operator_lineage_graph: 2,
    get_operator_lineage_group: 3,
    get_operator_rebuild: 2,
    get_operator_run_asset_attempt: 3,
    get_operator_run_events: 2,
    get_operator_run_flow: 2,
    get_operator_runner_overview: 2,
    get_operator_target_recovery: 2,
    get_run_detail: 2,
    get_schedule_entry: 2,
    introspect_operator_session: 2,
    list_logs: 3,
    list_operator_workspaces: 1,
    list_operator_run_windows: 2,
    list_run_stream_events: 3,
    replay_logs: 4,
    page_execution_groups: 2,
    page_operator_actors: 2,
    page_operator_audit: 2,
    page_operator_rebuild_items: 3,
    page_operator_rebuilds: 2,
    page_operator_sessions: 2,
    page_schedule_list_entries: 2,
    plan_missing_coverage_backfill: 3,
    preview_schedule_occurrences: 3,
    readiness: 0
  ]

  @command_calls [
    attach_operator_actor: 3,
    cancel_operator_rebuild: 4,
    cancel_operator_run: 3,
    change_operator_password: 3,
    create_operator_actor: 5,
    disable_schedule: 3,
    enable_schedule: 3,
    operator_external_login: 2,
    operator_password_login: 4,
    plan_operator_rebuild: 4,
    plan_operator_target_recovery: 4,
    reconcile_operator_rebuild: 3,
    reconcile_operator_target_recovery: 3,
    retry_operator_rebuild: 4,
    retry_operator_run_remaining: 3,
    revoke_operator_managed_session: 2,
    revoke_operator_session: 1,
    start_operator_rebuild: 4,
    start_operator_target_recovery: 4,
    submit_missing_coverage_backfill: 4,
    submit_operator_asset_backfill: 5,
    submit_operator_pipeline_backfill: 5,
    submit_operator_run: 5,
    switch_operator_workspace: 2,
    trusted_local_development_login: 3,
    update_operator_actor_membership: 4
  ]

  for {function, arity} <- @read_calls do
    args = Macro.generate_arguments(arity, __MODULE__)

    @doc false
    def unquote(function)(unquote_splicing(args)) do
      call(unquote(function), [unquote_splicing(args)], :read)
    end
  end

  for {function, arity} <- @command_calls do
    args = Macro.generate_arguments(arity, __MODULE__)

    @doc false
    def unquote(function)(unquote_splicing(args)) do
      call(unquote(function), [unquote_splicing(args)], :command)
    end
  end

  @doc false
  def subscribe_logs(operator_context, filter) do
    with {:ok, grant} <- authorize_logs_subscription(operator_context, filter) do
      FavnOrchestrator.activate_logs_subscription(grant)
    end
  end

  @doc false
  def unsubscribe_logs(subscription), do: FavnOrchestrator.unsubscribe_logs(subscription)

  @doc false
  def subscribe_run(operator_context, run_id) do
    with {:ok, grant} <- authorize_run_subscription(operator_context, run_id) do
      FavnOrchestrator.activate_run_subscription(grant)
    end
  end

  @doc false
  def unsubscribe_run(operator_context, run_id) do
    FavnOrchestrator.deactivate_run_subscription(operator_context, run_id)
  end

  @doc false
  def unsubscribe_run_wakeups(_operator_context), do: FavnOrchestrator.deactivate_run_wakeups()

  @doc false
  def subscribe_runs(operator_context) do
    with {:ok, grant} <- authorize_runs_subscription(operator_context) do
      FavnOrchestrator.activate_runs_subscription(grant)
    end
  end

  @doc false
  def unsubscribe_runs(operator_context) do
    FavnOrchestrator.deactivate_runs_subscription(operator_context)
  end

  @doc false
  def subscribe_operator_identity(operator_context) do
    FavnOrchestrator.subscribe_operator_identity(operator_context)
  end

  @doc "Validates and freezes the production View-to-Orchestrator node contract."
  @spec configure_from_env_if_configured(map()) :: :ok | {:error, map()}
  def configure_from_env_if_configured(env) when is_map(env) do
    if Application.get_env(:favn_view, :production_runtime_config, false) do
      with {:ok, config} <- validate(env),
           {:ok, _tls} <- Favn.DistributionTLS.validate(env),
           :ok <- Favn.DistributionTLS.validate_running_transport(env) do
        :persistent_term.put(@persistent_key, config)
        :ok
      else
        {:error, reason} -> {:error, %{status: :invalid, error: redact(reason)}}
      end
    else
      :ok
    end
  end

  @doc "Validates the remote-node settings without connecting or mutating runtime state."
  @spec validate(map()) :: {:ok, map()} | {:error, term()}
  def validate(env) when is_map(env) do
    with {:ok, target_node} <- node_name(env, "FAVN_CONTROL_PLANE_NODE"),
         :ok <- different_from_current_node(target_node),
         {:ok, timeout_ms} <- call_timeout_ms(env) do
      {:ok, %{target_node: target_node, call_timeout_ms: timeout_ms}}
    end
  end

  @doc "Returns whether facade calls are local or cross-node."
  @spec boundary() :: :same_beam_facade | :distributed_erlang
  def boundary do
    case :persistent_term.get(@persistent_key, :local) do
      :local -> :same_beam_facade
      %{target_node: _target_node} -> :distributed_erlang
    end
  end

  @doc false
  @spec target_node() :: node() | nil
  def target_node do
    case :persistent_term.get(@persistent_key, :local) do
      :local -> nil
      %{target_node: target_node} -> target_node
    end
  end

  @doc false
  @spec facade_calls() :: %{read: keyword(non_neg_integer()), command: keyword(non_neg_integer())}
  def facade_calls, do: %{read: @read_calls, command: @command_calls}

  @doc false
  @spec reset_for_test() :: :ok
  def reset_for_test do
    :persistent_term.erase(@persistent_key)
    :ok
  end

  defp call(function, args, kind) do
    case :persistent_term.get(@persistent_key, :local) do
      :local -> apply(FavnOrchestrator, function, args)
      config -> remote_call(config, function, args, kind)
    end
  end

  defp remote_call(config, function, args, kind) do
    :erpc.call(config.target_node, FavnOrchestrator, function, args, config.call_timeout_ms)
  catch
    :error, {:erpc, reason} when reason in [:noconnection, :timeout] -> transport_error(kind)
  end

  defp transport_error(:read), do: {:error, :orchestrator_unavailable}
  defp transport_error(:command), do: {:error, :orchestrator_outcome_unknown}

  # This creates one validated, deployment-owned node name frozen once at boot.
  # sobelow_skip ["DOS.StringToAtom"]
  defp node_name(env, name) do
    with {:ok, value} <- required(env, name),
         [local_name, host] <- String.split(value, "@", parts: 2),
         true <- valid_node_part?(local_name),
         true <- valid_node_host?(host) do
      {:ok, String.to_atom(value)}
    else
      {:error, _reason} = error -> error
      _invalid -> {:error, {:invalid_env, name, "long name@private-dns-name"}}
    end
  end

  defp different_from_current_node(target_node) do
    if Node.alive?() and target_node == node() do
      {:error, {:invalid_env, "FAVN_CONTROL_PLANE_NODE", "different from the View node"}}
    else
      :ok
    end
  end

  defp valid_node_part?(value),
    do: byte_size(value) in 1..255 and Regex.match?(~r/^[A-Za-z0-9_.-]+$/, value)

  defp valid_node_host?(host) do
    normalized = String.downcase(host)

    valid_node_part?(host) and normalized not in ["localhost", "nohost", "127.0.0.1", "::1"] and
      not String.ends_with?(normalized, ".localhost")
  end

  defp call_timeout_ms(env) do
    value =
      Map.get(
        env,
        "FAVN_VIEW_ORCHESTRATOR_CALL_TIMEOUT_MS",
        Integer.to_string(@default_call_timeout_ms)
      )

    case Integer.parse(value) do
      {timeout_ms, ""} when timeout_ms in 100..@max_call_timeout_ms ->
        {:ok, timeout_ms}

      _invalid ->
        {:error,
         {:invalid_env, "FAVN_VIEW_ORCHESTRATOR_CALL_TIMEOUT_MS", "100..#{@max_call_timeout_ms}"}}
    end
  end

  defp required(env, name) do
    case Map.get(env, name) do
      value when is_binary(value) ->
        value = String.trim(value)
        if value == "", do: {:error, {:missing_env, name}}, else: {:ok, value}

      _other ->
        {:error, {:missing_env, name}}
    end
  end

  defp redact({:missing_env, name}), do: {:missing_env, name}
  defp redact({:invalid_env, name, expected}), do: {:invalid_env, name, expected}
  defp redact(_reason), do: :invalid_runtime_config
end
