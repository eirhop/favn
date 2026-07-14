defmodule FavnOrchestrator.RunServer.Execution.FreshnessContext do
  @moduledoc """
  Loads and advances the freshness evidence used by one pipeline run.

  The context is an in-memory projection over persisted freshness states. It is
  initialized once per run, then advanced only after a stage has fully drained.
  """

  alias Favn.Manifest.Version
  alias Favn.Run.NodeResult
  alias FavnOrchestrator.AssetFreshnessState
  alias FavnOrchestrator.Freshness.Decider
  alias FavnOrchestrator.Freshness.StateWriter
  alias FavnOrchestrator.RefreshPolicy
  alias FavnOrchestrator.RunServer.Execution.ResultBuilder
  alias FavnOrchestrator.RunState
  alias FavnOrchestrator.Storage

  @status_by_name %{
    "running" => :running,
    "retrying" => :retrying,
    "ok" => :ok,
    "error" => :error,
    "cancelled" => :cancelled,
    "timed_out" => :timed_out,
    "skipped_fresh" => :skipped_fresh,
    "blocked" => :blocked
  }
  @statuses Map.values(@status_by_name)

  @type t :: %{
          assets_by_ref: map(),
          refresh_policy: RefreshPolicy.t(),
          prior_states: map(),
          current_states: map(),
          completed_node_keys: MapSet.t(Favn.Plan.node_key()),
          refreshed_node_keys: MapSet.t(Favn.Plan.node_key()),
          upstream_statuses: map(),
          now: DateTime.t()
        }

  @doc "Loads the persisted freshness evidence required by a pipeline plan."
  @spec initialize(RunState.t(), Version.t()) ::
          {:ok, t()} | {:error, {:freshness_state_lookup_failed, term()}}
  def initialize(%RunState{} = run_state, %Version{} = version) do
    assets_by_ref = assets_by_ref(version)
    refresh_policy = refresh_policy_from_metadata(run_state.metadata)
    now = DateTime.utc_now()

    with {:ok, prior_states} <-
           load_prior_states(run_state, assets_by_ref, refresh_policy, now) do
      {:ok,
       %{
         assets_by_ref: assets_by_ref,
         refresh_policy: refresh_policy,
         prior_states: prior_states,
         current_states: prior_states,
         completed_node_keys: MapSet.new(),
         refreshed_node_keys: MapSet.new(),
         upstream_statuses: %{},
         now: now
       }}
    end
  end

  @doc "Advances a context with the successful node results from a drained stage."
  @spec record_successes(
          RunState.t(),
          Version.t(),
          [Favn.Plan.node_key()],
          map(),
          t()
        ) :: {t(), RunState.t()}
  def record_successes(
        %RunState{} = run_state,
        %Version{} = version,
        node_keys,
        decisions,
        freshness_context
      )
      when is_list(node_keys) and is_map(decisions) and is_map(freshness_context) do
    run_state
    |> successful_node_keys(node_keys)
    |> Enum.reduce({freshness_context, run_state}, fn node_key, {context, current_run} ->
      state =
        StateWriter.build_success_state(
          current_run,
          version,
          node_key,
          Map.get(decisions, node_key, %{}),
          context
        )

      next_context = %{
        context
        | current_states:
            context.current_states
            |> Map.put(node_key, state)
            |> Map.put({state_asset_ref(state), state.freshness_key}, state),
          completed_node_keys: MapSet.put(context.completed_node_keys, node_key),
          refreshed_node_keys: MapSet.put(context.refreshed_node_keys, node_key),
          upstream_statuses: Map.put(context.upstream_statuses, node_key, :ok)
      }

      {next_context, current_run}
    end)
  end

  @doc """
  Advances a context after a stage that completed with a terminal failure.

  Successful siblings still refresh their downstream dependencies. Other
  attempted nodes retain their latest explicit status, or the run failure when
  persisted result data is malformed or absent.
  """
  @spec record_completed_after_failure(
          RunState.t(),
          Version.t(),
          [Favn.Plan.node_key()],
          map(),
          t()
        ) :: {t(), RunState.t()}
  def record_completed_after_failure(
        %RunState{} = run_state,
        %Version{} = version,
        node_keys,
        decisions,
        freshness_context
      )
      when is_list(node_keys) and is_map(decisions) and is_map(freshness_context) do
    successful = MapSet.new(successful_node_keys(run_state, node_keys))

    {context, next_run} =
      record_successes(run_state, version, node_keys, decisions, freshness_context)

    next_context =
      Enum.reduce(node_keys, context, fn node_key, acc ->
        if MapSet.member?(successful, node_key) do
          acc
        else
          status = latest_node_result_status(next_run, node_key) || run_state.status

          %{
            acc
            | completed_node_keys: MapSet.put(acc.completed_node_keys, node_key),
              upstream_statuses: Map.put(acc.upstream_statuses, node_key, status)
          }
        end
      end)

    {next_context, next_run}
  end

  defp load_prior_states(
         %RunState{plan: %Favn.Plan{} = plan},
         assets_by_ref,
         refresh_policy,
         now
       ) do
    keys =
      Decider.planned_lookup_keys(plan,
        assets_by_ref: assets_by_ref,
        refresh_policy: refresh_policy,
        now: now
      )

    case Storage.get_asset_freshness_states_by_keys(keys) do
      {:ok, states_by_key} -> {:ok, states_by_key |> Map.values() |> index_states()}
      {:error, reason} -> {:error, {:freshness_state_lookup_failed, reason}}
    end
  end

  defp index_states(states) do
    Enum.reduce(states, %{}, fn %AssetFreshnessState{} = state, acc ->
      acc
      |> Map.put({{state.asset_ref_module, state.asset_ref_name}, state.freshness_key}, state)
      |> maybe_put_state_by_node_key(state)
    end)
  end

  defp maybe_put_state_by_node_key(
         acc,
         %AssetFreshnessState{latest_success_node_key: node_key} = state
       )
       when is_tuple(node_key),
       do: Map.put(acc, node_key, state)

  defp maybe_put_state_by_node_key(acc, _state), do: acc

  defp assets_by_ref(%Version{manifest: %{assets: assets}}) when is_list(assets),
    do: Map.new(assets, fn asset -> {asset.ref, asset} end)

  defp assets_by_ref(%Version{}), do: %{}

  defp refresh_policy_from_metadata(metadata) when is_map(metadata) do
    value =
      Map.get(metadata, :refresh_policy) ||
        Map.get(metadata, "refresh_policy") ||
        Map.get(metadata, :refresh) ||
        Map.get(metadata, "refresh")

    case RefreshPolicy.from_value(value) do
      {:ok, policy} -> policy
      {:error, _reason} -> %RefreshPolicy{mode: :auto}
    end
  end

  defp successful_node_keys(%RunState{} = run_state, node_keys) do
    stage_node_keys = MapSet.new(node_keys)

    run_state
    |> ResultBuilder.node_results()
    |> Enum.filter(fn result ->
      MapSet.member?(stage_node_keys, node_result_node_key(result)) and
        node_result_status(result) == :ok
    end)
    |> Enum.map(&node_result_node_key/1)
    |> Enum.uniq()
  end

  defp node_result_node_key(%NodeResult{node_key: node_key}), do: node_key
  defp node_result_node_key(%{node_key: node_key}), do: node_key
  defp node_result_node_key(%{"node_key" => node_key}), do: node_key
  defp node_result_node_key(_result), do: nil

  defp node_result_status(%NodeResult{status: status}), do: normalize_status(status)
  defp node_result_status(%{status: status}), do: normalize_status(status)
  defp node_result_status(%{"status" => status}), do: normalize_status(status)
  defp node_result_status(_result), do: nil

  defp normalize_status(status) when status in @statuses, do: status
  defp normalize_status(status) when is_binary(status), do: Map.get(@status_by_name, status)
  defp normalize_status(_status), do: nil

  defp latest_node_result_status(%RunState{} = run_state, node_key) do
    run_state
    |> ResultBuilder.node_results()
    |> Enum.find(&(node_result_node_key(&1) == node_key))
    |> node_result_status()
  end

  defp state_asset_ref(%AssetFreshnessState{} = state),
    do: {state.asset_ref_module, state.asset_ref_name}
end
