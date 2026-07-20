defmodule FavnOrchestrator.RunServer.Execution.FreshnessContext do
  @moduledoc """
  Loads and advances the freshness evidence used by one pipeline run.

  The context is an in-memory projection over persisted freshness states. It is
  initialized once per run, then advanced only after a stage has fully drained.
  """

  alias Favn.Manifest.Index
  alias Favn.Manifest.Version
  alias FavnOrchestrator.AssetFreshnessState
  alias FavnOrchestrator.Freshness.StateLoader
  alias FavnOrchestrator.Freshness.StateWriter
  alias FavnOrchestrator.RefreshPolicy
  alias FavnOrchestrator.Persistence.SystemContext
  alias FavnOrchestrator.RunState

  @type t :: %{
          assets_by_ref: map(),
          refresh_policy: RefreshPolicy.t(),
          forced_node_keys: MapSet.t(Favn.Plan.node_key()),
          prior_states: map(),
          current_states: map(),
          completed_node_keys: MapSet.t(Favn.Plan.node_key()),
          refreshed_node_keys: MapSet.t(Favn.Plan.node_key()),
          upstream_statuses: map(),
          now: DateTime.t()
        }

  @doc "Loads the persisted freshness evidence required by a pipeline plan."
  @spec initialize(RunState.t(), Index.t() | Version.t()) ::
          {:ok, t()} | {:error, {:freshness_state_lookup_failed, term()}}
  def initialize(%RunState{} = run_state, manifest) do
    assets_by_ref = assets_by_ref(manifest)
    refresh_policy = refresh_policy_from_metadata(run_state.metadata)
    now = DateTime.utc_now()

    with {:ok, prior_states} <-
           load_prior_states(run_state, assets_by_ref, refresh_policy, now) do
      {:ok,
       %{
         assets_by_ref: assets_by_ref,
         refresh_policy: refresh_policy,
         forced_node_keys: RefreshPolicy.expand_force_set(refresh_policy, run_state.plan),
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
          %{optional(Favn.Plan.node_key()) => atom()},
          map(),
          t()
        ) :: {t(), RunState.t()}
  def record_successes(
        %RunState{} = run_state,
        %Version{} = version,
        node_statuses,
        decisions,
        freshness_context
      )
      when is_map(node_statuses) and is_map(decisions) and is_map(freshness_context) do
    node_statuses
    |> successful_node_keys()
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
          %{optional(Favn.Plan.node_key()) => atom()},
          map(),
          t()
        ) :: {t(), RunState.t()}
  def record_completed_after_failure(
        %RunState{} = run_state,
        %Version{} = version,
        node_statuses,
        decisions,
        freshness_context
      )
      when is_map(node_statuses) and is_map(decisions) and is_map(freshness_context) do
    successful = MapSet.new(successful_node_keys(node_statuses))

    {context, next_run} =
      record_successes(run_state, version, node_statuses, decisions, freshness_context)

    next_context =
      Enum.reduce(node_statuses, context, fn {node_key, status}, acc ->
        if MapSet.member?(successful, node_key) do
          acc
        else
          %{
            acc
            | completed_node_keys: MapSet.put(acc.completed_node_keys, node_key),
              upstream_statuses:
                Map.put(acc.upstream_statuses, node_key, status || run_state.status)
          }
        end
      end)

    {next_context, next_run}
  end

  defp load_prior_states(
         %RunState{
           workspace_id: workspace_id,
           deployment_id: deployment_id,
           plan: %Favn.Plan{} = plan
         },
         assets_by_ref,
         refresh_policy,
         now
       )
       when is_binary(workspace_id) and is_binary(deployment_id) do
    with {:ok, loaded} <-
           StateLoader.load(
             SystemContext.workspace(workspace_id, :freshness_lookup),
             deployment_id,
             plan,
             assets_by_ref,
             refresh_policy: refresh_policy,
             now: now
           ) do
      {:ok, loaded.indexed}
    else
      {:error, reason} -> {:error, {:freshness_state_lookup_failed, reason}}
    end
  end

  defp load_prior_states(%RunState{}, _assets_by_ref, _refresh_policy, _now),
    do: {:error, {:freshness_state_lookup_failed, :workspace_deployment_identity_required}}

  defp assets_by_ref(%Version{manifest: %{assets: assets}}) when is_list(assets),
    do: Map.new(assets, fn asset -> {asset.ref, asset} end)

  defp assets_by_ref(%Index{assets_by_ref: assets_by_ref}), do: assets_by_ref
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

  defp successful_node_keys(node_statuses) do
    node_statuses
    |> Enum.filter(fn {_node_key, status} -> status == :ok end)
    |> Enum.map(&elem(&1, 0))
  end

  defp state_asset_ref(%AssetFreshnessState{} = state),
    do: {state.asset_ref_module, state.asset_ref_name}
end
