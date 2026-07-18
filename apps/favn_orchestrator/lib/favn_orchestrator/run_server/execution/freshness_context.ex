defmodule FavnOrchestrator.RunServer.Execution.FreshnessContext do
  @moduledoc """
  Loads and advances the freshness evidence used by one pipeline run.

  The context is an in-memory projection over persisted freshness states. It is
  initialized once per run, then advanced only after a stage has fully drained.
  """

  alias Favn.Manifest.Index
  alias Favn.Manifest.Version
  alias FavnOrchestrator.AssetFreshnessState
  alias FavnOrchestrator.Freshness.Decider
  alias FavnOrchestrator.Freshness.StateWriter
  alias FavnOrchestrator.RefreshPolicy
  alias FavnOrchestrator.Persistence
  alias FavnOrchestrator.Persistence.Queries.FreshnessIdentity
  alias FavnOrchestrator.Persistence.Queries.GetFreshnessMany
  alias FavnOrchestrator.Persistence.SystemContext
  alias FavnOrchestrator.Persistence.TargetIdentity
  alias FavnOrchestrator.RunState

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
    keys = planned_keys(plan, assets_by_ref, refresh_policy, now)

    {identities, requested} =
      Enum.map_reduce(keys, %{}, fn {module, name, freshness_key} = key, acc ->
        target_id = TargetIdentity.for_asset({module, name})

        identity = %FreshnessIdentity{
          deployment_id: deployment_id,
          target_id: target_id,
          freshness_key: freshness_key
        }

        {identity, Map.put(acc, {target_id, freshness_key}, key)}
      end)

    query = %GetFreshnessMany{
      workspace_context: SystemContext.workspace(workspace_id, :freshness_lookup),
      identities: identities
    }

    with {:ok, results} <- Persistence.stores().operator_reads.get_freshness_many(query),
         {:ok, states} <- freshness_states(results, requested, plan) do
      {:ok, index_states(states)}
    else
      {:error, reason} -> {:error, {:freshness_state_lookup_failed, reason}}
    end
  end

  defp load_prior_states(%RunState{}, _assets_by_ref, _refresh_policy, _now),
    do: {:error, {:freshness_state_lookup_failed, :workspace_deployment_identity_required}}

  defp planned_keys(plan, assets_by_ref, refresh_policy, now) do
    Decider.planned_lookup_keys(plan,
      assets_by_ref: assets_by_ref,
      refresh_policy: refresh_policy,
      now: now
    )
  end

  defp freshness_states(results, requested, plan) do
    node_keys_by_fingerprint = node_keys_by_fingerprint(plan)

    Enum.reduce_while(results, {:ok, []}, fn result, {:ok, acc} ->
      key = Map.get(requested, {result.target_id, result.freshness_key})

      case persisted_freshness_state(result, key, node_keys_by_fingerprint) do
        {:ok, state} -> {:cont, {:ok, [state | acc]}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
    |> then(fn
      {:ok, states} -> {:ok, Enum.reverse(states)}
      error -> error
    end)
  end

  defp persisted_freshness_state(
         result,
         {module, name, freshness_key},
         node_keys_by_fingerprint
       ) do
    payload = result.payload || %{}

    AssetFreshnessState.new(%{
      asset_ref_module: module,
      asset_ref_name: name,
      freshness_key: freshness_key,
      status: freshness_status(result.status),
      freshness_version: field(payload, :freshness_version),
      latest_success_run_id: field(payload, :run_id),
      latest_success_node_key:
        node_key_by_fingerprint(node_keys_by_fingerprint, field(payload, :node_key_fingerprint)),
      latest_success_at: result.updated_at,
      latest_attempt_run_id: field(payload, :run_id),
      latest_attempt_status: freshness_status(result.status),
      latest_attempt_at: result.updated_at,
      manifest_version_id: field(payload, :manifest_version_id),
      manifest_content_hash: field(payload, :manifest_content_hash),
      input_versions: input_versions(payload, node_keys_by_fingerprint),
      metadata: %{"input_fingerprint" => field(payload, :input_fingerprint)},
      updated_at: result.updated_at
    })
  end

  defp persisted_freshness_state(_result, nil, _plan),
    do: {:error, :unexpected_freshness_identity}

  defp input_versions(payload, node_keys_by_fingerprint) do
    payload
    |> field(:input_versions)
    |> List.wrap()
    |> Enum.reduce([], fn encoded, acc ->
      case node_key_by_fingerprint(
             node_keys_by_fingerprint,
             field(encoded, :node_key_fingerprint)
           ) do
        nil ->
          acc

        node_key ->
          [
            %{
              upstream_node_key: node_key,
              upstream_ref: elem(node_key, 0),
              freshness_version: field(encoded, :freshness_version),
              success_run_id: field(encoded, :success_run_id)
            }
            | acc
          ]
      end
    end)
    |> Enum.reverse()
  end

  defp node_keys_by_fingerprint(%Favn.Plan{} = plan) do
    Map.new(plan.nodes, fn {node_key, _node} ->
      {FavnOrchestrator.AssetStepIdentity.node_fingerprint(node_key), node_key}
    end)
  end

  defp node_keys_by_fingerprint(_plan), do: %{}

  defp node_key_by_fingerprint(index, fingerprint) when is_map(index) and is_binary(fingerprint),
    do: Map.get(index, fingerprint)

  defp node_key_by_fingerprint(_index, _fingerprint), do: nil

  defp freshness_status(:fresh), do: :ok
  defp freshness_status(:stale), do: :error
  defp freshness_status(:failed), do: :error
  defp freshness_status(status) when status in @statuses, do: status
  defp freshness_status(_status), do: :error

  defp field(map, key) when is_map(map),
    do: Map.get(map, key) || Map.get(map, Atom.to_string(key))

  defp field(_value, _key), do: nil

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
