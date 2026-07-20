defmodule FavnOrchestrator.Freshness.StateLoader do
  @moduledoc """
  Loads and decodes the exact persisted freshness rows required by a plan.

  Runtime execution and operator read models share this module so node-key
  fingerprints and consumed input versions have one decoding contract.
  """

  alias Favn.Plan
  alias FavnOrchestrator.AssetFreshnessState
  alias FavnOrchestrator.Freshness.Decider
  alias FavnOrchestrator.Persistence
  alias FavnOrchestrator.Persistence.Queries.FreshnessIdentity
  alias FavnOrchestrator.Persistence.Queries.GetFreshnessMany
  alias FavnOrchestrator.Persistence.TargetIdentity
  alias FavnOrchestrator.Persistence.WorkspaceContext

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

  @type loaded :: %{
          required(:states) => [AssetFreshnessState.t()],
          required(:indexed) => map()
        }

  @doc """
  Fetches the exact freshness identities needed by `plan` and restores them.

  The read is bounded by the unique planned asset/freshness identities. Missing
  identities are represented by absent states so the shared decider can explain
  them precisely.
  """
  @spec load(WorkspaceContext.t(), String.t(), Plan.t(), map(), keyword()) ::
          {:ok, loaded()} | {:error, term()}
  def load(%WorkspaceContext{} = context, deployment_id, %Plan{} = plan, assets_by_ref, opts)
      when is_binary(deployment_id) and is_map(assets_by_ref) and is_list(opts) do
    now = Keyword.fetch!(opts, :now)

    keys =
      Decider.planned_lookup_keys(plan,
        assets_by_ref: assets_by_ref,
        refresh_policy: Keyword.get(opts, :refresh_policy),
        now: now
      )

    {identities, requested} = identities(deployment_id, keys)

    query = %GetFreshnessMany{workspace_context: context, identities: identities}

    with {:ok, results} <- Persistence.stores().operator_reads.get_freshness_many(query),
         {:ok, states} <- decode_many(results, requested, plan) do
      {:ok, %{states: states, indexed: index(states)}}
    end
  end

  @doc "Restores projected freshness rows with node identities from the exact plan."
  @spec decode_many([map()], map(), Plan.t()) ::
          {:ok, [AssetFreshnessState.t()]} | {:error, term()}
  def decode_many(results, requested, %Plan{} = plan)
      when is_list(results) and is_map(requested) do
    fingerprints = node_keys_by_fingerprint(plan)

    results
    |> Enum.reduce_while({:ok, []}, fn result, {:ok, acc} ->
      key = Map.get(requested, {result.target_id, result.freshness_key})

      case decode(result, key, fingerprints) do
        {:ok, state} -> {:cont, {:ok, [state | acc]}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
    |> then(fn
      {:ok, states} -> {:ok, Enum.reverse(states)}
      error -> error
    end)
  end

  @doc "Indexes restored states by asset/freshness identity and concrete node key."
  @spec index([AssetFreshnessState.t()]) :: map()
  def index(states) when is_list(states) do
    Enum.reduce(states, %{}, fn %AssetFreshnessState{} = state, acc ->
      acc
      |> Map.put({{state.asset_ref_module, state.asset_ref_name}, state.freshness_key}, state)
      |> maybe_put_node_key(state)
    end)
  end

  defp identities(deployment_id, keys) do
    Enum.map_reduce(keys, %{}, fn {module, name, freshness_key} = key, acc ->
      target_id = TargetIdentity.for_asset({module, name})

      identity = %FreshnessIdentity{
        deployment_id: deployment_id,
        target_id: target_id,
        freshness_key: freshness_key
      }

      {identity, Map.put(acc, {target_id, freshness_key}, key)}
    end)
  end

  defp decode(result, {module, name, freshness_key}, fingerprints) do
    payload = result.payload || %{}

    AssetFreshnessState.new(%{
      asset_ref_module: module,
      asset_ref_name: name,
      freshness_key: freshness_key,
      status: freshness_status(result.status),
      freshness_version: field(payload, :freshness_version),
      latest_success_run_id: field(payload, :run_id),
      latest_success_node_key:
        node_key_by_fingerprint(fingerprints, field(payload, :node_key_fingerprint)),
      latest_success_at: result.updated_at,
      latest_attempt_run_id: field(payload, :run_id),
      latest_attempt_status: freshness_status(result.status),
      latest_attempt_at: result.updated_at,
      manifest_version_id: field(payload, :manifest_version_id),
      manifest_content_hash: field(payload, :manifest_content_hash),
      input_versions: input_versions(payload, fingerprints),
      metadata: %{"input_fingerprint" => field(payload, :input_fingerprint)},
      updated_at: result.updated_at
    })
  end

  defp decode(_result, nil, _fingerprints), do: {:error, :unexpected_freshness_identity}

  defp input_versions(payload, fingerprints) do
    payload
    |> field(:input_versions)
    |> List.wrap()
    |> Enum.reduce([], fn encoded, acc ->
      case node_key_by_fingerprint(fingerprints, field(encoded, :node_key_fingerprint)) do
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

  defp node_keys_by_fingerprint(%Plan{} = plan) do
    Map.new(plan.nodes, fn {node_key, _node} ->
      {FavnOrchestrator.AssetStepIdentity.node_fingerprint(node_key), node_key}
    end)
  end

  defp node_key_by_fingerprint(index, fingerprint) when is_binary(fingerprint),
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

  defp maybe_put_node_key(
         acc,
         %AssetFreshnessState{latest_success_node_key: node_key} = state
       )
       when is_tuple(node_key),
       do: Map.put(acc, node_key, state)

  defp maybe_put_node_key(acc, _state), do: acc
end
