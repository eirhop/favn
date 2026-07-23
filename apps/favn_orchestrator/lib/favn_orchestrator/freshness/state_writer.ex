defmodule FavnOrchestrator.Freshness.StateWriter do
  @moduledoc """
  Builds execution-time asset freshness evidence for immutable materializations.

  Successful materializations carry the freshness version, manifest identity,
  and consumed input versions into the materialization ledger. PostgreSQL
  outbox projections derive the current freshness read model from that ledger.
  """

  alias Favn.Freshness.Key, as: FreshnessKey
  alias Favn.Manifest.Version
  alias FavnOrchestrator.AssetFreshnessState
  alias FavnOrchestrator.AssetStepIdentity
  alias FavnOrchestrator.Freshness.Staleness
  alias FavnOrchestrator.RunState

  @type decision :: map()
  @type freshness_context :: %{
          required(:current_states) => map(),
          optional(atom()) => term()
        }

  @doc """
  Builds the freshness state produced by a successful planned node.
  """
  @spec build_success_state(
          RunState.t(),
          Version.t(),
          Favn.Plan.node_key(),
          decision(),
          freshness_context()
        ) :: AssetFreshnessState.t()
  def build_success_state(
        %RunState{} = run_state,
        %Version{} = version,
        node_key,
        decision,
        freshness_context
      )
      when is_map(decision) and is_map(freshness_context) do
    node = Map.fetch!(run_state.plan.nodes, node_key)
    {module, name} = node.ref
    now = DateTime.utc_now()
    upstream_states = current_upstream_states(node, freshness_context)

    {:ok, state} =
      AssetFreshnessState.new(%{
        asset_ref_module: module,
        asset_ref_name: name,
        freshness_key: decision_freshness_key(decision),
        status: :ok,
        freshness_version: freshness_version(run_state, node_key),
        latest_success_run_id: run_state.id,
        latest_success_node_key: node_key,
        latest_success_at: now,
        latest_attempt_run_id: run_state.id,
        latest_attempt_status: :ok,
        latest_attempt_at: now,
        manifest_version_id: version.manifest_version_id,
        manifest_content_hash: version.content_hash,
        evidence_generation_id: Map.fetch!(node, :evidence_generation_id),
        input_versions: Staleness.consumed_input_versions(node, upstream_states),
        metadata: %{reason: Map.get(decision, :reason)},
        updated_at: now
      })

    state
  end

  defp current_upstream_states(%{upstream: upstream}, freshness_context) do
    Map.new(upstream, fn upstream_node_key ->
      {upstream_node_key, Map.get(freshness_context.current_states, upstream_node_key)}
    end)
  end

  defp freshness_version(%RunState{} = run_state, node_key) do
    "#{run_state.id}:#{AssetStepIdentity.node_fingerprint(node_key)}"
  end

  defp decision_freshness_key(decision),
    do: Map.get(decision, :freshness_key, FreshnessKey.latest())
end
