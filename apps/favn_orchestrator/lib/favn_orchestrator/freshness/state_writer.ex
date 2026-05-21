defmodule FavnOrchestrator.Freshness.StateWriter do
  @moduledoc """
  Builds and persists execution-time asset freshness state.

  Successful materializations replace the freshness version and consumed input
  versions. Non-success attempts preserve the last successful freshness fields
  while updating the latest attempt metadata.
  """

  alias Favn.Manifest.Version
  alias FavnOrchestrator.AssetFreshnessState
  alias FavnOrchestrator.Freshness.Staleness
  alias FavnOrchestrator.RunState
  alias FavnOrchestrator.Storage

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
        input_versions: Staleness.consumed_input_versions(node, upstream_states),
        metadata: %{reason: Map.get(decision, :reason)},
        updated_at: now
      })

    state
  end

  @doc """
  Builds and persists the freshness state for a successful planned node.
  """
  @spec put_success_state(
          RunState.t(),
          Version.t(),
          Favn.Plan.node_key(),
          decision(),
          freshness_context()
        ) :: {:ok, AssetFreshnessState.t()} | {:error, term()}
  def put_success_state(run_state, version, node_key, decision, freshness_context) do
    state = build_success_state(run_state, version, node_key, decision, freshness_context)

    with :ok <- Storage.put_asset_freshness_state(state) do
      {:ok, state}
    end
  end

  @doc """
  Builds the freshness state for a non-success attempt, preserving prior success.
  """
  @spec build_attempt_state(
          RunState.t(),
          Version.t(),
          Favn.Plan.node_key(),
          atom(),
          String.t(),
          decision()
        ) ::
          AssetFreshnessState.t()
  def build_attempt_state(
        %RunState{} = run_state,
        %Version{} = version,
        node_key,
        status,
        freshness_key,
        decision
      )
      when is_atom(status) and is_binary(freshness_key) and is_map(decision) do
    node = Map.fetch!(run_state.plan.nodes, node_key)
    {module, name} = node.ref
    now = DateTime.utc_now()

    previous =
      case Storage.get_asset_freshness_state(module, name, freshness_key) do
        {:ok, %AssetFreshnessState{} = state} -> state
        _other -> nil
      end

    {:ok, state} =
      AssetFreshnessState.new(%{
        asset_ref_module: module,
        asset_ref_name: name,
        freshness_key: freshness_key,
        status: status,
        freshness_version: previous_freshness_version(previous),
        latest_success_run_id: previous_latest_success_run_id(previous),
        latest_success_node_key: previous_latest_success_node_key(previous),
        latest_success_at: previous_latest_success_at(previous),
        latest_attempt_run_id: run_state.id,
        latest_attempt_status: status,
        latest_attempt_at: now,
        manifest_version_id: version.manifest_version_id,
        manifest_content_hash: version.content_hash,
        input_versions: previous_input_versions(previous),
        metadata: attempt_freshness_metadata(previous, decision),
        updated_at: now
      })

    state
  end

  @doc """
  Builds and persists the freshness state for a non-success attempt.
  """
  @spec put_attempt_state(
          RunState.t(),
          Version.t(),
          Favn.Plan.node_key(),
          atom(),
          String.t(),
          decision()
        ) ::
          {:ok, AssetFreshnessState.t()} | {:error, term()}
  def put_attempt_state(run_state, version, node_key, status, freshness_key, decision) do
    state = build_attempt_state(run_state, version, node_key, status, freshness_key, decision)

    with :ok <- Storage.put_asset_freshness_state(state) do
      {:ok, state}
    end
  end

  defp current_upstream_states(%{upstream: upstream}, freshness_context) do
    Map.new(upstream, fn upstream_node_key ->
      {upstream_node_key, Map.get(freshness_context.current_states, upstream_node_key)}
    end)
  end

  defp freshness_version(%RunState{} = run_state, node_key) do
    encoded_node_key = node_key |> :erlang.term_to_binary() |> Base.encode16(case: :lower)
    "#{run_state.id}:#{encoded_node_key}"
  end

  defp decision_freshness_key(decision),
    do: Map.get(decision, :freshness_key, Favn.Freshness.Key.latest())

  defp previous_freshness_version(%AssetFreshnessState{} = state), do: state.freshness_version
  defp previous_freshness_version(_state), do: nil

  defp previous_latest_success_run_id(%AssetFreshnessState{} = state),
    do: state.latest_success_run_id

  defp previous_latest_success_run_id(_state), do: nil

  defp previous_latest_success_node_key(%AssetFreshnessState{} = state),
    do: state.latest_success_node_key

  defp previous_latest_success_node_key(_state), do: nil

  defp previous_latest_success_at(%AssetFreshnessState{} = state), do: state.latest_success_at
  defp previous_latest_success_at(_state), do: nil

  defp previous_input_versions(%AssetFreshnessState{} = state), do: state.input_versions
  defp previous_input_versions(_state), do: %{}

  defp attempt_freshness_metadata(%AssetFreshnessState{metadata: metadata}, decision)
       when is_map(metadata) do
    Map.merge(metadata, %{latest_attempt_reason: Map.get(decision, :reason)})
  end

  defp attempt_freshness_metadata(_state, decision),
    do: %{latest_attempt_reason: Map.get(decision, :reason)}
end
