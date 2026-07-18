defmodule FavnOrchestrator.MaterializationClaims do
  @moduledoc """
  Orchestrator-owned materialization claim lifecycle helpers.

  This module owns persisted claim acquire/complete/fail transitions used by run
  scheduling. It does not execute assets, decide freshness, or manage runner
  processes.
  """

  alias Favn.Contracts.RunnerResult
  alias Favn.Contracts.RunnerWork
  alias Favn.Freshness.Key, as: FreshnessKey
  alias Favn.Manifest.Version
  alias FavnOrchestrator.AssetFreshnessState
  alias FavnOrchestrator.AssetStepIdentity
  alias FavnOrchestrator.Freshness.Staleness
  alias FavnOrchestrator.MaterializationClaim.Identity
  alias FavnOrchestrator.Persistence
  alias FavnOrchestrator.Persistence.Commands.ClaimMaterialization
  alias FavnOrchestrator.Persistence.Commands.FinishMaterialization
  alias FavnOrchestrator.Persistence.Results.MaterializationDecision
  alias FavnOrchestrator.Persistence.SystemContext
  alias FavnOrchestrator.Persistence.TargetIdentity
  alias FavnOrchestrator.RunState
  alias FavnOrchestrator.Storage.JsonSafe

  @materialization_claim_timeout_buffer_ms 60_000

  @type claim :: map()
  @type node_key :: Favn.Plan.node_key()

  @spec acquire(RunState.t(), Version.t(), node_key(), map(), map(), RunnerWork.t()) ::
          {:ok, claim()}
          | {:already_succeeded, claim()}
          | {:already_claimed, claim()}
          | {:error, term()}
  def acquire(
        %RunState{} = run_state,
        %Version{} = version,
        node_key,
        decisions,
        freshness_context,
        %RunnerWork{} = work
      )
      when is_map(decisions) and is_map(freshness_context) do
    node = Map.fetch!(run_state.plan.nodes, node_key)
    {module, name} = node.ref
    now = DateTime.utc_now()
    freshness_key = decision_freshness_key(decisions, node_key)

    input_versions =
      Staleness.consumed_input_versions(node, current_upstream_states(node, freshness_context))

    input_fingerprint = Identity.input_fingerprint(input_versions)
    producer_identity = producer_identity(run_state, version, node_key, decisions)

    claim = %{
      claim_key:
        Identity.claim_key(node.ref, freshness_key, input_fingerprint, producer_identity),
      workspace_id: run_state.workspace_id,
      deployment_id: run_state.deployment_id,
      run_id: run_state.id,
      asset_step_id: work.asset_step_id,
      node_key: node_key,
      asset_ref_module: module,
      asset_ref_name: name,
      freshness_key: freshness_key,
      input_fingerprint: input_fingerprint,
      input_versions: input_versions,
      manifest_version_id: version.manifest_version_id,
      manifest_content_hash: version.content_hash,
      owner_id: run_state.storage_owner_id,
      status: :claimed,
      claimed_at: now,
      heartbeat_at: now,
      expires_at: DateTime.add(now, ttl_ms(run_state), :millisecond)
    }

    with :ok <- validate_authority(run_state),
         {:ok, %MaterializationDecision{} = decision} <-
           Persistence.stores().materialization.claim(%ClaimMaterialization{
             workspace_context:
               SystemContext.workspace(run_state.workspace_id, :materialization_claim),
             command_id: command_id("claim", claim.claim_key, run_state.id),
             claim_key: claim.claim_key,
             deployment_id: run_state.deployment_id,
             target_kind: :asset,
             target_id: TargetIdentity.for_asset(node.ref),
             partition_key: freshness_key,
             run_id: run_state.id,
             owner_id: run_state.storage_owner_id,
             lease_duration_ms: ttl_ms(run_state),
             occurred_at: now
           }) do
      classify_claim(decision, claim)
    end
  end

  @spec complete(claim() | nil, RunnerResult.t(), AssetFreshnessState.t()) ::
          :ok | {:error, term()}
  def complete(nil, %RunnerResult{}, %AssetFreshnessState{}), do: :ok

  def complete(claim, %RunnerResult{} = result, %AssetFreshnessState{} = freshness_state)
      when is_map(claim) do
    complete_v2(claim, result, freshness_state)
  end

  defp complete_v2(claim, result, freshness_state) do
    case finish(claim, :succeeded,
           materialization_id: materialization_id(claim),
           payload: %{
             "freshness_version" => freshness_state.freshness_version,
             "result_status" => Atom.to_string(result.status),
             "asset_step_id" => field(claim, :asset_step_id),
             "input_fingerprint" => field(claim, :input_fingerprint),
             "input_versions" => input_versions_payload(field(claim, :input_versions)),
             "node_key_fingerprint" =>
               AssetStepIdentity.node_fingerprint(field(claim, :node_key)),
             "run_id" => field(claim, :run_id),
             "manifest_version_id" => field(claim, :manifest_version_id),
             "manifest_content_hash" => field(claim, :manifest_content_hash)
           }
         ) do
      {:ok, %MaterializationDecision{}} -> :ok
      {:error, reason} -> {:error, {:complete_materialization_claim_failed, reason}}
    end
  end

  @spec fail(claim() | nil, term()) :: :ok | {:error, term()}
  def fail(nil, _reason), do: :ok

  def fail(claim, reason) when is_map(claim) do
    fail_v2(claim, reason)
  end

  defp fail_v2(claim, reason) do
    case finish(claim, :failed,
           error: JsonSafe.error(%{status: failure_status(reason), reason: reason})
         ) do
      {:ok, %MaterializationDecision{}} -> :ok
      {:error, reason} -> {:error, {:fail_materialization_claim_failed, reason}}
    end
  end

  @spec fail_entry(map(), term()) :: :ok | {:error, term()}
  def fail_entry(%{materialization_claim: claim}, reason), do: fail(claim, reason)
  def fail_entry(_entry, _reason), do: :ok

  @spec scope(claim()) :: map()
  def scope(claim) when is_map(claim), do: %{kind: :materialization_claim, key: key(claim)}

  @spec skip_reason(claim()) :: :concurrent_materialization_succeeded | :existing_success
  def skip_reason(claim) when is_map(claim) do
    case Map.get(claim, :status) || Map.get(claim, "status") do
      status when status in [:succeeded, "succeeded", :ok, "ok"] ->
        :concurrent_materialization_succeeded

      _other ->
        :existing_success
    end
  end

  @spec reusable_success?(map(), node_key()) :: boolean()
  def reusable_success?(decisions, node_key) when is_map(decisions) do
    decisions
    |> Map.get(node_key, %{})
    |> Map.get(:reason)
    |> reusable_reason?()
  end

  @spec key(claim()) :: String.t() | nil
  def key(claim) when is_map(claim), do: Map.get(claim, :claim_key) || Map.get(claim, "claim_key")

  defp finish(claim, status, attrs) do
    Persistence.stores().materialization.finish(%FinishMaterialization{
      workspace_context:
        SystemContext.workspace(field(claim, :workspace_id), :materialization_finish),
      command_id: command_id("finish", key(claim), status),
      claim_key: key(claim),
      owner_id: field(claim, :owner_id),
      fencing_token: field(claim, :fencing_token),
      expected_version: field(claim, :version),
      status: status,
      materialization_id: Keyword.get(attrs, :materialization_id),
      payload: Keyword.get(attrs, :payload),
      error: Keyword.get(attrs, :error),
      occurred_at: DateTime.utc_now()
    })
  end

  defp classify_claim(
         %MaterializationDecision{status: :claimed, claim: persisted},
         requested
       ),
       do: {:ok, merge_claim(requested, persisted)}

  defp classify_claim(
         %MaterializationDecision{status: :competing, claim: persisted},
         requested
       ),
       do: {:already_claimed, merge_claim(requested, persisted)}

  defp classify_claim(%MaterializationDecision{status: :materialized}, requested),
    do: {:already_succeeded, Map.put(requested, :status, :succeeded)}

  defp classify_claim(%MaterializationDecision{status: status}, _requested),
    do: {:error, {:unexpected_materialization_decision, status}}

  defp merge_claim(requested, persisted) do
    requested
    |> Map.put(:workspace_id, persisted.workspace_id)
    |> Map.put(:owner_id, persisted.owner_id)
    |> Map.put(:fencing_token, persisted.fencing_token)
    |> Map.put(:version, persisted.version)
    |> Map.put(:status, persisted.status)
    |> Map.put(:expires_at, persisted.expires_at)
  end

  defp validate_authority(%RunState{
         workspace_id: workspace_id,
         deployment_id: deployment_id,
         storage_owner_id: owner_id
       })
       when is_binary(workspace_id) and workspace_id != "" and is_binary(deployment_id) and
              deployment_id != "" and is_binary(owner_id) and owner_id != "",
       do: :ok

  defp validate_authority(%RunState{}),
    do: {:error, :materialization_run_authority_required}

  defp materialization_id(claim), do: "mat:" <> key(claim)

  defp command_id(operation, first, second) do
    digest =
      :crypto.hash(:sha256, :erlang.term_to_binary({operation, first, second}))
      |> Base.url_encode64(padding: false)

    "materialization:#{operation}:#{digest}"
  end

  defp field(map, key), do: Map.get(map, key, Map.get(map, Atom.to_string(key)))

  defp input_versions_payload(input_versions) when is_list(input_versions) do
    Enum.map(input_versions, fn input_version ->
      %{
        "node_key_fingerprint" =>
          input_version |> field(:upstream_node_key) |> AssetStepIdentity.node_fingerprint(),
        "freshness_version" => field(input_version, :freshness_version),
        "success_run_id" => field(input_version, :success_run_id)
      }
    end)
  end

  defp input_versions_payload(_input_versions), do: []

  defp reusable_reason?(reason) when reason in [:upstream_refreshed, :upstream_version_changed],
    do: true

  defp reusable_reason?(_reason), do: false

  defp producer_identity(%RunState{} = run_state, %Version{} = version, node_key, decisions) do
    base = version.content_hash || version.manifest_version_id || "unknown_manifest"

    if reusable_success?(decisions, node_key) do
      base
    else
      node_token = AssetStepIdentity.node_fingerprint(node_key)
      Enum.join([base, run_state.id, node_token], ":")
    end
  end

  defp current_upstream_states(%{upstream: upstream}, freshness_context) do
    Map.new(upstream, fn upstream_node_key ->
      {upstream_node_key, Map.get(freshness_context.current_states, upstream_node_key)}
    end)
  end

  defp decision_freshness_key(decisions, node_key) when is_map(decisions) do
    decisions
    |> Map.get(node_key, %{})
    |> Map.get(:freshness_key, FreshnessKey.latest())
  end

  defp ttl_ms(%RunState{timeout_ms: timeout_ms}) when is_integer(timeout_ms) and timeout_ms > 0,
    do: timeout_ms + @materialization_claim_timeout_buffer_ms

  defp ttl_ms(%RunState{}), do: 360_000

  defp failure_status(:timeout), do: :timed_out
  defp failure_status(:await_timeout), do: :timed_out
  defp failure_status(:external_cancel), do: :cancelled
  defp failure_status(:stopped_pending_await), do: :cancelled
  defp failure_status(:cancelled), do: :cancelled
  defp failure_status(:timed_out), do: :timed_out
  defp failure_status(_reason), do: :failed
end
