defmodule FavnOrchestrator.MaterializationClaims do
  @moduledoc """
  Orchestrator-owned materialization claim lifecycle helpers.

  This module owns persisted claim acquire/complete/fail transitions used by run
  scheduling. It does not execute assets, decide freshness, or manage runner
  processes.
  """

  alias Favn.Contracts.RunnerResult
  alias Favn.Contracts.RunnerWork
  alias Favn.Manifest.Version
  alias FavnOrchestrator.AssetFreshnessState
  alias FavnOrchestrator.Freshness.Staleness
  alias FavnOrchestrator.MaterializationClaim.Identity
  alias FavnOrchestrator.RunState
  alias FavnOrchestrator.Storage

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
      status: :claimed,
      claimed_at: now,
      heartbeat_at: now,
      expires_at: DateTime.add(now, ttl_ms(run_state), :millisecond)
    }

    case Storage.try_acquire_materialization_claim(claim) do
      {:ok, claim} -> {:ok, claim}
      {:already_succeeded, claim} -> {:already_succeeded, claim}
      {:already_claimed, claim} -> {:already_claimed, claim}
      {:error, {:already_succeeded, claim}} -> {:already_succeeded, claim}
      {:error, {:already_claimed, claim}} -> {:already_claimed, claim}
      {:error, reason} -> {:error, reason}
    end
  end

  @spec complete(claim() | nil, RunnerResult.t(), AssetFreshnessState.t()) ::
          :ok | {:error, term()}
  def complete(nil, %RunnerResult{}, %AssetFreshnessState{}), do: :ok

  def complete(claim, %RunnerResult{} = result, %AssetFreshnessState{} = freshness_state)
      when is_map(claim) do
    case Storage.complete_materialization_claim(key(claim), %{
           finished_at: DateTime.utc_now(),
           freshness_version: freshness_state.freshness_version,
           metadata: %{result_status: result.status}
         }) do
      :ok -> :ok
      {:ok, _claim} -> :ok
      {:error, reason} -> {:error, {:complete_materialization_claim_failed, reason}}
    end
  end

  @spec fail(claim() | nil, term()) :: :ok | {:error, term()}
  def fail(nil, _reason), do: :ok

  def fail(claim, reason) when is_map(claim) do
    case Storage.fail_materialization_claim(key(claim), %{
           status: failure_status(reason),
           finished_at: DateTime.utc_now(),
           error: reason
         }) do
      :ok -> :ok
      {:ok, _claim} -> :ok
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

  defp reusable_reason?(reason) when reason in [:upstream_refreshed, :upstream_version_changed],
    do: true

  defp reusable_reason?(_reason), do: false

  defp producer_identity(%RunState{} = run_state, %Version{} = version, node_key, decisions) do
    base = version.content_hash || version.manifest_version_id || "unknown_manifest"

    if reusable_success?(decisions, node_key) do
      base
    else
      node_token = node_key |> :erlang.term_to_binary() |> Base.encode16(case: :lower)
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
    |> Map.get(:freshness_key, Favn.Freshness.Key.latest())
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
