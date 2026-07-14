defmodule FavnOrchestrator.Storage.Adapter.Memory.MaterializationClaims do
  @moduledoc """
  Pure materialization-claim transitions for the in-memory adapter.

  Claim completion and failure are guarded by the `:claimed` state, matching the
  compare-and-set behavior of database adapters.
  """

  alias FavnOrchestrator.MaterializationClaim
  alias FavnOrchestrator.Storage.Adapter.Memory.State

  @doc false
  @spec acquire(State.t(), MaterializationClaim.t()) :: {term(), State.t()}
  def acquire(%State{} = state, %MaterializationClaim{} = claim) do
    {claims, _expired_count} = expire_values(state.materialization_claims, claim.claimed_at)
    existing = Map.get(claims, claim.claim_key)

    case acquire_decision(existing, claim.claimed_at) do
      decision when decision in [:insert, :reclaim] ->
        next_claims = Map.put(claims, claim.claim_key, claim)
        {{:ok, claim}, %{state | materialization_claims: next_claims}}

      {:already_succeeded, existing} ->
        {{:already_succeeded, existing}, %{state | materialization_claims: claims}}

      {:already_claimed, existing} ->
        {{:already_claimed, existing}, %{state | materialization_claims: claims}}
    end
  end

  @doc false
  @spec complete(State.t(), String.t(), map()) ::
          {{:ok, MaterializationClaim.t()} | {:error, :not_found}, State.t()}
  def complete(%State{} = state, claim_key, completion) do
    transition_claimed(state, claim_key, &apply_completion(&1, completion))
  end

  @doc false
  @spec fail(State.t(), String.t(), map()) ::
          {{:ok, MaterializationClaim.t()} | {:error, :not_found}, State.t()}
  def fail(%State{} = state, claim_key, failure) do
    transition_claimed(state, claim_key, &apply_failure(&1, failure))
  end

  @doc false
  @spec expire(State.t(), DateTime.t()) :: {non_neg_integer(), State.t()}
  def expire(%State{} = state, %DateTime{} = now) do
    {claims, expired_count} = expire_values(state.materialization_claims, now)
    {expired_count, %{state | materialization_claims: claims}}
  end

  @doc false
  @spec get(State.t(), String.t()) :: {:ok, MaterializationClaim.t()} | {:error, :not_found}
  def get(%State{} = state, claim_key) do
    case Map.fetch(state.materialization_claims, claim_key) do
      {:ok, claim} -> {:ok, claim}
      :error -> {:error, :not_found}
    end
  end

  @doc false
  @spec list(State.t(), keyword()) :: [MaterializationClaim.t()]
  def list(%State{} = state, filters) do
    read_filters = Keyword.drop(filters, [:limit, :offset])

    state.materialization_claims
    |> Map.values()
    |> Enum.filter(fn claim ->
      Enum.all?(read_filters, fn {key, expected} -> Map.get(claim, key) == expected end)
    end)
    |> Enum.sort_by(& &1.claim_key)
  end

  defp transition_claimed(state, claim_key, transition) do
    case Map.fetch(state.materialization_claims, claim_key) do
      {:ok, %MaterializationClaim{status: :claimed} = claim} ->
        updated = transition.(claim)
        claims = Map.put(state.materialization_claims, claim_key, updated)
        {{:ok, updated}, %{state | materialization_claims: claims}}

      _not_claimed_or_missing ->
        {{:error, :not_found}, state}
    end
  end

  defp acquire_decision(nil, %DateTime{}), do: :insert

  defp acquire_decision(%MaterializationClaim{status: :succeeded} = claim, %DateTime{}),
    do: {:already_succeeded, claim}

  defp acquire_decision(%MaterializationClaim{status: :claimed} = claim, %DateTime{} = now) do
    if MaterializationClaim.active?(claim, now), do: {:already_claimed, claim}, else: :reclaim
  end

  defp acquire_decision(%MaterializationClaim{}, %DateTime{}), do: :reclaim

  defp expire_values(claims, %DateTime{} = now) do
    Enum.reduce(claims, {%{}, 0}, fn {claim_key, claim}, {acc, count} ->
      if claim.status == :claimed and DateTime.compare(claim.expires_at, now) != :gt do
        expired = %{claim | status: :expired, finished_at: now}
        {Map.put(acc, claim_key, expired), count + 1}
      else
        {Map.put(acc, claim_key, claim), count}
      end
    end)
  end

  defp apply_completion(%MaterializationClaim{} = claim, completion) do
    %{
      claim
      | status: :succeeded,
        freshness_version: field(completion, :freshness_version) || claim.freshness_version,
        finished_at: field(completion, :finished_at) || DateTime.utc_now(),
        metadata: field(completion, :metadata) || claim.metadata,
        error: field(completion, :error)
    }
  end

  defp apply_failure(%MaterializationClaim{} = claim, failure) do
    %{
      claim
      | status: failure_status(field(failure, :status) || :failed),
        error: field(failure, :error),
        finished_at: field(failure, :finished_at) || DateTime.utc_now(),
        metadata: field(failure, :metadata) || claim.metadata
    }
  end

  defp failure_status(status) when is_atom(status) do
    if status in MaterializationClaim.terminal_failure_statuses(), do: status, else: :failed
  end

  defp failure_status(status) when is_binary(status) do
    Enum.find(
      MaterializationClaim.terminal_failure_statuses(),
      :failed,
      &(Atom.to_string(&1) == status)
    )
  end

  defp field(map, name), do: Map.get(map, name) || Map.get(map, Atom.to_string(name))
end
