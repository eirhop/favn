defmodule FavnOrchestrator.Storage.Adapter.Memory.ExecutionAdmission do
  @moduledoc """
  Execution lease and admission-waiter state for the in-memory adapter.

  Capacity counts are computed once per acquisition rather than rescanning all
  leases for every requested scope.
  """

  alias FavnOrchestrator.ExecutionAdmission.LeaseRelease
  alias FavnOrchestrator.Storage.Adapter.Memory.State
  alias FavnOrchestrator.Storage.ExecutionLeaseCodec

  @doc false
  @spec acquire(State.t(), map()) :: {{:ok, map()} | {:error, term()}, State.t()}
  def acquire(%State{} = state, lease) do
    {_expired_count, active_leases} = prune_leases(state.execution_leases, lease.acquired_at)
    lease_ids_by_run = lease_ids_by_run(active_leases)

    case ensure_capacity(active_leases, lease.scopes) do
      :ok ->
        next_leases = Map.put(active_leases, lease.lease_id, lease)
        next_index = put_lease_id(lease_ids_by_run, lease)

        {{:ok, lease},
         %{
           state
           | execution_leases: next_leases,
             execution_lease_ids_by_run: next_index
         }}

      {:error, reason} ->
        {{:error, reason},
         %{
           state
           | execution_leases: active_leases,
             execution_lease_ids_by_run: lease_ids_by_run
         }}
    end
  end

  @doc false
  @spec release(State.t(), String.t()) :: State.t()
  def release(%State{} = state, lease_id) do
    {lease, leases} = Map.pop(state.execution_leases, lease_id)

    index =
      if lease,
        do: delete_lease_id(state.execution_lease_ids_by_run, lease),
        else: state.execution_lease_ids_by_run

    %{state | execution_leases: leases, execution_lease_ids_by_run: index}
  end

  @doc false
  @spec release_for_run(State.t(), String.t()) :: {LeaseRelease.t(), State.t()}
  def release_for_run(%State{} = state, run_id) do
    lease_ids = Map.get(state.execution_lease_ids_by_run, run_id, MapSet.new())
    {released, leases} = pop_leases(state.execution_leases, lease_ids)

    next_state = %{
      state
      | execution_leases: leases,
        execution_lease_ids_by_run: Map.delete(state.execution_lease_ids_by_run, run_id)
    }

    {lease_release(run_id, released), next_state}
  end

  @doc false
  @spec expire(State.t(), DateTime.t()) :: {non_neg_integer(), State.t()}
  def expire(%State{} = state, %DateTime{} = now) do
    {expired_count, active_leases} = prune_leases(state.execution_leases, now)

    {expired_count,
     %{
       state
       | execution_leases: active_leases,
         execution_lease_ids_by_run: lease_ids_by_run(active_leases)
     }}
  end

  @doc false
  @spec list(State.t()) :: [map()]
  def list(%State{} = state) do
    state.execution_leases |> Map.values() |> Enum.sort_by(& &1.lease_id)
  end

  @doc false
  @spec upsert_waiter(State.t(), map()) :: {map(), State.t()}
  def upsert_waiter(%State{} = state, waiter) do
    next_waiter = next_waiter(waiter, state.execution_admission_waiters)
    waiters = Map.put(state.execution_admission_waiters, next_waiter.waiter_id, next_waiter)
    {next_waiter, %{state | execution_admission_waiters: waiters}}
  end

  @doc false
  @spec delete_waiter(State.t(), String.t()) :: State.t()
  def delete_waiter(%State{} = state, waiter_id) do
    %{
      state
      | execution_admission_waiters: Map.delete(state.execution_admission_waiters, waiter_id)
    }
  end

  @doc false
  @spec delete_waiters_for_run(State.t(), String.t()) :: {non_neg_integer(), State.t()}
  def delete_waiters_for_run(%State{} = state, run_id) do
    {deleted, active} =
      Enum.reduce(state.execution_admission_waiters, {0, %{}}, fn
        {_waiter_id, %{run_id: ^run_id}}, {deleted, active} ->
          {deleted + 1, active}

        {waiter_id, waiter}, {deleted, active} ->
          {deleted, Map.put(active, waiter_id, waiter)}
      end)

    {deleted, %{state | execution_admission_waiters: active}}
  end

  @doc false
  @spec list_waiters_for_scope(State.t(), map(), keyword()) :: [map()]
  def list_waiters_for_scope(%State{} = state, scope, opts) do
    identity = ExecutionLeaseCodec.scope_identity(scope)

    state.execution_admission_waiters
    |> Map.values()
    |> Enum.filter(&(ExecutionLeaseCodec.scope_identity(&1.blocked_scope) == identity))
    |> Enum.sort(&waiter_before?/2)
    |> Enum.take(waiter_limit(opts))
  end

  @doc false
  @spec expire_waiters(State.t(), DateTime.t()) :: {non_neg_integer(), State.t()}
  def expire_waiters(%State{} = state, %DateTime{} = now) do
    {expired, active} =
      Enum.reduce(state.execution_admission_waiters, {0, %{}}, fn
        {waiter_id, %{deadline_at: nil} = waiter}, {count, active} ->
          {count, Map.put(active, waiter_id, waiter)}

        {waiter_id, waiter}, {count, active} ->
          if DateTime.compare(waiter.deadline_at, now) == :gt,
            do: {count, Map.put(active, waiter_id, waiter)},
            else: {count + 1, active}
      end)

    {expired, %{state | execution_admission_waiters: active}}
  end

  defp prune_leases(leases, now) do
    Enum.reduce(leases, {0, %{}}, fn {lease_id, lease}, {expired, active} ->
      if DateTime.compare(lease.expires_at, now) == :gt,
        do: {expired, Map.put(active, lease_id, lease)},
        else: {expired + 1, active}
    end)
  end

  defp lease_ids_by_run(leases) do
    Enum.reduce(leases, %{}, fn {_lease_id, lease}, index -> put_lease_id(index, lease) end)
  end

  defp put_lease_id(index, lease) do
    Map.update(index, lease.run_id, MapSet.new([lease.lease_id]), &MapSet.put(&1, lease.lease_id))
  end

  defp delete_lease_id(index, lease) do
    remaining =
      index
      |> Map.get(lease.run_id, MapSet.new())
      |> MapSet.delete(lease.lease_id)

    if MapSet.size(remaining) == 0,
      do: Map.delete(index, lease.run_id),
      else: Map.put(index, lease.run_id, remaining)
  end

  defp pop_leases(leases, lease_ids) do
    Enum.reduce(lease_ids, {[], leases}, fn lease_id, {released, active} ->
      case Map.pop(active, lease_id) do
        {nil, next_active} -> {released, next_active}
        {lease, next_active} -> {[lease | released], next_active}
      end
    end)
  end

  defp lease_release(run_id, leases) do
    scopes =
      leases |> Enum.flat_map(& &1.scopes) |> Enum.uniq_by(&ExecutionLeaseCodec.scope_identity/1)

    LeaseRelease.new(run_id, length(leases), scopes)
  end

  defp ensure_capacity(leases, requested_scopes) do
    counts =
      Enum.reduce(leases, %{}, fn {_lease_id, lease}, counts ->
        Enum.reduce(lease.scopes, counts, fn scope, acc ->
          Map.update(acc, ExecutionLeaseCodec.scope_identity(scope), 1, &(&1 + 1))
        end)
      end)

    Enum.find_value(requested_scopes, :ok, fn scope ->
      if Map.get(counts, ExecutionLeaseCodec.scope_identity(scope), 0) >= scope.limit,
        do: {:error, {:execution_capacity_exceeded, scope}}
    end)
  end

  defp next_waiter(waiter, waiters) do
    case Map.get(waiters, waiter.waiter_id) do
      nil ->
        waiter

      existing ->
        %{
          waiter
          | inserted_at: existing.inserted_at,
            wake_generation: existing.wake_generation + 1
        }
    end
  end

  defp waiter_before?(left, right) do
    case DateTime.compare(left.inserted_at, right.inserted_at) do
      :lt -> true
      :gt -> false
      :eq -> left.waiter_id <= right.waiter_id
    end
  end

  defp waiter_limit(opts) do
    case Keyword.get(opts, :limit, 50) do
      limit when is_integer(limit) and limit > 0 -> limit
      _other -> 50
    end
  end
end
