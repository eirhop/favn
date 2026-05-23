defmodule FavnOrchestrator.ExecutionAdmission do
  @moduledoc """
  Orchestrator-owned admission control for asset step execution.

  Admission leases are persisted through `FavnOrchestrator.Storage` so pipeline
  run limits and shared execution pools are enforced by the control plane rather
  than by a runner-local process.
  """

  alias FavnOrchestrator.ExecutionAdmission.Coordinator
  alias FavnOrchestrator.ExecutionAdmission.Waiter
  alias FavnOrchestrator.RunState
  alias FavnOrchestrator.Storage

  @default_lease_ttl_ms 300_000
  @lease_timeout_buffer_ms 60_000

  @type lease :: map()
  @type queue_reason :: :pipeline_concurrency | :execution_pool | :global_concurrency
  @type entry :: %{
          required(:asset_step_id) => String.t(),
          optional(:execution_pool) => atom() | String.t() | nil
        }

  @spec acquire(RunState.t(), entry()) ::
          {:ok, lease() | nil} | {:queued, queue_reason(), map()} | {:error, term()}
  def acquire(%RunState{} = run, entry) when is_map(entry) do
    case acquire_result(run, entry) do
      {:queued, queue_reason, scope, _requested_scopes} -> {:queued, queue_reason, scope}
      other -> other
    end
  end

  @spec acquire_or_wait(RunState.t(), entry(), keyword()) ::
          {:ok, lease() | nil} | {:waiting, Waiter.t()} | {:error, term()}
  def acquire_or_wait(%RunState{} = run, entry, opts \\ [])
      when is_map(entry) and is_list(opts) do
    case acquire_result(run, entry) do
      {:ok, lease} ->
        {:ok, lease}

      {:queued, queue_reason, scope, requested_scopes} ->
        with {:ok, waiter} <- Waiter.new(run, entry, requested_scopes, queue_reason, scope, opts),
             {:ok, waiter} <- Storage.upsert_execution_admission_waiter(waiter) do
          :ok = Coordinator.register(waiter, self())
          acquire_after_waiter_registration(run, entry, waiter)
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp acquire_result(%RunState{} = run, entry) when is_map(entry) do
    with :ok <- validate_execution_pool(entry) do
      scopes = admission_scopes(run, entry)

      if scopes == [] do
        {:ok, nil}
      else
        now = DateTime.utc_now()
        ttl_ms = lease_ttl_ms(run)

        lease = %{
          lease_id: lease_id(run.id, entry.asset_step_id),
          run_id: run.id,
          asset_step_id: entry.asset_step_id,
          scopes: scopes,
          acquired_at: now,
          expires_at: DateTime.add(now, ttl_ms, :millisecond)
        }

        case Storage.try_acquire_execution_lease(lease) do
          {:ok, lease} ->
            {:ok, lease}

          {:error, {:execution_capacity_exceeded, scope}} ->
            {:queued, queue_reason(scope), scope, scopes}

          {:error, reason} ->
            {:error, reason}
        end
      end
    end
  end

  @spec release(lease() | nil) :: :ok | {:error, term()}
  def release(nil), do: :ok

  def release(%{lease_id: lease_id} = lease) when is_binary(lease_id) do
    case Storage.release_execution_lease(lease_id) do
      :ok ->
        lease
        |> Map.get(:scopes, Map.get(lease, "scopes", []))
        |> Coordinator.notify_scopes()

        :ok

      {:error, reason} ->
        {:error, reason}
    end
  end

  @spec cancel_wait(String.t() | Waiter.t()) :: :ok | {:error, term()}
  def cancel_wait(%Waiter{waiter_id: waiter_id}), do: cancel_wait(waiter_id)

  def cancel_wait(waiter_id) when is_binary(waiter_id) do
    :ok = Coordinator.cancel(waiter_id)
    Storage.delete_execution_admission_waiter(waiter_id)
  end

  @spec cancel_run_waits(String.t()) :: :ok | {:error, term()}
  def cancel_run_waits(run_id) when is_binary(run_id) do
    case Storage.delete_execution_admission_waiters_for_run(run_id) do
      {:ok, _count} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  @spec release_run(String.t()) :: :ok | {:error, term()}
  def release_run(run_id) when is_binary(run_id) do
    case Storage.list_execution_leases() do
      {:ok, leases} ->
        leases
        |> Enum.filter(&(Map.get(&1, :run_id) == run_id || Map.get(&1, "run_id") == run_id))
        |> Enum.each(fn lease ->
          lease_id = Map.get(lease, :lease_id) || Map.get(lease, "lease_id")

          if is_binary(lease_id) do
            release(lease)
          end
        end)

        cancel_run_waits(run_id)

      {:error, _reason} ->
        cancel_run_waits(run_id)
    end
  end

  defp acquire_after_waiter_registration(%RunState{} = run, entry, %Waiter{} = waiter) do
    case acquire_result(run, entry) do
      {:ok, lease} ->
        case cancel_wait(waiter) do
          :ok ->
            {:ok, lease}

          {:error, reason} ->
            :ok = release(lease)
            {:error, {:execution_admission_waiter_cleanup_failed, reason}}
        end

      {:queued, _queue_reason, _scope, _requested_scopes} ->
        {:waiting, waiter}

      {:error, reason} ->
        _cleanup_result = cancel_wait(waiter)
        {:error, reason}
    end
  end

  @spec admission_scopes(RunState.t(), entry()) :: [map()]
  def admission_scopes(%RunState{} = run, entry) when is_map(entry) do
    []
    |> maybe_add_run_scope(run)
    |> maybe_add_pool_scope(entry)
    |> maybe_add_global_scope()
    |> Enum.reverse()
  end

  defp maybe_add_run_scope(scopes, %RunState{} = run) do
    case pipeline_max_concurrency(run) do
      limit when is_integer(limit) and limit > 0 ->
        [%{kind: :run, key: run.id, limit: limit} | scopes]

      _other ->
        scopes
    end
  end

  defp maybe_add_pool_scope(scopes, entry) do
    pool = Map.get(entry, :execution_pool) || Map.get(entry, "execution_pool")

    case pool_limit(pool) do
      {:ok, key, limit} -> [%{kind: :pool, key: key, limit: limit} | scopes]
      :none -> scopes
    end
  end

  defp maybe_add_global_scope(scopes) do
    case pool_limit(:global) do
      {:ok, key, limit} -> [%{kind: :global, key: key, limit: limit} | scopes]
      :none -> scopes
    end
  end

  defp pipeline_max_concurrency(%RunState{metadata: %{pipeline_execution_policy: policy}})
       when is_map(policy) do
    Map.get(policy, :max_concurrency) || Map.get(policy, "max_concurrency")
  end

  defp pipeline_max_concurrency(%RunState{}), do: nil

  defp validate_execution_pool(entry) do
    case Map.get(entry, :execution_pool) || Map.get(entry, "execution_pool") do
      nil ->
        :ok

      pool ->
        case pool_limit(pool) do
          {:ok, _key, _limit} -> :ok
          :none -> {:error, {:unknown_execution_pool, pool}}
        end
    end
  end

  defp pool_limit(nil), do: :none

  defp pool_limit(pool) when is_atom(pool) or is_binary(pool) do
    key = to_string(pool)

    case Map.get(execution_pools(), key) do
      limit when is_integer(limit) and limit > 0 -> {:ok, key, limit}
      _other -> :none
    end
  end

  defp execution_pools do
    :favn
    |> Application.get_env(:execution_pools, [])
    |> Enum.reduce(%{}, fn {name, opts}, acc ->
      case execution_pool_limit(opts) do
        limit when is_integer(limit) and limit > 0 -> Map.put(acc, to_string(name), limit)
        _other -> acc
      end
    end)
  end

  defp execution_pool_limit(limit) when is_integer(limit), do: limit
  defp execution_pool_limit(opts) when is_list(opts), do: Keyword.get(opts, :max_concurrency)

  defp execution_pool_limit(opts) when is_map(opts),
    do: Map.get(opts, :max_concurrency) || Map.get(opts, "max_concurrency")

  defp execution_pool_limit(_opts), do: nil

  defp execution_lease_ttl_ms do
    :favn
    |> Application.get_env(:execution_lease_ttl_ms, @default_lease_ttl_ms)
    |> case do
      ttl when is_integer(ttl) and ttl > 0 -> ttl
      _other -> @default_lease_ttl_ms
    end
  end

  defp lease_ttl_ms(%RunState{timeout_ms: timeout_ms})
       when is_integer(timeout_ms) and timeout_ms > 0 do
    max(execution_lease_ttl_ms(), timeout_ms + @lease_timeout_buffer_ms)
  end

  defp lease_ttl_ms(%RunState{}), do: execution_lease_ttl_ms()

  defp lease_id(run_id, asset_step_id), do: "#{run_id}:#{asset_step_id}"

  defp queue_reason(%{kind: :run}), do: :pipeline_concurrency
  defp queue_reason(%{kind: "run"}), do: :pipeline_concurrency
  defp queue_reason(%{kind: :global}), do: :global_concurrency
  defp queue_reason(%{kind: "global"}), do: :global_concurrency
  defp queue_reason(_scope), do: :execution_pool
end
