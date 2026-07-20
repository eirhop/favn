defmodule FavnOrchestrator.ExecutionAdmission do
  @moduledoc """
  Orchestrator-owned admission control for asset step execution.

  Admission leases are persisted by the PostgreSQL admission capability so
  pipeline run limits and shared execution pools hold across orchestrator nodes.
  """

  alias FavnOrchestrator.ExecutionAdmission.Coordinator
  alias FavnOrchestrator.ExecutionAdmission.Identity
  alias FavnOrchestrator.ExecutionAdmission.Waiter
  alias FavnOrchestrator.Persistence
  alias FavnOrchestrator.Persistence.CapacityConfiguration
  alias FavnOrchestrator.Persistence.CapacityIdentity
  alias FavnOrchestrator.Persistence.Commands.AdmitExecution
  alias FavnOrchestrator.Persistence.Commands.CapacityRequest
  alias FavnOrchestrator.Persistence.Commands.ReleaseExecutionLease
  alias FavnOrchestrator.Persistence.Commands.ReleaseRunLeases
  alias FavnOrchestrator.Persistence.Results.Admission
  alias FavnOrchestrator.Persistence.Results.AdmissionWaiter
  alias FavnOrchestrator.Persistence.Results.CapacityRelease
  alias FavnOrchestrator.Persistence.Results.ExecutionLease
  alias FavnOrchestrator.Persistence.SystemContext
  alias FavnOrchestrator.RunState

  @default_lease_ttl_ms 300_000
  @lease_timeout_buffer_ms 60_000

  @type lease :: map()
  @type queue_reason :: :pipeline_concurrency | :execution_pool | :global_concurrency
  @type entry :: %{
          required(:asset_step_id) => String.t(),
          optional(:execution_pool) => atom() | String.t() | nil,
          optional(:stage) => non_neg_integer(),
          optional(:attempt) => pos_integer()
        }

  @spec acquire(RunState.t(), entry()) ::
          {:ok, lease() | nil} | {:queued, queue_reason(), map()} | {:error, term()}
  def acquire(%RunState{} = run, entry) when is_map(entry) do
    with {:ok, entry} <- normalize_entry(entry) do
      case acquire_result(run, entry) do
        {:waiting, %Waiter{} = waiter} ->
          {:queued, waiter.queue_reason, waiter.blocked_scope}

        other ->
          other
      end
    end
  end

  @spec acquire_or_wait(RunState.t(), entry(), keyword()) ::
          {:ok, lease() | nil} | {:waiting, Waiter.t()} | {:error, term()}
  def acquire_or_wait(%RunState{} = run, entry, opts \\ [])
      when is_map(entry) and is_list(opts) do
    entry = Map.merge(entry, Map.new(Keyword.take(opts, [:stage, :attempt])))

    with {:ok, entry} <- normalize_entry(entry) do
      case acquire_result(run, entry) do
        {:ok, lease} ->
          {:ok, lease}

        {:waiting, %Waiter{} = waiter} ->
          register_waiter(run, entry, waiter)

        {:error, reason} ->
          {:error, reason}
      end
    end
  end

  defp register_waiter(run, entry, waiter) do
    case Coordinator.register(waiter, self()) do
      :ok ->
        acquire_after_waiter_registration(run, entry, waiter)

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp acquire_result(%RunState{} = run, entry) when is_map(entry) do
    with :ok <- validate_run_admissible(run),
         :ok <- validate_execution_pool(entry) do
      acquire_v2(run, entry)
    end
  end

  defp acquire_v2(%RunState{} = run, entry) do
    scopes = v2_admission_scopes(run, entry)

    if scopes == [] do
      {:ok, nil}
    else
      with :ok <- validate_v2_authority(run),
           {:ok, %Admission{} = admission} <-
             Persistence.stores().admission.admit(admit_command(run, entry, scopes)) do
        case admission do
          %Admission{status: :admitted, lease: %ExecutionLease{} = lease} ->
            {:ok, lease_map(lease, scopes)}

          %Admission{status: :waiting, waiter: %AdmissionWaiter{} = waiter} ->
            {:waiting, waiter_struct(waiter, scopes, entry)}
        end
      end
    end
  end

  defp validate_run_admissible(%RunState{id: run_id, status: status} = run) do
    if RunState.execution_admissible?(run) do
      :ok
    else
      {:error, {:run_not_admissible, run_id, status}}
    end
  end

  @spec release(lease() | nil) :: :ok | {:error, term()}
  def release(nil), do: :ok

  def release(%{workspace_id: workspace_id} = lease) when is_binary(workspace_id) do
    command = %ReleaseExecutionLease{
      workspace_context: SystemContext.workspace(workspace_id, :admission_release),
      lease_id: field(lease, :lease_id),
      owner_id: field(lease, :owner_id),
      owner_generation: field(lease, :owner_generation)
    }

    case Persistence.stores().admission.release_lease(command) do
      {:ok, %CapacityRelease{} = release} ->
        Coordinator.notify_scopes(Map.get(lease, :scopes, []))
        notify_scope_ids(release.freed_scope_ids)
        :ok

      {:error, reason} ->
        {:error, reason}
    end
  end

  @spec cancel_wait(String.t() | Waiter.t()) :: :ok | {:error, term()}
  def cancel_wait(%Waiter{workspace_id: workspace_id, waiter_id: waiter_id})
      when is_binary(workspace_id) do
    Coordinator.cancel(waiter_id)
  end

  def cancel_wait(waiter_id) when is_binary(waiter_id) do
    Coordinator.cancel(waiter_id)
  end

  @spec release_run(RunState.t()) :: :ok | {:error, term()}
  def release_run(%RunState{workspace_id: workspace_id} = run) when is_binary(workspace_id) do
    release_run_v2(run, [])
  end

  defp acquire_after_waiter_registration(%RunState{} = run, entry, %Waiter{} = waiter) do
    case acquire_result(run, entry) do
      {:ok, lease} ->
        case cancel_wait(waiter) do
          :ok ->
            {:ok, lease}

          {:error, reason} ->
            case release(lease) do
              :ok ->
                {:error, {:execution_admission_waiter_cleanup_failed, reason}}

              {:error, release_error} ->
                {:error,
                 {:execution_admission_waiter_cleanup_failed, reason,
                  {:lease_release_failed, release_error}}}
            end
        end

      {:waiting, %Waiter{}} ->
        {:waiting, waiter}

      {:error, reason} ->
        case cancel_wait(waiter) do
          :ok ->
            {:error, reason}

          {:error, cleanup_error} ->
            {:error, {:execution_admission_waiter_cleanup_failed, reason, cleanup_error}}
        end
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

  defp pipeline_max_concurrency(%RunState{metadata: metadata}) when is_map(metadata) do
    case Map.get(metadata, "pipeline_execution_policy") do
      policy when is_map(policy) ->
        Map.get(policy, :max_concurrency) || Map.get(policy, "max_concurrency")

      _other ->
        nil
    end
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
    case Application.get_env(:favn, :execution_pools, []) do
      pools when is_list(pools) or is_map(pools) ->
        Enum.reduce(pools, %{}, fn
          {name, opts}, acc when is_atom(name) or is_binary(name) ->
            case execution_pool_limit(opts) do
              limit when is_integer(limit) and limit > 0 -> Map.put(acc, to_string(name), limit)
              _other -> acc
            end

          _invalid, acc ->
            acc
        end)

      _invalid ->
        %{}
    end
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

  defp v2_admission_scopes(%RunState{} = run, entry) do
    []
    |> maybe_add_v2_run_scope(run)
    |> maybe_add_v2_configured_scope(run.workspace_id, Map.get(entry, :execution_pool))
    |> maybe_add_v2_configured_scope(run.workspace_id, :global)
    |> Enum.reverse()
    |> Enum.uniq_by(& &1.scope_id)
  end

  defp maybe_add_v2_run_scope(scopes, %RunState{} = run) do
    case pipeline_max_concurrency(run) do
      limit when is_integer(limit) and limit > 0 ->
        [
          %{
            scope_id: CapacityIdentity.scope_id(run.workspace_id, :run, run.id),
            kind: :run,
            key: run.id,
            limit: limit
          }
          | scopes
        ]

      _unlimited ->
        scopes
    end
  end

  defp maybe_add_v2_configured_scope(scopes, workspace_id, pool) do
    case CapacityConfiguration.execution_scope(workspace_id, pool) do
      {:ok, scope} -> [scope | scopes]
      :unlimited -> scopes
    end
  end

  defp validate_v2_authority(%RunState{
         workspace_id: workspace_id,
         storage_owner_id: owner_id,
         storage_fencing_token: generation
       })
       when is_binary(workspace_id) and workspace_id != "" and is_binary(owner_id) and
              owner_id != "" and is_integer(generation) and generation > 0,
       do: :ok

  defp validate_v2_authority(%RunState{}), do: {:error, :execution_admission_authority_required}

  defp admit_command(run, entry, scopes) do
    lease_id = Identity.lease_id(run.id, entry.asset_step_id, entry.stage, entry.attempt)
    waiter_id = Waiter.waiter_id(run.id, entry.asset_step_id, entry.stage, entry.attempt)
    now = DateTime.utc_now()

    %AdmitExecution{
      workspace_context: SystemContext.workspace(run.workspace_id, :execution_admission),
      command_id: unique_command_id("admit", lease_id, run.storage_fencing_token),
      lease_id: lease_id,
      waiter_id: waiter_id,
      run_id: run.id,
      step_id: entry.asset_step_id,
      owner_id: run.storage_owner_id,
      owner_generation: run.storage_fencing_token,
      lease_duration_ms: lease_ttl_ms(run),
      waiter_ttl_ms: lease_ttl_ms(run),
      requests: Enum.map(scopes, &%CapacityRequest{scope_id: &1.scope_id}),
      occurred_at: now
    }
  end

  defp lease_map(%ExecutionLease{} = lease, scopes) do
    %{
      workspace_id: lease.workspace_id,
      lease_id: lease.lease_id,
      run_id: lease.run_id,
      asset_step_id: lease.step_id,
      owner_id: lease.owner_id,
      owner_generation: lease.owner_generation,
      scopes: scopes,
      expires_at: lease.expires_at
    }
  end

  defp waiter_struct(%AdmissionWaiter{} = waiter, scopes, entry) do
    blocked_scope =
      Enum.find(scopes, &(&1.scope_id == waiter.blocking_scope_id)) || List.first(scopes)

    {:ok, normalized} =
      Waiter.normalize(%{
        workspace_id: waiter.workspace_id,
        waiter_id: waiter.waiter_id,
        run_id: waiter.run_id,
        asset_step_id: waiter.step_id,
        queue_reason: queue_reason(blocked_scope),
        blocked_scope: Map.take(blocked_scope, [:kind, :key, :limit]),
        requested_scopes: Enum.map(scopes, &Map.take(&1, [:kind, :key, :limit])),
        stage: entry.stage,
        attempt: entry.attempt,
        inserted_at: DateTime.utc_now(),
        updated_at: DateTime.utc_now(),
        deadline_at: waiter.expires_at,
        wake_generation: waiter.claim_generation || 0
      })

    normalized
  end

  defp release_run_v2(run, released_ids) do
    command = %ReleaseRunLeases{
      workspace_context: SystemContext.workspace(run.workspace_id, :admission_release_run),
      run_id: run.id,
      limit: 500
    }

    case Persistence.stores().admission.release_run_leases(command) do
      {:ok, %CapacityRelease{} = release} ->
        notify_scope_ids(release.freed_scope_ids)

        if release.released_lease_ids == [] and release.expired_waiter_ids == [] do
          _all_released = released_ids
          :ok
        else
          release_run_v2(run, release.released_lease_ids ++ released_ids)
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp notify_scope_ids(_scope_ids), do: :ok

  defp command_id(operation, identity, generation) do
    digest =
      :crypto.hash(:sha256, :erlang.term_to_binary({operation, identity, generation}))
      |> Base.url_encode64(padding: false)

    "admission:#{operation}:#{digest}"
  end

  defp unique_command_id(operation, identity, generation) do
    nonce = :crypto.strong_rand_bytes(16)
    command_id(operation, {identity, nonce}, generation)
  end

  defp normalize_entry(entry) do
    asset_step_id = field(entry, :asset_step_id)
    execution_pool = field(entry, :execution_pool)
    stage = field(entry, :stage) || 0
    attempt = field(entry, :attempt) || 1

    cond do
      not (is_binary(asset_step_id) and byte_size(asset_step_id) > 0) ->
        {:error, {:invalid_execution_admission_entry, :asset_step_id}}

      not (is_nil(execution_pool) or is_atom(execution_pool) or is_binary(execution_pool)) ->
        {:error, {:invalid_execution_admission_entry, :execution_pool}}

      not (is_integer(stage) and stage >= 0) ->
        {:error, {:invalid_execution_admission_entry, :stage}}

      not (is_integer(attempt) and attempt > 0) ->
        {:error, {:invalid_execution_admission_entry, :attempt}}

      true ->
        {:ok,
         %{
           asset_step_id: asset_step_id,
           execution_pool: execution_pool,
           stage: stage,
           attempt: attempt
         }}
    end
  end

  defp field(map, key) do
    case Map.fetch(map, key) do
      {:ok, value} -> value
      :error -> Map.get(map, Atom.to_string(key))
    end
  end

  defp queue_reason(%{kind: :run}), do: :pipeline_concurrency
  defp queue_reason(%{kind: "run"}), do: :pipeline_concurrency
  defp queue_reason(%{kind: :global}), do: :global_concurrency
  defp queue_reason(%{kind: "global"}), do: :global_concurrency
  defp queue_reason(_scope), do: :execution_pool
end
