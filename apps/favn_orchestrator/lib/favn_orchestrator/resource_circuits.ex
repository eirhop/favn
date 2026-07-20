defmodule FavnOrchestrator.ResourceCircuits do
  @moduledoc """
  Durable admission and terminal-outcome boundary for configured resource circuits.

  Circuit identity is workspace plus resource kind and name. A half-open permit
  belongs to one asset step; unrelated work is blocked until that probe settles
  or its lease expires.
  """

  alias Favn.Contracts.ResourceOutcome
  alias Favn.Contracts.RunnerError
  alias Favn.Contracts.RunnerResult
  alias Favn.Contracts.RunnerWork
  alias Favn.Manifest.Asset
  alias Favn.Manifest.Index
  alias Favn.RelationRef
  alias Favn.Resource.Ref
  alias Favn.ResourceRecovery.Policy, as: RecoveryPolicy
  alias FavnOrchestrator.Persistence
  alias FavnOrchestrator.Persistence.Commands.AcquireResourceCircuits
  alias FavnOrchestrator.Persistence.Commands.RecordResourceOutcomes
  alias FavnOrchestrator.Persistence.Commands.RecordResourceRecoveryCandidate
  alias FavnOrchestrator.Persistence.Commands.ReleaseResourceCircuitPermits
  alias FavnOrchestrator.Persistence.Commands.ResourceCircuitRequest
  alias FavnOrchestrator.Persistence.Results.ResourceCircuitAdmission
  alias FavnOrchestrator.Persistence.Results.ResourceCircuitBlocker
  alias FavnOrchestrator.Persistence.Results.ResourceCircuitPermit
  alias FavnOrchestrator.Persistence.Results.ResourceCircuitUpdate
  alias FavnOrchestrator.Persistence.SystemContext
  alias FavnOrchestrator.ResourceConfiguration
  alias FavnOrchestrator.ResourceRecovery
  alias FavnOrchestrator.RunState

  @default_probe_lease_ms 900_000

  @type admission ::
          {:ok, [ResourceCircuitPermit.t()]}
          | {:blocked, [ResourceCircuitBlocker.t()]}
          | {:error, term()}

  @doc "Checks all configured circuits used by one planned node."
  @spec acquire(RunState.t(), RunnerWork.t(), Index.t()) :: admission()
  def acquire(%RunState{} = run, %RunnerWork{} = work, %Index{} = index) do
    with {:ok, requests} <- requests(work, index) do
      case requests do
        [] ->
          {:ok, []}

        [_ | _] ->
          command = acquire_command(run, work, requests)

          case Persistence.stores().resource_circuits.acquire(command) do
            {:ok, %ResourceCircuitAdmission{status: :allowed, permits: permits}} ->
              {:ok, permits}

            {:ok, %ResourceCircuitAdmission{status: :blocked, blockers: blockers}} ->
              {:blocked, blockers}

            {:error, reason} ->
              {:error, reason}
          end
      end
    end
  end

  @doc "Records a circuit-blocked node as eligible for opt-in linked recovery."
  @spec record_blocked(RunState.t(), RunnerWork.t(), [ResourceCircuitBlocker.t()]) ::
          :ok | {:error, term()}
  def record_blocked(%RunState{} = run, %RunnerWork{} = work, blockers) when is_list(blockers) do
    with %RecoveryPolicy{} = policy <- recovery_policy(run),
         %ResourceCircuitBlocker{resource: %Ref{} = resource} <- first_blocker(blockers) do
      record_recovery_candidate(run, work, resource, :blocked, policy)
    else
      nil -> :ok
    end
  end

  @doc "Releases a half-open permit that produced no resource-health outcome."
  @spec release(RunState.t(), [ResourceCircuitPermit.t()]) :: :ok | {:error, term()}
  def release(%RunState{} = run, permits) when is_list(permits) do
    case permits do
      [] ->
        :ok

      [%ResourceCircuitPermit{owner_id: owner_id} | _rest] ->
        Persistence.stores().resource_circuits.release_permits(%ReleaseResourceCircuitPermits{
          workspace_context: SystemContext.workspace(run.workspace_id, :resource_circuit_release),
          owner_id: owner_id,
          permits: permits,
          occurred_at: DateTime.utc_now()
        })
    end
  end

  @doc "Records terminal node outcomes and schedules recovery when a probe closes a circuit."
  @spec settle(
          RunState.t(),
          map(),
          :ok | :error,
          RunnerResult.t() | RunnerError.t() | term()
        ) :: :ok | {:error, term()}
  def settle(%RunState{} = run, entry, status, value) when status in [:ok, :error] do
    permits = Map.get(entry, :resource_circuit_permits, [])

    if permits == [] do
      :ok
    else
      outcomes = terminal_outcomes(permits, status, value)
      unreported_permits = unreported_permits(permits, outcomes)

      with {:ok, %ResourceCircuitUpdate{} = update} <-
             record_terminal_outcomes(run, entry, permits, outcomes),
           :ok <- release(run, unreported_permits) do
        Enum.each(update.closed_resources, &ResourceRecovery.enqueue(run.workspace_id, &1))
        :ok
      end
    end
  end

  defp requests(work, index) do
    work
    |> resource_refs(index)
    |> Enum.reduce_while({:ok, []}, fn ref, {:ok, acc} ->
      case ResourceConfiguration.circuit_breaker(ref) do
        {:ok, nil} ->
          {:cont, {:ok, acc}}

        {:ok, policy} ->
          {:cont, {:ok, [%ResourceCircuitRequest{resource: ref, policy: policy} | acc]}}

        {:error, reason} ->
          {:halt, {:error, reason}}
      end
    end)
    |> then(fn
      {:ok, requests} -> {:ok, Enum.reverse(requests)}
      error -> error
    end)
  end

  defp resource_refs(work, index) do
    pool_refs =
      case RunnerWork.execution_pool(work) do
        nil -> []
        pool -> [Ref.new!(:execution_pool, pool)]
      end

    connection_refs =
      case Index.fetch_asset(index, RunnerWork.asset_ref(work)) do
        {:ok, %Asset{} = asset} -> asset_connection_refs(asset)
        {:error, :asset_not_found} -> []
      end

    (pool_refs ++ connection_refs)
    |> Enum.uniq_by(&{&1.kind, &1.name})
    |> Enum.sort_by(&{&1.kind, &1.name})
  end

  defp asset_connection_refs(%Asset{} = asset) do
    [asset.relation | asset.relation_inputs]
    |> Enum.flat_map(&connection_names/1)
    |> Enum.reject(&is_nil/1)
    |> Enum.map(&Ref.new!(:connection, &1))
  end

  defp connection_names(%RelationRef{connection: connection}), do: [connection]
  defp connection_names(%{relation_ref: relation}), do: connection_names(relation)
  defp connection_names(%{"relation_ref" => relation}), do: connection_names(relation)
  defp connection_names(%{connection: connection}), do: [connection]
  defp connection_names(%{"connection" => connection}), do: [connection]
  defp connection_names(_value), do: []

  defp acquire_command(run, work, requests) do
    owner_id = permit_owner_id(run, work)

    %AcquireResourceCircuits{
      workspace_context: SystemContext.workspace(run.workspace_id, :resource_circuit_admission),
      command_id: "resource-circuit-acquire:#{owner_id}",
      owner_id: owner_id,
      run_id: run.id,
      asset_step_id: work.asset_step_id,
      requests: requests,
      probe_lease_ms: probe_lease_ms(run),
      occurred_at: DateTime.utc_now()
    }
  end

  defp outcome_command(run, entry, permits, outcomes) do
    owner_id = permits |> List.first() |> Map.fetch!(:owner_id)

    %RecordResourceOutcomes{
      workspace_context: SystemContext.workspace(run.workspace_id, :resource_circuit_outcome),
      command_id:
        "resource-circuit-outcome:#{short_hash({run.id, entry.asset_step_id, entry.attempt, outcomes})}",
      owner_id: owner_id,
      run_id: run.id,
      asset_step_id: entry.asset_step_id,
      attempt: entry.attempt,
      permits: permits,
      outcomes: outcomes,
      recovery_candidates: safe_failure_candidates(run, entry, outcomes),
      occurred_at: DateTime.utc_now()
    }
  end

  defp terminal_outcomes(permits, :ok, value) do
    automatic =
      Enum.map(permits, fn permit ->
        ResourceOutcome.new!(
          resource: permit.resource,
          status: :success,
          category: :work_succeeded
        )
      end)

    merge_outcomes(automatic, explicit_outcomes(value))
  end

  defp terminal_outcomes(_permits, :error, value), do: explicit_outcomes(value)

  defp explicit_outcomes(%RunnerResult{resource_outcomes: outcomes, error: error}),
    do: merge_outcomes(outcomes, explicit_outcomes(error))

  defp explicit_outcomes(%RunnerError{resource_outcomes: outcomes}), do: outcomes
  defp explicit_outcomes(_value), do: []

  defp merge_outcomes(left, right) do
    (left ++ right)
    |> Map.new(&{{&1.resource.kind, &1.resource.name}, &1})
    |> Map.values()
    |> Enum.sort_by(&{&1.resource.kind, &1.resource.name})
  end

  defp record_terminal_outcomes(_run, _entry, _permits, []),
    do: {:ok, %ResourceCircuitUpdate{closed_resources: []}}

  defp record_terminal_outcomes(run, entry, permits, outcomes) do
    Persistence.stores().resource_circuits.record_outcomes(
      outcome_command(run, entry, permits, outcomes)
    )
  end

  defp unreported_permits(permits, outcomes) do
    reported = MapSet.new(outcomes, &{&1.resource.kind, &1.resource.name})

    Enum.reject(permits, fn permit ->
      MapSet.member?(reported, {permit.resource.kind, permit.resource.name})
    end)
  end

  defp safe_failure_candidates(run, entry, outcomes) do
    case recovery_policy(run) do
      %RecoveryPolicy{} = policy ->
        outcomes
        |> Enum.filter(&(&1.status == :failure and &1.safe_to_repeat?))
        |> Enum.map(fn %ResourceOutcome{resource: resource} ->
          recovery_candidate(run, entry.node_key, resource, :safe_failure, policy)
        end)

      nil ->
        []
    end
  end

  defp record_recovery_candidate(run, work, resource, reason, policy) do
    node_key = RunnerWork.node_key(work)
    candidate = recovery_candidate(run, node_key, resource, reason, policy)

    Persistence.stores().resource_circuits.record_recovery_candidate(candidate)
  end

  defp recovery_candidate(run, node_key, resource, reason, policy) do
    %RecordResourceRecoveryCandidate{
      workspace_context: SystemContext.workspace(run.workspace_id, :resource_recovery_candidate),
      candidate_id: candidate_id(run.id, node_key),
      source_run_id: run.id,
      node_key: node_key,
      resource: resource,
      reason: reason,
      max_age_ms: policy.max_age_ms,
      occurred_at: DateTime.utc_now()
    }
  end

  defp recovery_policy(%RunState{metadata: metadata}) when is_map(metadata) do
    metadata
    |> field(:pipeline_execution_policy, %{})
    |> field(:resource_recovery)
    |> RecoveryPolicy.from_value()
    |> case do
      {:ok, %RecoveryPolicy{} = policy} -> policy
      _other -> nil
    end
  end

  defp recovery_policy(_run), do: nil

  defp first_blocker([%ResourceCircuitBlocker{} = blocker | _rest]), do: blocker
  defp first_blocker(_blockers), do: nil

  defp permit_owner_id(run, work),
    do: "resource-circuit-owner:#{short_hash({run.id, work.asset_step_id})}"

  defp probe_lease_ms(%RunState{timeout_ms: timeout_ms}) when is_integer(timeout_ms),
    do: max(timeout_ms + 60_000, @default_probe_lease_ms)

  defp probe_lease_ms(_run), do: @default_probe_lease_ms

  defp candidate_id(run_id, node_key),
    do: "recovery:#{run_id}:#{short_hash(node_key)}"

  defp short_hash(value) do
    value
    |> :erlang.term_to_binary([:deterministic])
    |> then(&:crypto.hash(:sha256, &1))
    |> Base.url_encode64(padding: false)
  end

  defp field(map, key, default \\ nil),
    do: Map.get(map, key, Map.get(map, Atom.to_string(key), default))
end
