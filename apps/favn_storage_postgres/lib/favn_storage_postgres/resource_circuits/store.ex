defmodule FavnStoragePostgres.ResourceCircuits.Store do
  @moduledoc false

  @behaviour FavnOrchestrator.Persistence.ResourceCircuitStore

  import Ecto.Query

  alias Favn.Resource.Ref
  alias FavnOrchestrator.Persistence.Commands.AcquireResourceCircuits
  alias FavnOrchestrator.Persistence.Commands.ClaimResourceRecovery
  alias FavnOrchestrator.Persistence.Commands.CompleteResourceRecovery
  alias FavnOrchestrator.Persistence.Commands.ListPendingResourceRecoveries
  alias FavnOrchestrator.Persistence.Commands.RecordResourceOutcomes
  alias FavnOrchestrator.Persistence.Commands.RecordResourceRecoveryCandidate
  alias FavnOrchestrator.Persistence.Commands.ReleaseResourceCircuitPermits
  alias FavnOrchestrator.Persistence.Commands.ResourceCircuitRequest
  alias FavnOrchestrator.Persistence.Error
  alias FavnOrchestrator.Persistence.Results.ResourceCircuitAdmission
  alias FavnOrchestrator.Persistence.Results.ResourceCircuitBlocker
  alias FavnOrchestrator.Persistence.Results.ResourceCircuitPermit
  alias FavnOrchestrator.Persistence.Results.ResourceCircuitUpdate
  alias FavnOrchestrator.Persistence.Results.ResourceRecoveryBatch
  alias FavnOrchestrator.Persistence.Results.ResourceRecoveryWakeup

  alias FavnOrchestrator.Persistence.Results.ResourceRecoveryCandidate,
    as: RecoveryCandidateResult

  alias FavnOrchestrator.Persistence.WorkspaceContext
  alias FavnOrchestrator.Persistence.PlatformContext
  alias FavnOrchestrator.Storage.PayloadCodec
  alias FavnStoragePostgres.ErrorMapper
  alias FavnStoragePostgres.Repo
  alias FavnStoragePostgres.Schemas.ResourceCircuit
  alias FavnStoragePostgres.Schemas.ResourceCircuitOutcome
  alias FavnStoragePostgres.Schemas.ResourceRecoveryCandidate

  @impl true
  def acquire(%AcquireResourceCircuits{} = command) do
    with :ok <- validate_acquire(command) do
      transaction(fn -> acquire!(command) end)
    end
  end

  @impl true
  def record_outcomes(%RecordResourceOutcomes{} = command) do
    with :ok <- validate_outcomes(command) do
      transaction(fn -> record_outcomes!(command) end)
    end
  end

  @impl true
  def record_recovery_candidate(%RecordResourceRecoveryCandidate{} = command) do
    with :ok <- validate_candidate(command) do
      insert_recovery_candidate!(command)
      :ok
    else
      {:error, %Error{} = error} -> {:error, error}
    end
  rescue
    error -> {:error, ErrorMapper.map(error)}
  end

  @impl true
  def release_permits(%ReleaseResourceCircuitPermits{} = command) do
    with :ok <- validate_release(command) do
      transaction(fn -> release_permits!(command) end)
      |> case do
        {:ok, :ok} -> :ok
        {:error, reason} -> {:error, reason}
      end
    end
  end

  @impl true
  def claim_recovery(%ClaimResourceRecovery{} = command) do
    with :ok <- validate_claim(command) do
      transaction(fn -> claim_recovery!(command) end)
    end
  end

  @impl true
  def complete_recovery(%CompleteResourceRecovery{} = command) do
    with :ok <- validate_complete(command) do
      transaction(fn -> complete_recovery!(command) end)
      |> case do
        {:ok, :ok} -> :ok
        {:error, reason} -> {:error, reason}
      end
    end
  end

  @impl true
  def list_pending_recoveries(%ListPendingResourceRecoveries{} = command) do
    with :ok <- validate_list_pending(command) do
      rows =
        from(candidate in ResourceRecoveryCandidate,
          join: circuit in ResourceCircuit,
          on:
            circuit.workspace_id == candidate.workspace_id and
              circuit.resource_kind == candidate.resource_kind and
              circuit.resource_name == candidate.resource_name,
          where:
            circuit.state == "closed" and candidate.expires_at > ^command.occurred_at and
              (candidate.status == "pending" or
                 (candidate.status == "claimed" and
                    candidate.claim_expires_at <= ^command.occurred_at)),
          group_by: [candidate.workspace_id, candidate.resource_kind, candidate.resource_name],
          order_by: [asc: min(candidate.inserted_at)],
          limit: ^command.limit,
          select: {candidate.workspace_id, candidate.resource_kind, candidate.resource_name}
        )
        |> Repo.all()

      {:ok,
       Enum.map(rows, fn {workspace_id, kind, name} ->
         %ResourceRecoveryWakeup{
           workspace_id: workspace_id,
           resource: Ref.new!(resource_kind(kind), name)
         }
       end)}
    end
  rescue
    error -> {:error, ErrorMapper.map(error)}
  end

  defp acquire!(command) do
    workspace_id = command.workspace_context.workspace_id

    circuits =
      command.requests
      |> Enum.sort_by(&resource_identity(&1.resource))
      |> Enum.map(fn request -> ensure_and_lock!(workspace_id, request, command.occurred_at) end)

    blockers =
      Enum.flat_map(circuits, &blockers(&1, command.occurred_at, command.owner_id))

    if blockers == [] do
      permits =
        Enum.map(circuits, fn circuit ->
          probe_available? = probe_available?(circuit, command.occurred_at)
          probe? = probe_available? or probe_owned?(circuit, command.owner_id)

          if probe_available? do
            circuit
            |> Ecto.Changeset.change(%{
              state: "half_open",
              probe_owner_id: command.owner_id,
              probe_expires_at:
                DateTime.add(command.occurred_at, command.probe_lease_ms, :millisecond),
              version: circuit.version + 1,
              updated_at: command.occurred_at
            })
            |> Repo.update!()
          end

          %ResourceCircuitPermit{
            resource: resource_ref(circuit),
            owner_id: command.owner_id,
            probe?: probe?
          }
        end)

      %ResourceCircuitAdmission{status: :allowed, permits: permits}
    else
      %ResourceCircuitAdmission{status: :blocked, blockers: blockers}
    end
  end

  defp ensure_and_lock!(workspace_id, %ResourceCircuitRequest{} = request, now) do
    resource = request.resource
    policy = request.policy

    Repo.insert_all(
      ResourceCircuit,
      [
        %{
          workspace_id: workspace_id,
          resource_kind: Atom.to_string(resource.kind),
          resource_name: resource.name,
          state: "closed",
          consecutive_failures: 0,
          failure_threshold: policy.failure_threshold,
          probe_after_ms: policy.probe_after_ms,
          version: 1,
          inserted_at: now,
          updated_at: now
        }
      ],
      on_conflict: :nothing
    )

    circuit = lock_circuit!(workspace_id, resource)

    if circuit.failure_threshold != policy.failure_threshold or
         circuit.probe_after_ms != policy.probe_after_ms do
      circuit
      |> Ecto.Changeset.change(%{
        failure_threshold: policy.failure_threshold,
        probe_after_ms: policy.probe_after_ms,
        version: circuit.version + 1,
        updated_at: now
      })
      |> Repo.update!()
    else
      circuit
    end
  end

  defp blockers(%ResourceCircuit{state: "closed"}, _now, _owner_id), do: []

  defp blockers(%ResourceCircuit{state: "half_open"} = circuit, _now, owner_id)
       when circuit.probe_owner_id == owner_id,
       do: []

  defp blockers(%ResourceCircuit{} = circuit, now, _owner_id) do
    if probe_available?(circuit, now) do
      []
    else
      [
        %ResourceCircuitBlocker{
          resource: resource_ref(circuit),
          state: circuit_state(circuit.state),
          failure_threshold: circuit.failure_threshold,
          consecutive_failures: circuit.consecutive_failures,
          retry_at: retry_at(circuit),
          probe_owner_id: circuit.probe_owner_id
        }
      ]
    end
  end

  defp probe_available?(%ResourceCircuit{state: "open", next_probe_at: next_probe_at}, now),
    do: not is_nil(next_probe_at) and DateTime.compare(next_probe_at, now) != :gt

  defp probe_available?(%ResourceCircuit{state: "half_open", probe_expires_at: expires_at}, now),
    do: not is_nil(expires_at) and DateTime.compare(expires_at, now) != :gt

  defp probe_available?(_circuit, _now), do: false

  defp probe_owned?(%ResourceCircuit{state: "half_open", probe_owner_id: owner_id}, owner_id),
    do: true

  defp probe_owned?(_circuit, _owner_id), do: false

  defp retry_at(%ResourceCircuit{state: "open", next_probe_at: value}), do: value
  defp retry_at(%ResourceCircuit{state: "half_open", probe_expires_at: value}), do: value

  defp record_outcomes!(command) do
    workspace_id = command.workspace_context.workspace_id
    permits = Map.new(command.permits, &{resource_identity(&1.resource), &1})

    {open_resources, closed_resources} =
      command.outcomes
      |> Enum.sort_by(&resource_identity(&1.resource))
      |> Enum.reduce({[], []}, fn outcome, transitions ->
        case Map.fetch(permits, resource_identity(outcome.resource)) do
          {:ok, permit} -> record_outcome!(workspace_id, command, permit, outcome, transitions)
          :error -> transitions
        end
      end)

    insert_open_recovery_candidate!(command.recovery_candidates, open_resources)

    %ResourceCircuitUpdate{closed_resources: Enum.reverse(closed_resources)}
  end

  defp release_permits!(command) do
    workspace_id = command.workspace_context.workspace_id

    command.permits
    |> Enum.filter(& &1.probe?)
    |> Enum.sort_by(&resource_identity(&1.resource))
    |> Enum.each(fn permit ->
      circuit = lock_circuit!(workspace_id, permit.resource)

      if circuit.state == "half_open" and circuit.probe_owner_id == command.owner_id do
        update_circuit!(circuit, %{
          state: "open",
          next_probe_at: command.occurred_at,
          probe_owner_id: nil,
          probe_expires_at: nil,
          version: circuit.version + 1,
          updated_at: command.occurred_at
        })
      end
    end)

    :ok
  end

  defp record_outcome!(workspace_id, command, permit, outcome, transitions) do
    outcome_id = outcome_id(command, outcome.resource)

    {inserted, nil} =
      Repo.insert_all(
        ResourceCircuitOutcome,
        [
          %{
            workspace_id: workspace_id,
            outcome_id: outcome_id,
            resource_kind: Atom.to_string(outcome.resource.kind),
            resource_name: outcome.resource.name,
            run_id: command.run_id,
            asset_step_id: command.asset_step_id,
            attempt: command.attempt,
            status: Atom.to_string(outcome.status),
            category: category(outcome.category),
            occurred_at: command.occurred_at,
            inserted_at: command.occurred_at
          }
        ],
        on_conflict: :nothing
      )

    if inserted == 0 do
      transitions
    else
      circuit = lock_circuit!(workspace_id, outcome.resource)
      apply_outcome!(circuit, permit, outcome, command.occurred_at, transitions)
    end
  end

  defp apply_outcome!(
         %ResourceCircuit{state: "closed"} = circuit,
         _permit,
         outcome,
         now,
         {open, closed}
       ) do
    case outcome.status do
      :success ->
        update_circuit!(circuit, closed_attrs(circuit, outcome, now))
        {open, closed}

      :failure ->
        failures = circuit.consecutive_failures + 1

        if failures >= circuit.failure_threshold do
          update_circuit!(circuit, open_attrs(circuit, outcome, now, failures))
          {[outcome.resource | open], closed}
        else
          update_circuit!(circuit, %{
            consecutive_failures: failures,
            last_category: category(outcome.category),
            last_outcome_at: now,
            version: circuit.version + 1,
            updated_at: now
          })

          {open, closed}
        end
    end
  end

  defp apply_outcome!(
         %ResourceCircuit{state: "half_open"} = circuit,
         permit,
         outcome,
         now,
         {open, closed}
       ) do
    if circuit.probe_owner_id == permit.owner_id do
      case outcome.status do
        :success ->
          update_circuit!(circuit, closed_attrs(circuit, outcome, now))
          {open, [resource_ref(circuit) | closed]}

        :failure ->
          update_circuit!(
            circuit,
            open_attrs(circuit, outcome, now, max(circuit.consecutive_failures, 1))
          )

          {[outcome.resource | open], closed}
      end
    else
      {open, closed}
    end
  end

  defp apply_outcome!(
         %ResourceCircuit{state: "open"} = circuit,
         _permit,
         outcome,
         now,
         {open, closed}
       ) do
    case outcome.status do
      :success ->
        update_circuit!(circuit, closed_attrs(circuit, outcome, now))
        {open, [resource_ref(circuit) | closed]}

      :failure ->
        failures = circuit.consecutive_failures + 1
        update_circuit!(circuit, open_attrs(circuit, outcome, now, failures))
        {[outcome.resource | open], closed}
    end
  end

  defp closed_attrs(circuit, outcome, now) do
    %{
      state: "closed",
      consecutive_failures: 0,
      opened_at: nil,
      next_probe_at: nil,
      probe_owner_id: nil,
      probe_expires_at: nil,
      last_category: category(outcome.category),
      last_outcome_at: now,
      version: circuit.version + 1,
      updated_at: now
    }
  end

  defp open_attrs(circuit, outcome, now, failures) do
    %{
      state: "open",
      consecutive_failures: failures,
      opened_at: now,
      next_probe_at: DateTime.add(now, circuit.probe_after_ms, :millisecond),
      probe_owner_id: nil,
      probe_expires_at: nil,
      last_category: category(outcome.category),
      last_outcome_at: now,
      version: circuit.version + 1,
      updated_at: now
    }
  end

  defp update_circuit!(circuit, attrs) do
    circuit |> Ecto.Changeset.change(attrs) |> Repo.update!()
  end

  defp insert_open_recovery_candidate!(candidates, open_resources) do
    open_identities = MapSet.new(open_resources, &resource_identity/1)

    candidates
    |> Enum.sort_by(&resource_identity(&1.resource))
    |> Enum.find(fn candidate ->
      MapSet.member?(open_identities, resource_identity(candidate.resource))
    end)
    |> case do
      %RecordResourceRecoveryCandidate{} = candidate -> insert_recovery_candidate!(candidate)
      nil -> :ok
    end
  end

  defp insert_recovery_candidate!(command) do
    {:ok, node_key} = PayloadCodec.encode(command.node_key)
    now = command.occurred_at

    attrs = %{
      workspace_id: command.workspace_context.workspace_id,
      candidate_id: command.candidate_id,
      source_run_id: command.source_run_id,
      node_key: node_key,
      resource_kind: Atom.to_string(command.resource.kind),
      resource_name: command.resource.name,
      reason: Atom.to_string(command.reason),
      status: "pending",
      expires_at: DateTime.add(now, command.max_age_ms, :millisecond),
      inserted_at: now,
      updated_at: now
    }

    Repo.insert_all(ResourceRecoveryCandidate, [attrs], on_conflict: :nothing)
    :ok
  end

  defp claim_recovery!(command) do
    workspace_id = command.workspace_context.workspace_id
    resource = command.resource
    now = command.occurred_at

    circuit = lock_circuit!(workspace_id, resource)

    if circuit.state == "closed" do
      claim_closed_recovery!(command, workspace_id, resource, now)
    else
      %ResourceRecoveryBatch{candidates: []}
    end
  end

  defp claim_closed_recovery!(command, workspace_id, resource, now) do
    candidates =
      from(candidate in ResourceRecoveryCandidate,
        where:
          candidate.workspace_id == ^workspace_id and
            candidate.resource_kind == ^Atom.to_string(resource.kind) and
            candidate.resource_name == ^resource.name and candidate.expires_at > ^now and
            (candidate.status == "pending" or
               (candidate.status == "claimed" and candidate.claim_expires_at <= ^now)),
        order_by: [asc: candidate.inserted_at, asc: candidate.candidate_id],
        limit: ^command.limit,
        lock: "FOR UPDATE SKIP LOCKED"
      )
      |> Repo.all()

    candidate_ids = Enum.map(candidates, & &1.candidate_id)

    if candidate_ids != [] do
      from(candidate in ResourceRecoveryCandidate,
        where:
          candidate.workspace_id == ^workspace_id and candidate.candidate_id in ^candidate_ids
      )
      |> Repo.update_all(
        set: [
          status: "claimed",
          claim_owner: command.owner_id,
          claim_expires_at: DateTime.add(now, command.claim_lease_ms, :millisecond),
          updated_at: now
        ]
      )
    end

    %ResourceRecoveryBatch{candidates: Enum.map(candidates, &candidate_result!/1)}
  end

  defp complete_recovery!(command) do
    status = Atom.to_string(command.status)

    updates =
      if command.status == :submitted do
        [
          status: status,
          recovery_run_id: command.recovery_run_id,
          claim_expires_at: command.occurred_at,
          updated_at: command.occurred_at
        ]
      else
        [
          status: status,
          claim_owner: nil,
          claim_expires_at: nil,
          updated_at: command.occurred_at
        ]
      end

    {updated, nil} =
      from(candidate in ResourceRecoveryCandidate,
        where:
          candidate.workspace_id == ^command.workspace_context.workspace_id and
            candidate.candidate_id in ^command.candidate_ids and
            candidate.status == "claimed" and candidate.claim_owner == ^command.owner_id
      )
      |> Repo.update_all(set: updates)

    if updated == length(command.candidate_ids) do
      :ok
    else
      Repo.rollback(
        Error.new(:conflict, "resource recovery claim ownership changed before completion")
      )
    end
  end

  defp candidate_result!(candidate) do
    {:ok, node_key} = PayloadCodec.decode(candidate.node_key)

    %RecoveryCandidateResult{
      candidate_id: candidate.candidate_id,
      source_run_id: candidate.source_run_id,
      node_key: node_key,
      resource: Ref.new!(resource_kind(candidate.resource_kind), candidate.resource_name),
      reason: recovery_reason(candidate.reason)
    }
  end

  defp lock_circuit!(workspace_id, %Ref{} = resource) do
    from(circuit in ResourceCircuit,
      where:
        circuit.workspace_id == ^workspace_id and
          circuit.resource_kind == ^Atom.to_string(resource.kind) and
          circuit.resource_name == ^resource.name,
      lock: "FOR UPDATE"
    )
    |> Repo.one!()
  end

  defp resource_ref(circuit),
    do: Ref.new!(resource_kind(circuit.resource_kind), circuit.resource_name)

  defp circuit_state("open"), do: :open
  defp circuit_state("half_open"), do: :half_open
  defp resource_kind("execution_pool"), do: :execution_pool
  defp resource_kind("connection"), do: :connection
  defp recovery_reason("blocked"), do: :blocked
  defp recovery_reason("safe_failure"), do: :safe_failure

  defp resource_identity(%Ref{} = resource), do: {resource.kind, resource.name}
  defp category(nil), do: nil
  defp category(value) when is_atom(value), do: Atom.to_string(value)
  defp category(value) when is_binary(value), do: value

  defp outcome_id(command, resource) do
    identity =
      {command.run_id, command.asset_step_id, command.attempt, resource.kind, resource.name}

    "resource-outcome:#{short_hash(identity)}"
  end

  defp short_hash(value) do
    value
    |> :erlang.term_to_binary([:deterministic])
    |> then(&:crypto.hash(:sha256, &1))
    |> Base.url_encode64(padding: false)
  end

  defp transaction(fun) do
    case Repo.transaction(fun) do
      {:ok, result} -> {:ok, result}
      {:error, %Error{} = error} -> {:error, error}
      {:error, reason} -> {:error, ErrorMapper.map(reason)}
    end
  rescue
    error -> {:error, ErrorMapper.map(error)}
  end

  defp validate_acquire(command) do
    valid? =
      workspace_context?(command.workspace_context) and valid_id?(command.command_id) and
        valid_id?(command.owner_id) and valid_id?(command.run_id) and
        valid_id?(command.asset_step_id) and valid_duration?(command.probe_lease_ms) and
        match?(%DateTime{}, command.occurred_at) and is_list(command.requests) and
        command.requests != [] and Enum.all?(command.requests, &valid_request?/1)

    if valid?, do: :ok, else: {:error, ErrorMapper.map(:invalid)}
  end

  defp validate_outcomes(command) do
    valid? =
      workspace_context?(command.workspace_context) and valid_id?(command.command_id) and
        valid_id?(command.owner_id) and valid_id?(command.run_id) and
        valid_id?(command.asset_step_id) and is_integer(command.attempt) and
        command.attempt > 0 and is_list(command.permits) and is_list(command.outcomes) and
        is_list(command.recovery_candidates) and
        Enum.all?(command.recovery_candidates, &(validate_candidate(&1) == :ok)) and
        Enum.all?(command.recovery_candidates, fn candidate ->
          candidate.workspace_context.workspace_id == command.workspace_context.workspace_id and
            candidate.source_run_id == command.run_id
        end) and
        match?(%DateTime{}, command.occurred_at)

    if valid?, do: :ok, else: {:error, ErrorMapper.map(:invalid)}
  end

  defp validate_candidate(command) do
    valid? =
      workspace_context?(command.workspace_context) and valid_id?(command.candidate_id) and
        valid_id?(command.source_run_id) and match?(%Ref{}, command.resource) and
        command.reason in [:blocked, :safe_failure] and valid_duration?(command.max_age_ms) and
        match?(%DateTime{}, command.occurred_at)

    if valid?, do: :ok, else: {:error, ErrorMapper.map(:invalid)}
  end

  defp validate_release(command) do
    valid? =
      workspace_context?(command.workspace_context) and valid_id?(command.owner_id) and
        is_list(command.permits) and command.permits != [] and
        Enum.all?(command.permits, &match?(%ResourceCircuitPermit{}, &1)) and
        match?(%DateTime{}, command.occurred_at)

    if valid?, do: :ok, else: {:error, ErrorMapper.map(:invalid)}
  end

  defp validate_claim(command) do
    valid? =
      workspace_context?(command.workspace_context) and valid_id?(command.command_id) and
        valid_id?(command.owner_id) and match?(%Ref{}, command.resource) and
        is_integer(command.limit) and command.limit in 1..500 and
        valid_duration?(command.claim_lease_ms) and match?(%DateTime{}, command.occurred_at)

    if valid?, do: :ok, else: {:error, ErrorMapper.map(:invalid)}
  end

  defp validate_complete(command) do
    valid? =
      workspace_context?(command.workspace_context) and valid_id?(command.owner_id) and
        is_list(command.candidate_ids) and command.candidate_ids != [] and
        Enum.all?(command.candidate_ids, &valid_id?/1) and
        command.status in [:submitted, :pending] and
        (command.status == :pending or valid_id?(command.recovery_run_id)) and
        match?(%DateTime{}, command.occurred_at)

    if valid?, do: :ok, else: {:error, ErrorMapper.map(:invalid)}
  end

  defp validate_list_pending(command) do
    valid? =
      PlatformContext.valid?(command.platform_context) and is_integer(command.limit) and
        command.limit in 1..500 and match?(%DateTime{}, command.occurred_at)

    if valid?, do: :ok, else: {:error, ErrorMapper.map(:invalid)}
  end

  defp valid_request?(%ResourceCircuitRequest{resource: %Ref{}, policy: policy}),
    do: match?(%Favn.CircuitBreaker.Policy{}, policy)

  defp valid_request?(_request), do: false
  defp workspace_context?(context), do: WorkspaceContext.valid?(context)
  defp valid_duration?(value), do: is_integer(value) and value > 0
  defp valid_id?(value), do: is_binary(value) and value != "" and byte_size(value) <= 255
end
