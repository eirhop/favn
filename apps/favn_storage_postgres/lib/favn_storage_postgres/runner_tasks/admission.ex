defmodule FavnStoragePostgres.RunnerTasks.Admission do
  @moduledoc false

  alias Favn.Contracts.RunnerWork
  alias FavnOrchestrator.AssetRunnerTasks
  alias FavnOrchestrator.ExecutionAdmission.Identity
  alias FavnOrchestrator.ResourceCircuits
  alias Ecto.Adapters.SQL
  alias FavnOrchestrator.MaterializationClaims
  alias FavnOrchestrator.Persistence.Commands, as: C
  alias FavnOrchestrator.Persistence.Error
  alias FavnOrchestrator.Persistence.Results.Admission, as: CapacityDecision
  alias FavnOrchestrator.Persistence.Results.RunnerTaskAdmission
  alias FavnOrchestrator.RunnerTaskContext
  alias FavnOrchestrator.RunServer.Execution.ActiveTaskSet
  alias FavnOrchestrator.RunServer.Execution.AdmissionIntent
  alias FavnOrchestrator.Storage.RunEventCodec
  alias FavnStoragePostgres.Admission.Store, as: Capacity
  alias FavnStoragePostgres.CancellationOwnership
  alias FavnStoragePostgres.CanonicalJSON
  alias FavnStoragePostgres.ErrorMapper
  alias FavnStoragePostgres.Materialization.Store, as: Materialization
  alias FavnStoragePostgres.Repo
  alias FavnStoragePostgres.ResourceCircuits.Store, as: Circuits
  alias FavnStoragePostgres.RunnerTasks.Store, as: Tasks
  alias FavnStoragePostgres.RunnerTasks.WriteOwnership
  alias FavnStoragePostgres.Runs.Store, as: Runs
  alias FavnStoragePostgres.Schemas.{Run, RunEvent, RunnerTask}
  alias FavnStoragePostgres.TargetOperationLocks.Store, as: TargetLocks

  def matches_work?(command, %RunnerWork{} = work) do
    run = command.transition.run
    intent = command.intent
    node = RunnerWork.node_key(work)

    AdmissionIntent.new(run, work, intent.context, intent.occurred_at) == {:ok, intent} and
      work.stage == intent.stage and work.attempt == intent.attempt and
      work.deadline_at == intent.deadline_at and
      command.enqueue.domain_identity ==
        AssetRunnerTasks.domain_identity(run, work, node, intent.attempt) and
      command.transition.event.data[:asset_ref] == work.asset_ref
  end

  def matches_work?(_, _), do: false

  def admit(%C.AdmitRunnerTask{} = command) do
    with {:ok, encoded_intent} <- AdmissionIntent.encode(command.intent),
         {:ok, event_json} <- RunEventCodec.encode(command.transition.event),
         :ok <- validate(command, encoded_intent) do
      event_hash = event_json |> Jason.decode!() |> CanonicalJSON.hash() |> elem(1)

      case FavnStoragePostgres.RunTransaction.transaction(
             fn ->
               if command.acquisition_observer do
                 {observer, reference} = command.acquisition_observer
                 send(observer, {:acquisition_checked_out, reference})
               end

               admit!(command, encoded_intent, event_hash)
             end,
             timeout: 20_000,
             queue: false
           ) do
        {:error, {:not_admitted, result}} -> {:ok, result}
        result -> result
      end
    else
      {:error, %Error{}} = error -> error
      {:error, reason} -> {:error, invalid(reason)}
    end
  rescue
    error ->
      {:error, ErrorMapper.map(error)}
  end

  defp admit!(command, encoded_intent, event_hash) do
    task = command.enqueue
    workspace = task.workspace_context.workspace_id
    CancellationOwnership.lock!(workspace, task.run_id)
    Runs.validate_fence!(command.transition)

    # Existing accepted work is evidence even after the deadline or later events.
    # Do not call enqueue (whose receipt expires), acquire, or overwrite the run.
    case Repo.get_by(RunnerTask, workspace_id: workspace, task_id: task.task_id) do
      nil ->
        if command.reconcile_only?,
          do:
            Repo.rollback(
              Error.new(:conflict, "run admission paused before submission", retryable?: true)
            )

        %{rows: rows} =
          SQL.query!(
            Repo,
            """
            SELECT 1 FROM favn_control.run_ownerships
            WHERE workspace_id=$1 AND run_id=$2 AND claim_purpose='execution'
              AND recovery_disposition='automatic'
            """,
            [workspace, task.run_id]
          )

        if rows == [], do: Repo.rollback(Error.new(:fenced, "run cannot admit execution"))
        acquire!(command, encoded_intent)

      saved ->
        replay!(command, saved, event_hash)
    end
  end

  defp acquire!(command, encoded_intent) do
    task = command.enqueue
    workspace = task.workspace_context.workspace_id
    CancellationOwnership.guard!(workspace, task.run_id)
    row = Repo.get_by(Run, workspace_id: workspace, run_id: task.run_id)

    unless row && row.status == "running" && !row.retiring &&
             row.event_sequence == command.transition.expected_sequence &&
             get_in(row.snapshot, ["metadata", AdmissionIntent.metadata_key()]) == encoded_intent do
      Repo.rollback(invalid(:admission_intent_mismatch))
    end

    validate_checkpoint!(command)
    WriteOwnership.lock_target!(workspace, task.write_target_id)
    Circuits.lock_admission_policy!(workspace)
    now = live_authority!(command)
    capacity = capacity(command.capacity, now)

    if capacity && capacity.status == :waiting do
      live_authority!(command)
      %RunnerTaskAdmission{status: :waiting, capacity: capacity}
    else
      permits = permits(command.circuits, now)
      lock = target_lock(command.target_lock, now)
      claim = claim(command, lock, now)
      context = Map.put(command.intent.context, :materialization_claim, claim)

      context =
        if context.kind == :pipeline,
          do: Map.put(context, :resource_circuit_permits, permits),
          else: context

      encoded_context = unwrap(RunnerTaskContext.encode(context))

      sealed = %{
        task
        | orchestration_context: encoded_context,
          write_claim_key: claim && claim.claim_key,
          write_claim_fence: claim && claim.fencing_token
      }

      live_authority!(command)
      saved = unwrap(Tasks.enqueue_admitted(sealed, command))
      live_authority!(command)
      transition = unwrap(Runs.commit_transition(command.transition))
      live_authority!(command)

      %RunnerTaskAdmission{
        status: :admitted,
        task: saved,
        transition: transition,
        capacity: capacity,
        context: context
      }
    end
  end

  defp replay!(command, saved, event_hash) do
    task = command.enqueue

    event =
      Repo.get_by(RunEvent,
        workspace_id: saved.workspace_id,
        run_id: saved.run_id,
        sequence: command.transition.event.sequence
      )

    fields = [
      :task_id,
      :domain_identity,
      :run_id,
      :asset_step_id,
      :manifest_version_id,
      :manifest_content_hash,
      :payload_hash,
      :deadline_at,
      :runner_pool,
      :required_runner_release_id,
      :write_target_id
    ]

    unless event && event.event_hash == event_hash &&
             saved.task_kind == "asset_attempt" &&
             Enum.all?(fields, &(Map.get(saved, &1) == Map.get(task, &1))) do
      Repo.rollback(invalid(:admission_replay_mismatch))
    end

    restored = Tasks.admission_task(saved)

    case restored do
      %{data_state: :available, orchestration_context: context, payload: work} ->
        unless matches_work?(command, work), do: Repo.rollback(invalid(:admission_work_mismatch))
        bare = context |> Map.put(:materialization_claim, nil)

        bare =
          if bare.kind == :pipeline, do: Map.put(bare, :resource_circuit_permits, []), else: bare

        unless bare == command.intent.context,
          do: Repo.rollback(invalid(:admission_context_mismatch))

        lease =
          if command.capacity,
            do: Capacity.saved_lease(command.capacity)

        %RunnerTaskAdmission{
          status: :admitted,
          task: restored,
          context: context,
          capacity: if(lease, do: %CapacityDecision{status: :admitted, lease: lease}),
          replayed?: true
        }

      {:error, reason} ->
        Repo.rollback(reason)

      _ ->
        Repo.rollback(invalid(:admission_task_unreadable))
    end
  end

  defp validate_checkpoint!(%{intent: %{context: %{kind: :sequential}}}), do: :ok

  defp validate_checkpoint!(command) do
    ref = command.intent.context.freshness_checkpoint

    checkpoint =
      Repo.get_by(FavnStoragePostgres.Schemas.RunExecutionCheckpoint,
        workspace_id: command.enqueue.workspace_context.workspace_id,
        run_id: command.enqueue.run_id
      )

    unless checkpoint && checkpoint.checkpoint_version == ref.version &&
             checkpoint.checkpoint_revision == ref.revision &&
             checkpoint.checkpoint_sequence == ref.sequence &&
             checkpoint.stage == ref.stage && checkpoint.attempt == ref.attempt &&
             checkpoint.payload_hash == ref.payload_hash,
           do: Repo.rollback(invalid(:admission_checkpoint_mismatch))
  end

  defp live_authority!(command) do
    Runs.validate_fence!(command.transition)
    %{rows: [[now]]} = SQL.query!(Repo, "SELECT clock_timestamp()", [])

    unless DateTime.compare(command.intent.deadline_at, now) == :gt,
      do:
        Repo.rollback(
          Error.new(:conflict, "original runner task deadline has expired",
            details: %{reason_code: "admission_deadline_expired", task_id: command.intent.task_id}
          )
        )

    now
  end

  defp capacity(nil, _now), do: nil

  defp capacity(command, now),
    do: unwrap(Capacity.admit_saved_intent(%{command | occurred_at: now}))

  defp permits(nil, _now), do: []

  defp permits(command, now) do
    case unwrap(Circuits.acquire(%{command | occurred_at: now})) do
      %{status: :allowed, permits: permits} -> permits
      %{status: :blocked, blockers: blockers} -> not_admitted(:blocked, blockers)
    end
  end

  defp target_lock(nil, _now), do: nil

  defp target_lock(command, now) do
    case unwrap(TargetLocks.acquire_many(%{command | occurred_at: now})) do
      [lock] -> lock
      _ -> Repo.rollback(invalid(:invalid_admission_target_lock))
    end
  end

  defp claim(%{claim: nil}, nil, _now), do: nil

  defp claim(command, lock, now) do
    decision = unwrap(Materialization.claim(%{command.claim | occurred_at: now}))

    if command.intent.context.kind == :sequential do
      case MaterializationClaims.resolve_sequential(decision) do
        {:ok, claim} -> claim
        {:error, reason} -> not_admitted(:already_claimed, reason)
      end
    else
      requested = Map.put(command.claim_context, :target_operation_lock, lock)

      case MaterializationClaims.classify_claim(decision, requested) do
        {:ok, claim} ->
          claim

        {status, claim} when status in [:already_claimed, :already_succeeded] ->
          not_admitted(status, Map.drop(claim, [:target_operation_lock]))

        {:error, reason} ->
          Repo.rollback(invalid(reason))
      end
    end
  end

  defp not_admitted(status, reason),
    do: Repo.rollback({:not_admitted, %RunnerTaskAdmission{status: status, reason: reason}})

  defp unwrap({:ok, value}), do: value
  defp unwrap({:error, %Error{} = error}), do: Repo.rollback(error)
  defp unwrap({:error, reason}), do: Repo.rollback(invalid(reason))

  defp validate(command, encoded) do
    task = command.enqueue
    transition = command.transition
    run = transition.run
    workspace = task.workspace_context.workspace_id
    intent = command.intent
    {:ok, fingerprint} = AdmissionIntent.fingerprint(intent)
    data = transition.event.data

    valid? =
      task.task_kind == :asset_attempt && is_nil(task.operation_id) &&
        task.task_id == intent.task_id && task.asset_step_id == intent.asset_step_id &&
        task.deadline_at == intent.deadline_at && task.run_id == run.id &&
        workspace == run.workspace_id && workspace == transition.workspace_context.workspace_id &&
        task.manifest_version_id == run.manifest_version_id &&
        task.manifest_content_hash == run.manifest_content_hash &&
        transition.owner_id == run.storage_owner_id &&
        transition.fencing_token == run.storage_fencing_token &&
        is_binary(transition.owner_id) && is_integer(transition.fencing_token) &&
        transition.fencing_token > 0 &&
        transition.event.run_id == run.id &&
        transition.event.sequence == transition.expected_sequence + 1 &&
        transition.event.event_type in [:step_started, :step_retry_started] &&
        data[:runner_task_id] == intent.task_id && data[:asset_step_id] == intent.asset_step_id &&
        data[:stage] == intent.stage && data[:attempt] == intent.attempt &&
        task.orchestration_context == encoded["context"] &&
        data[:admission_intent_hash] == fingerprint &&
        task.task_id in ActiveTaskSet.active_runner_task_ids(run) &&
        !Map.has_key?(run.metadata, AdmissionIntent.metadata_key()) &&
        Enum.all?([command.capacity, command.circuits, command.claim, command.target_lock], fn
          nil -> true
          nested -> nested.workspace_context.workspace_id == workspace
        end) && valid_capacity?(command) && valid_circuits?(command) && valid_claim?(command)

    if valid?, do: :ok, else: {:error, invalid(:invalid_runner_task_admission)}
  end

  defp valid_capacity?(%{capacity: nil}), do: true

  defp valid_capacity?(%{capacity: c, enqueue: task, transition: transition, intent: intent}),
    do:
      c.run_id == task.run_id && c.step_id == task.asset_step_id &&
        c.owner_id == transition.owner_id && c.owner_generation == transition.fencing_token &&
        c.lease_id ==
          Identity.lease_id(task.run_id, task.asset_step_id, intent.stage, intent.attempt) &&
        c.waiter_id ==
          Identity.waiter_id(task.run_id, task.asset_step_id, intent.stage, intent.attempt)

  defp valid_circuits?(%{circuits: nil}), do: true

  defp valid_circuits?(%{circuits: c, enqueue: task, intent: intent}),
    do:
      intent.context.kind == :pipeline && c.run_id == task.run_id &&
        c.asset_step_id == task.asset_step_id &&
        c.owner_id ==
          ResourceCircuits.permit_owner_id(%{id: task.run_id}, %{
            asset_step_id: task.asset_step_id
          })

  defp valid_claim?(%{claim: nil, target_lock: nil, claim_context: nil}), do: true

  defp valid_claim?(%{claim: c, enqueue: task, transition: transition} = command)
       when not is_nil(c) do
    valid_lock? =
      case command.target_lock do
        nil ->
          true

        lock ->
          lock.target_ids == [c.target_id] && lock.operation_id == c.operation_id &&
            lock.lease_owner == transition.owner_id && lock.operation_type == :materialization
      end

    c.run_id == task.run_id && c.owner_id == transition.owner_id &&
      c.target_id == task.write_target_id && c.deployment_id == transition.run.deployment_id &&
      valid_lock? && valid_claim_context?(command)
  end

  defp valid_claim?(_), do: false

  defp valid_claim_context?(%{intent: %{context: %{kind: :sequential}}, claim_context: nil}),
    do: true

  defp valid_claim_context?(%{claim_context: prepared, claim: c, enqueue: task, intent: intent})
       when is_map(prepared) do
    prepared[:claim_key] == c.claim_key && prepared[:run_id] == c.run_id &&
      prepared[:workspace_id] == c.workspace_context.workspace_id &&
      prepared[:asset_step_id] == task.asset_step_id &&
      prepared[:target_generation_id] == c.target_generation_id &&
      prepared[:evidence_generation_id] == c.evidence_generation_id &&
      prepared[:freshness_key] == intent.context[:freshness_key] &&
      prepared[:node_key] == intent.context.decision.node_key &&
      is_nil(prepared[:target_operation_lock])
  end

  defp valid_claim_context?(_), do: false

  defp invalid(reason),
    do:
      Error.new(:invalid, "invalid runner task admission",
        details: %{
          reason_code:
            if(is_atom(reason), do: Atom.to_string(reason), else: "admission_validation_failed"),
          reason: inspect(reason, limit: 10)
        }
      )
end
