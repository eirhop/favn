defmodule FavnStoragePostgres.RunnerTasks.WriteOwnership do
  @moduledoc false

  import Ecto.Query
  alias Ecto.Adapters.SQL
  alias FavnOrchestrator.Persistence.Error
  alias FavnStoragePostgres.Repo
  alias FavnStoragePostgres.Schemas.MaterializationClaim
  alias FavnStoragePostgres.Schemas.RunnerTask
  alias FavnStoragePostgres.Schemas.TargetOperationLock

  @unresolved ["in_flight", "outcome_unknown"]

  # All effect mutations acquire target advisories before owner/task row locks.
  def lock_target!(_workspace, nil), do: :ok

  def lock_target!(workspace, target) do
    SQL.query!(Repo, "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))", [
      "favn:target-operation:" <> workspace <> ":" <> target
    ])

    :ok
  end

  def lock_task_target!(workspace, task_id) do
    case Repo.get_by(RunnerTask, workspace_id: workspace, task_id: task_id) do
      nil -> :ok
      task -> lock_target!(workspace, task.write_target_id)
    end
  end

  # Claim selection already holds SKIP LOCKED task rows. Never wait for a target
  # advisory in that order; skip a busy target until the next bounded claim.
  def try_lock_target!(_workspace, nil), do: true

  def try_lock_target!(workspace, target) do
    %{rows: [[acquired?]]} =
      SQL.query!(
        Repo,
        "SELECT pg_try_advisory_xact_lock(hashtextextended($1, 0))",
        ["favn:target-operation:" <> workspace <> ":" <> target]
      )

    acquired?
  end

  def unresolved?(%{effect_state: state}), do: state in @unresolved

  def target_unresolved?(workspace, target, except_task \\ nil) do
    target_effect?(workspace, target, except_task, @unresolved)
  end

  defp target_effect?(workspace, target, except_task, states) do
    except_task = except_task || ""

    Repo.exists?(
      from(c in MaterializationClaim,
        where:
          c.workspace_id == ^workspace and c.target_id == ^target and
            c.effect_state in ^states and
            (is_nil(c.effect_task_id) or c.effect_task_id != ^except_task)
      )
    ) or
      Repo.exists?(
        from(l in TargetOperationLock,
          where:
            l.workspace_id == ^workspace and l.target_id == ^target and
              l.effect_state in ^states and
              (is_nil(l.effect_task_id) or l.effect_task_id != ^except_task)
        )
      )
  end

  def guard_target!(workspace, target, except_task \\ nil) do
    cond do
      target_effect?(workspace, target, except_task, ["outcome_unknown"]) ->
        conflict!()

      target_effect?(workspace, target, except_task, ["in_flight"]) ->
        Repo.rollback(
          Error.new(:conflict, "target write is in progress",
            retryable?: true,
            details: %{reason_code: "target_write_in_progress"}
          )
        )

      true ->
        :ok
    end
  end

  def bind!(%{write_target_id: nil} = task), do: task

  def bind!(task) do
    lock_target!(task.workspace_id, task.write_target_id)

    owner =
      case task do
        %{write_claim_key: key} when is_binary(key) ->
          claim = owner!(task)

          unless claim.fencing_token == task.write_claim_fence and claim.status == "claimed" and
                   claim.run_id == task.run_id and claim.target_id == task.write_target_id and
                   live?(claim.expires_at),
                 do: fenced!()

          claim

        _operation ->
          operation_owner!(task)
      end

    unless is_nil(owner.effect_task_id) or owner.effect_task_id == task.task_id or
             owner.effect_state == "resolved",
           do: conflict!()

    owner = owner |> Ecto.Changeset.change(effect_task_id: task.task_id) |> Repo.update!()

    if is_struct(owner, TargetOperationLock),
      do: %{task | write_lock_fence: owner.fencing_token},
      else: task
  end

  def start!(%{write_target_id: nil}), do: :ok

  def start!(task) do
    guard_target!(task.workspace_id, task.write_target_id, task.task_id)
    owner = owner!(task)

    {fence, expires_at} =
      case owner do
        %MaterializationClaim{} ->
          if owner.status != "claimed", do: fenced!()
          {task.write_claim_fence, owner.expires_at}

        %TargetOperationLock{} ->
          {task.write_lock_fence, owner.lease_expires_at}
      end

    unless owner.effect_task_id == task.task_id and owner.fencing_token == fence and
             live?(expires_at),
           do: fenced!()

    owner
    |> Ecto.Changeset.change(
      effect_state: "in_flight",
      effect_assignment_generation: task.assignment_generation,
      effect_started_at: now!(),
      effect_resolution: nil
    )
    |> Repo.update!()

    :ok
  end

  def complete!(%{write_target_id: nil}, _command), do: :ok

  def complete!(%{status: status} = task, command) when status in ["assigned", "preparing"] do
    # Started is the durable write barrier. Preparation failure can settle even
    # when its unstarted owner has expired or been replaced.
    if command.outcome == :succeeded, do: fenced!()
    finish_unstarted!(task)
  end

  def complete!(%{status: "cancelling"} = task, command) do
    case find_owner(task) do
      %{effect_task_id: id, effect_state: state} = owner
      when id == task.task_id and state in @unresolved ->
        complete_started!(task, command, owner)

      _unstarted_or_superseded ->
        if command.outcome == :succeeded, do: fenced!()
        finish_unstarted!(task)
    end
  end

  def complete!(task, command), do: complete_started!(task, command, owner!(task))

  defp complete_started!(task, command, owner) do
    unless owner.effect_task_id == task.task_id and unresolved?(owner) and
             owner.effect_assignment_generation == task.assignment_generation and
             owner.fencing_token == (task.write_claim_fence || task.write_lock_fence),
           do: fenced!()

    resolved? =
      command.outcome == :succeeded or
        (command.outcome == :failed and match?(%{outcome: :safe_failure}, command.error))

    state = if resolved?, do: "resolved", else: "outcome_unknown"

    evidence =
      if resolved?,
        do: %{
          "task_id" => task.task_id,
          "assignment_generation" => task.assignment_generation,
          "result_version" => command.result_version,
          "outcome" => Atom.to_string(command.outcome)
        }

    owner =
      owner
      |> Ecto.Changeset.change(effect_state: state, effect_resolution: evidence)
      |> Repo.update!()

    if resolved?, do: release_owner!(task, owner)
    :ok
  end

  def preserve!(%{write_target_id: nil}), do: :ok

  def preserve!(task) do
    case find_owner(task) do
      %{effect_task_id: id} = owner when id == task.task_id ->
        if unresolved?(owner),
          do: owner |> Ecto.Changeset.change(effect_state: "outcome_unknown") |> Repo.update!()

      _missing_or_superseded ->
        :ok
    end

    :ok
  end

  def guard_requeue!(%{write_target_id: nil}), do: :ok

  def guard_requeue!(task) do
    unless requeue_owner?(task), do: conflict!()
    :ok
  end

  def requeue_owner?(%{write_target_id: nil}), do: true

  def requeue_owner?(task) do
    case find_owner(task) do
      %{effect_task_id: id, fencing_token: fence} = owner ->
        expires_at = Map.get(owner, :expires_at) || Map.get(owner, :lease_expires_at)

        id == task.task_id and fence == (task.write_claim_fence || task.write_lock_fence) and
          not unresolved?(owner) and Map.get(owner, :status, "claimed") == "claimed" and
          live?(expires_at)

      nil ->
        false
    end
  end

  def prepare_retry!(
        %{
          task_kind: "generation_marker_initialize",
          status: "failed",
          retry_class: "safe_to_retry"
        } = task
      ) do
    # Only an explicitly safe terminal task can restore a deleted task-owned
    # lock. The task generation still fences every old assignment.
    rebound = bind!(task)
    unless rebound.write_lock_fence == task.write_lock_fence, do: fenced!()
    guard_requeue!(task)
  end

  def prepare_retry!(task), do: guard_requeue!(task)

  def resolve!(task, command, evidence) do
    owner = owner!(task)
    fence = task.write_claim_fence || task.write_lock_fence

    unless task.status in ["unknown", "failed", "cancelled"] and unresolved?(owner) and
             owner.effect_task_id == task.task_id and owner.fencing_token == fence and
             fence == command.expected_owner_fence and
             owner.effect_assignment_generation == command.expected_assignment_generation and
             DateTime.compare(command.stopped_at, owner.effect_started_at) != :lt,
           do: fenced!()

    resolution = %{
      "task_id" => task.task_id,
      "target_id" => task.write_target_id,
      "assignment_generation" => command.expected_assignment_generation,
      "owner_fence" => fence,
      "claim_key" => task.write_claim_key,
      "actor_id" => command.workspace_context.principal_id,
      "stopped_at" => DateTime.to_iso8601(command.stopped_at),
      "stop_mechanism" => Atom.to_string(command.stop_mechanism),
      "runner_stopped" => true,
      "backend_stopped" => true,
      "evidence_reference" => command.evidence_reference,
      "reason" => command.reason,
      "disposition" => Atom.to_string(command.disposition),
      "evidence" => evidence,
      "observation_task_ids" => command.observation_task_ids,
      "resolved_at" => DateTime.to_iso8601(now!())
    }

    owner =
      owner
      |> Ecto.Changeset.change(effect_state: "resolved", effect_resolution: resolution)
      |> Repo.update!()

    release_owner!(task, owner)
    resolution
  end

  def finish_unstarted!(%{write_target_id: nil}), do: :ok

  def finish_unstarted!(task) do
    expected_fence = task.write_claim_fence || task.write_lock_fence

    case find_owner(task) do
      %{effect_task_id: id, fencing_token: fence} = owner
      when id == task.task_id and fence == expected_fence ->
        unless unresolved?(owner), do: release_owner!(task, owner)

      _missing_or_superseded ->
        :ok
    end

    :ok
  end

  defp release_owner!(task, %MaterializationClaim{purpose: "ownership_only"} = claim) do
    unless claim.effect_task_id == task.task_id and claim.fencing_token == task.write_claim_fence and
             claim.effect_state in ["not_started", "resolved"],
           do: fenced!()

    claim
    |> Ecto.Changeset.change(
      status: "released",
      completed_at: now!(),
      updated_at: now!(),
      version: claim.version + 1
    )
    |> Repo.update!()

    :ok
  end

  defp release_owner!(task, owner), do: release_task_lock!(task, owner)

  # An expired lease cannot invalidate a success already committed with the exact
  # original owner. This permits settlement, never renewed execution authority.
  def committed_settlement?(claim, status) do
    case claim do
      %{
        effect_state: "resolved",
        effect_resolution: %{
          "task_id" => id,
          "assignment_generation" => generation,
          "result_version" => version,
          "outcome" => outcome
        }
      } ->
        Repo.exists?(
          from(t in RunnerTask,
            where:
              t.workspace_id == ^claim.workspace_id and
                t.task_id == ^id and t.task_id == ^claim.effect_task_id and
                t.run_id == ^claim.run_id and
                t.write_claim_key == ^claim.claim_key and
                t.write_claim_fence == ^claim.fencing_token and
                t.assignment_generation == ^generation and
                t.assignment_generation == ^claim.effect_assignment_generation and
                t.result_version == ^version and t.status == ^outcome and
                t.status in ["succeeded", "failed"]
          )
        ) and
          (status == :failed or outcome == "succeeded")

      _unproved ->
        false
    end
  end

  defp release_task_lock!(task, %TargetOperationLock{} = lock) do
    if task.task_kind == "generation_marker_initialize" and lock.lease_owner == task.task_id and
         lock.effect_task_id == task.task_id and lock.fencing_token == task.write_lock_fence and
         lock.effect_state in ["not_started", "resolved"] do
      guard_target!(task.workspace_id, task.write_target_id)
      Repo.delete!(lock)
    end

    :ok
  end

  defp release_task_lock!(_task, _owner), do: :ok

  defp owner!(task), do: find_owner(task) || fenced!()

  defp find_owner(%{write_claim_key: key} = task) when is_binary(key) do
    Repo.one(
      from(c in MaterializationClaim,
        where: c.workspace_id == ^task.workspace_id and c.claim_key == ^key,
        lock: "FOR UPDATE"
      )
    )
  end

  defp find_owner(task) do
    Repo.one(
      from(l in TargetOperationLock,
        where: l.workspace_id == ^task.workspace_id and l.target_id == ^task.write_target_id,
        lock: "FOR UPDATE"
      )
    )
  end

  defp operation_owner!(task) do
    existing =
      Repo.one(
        from(l in TargetOperationLock,
          where: l.workspace_id == ^task.workspace_id and l.target_id == ^task.write_target_id,
          lock: "FOR UPDATE"
        )
      )

    case existing do
      nil when task.task_kind == "generation_marker_initialize" ->
        guard_target!(task.workspace_id, task.write_target_id)
        now = now!()

        Repo.insert!(%TargetOperationLock{
          workspace_id: task.workspace_id,
          target_id: task.write_target_id,
          operation_id: task.write_operation_id,
          operation_type: "target_recovery",
          fencing_token: 1,
          lease_owner: task.task_id,
          lease_expires_at: DateTime.add(now, 3_600, :second),
          version: 1,
          inserted_at: now,
          updated_at: now
        })

      %TargetOperationLock{} = lock ->
        unless lock.operation_id == task.write_operation_id and live?(lock.lease_expires_at),
          do: fenced!()

        lock

      _missing ->
        fenced!()
    end
  end

  defp live?(datetime),
    do: match?(%DateTime{}, datetime) and DateTime.compare(datetime, now!()) == :gt

  defp now! do
    %{rows: [[now]]} = SQL.query!(Repo, "SELECT clock_timestamp()", [])
    now
  end

  defp fenced!, do: Repo.rollback(Error.new(:fenced, "runner task write owner is stale"))

  defp conflict!,
    do:
      Repo.rollback(
        Error.new(:conflict, "target write outcome remains unresolved",
          details: %{reason_code: "target_write_outcome_unknown"}
        )
      )
end
