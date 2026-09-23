defmodule FavnStoragePostgres.Rebuilds.Validation do
  @moduledoc false
  import Ecto.Query
  alias FavnOrchestrator.Persistence.Error
  alias FavnOrchestrator.Rebuild.Validation, as: Attempt
  alias FavnOrchestrator.RunnerTaskContext
  alias FavnStoragePostgres.Repo
  alias FavnStoragePostgres.Schemas.RebuildOperation
  alias FavnStoragePostgres.Schemas.RunnerTask
  alias FavnStoragePostgres.RunnerTasks.Store, as: Tasks

  @reads ~w(runtime_input_resolution generation_capabilities generation_marker_read relation_inspection)
  @terminal ~w(succeeded failed cancelled unknown)

  def begin!(operation, %Attempt{} = attempt, plan_hash) do
    now = now!()
    current = Attempt.decode(operation.validation_request)

    cond do
      current && current.status == "active" ->
        reject!(:conflict, "rebuild_validation_busy")

      operation.plan_hash != plan_hash ->
        reject!(:conflict, "rebuild_plan_stale")

      operation.state != %{start: "planned", retry: "failed"}[attempt.purpose] ->
        reject!(:conflict, "rebuild_validation_not_allowed")

      operation.cancel_requested ->
        reject!(:conflict, "rebuild_cancelled")

      attempt.purpose == :retry and operation.action_count == 0 ->
        reject!(:conflict, "rebuild_new_plan_required")

      attempt.purpose == :retry and
          (operation.cleanup_state != "not_started" or operation.unknown_outcome not in [nil, %{}]) ->
        reject!(:conflict, "rebuild_retry_unsafe")

      true ->
        ensure_settled!(operation)

        attempt = %{
          attempt
          | deadline_at: min_deadline(attempt.deadline_at, DateTime.add(now, 300, :second))
        }

        attempt =
          if attempt.receipt,
            do: %{
              attempt
              | deadline_at: min_deadline(attempt.deadline_at, attempt.receipt.expires_at)
            },
            else: attempt

        deadline =
          if attempt.purpose == :start,
            do: min_deadline(attempt.deadline_at, operation.plan_payload["expires_at"]),
            else: attempt.deadline_at

        if DateTime.compare(deadline, now) != :gt, do: reject!(:conflict, "rebuild_plan_expired")

        attempt = %{
          attempt
          | deadline_at: deadline,
            fencing_token: operation.dispatcher_fencing_token + 1
        }

        {:ok, operation |> Ecto.Changeset.change(initial_attrs(attempt, now)) |> Repo.update!()}
    end
  catch
    {:validation_rejected, error} -> {:error, error}
  end

  def initial_attrs(%Attempt{} = attempt, now) do
    %{
      validation_request: Attempt.encode(attempt),
      dispatcher_owner: attempt.owner_id,
      dispatcher_fencing_token: attempt.fencing_token,
      dispatcher_expires_at: DateTime.add(now, 30, :second)
    }
  end

  def guard!(operation, %Attempt{} = attempt) do
    current = Attempt.decode(operation.validation_request)

    unless current == attempt and live?(operation, now!()),
      do: fail!(:fenced, "rebuild_validation_interrupted")

    :ok
  end

  def guard!(_, _), do: fail!(:fenced, "rebuild_validation_required")

  def accepted!(operation, attempt, payload, items) do
    guard!(operation, attempt)
    ensure_evidence!(operation, attempt, payload, items)
    Attempt.encode(%{attempt | status: "accepted"})
  end

  def close!(operation, attempt, reason, expired_only \\ false) do
    current = Attempt.decode(operation.validation_request)

    if current && current.attempt_id == attempt.attempt_id && current.status == "active" &&
         (not expired_only or not live?(operation, now!())) do
      Tasks.cancel_validation_in_transaction(
        operation.workspace_id,
        operation.operation_id,
        Attempt.task_context(current),
        now!()
      )

      error =
        if match?(%Error{}, reason),
          do: reason,
          else: Attempt.failure(operation.operation_id, current.purpose)

      FavnStoragePostgres.Rebuilds.ValidationReceipt.finish!(operation, current, {:error, error})

      attrs = %{
        validation_request:
          Attempt.encode(%{current | status: "failed", failure: Attempt.encode_error(error)}),
        dispatcher_owner: nil,
        dispatcher_expires_at: nil
      }

      attrs =
        if current.purpose == :plan and operation.state != "cancelled",
          do:
            Map.merge(attrs, %{
              state: "failed",
              phase: "terminal",
              completed_at: now!(),
              terminal_error: %{
                "outcome" => "safe_failure",
                "reason" => error.details[:reason_code]
              }
            }),
          else: attrs

      operation |> Ecto.Changeset.change(attrs) |> Repo.update!()
    else
      operation
    end
  end

  def expire!(workspace, limit) when limit in 1..100 do
    now = now!()

    rows =
      Repo.all(
        from(o in RebuildOperation,
          where:
            o.workspace_id == ^workspace and
              fragment("?->>'status' = 'active'", o.validation_request) and
              (o.dispatcher_expires_at <= ^now or
                 fragment("(?->>'deadline_at')::timestamptz <= ?", o.validation_request, ^now)),
          order_by: [asc: o.dispatcher_expires_at],
          limit: ^limit,
          lock: "FOR UPDATE SKIP LOCKED"
        )
      )

    Enum.each(
      rows,
      &close!(&1, Attempt.decode(&1.validation_request), "rebuild_validation_interrupted")
    )

    length(rows)
  end

  def lock_command!(%{workspace_context: context, task_id: task_id} = command) do
    task =
      if Map.has_key?(command, :orchestration_context),
        do: command,
        else: Repo.get_by(RunnerTask, workspace_id: context.workspace_id, task_id: task_id)

    case task && token(task) do
      nil ->
        if task && Map.get(task, :operation_id) && to_string(Map.get(task, :task_kind)) in @reads do
          operation = lock!(context.workspace_id, task.operation_id)

          if admission?(command) and
               (operation.state == "planning" or
                  (operation.validation_request &&
                     operation.validation_request["status"] == "active") or
                  to_string(task.task_kind) == "runtime_input_resolution"),
             do: fail!(:invalid, "rebuild_validation_required")
        end

      token ->
        operation = lock!(context.workspace_id, task.operation_id)

        if admission?(command) do
          guard_token!(operation, token)

          unless Map.get(task, :deadline_at) ==
                   Attempt.decode(operation.validation_request).deadline_at,
                 do: fail!(:fenced, "rebuild_validation_deadline_mismatch")
        end
    end
  end

  def lock_command!(_), do: :ok

  def candidate_live?(task) do
    case token(task) do
      nil ->
        task.task_kind != "runtime_input_resolution"

      token ->
        row =
          Repo.one(
            from(o in RebuildOperation,
              where:
                o.workspace_id == ^task.workspace_id and o.operation_id == ^task.operation_id,
              lock: "FOR UPDATE SKIP LOCKED"
            )
          )

        row && token_matches?(row, token) && live?(row, now!()) &&
          task.deadline_at == Attempt.decode(row.validation_request).deadline_at
    end
  end

  def token(%{orchestration_context: context}) do
    case RunnerTaskContext.decode(context, nil) do
      {:ok, %{kind: :rebuild_validation} = token} -> token
      _ -> nil
    end
  end

  def lock!(workspace, operation_id) do
    operation =
      Repo.one(
        from(o in RebuildOperation,
          where: o.workspace_id == ^workspace and o.operation_id == ^operation_id,
          lock: "FOR UPDATE"
        )
      ) || Repo.rollback(Error.new(:not_found, "operation history not found"))

    if operation.retiring, do: fail!(:expired, "rebuild_expired")
    operation
  end

  defp guard_token!(operation, token) do
    unless token_matches?(operation, token) && live?(operation, now!()),
      do: fail!(:fenced, "rebuild_validation_interrupted")
  end

  defp token_matches?(operation, token) do
    case Attempt.decode(operation.validation_request) do
      nil -> false
      attempt -> Attempt.task_context(attempt) == token
    end
  end

  defp live?(operation, now) do
    v = Attempt.decode(operation.validation_request)

    v && v.status == "active" && not operation.cancel_requested &&
      operation.state == %{plan: "planning", start: "planned", retry: "failed"}[v.purpose] &&
      operation.dispatcher_owner == v.owner_id &&
      operation.dispatcher_fencing_token == v.fencing_token &&
      operation.dispatcher_expires_at &&
      DateTime.compare(operation.dispatcher_expires_at, now) == :gt &&
      DateTime.compare(v.deadline_at, now) == :gt
  end

  defp admission?(%{task_kind: _kind, payload: _payload}), do: true
  defp admission?(%{transition: transition}), do: transition in [:preparing, :running]
  defp admission?(%{outcome: outcome}), do: outcome == :succeeded
  defp admission?(%{expected_result_version: _}), do: true
  defp admission?(_), do: false

  defp ensure_settled!(operation) do
    if Repo.exists?(
         from(t in RunnerTask,
           where:
             t.workspace_id == ^operation.workspace_id and
               t.operation_id == ^operation.operation_id and t.task_kind not in ^@reads and
               (t.status not in ^@terminal or t.status == "unknown")
         )
       ),
       do: reject!(:conflict, "rebuild_retry_unsafe")

    if Repo.exists?(
         from(t in RunnerTask,
           where:
             t.workspace_id == ^operation.workspace_id and
               t.operation_id == ^operation.operation_id and t.task_kind in ^@reads and
               t.status not in ^@terminal
         )
       ),
       do: reject!(:conflict, "rebuild_validation_settling")
  end

  defp ensure_evidence!(operation, attempt, payload, items) do
    payload = payload |> Favn.Manifest.Serializer.encode_canonical!() |> Jason.decode!()

    {:ok, version} =
      FavnStoragePostgres.Registry.Store.get_manifest(
        %FavnOrchestrator.Persistence.Queries.ManifestSelector.ById{
          manifest_version_id: operation.manifest_version_id
        }
      )

    capabilities = Map.keys(Map.fetch!(payload, "capabilities"))

    active =
      payload
      |> Map.fetch!("binding_snapshot")
      |> Enum.filter(fn {_, binding} -> is_binary(binding["active_generation_id"]) end)
      |> Enum.map(&elem(&1, 0))

    evidence =
      Enum.map(
        capabilities,
        &{:generation_capabilities, {:rebuild_capabilities, operation.operation_id, &1}}
      ) ++
        Enum.flat_map(active, fn target ->
          [
            {:generation_marker_read, {:rebuild_marker_read, operation.operation_id, target}},
            {:relation_inspection, {:rebuild_active_inspection, operation.operation_id, target}}
          ]
        end) ++
        (items
         |> Enum.filter(& &1.runtime_input_expectation)
         |> Enum.map(fn item ->
           {:runtime_input_resolution,
            {:rebuild_inputs, operation.operation_id, item.target_id, item.item_id}}
         end))

    ids =
      Enum.map(evidence, fn {kind, identity} ->
        FavnOrchestrator.OperationRunnerTasks.task_id(
          operation.workspace_id,
          kind,
          {identity, attempt.attempt_id},
          version
        )
      end)

    {:ok, context} = RunnerTaskContext.encode(Attempt.task_context(attempt))

    count =
      Repo.aggregate(
        from(t in RunnerTask,
          where:
            t.workspace_id == ^operation.workspace_id and
              t.operation_id == ^operation.operation_id and t.task_id in ^ids and
              t.status == "succeeded" and
              t.orchestration_context == ^context
        ),
        :count
      )

    incomplete =
      Repo.exists?(
        from(t in RunnerTask,
          where:
            t.workspace_id == ^operation.workspace_id and
              t.operation_id == ^operation.operation_id and t.orchestration_context == ^context and
              t.status != "succeeded"
        )
      )

    unless ids != [] and not incomplete and count == length(Enum.uniq(ids)),
      do: fail!(:conflict, "rebuild_validation_evidence_missing")
  end

  defp min_deadline(deadline, %DateTime{} = expires),
    do: if(DateTime.compare(deadline, expires) == :gt, do: expires, else: deadline)

  defp min_deadline(deadline, value) when is_binary(value) do
    case DateTime.from_iso8601(value) do
      {:ok, expires, 0} -> min_deadline(deadline, expires)
      _ -> reject!(:conflict, "rebuild_plan_expired")
    end
  end

  defp min_deadline(_, _), do: reject!(:conflict, "rebuild_plan_expired")

  def now! do
    %{rows: [[now]]} = Ecto.Adapters.SQL.query!(Repo, "SELECT clock_timestamp()", [])
    now
  end

  defp reject!(kind, reason),
    do:
      throw(
        {:validation_rejected,
         Error.new(kind, "Rebuild input checks could not complete; retry manually",
           details: %{reason_code: reason}
         )}
      )

  defp fail!(kind, reason),
    do:
      Repo.rollback(
        Error.new(kind, "Rebuild input checks could not complete; retry manually",
          details: %{reason_code: reason}
        )
      )
end
