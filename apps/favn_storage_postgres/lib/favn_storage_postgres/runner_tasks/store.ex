defmodule FavnStoragePostgres.RunnerTasks.Store do
  @moduledoc false
  @behaviour FavnOrchestrator.Persistence.RunnerTaskStore

  import Ecto.Query

  alias Ecto.Adapters.SQL
  alias FavnOrchestrator.Persistence.Commands, as: C
  alias FavnOrchestrator.Persistence.Commands.PinRuntimeInputs
  alias FavnOrchestrator.Persistence.Error
  alias FavnOrchestrator.Persistence.Queries, as: Q
  alias FavnOrchestrator.Persistence.Results.RunnerCapacityDemand
  alias FavnOrchestrator.Persistence.Results.RunnerTask, as: RunnerTaskResult
  alias Favn.Contracts.RunnerError
  alias FavnStoragePostgres.CanonicalJSON
  alias FavnStoragePostgres.ErrorMapper
  alias FavnStoragePostgres.Repo
  alias FavnStoragePostgres.RunnerTasks.Codec
  alias FavnStoragePostgres.Runs.Store, as: RunsStore
  alias FavnStoragePostgres.Schemas.RunnerCapacityDemand, as: Demand
  alias FavnStoragePostgres.Schemas.RunnerTask
  alias FavnStoragePostgres.Schemas.RunnerTaskCommand
  alias FavnStoragePostgres.Schemas.RunnerTaskLogBatch

  @active_statuses ~w(assigned preparing running cancelling)
  @terminal_statuses ~w(succeeded failed cancelled unknown)
  @task_kind_by_string Map.new(Favn.Contracts.RunnerTask.task_kinds(), &{Atom.to_string(&1), &1})
  @retry_class_by_string Map.new(
                           Favn.Contracts.RunnerTask.retry_classes(),
                           &{Atom.to_string(&1), &1}
                         )
  @status_by_string %{
    "queued" => :queued,
    "assigned" => :assigned,
    "preparing" => :preparing,
    "running" => :running,
    "cancelling" => :cancelling,
    "succeeded" => :succeeded,
    "failed" => :failed,
    "cancelled" => :cancelled,
    "unknown" => :unknown
  }

  @impl true
  def enqueue(%C.EnqueueRunnerTask{} = command) do
    idempotent_transact(command, "enqueue", fn ->
      validate_enqueue!(command)
      workspace_id = command.workspace_context.workspace_id

      row = %{
        workspace_id: workspace_id,
        task_id: command.task_id,
        domain_identity: command.domain_identity,
        task_kind: Atom.to_string(command.task_kind),
        run_id: command.run_id,
        operation_id: command.operation_id,
        asset_step_id: command.asset_step_id,
        runner_pool: command.runner_pool,
        required_runner_release_id: command.required_runner_release_id,
        required_capability: command.required_capability,
        retry_class: Atom.to_string(command.retry_class),
        status: "queued",
        enqueued_at: command.occurred_at,
        deadline_at: command.deadline_at,
        payload_version: 13,
        payload: command.payload,
        payload_hash: command.payload_hash,
        orchestration_context: command.orchestration_context,
        assignment_generation: 0,
        last_command_id: command.command_id,
        inserted_at: command.occurred_at,
        updated_at: command.occurred_at
      }

      case Repo.insert_all(RunnerTask, [row], on_conflict: :nothing) do
        {1, _} ->
          add_queued_demand!(
            command.runner_pool,
            command.required_runner_release_id,
            command.occurred_at,
            command.occurred_at
          )

          fetch_task!(workspace_id, command.task_id)

        {0, _} ->
          existing =
            Repo.one!(
              from(task in RunnerTask,
                where:
                  task.workspace_id == ^workspace_id and
                    (task.task_id == ^command.task_id or
                       task.domain_identity == ^command.domain_identity),
                limit: 1
              )
            )

          if same_enqueued_task?(existing, command) do
            existing
          else
            Repo.rollback(
              Error.new(:conflict, "runner task identity was reused with different work")
            )
          end
      end
    end)
  end

  @impl true
  def claim(%C.ClaimRunnerTask{} = command) do
    idempotent_transact(command, "claim", fn ->
      validate_claim!(command)
      task_kinds = Enum.map(command.supported_task_kinds, &Atom.to_string/1)
      lock_runner_claim_key!(command.runner_instance_id, command.runner_session_generation)

      task =
        active_runner_task(command) ||
          Repo.one(
            from(task in RunnerTask,
              where:
                task.status == "queued" and task.runner_pool == ^command.runner_pool and
                  task.required_runner_release_id == ^command.required_runner_release_id and
                  task.task_kind in ^task_kinds and
                  (is_nil(task.required_capability) or
                     task.required_capability in ^command.capabilities),
              order_by: [asc: task.enqueued_at, asc: task.workspace_id, asc: task.task_id],
              limit: 1,
              lock: "FOR UPDATE SKIP LOCKED"
            )
          )

      case task do
        nil ->
          nil

        %RunnerTask{status: status} = active when status in @active_statuses ->
          if active_runner_matches_claim?(active, command) do
            active
          else
            Repo.rollback(Error.new(:conflict, "runner already owns an incompatible active task"))
          end

        %RunnerTask{status: "queued"} ->
          expires_at = DateTime.add(command.occurred_at, command.lease_duration_ms, :millisecond)

          {1, _} =
            Repo.update_all(
              from(row in RunnerTask,
                where: row.workspace_id == ^task.workspace_id and row.task_id == ^task.task_id
              ),
              set: [
                status: "assigned",
                assigned_runner_instance_id: command.runner_instance_id,
                assigned_runner_session_generation: command.runner_session_generation,
                assignment_generation: task.assignment_generation + 1,
                assignment_expires_at: expires_at,
                last_command_id: command.command_id,
                updated_at: command.occurred_at
              ]
            )

          move_queued_to_active!(
            task.runner_pool,
            task.required_runner_release_id,
            command.occurred_at
          )

          fetch_task!(task.workspace_id, task.task_id)
      end
    end)
  end

  @impl true
  def transition(%C.TransitionRunnerTask{} = command) do
    idempotent_transact(command, "transition", fn ->
      task = fenced_task!(command)
      validate_transition!(command)
      {status, expires_at} = transition_values!(task, command)

      {1, _} =
        Repo.update_all(task_query(task),
          set: [
            status: status,
            assignment_expires_at: expires_at,
            last_command_id: command.command_id,
            updated_at: command.occurred_at
          ]
        )

      fetch_task!(task.workspace_id, task.task_id)
    end)
  end

  @impl true
  def persist_runtime_inputs(%C.PersistRunnerTaskRuntimeInputs{} = command) do
    idempotent_transact(command, "runtime_inputs", fn ->
      task = fenced_task!(command)

      unless task.status in ~w(assigned preparing) do
        Repo.rollback(Error.new(:conflict, "runtime inputs are not accepted in this task state"))
      end

      unless bounded_id(command.resolution_id) == :ok do
        Repo.rollback(Error.new(:invalid, "invalid runtime input resolution identity"))
      end

      {attrs, persisted_status, persisted_fingerprint, persisted_resolution_error} =
        case command.status do
          :resolved ->
            pin_fingerprint =
              case command.runtime_input_pin do
                %Favn.RuntimeInput.Pin{payload_fingerprint: encoded} ->
                  case Base.decode16(encoded, case: :mixed) do
                    {:ok, decoded} -> decoded
                    :error -> nil
                  end

                _other ->
                  nil
              end

            unless pin_fingerprint == command.payload_fingerprint and
                     is_binary(command.payload_fingerprint) and
                     byte_size(command.payload_fingerprint) == 32 do
              Repo.rollback(Error.new(:invalid, "runtime input fingerprint must be sha256"))
            end

            pin_runtime_inputs!(command, command.runtime_input_pin)

            {
              [
                runtime_input_resolution_status: "resolved",
                runtime_input_payload_fingerprint: command.payload_fingerprint,
                runtime_input_error: nil
              ],
              "resolved",
              command.payload_fingerprint,
              nil
            }

          :failed ->
            if is_nil(command.error) or not is_nil(command.runtime_input_pin) do
              Repo.rollback(Error.new(:invalid, "runtime input failure requires an error"))
            end

            resolution_error = persisted_error(command.error)

            {
              [
                runtime_input_resolution_status: "failed",
                runtime_input_payload_fingerprint: nil,
                runtime_input_error: resolution_error
              ],
              "failed",
              nil,
              resolution_error
            }

          _other ->
            Repo.rollback(Error.new(:invalid, "invalid runtime input resolution status"))
        end

      if task.runtime_input_resolution_status do
        if task.runtime_input_resolution_id == command.resolution_id and
             task.runtime_input_resolution_status == persisted_status and
             task.runtime_input_payload_fingerprint == persisted_fingerprint and
             task.runtime_input_error == persisted_resolution_error do
          task
        else
          Repo.rollback(Error.new(:conflict, "runner task runtime input result conflict"))
        end
      else
        update_task!(
          task,
          command,
          attrs ++
            [
              runtime_input_resolution_id: command.resolution_id,
              runtime_inputs_resolved_at: command.occurred_at
            ]
        )
      end
    end)
  end

  defp pin_runtime_inputs!(command, pin) do
    pin_command = %PinRuntimeInputs{
      workspace_context: command.workspace_context,
      command_id: "runner-task-pin:" <> command.resolution_id,
      run_id: pin.run_id,
      pins: [pin]
    }

    case RunsStore.pin_runtime_inputs(pin_command) do
      {:ok, [_persisted]} -> :ok
      {:error, %Error{} = error} -> Repo.rollback(error)
      {:error, reason} -> Repo.rollback(ErrorMapper.map(reason))
    end
  end

  @impl true
  def append_log_batch(%C.AppendRunnerTaskLogBatch{} = command) do
    idempotent_transact(command, "append_log_batch", fn ->
      task = fenced_task!(command)
      validate_log_batch!(command)

      row = %{
        workspace_id: task.workspace_id,
        task_id: task.task_id,
        batch_id: command.batch_id,
        assignment_generation: command.assignment_generation,
        sequence: command.sequence,
        entries: command.entries,
        payload_hash: command.payload_hash,
        inserted_at: command.occurred_at
      }

      case Repo.insert_all(RunnerTaskLogBatch, [row], on_conflict: :nothing) do
        {1, _} ->
          :persisted

        {0, _} ->
          existing =
            Repo.get_by(RunnerTaskLogBatch,
              workspace_id: task.workspace_id,
              task_id: task.task_id,
              batch_id: command.batch_id
            ) ||
              Repo.get_by(RunnerTaskLogBatch,
                workspace_id: task.workspace_id,
                task_id: task.task_id,
                assignment_generation: command.assignment_generation,
                sequence: command.sequence
              )

          if existing && existing.batch_id == command.batch_id &&
               existing.assignment_generation == command.assignment_generation &&
               existing.payload_hash == command.payload_hash &&
               existing.sequence == command.sequence,
             do: :already_persisted,
             else: Repo.rollback(Error.new(:conflict, "runner task log batch conflict"))
      end
    end)
  end

  @impl true
  def complete(%C.CompleteRunnerTask{} = command) do
    idempotent_transact(command, "complete", fn ->
      task = fenced_task!(command)
      validate_completion!(task, command)
      status = terminal_status!(command.outcome)
      error = persisted_error(command.error)

      if task.status in @terminal_statuses do
        if task.status != status or task.result_version != command.result_version or
             task.retry_class != Atom.to_string(command.retry_class) or
             task.result != command.result or task.error != error do
          Repo.rollback(Error.new(:conflict, "runner task terminal result conflict"))
        end

        task
      else
        {1, _} =
          Repo.update_all(task_query(task),
            set: [
              status: status,
              retry_class: Atom.to_string(command.retry_class),
              result_version: command.result_version,
              result: command.result,
              error: error,
              terminal_at: command.occurred_at,
              assignment_expires_at: nil,
              last_command_id: command.command_id,
              updated_at: command.occurred_at
            ]
          )

        remove_active_demand!(
          task.runner_pool,
          task.required_runner_release_id,
          command.occurred_at
        )

        fetch_task!(task.workspace_id, task.task_id)
      end
    end)
  end

  @impl true
  def request_cancellation(%C.RequestRunnerTaskCancellation{} = command) do
    idempotent_transact(command, "request_cancellation", fn ->
      workspace_id = command.workspace_context.workspace_id
      task = lock_task!(workspace_id, command.task_id)

      cond do
        task.status in @terminal_statuses ->
          task

        task.status == "queued" ->
          update_task!(task, command,
            status: "cancelled",
            cancellation_requested_at: command.occurred_at,
            terminal_at: command.occurred_at
          )

        true ->
          update_task!(task, command,
            status: "cancelling",
            cancellation_requested_at: command.occurred_at
          )
      end
    end)
  end

  @impl true
  def acknowledge_cancellation(%C.AcknowledgeRunnerTaskCancellation{} = command) do
    idempotent_transact(command, "acknowledge_cancellation", fn ->
      task = fenced_task!(command)

      unless task.status == "cancelling" and not is_nil(task.cancellation_requested_at) do
        Repo.rollback(Error.new(:conflict, "runner task cancellation is not pending"))
      end

      update_task!(task, command, cancellation_acknowledged_at: command.occurred_at)
    end)
  end

  @impl true
  def release(%C.ReleaseRunnerTask{} = command) do
    idempotent_transact(command, "release", fn ->
      task = fenced_task!(command)

      unless command.disposition in [:requeue, :unknown, :cancelled] do
        Repo.rollback(Error.new(:invalid, "invalid runner task release disposition"))
      end

      if command.disposition == :unknown and is_nil(command.reason) do
        Repo.rollback(Error.new(:invalid, "unknown runner task release requires a reason"))
      end

      if command.disposition == :requeue and not proven_safe_to_requeue?(task) do
        Repo.rollback(Error.new(:conflict, "runner task is not proven safe to requeue"))
      end

      attrs =
        case command.disposition do
          :requeue ->
            [
              status: "queued",
              assigned_runner_instance_id: nil,
              assigned_runner_session_generation: nil,
              assignment_expires_at: nil
            ]

          :unknown ->
            [
              status: "unknown",
              result_version: 0,
              error: persisted_error(command.reason),
              terminal_at: command.occurred_at,
              assignment_expires_at: nil
            ]

          :cancelled ->
            [
              status: "cancelled",
              error: persisted_error(command.reason),
              terminal_at: command.occurred_at,
              assignment_expires_at: nil
            ]
        end

      update_task!(task, command, attrs)
    end)
  end

  @impl true
  def recover_expired(%C.RecoverRunnerTasks{} = command) do
    idempotent_transact(command, "recover_expired", fn ->
      validate_recovery!(command)

      tasks =
        Repo.all(
          from(task in RunnerTask,
            where:
              task.status in ^@active_statuses and
                task.assignment_expires_at <= ^command.occurred_at,
            order_by: [
              asc: task.assignment_expires_at,
              asc: task.workspace_id,
              asc: task.task_id
            ],
            limit: ^command.limit,
            lock: "FOR UPDATE SKIP LOCKED"
          )
        )

      expires_at = DateTime.add(command.occurred_at, command.lease_duration_ms, :millisecond)

      Enum.map(tasks, fn task ->
        {1, _} =
          Repo.update_all(task_query(task),
            set: [
              assigned_runner_instance_id: command.owner_id,
              assigned_runner_session_generation: 0,
              assignment_generation: task.assignment_generation + 1,
              assignment_expires_at: expires_at,
              last_command_id: command.command_id,
              updated_at: command.occurred_at
            ]
          )

        fetch_task!(task.workspace_id, task.task_id) |> to_result()
      end)
    end)
  end

  @impl true
  def reconcile_demand(%C.ReconcileRunnerCapacityDemand{} = command) do
    idempotent_transact(command, "reconcile_demand", fn ->
      validate_platform_runner_context!(command.platform_context)
      validate_pool_release!(command.runner_pool, command.required_runner_release_id)

      case command.mode do
        :audit ->
          audit_demand!(
            command.runner_pool,
            command.required_runner_release_id,
            command.occurred_at
          )

        :repair ->
          rebuild_demand!(
            command.runner_pool,
            command.required_runner_release_id,
            command.occurred_at
          )

        _other ->
          Repo.rollback(Error.new(:invalid, "invalid runner capacity reconciliation mode"))
      end
    end)
  end

  @impl true
  def get(%Q.GetRunnerTask{} = query) do
    read(fn ->
      if bounded_id(query.task_id) == :ok do
        query.workspace_context.workspace_id
        |> fetch_task!(query.task_id)
        |> to_result()
      else
        {:error, Error.new(:invalid, "invalid runner task identity")}
      end
    end)
  end

  @impl true
  def page_run(%Q.PageRunRunnerTasks{} = query) do
    read(fn ->
      if valid_page_query?(query) do
        statuses =
          case query.statuses do
            :all -> nil
            values when is_list(values) -> Enum.map(values, &Atom.to_string/1)
          end

        base =
          from(task in RunnerTask,
            where:
              task.workspace_id == ^query.workspace_context.workspace_id and
                task.run_id == ^query.run_id,
            order_by: [asc: task.enqueued_at, asc: task.task_id],
            limit: ^query.limit
          )

        base =
          case query.cursor do
            nil ->
              base

            {%DateTime{} = enqueued_at, task_id} ->
              from(task in base,
                where:
                  task.enqueued_at > ^enqueued_at or
                    (task.enqueued_at == ^enqueued_at and task.task_id > ^task_id)
              )
          end

        base = if statuses, do: from(task in base, where: task.status in ^statuses), else: base
        base |> Repo.all() |> Enum.map(&to_result/1)
      else
        {:error, Error.new(:invalid, "invalid runner task page query")}
      end
    end)
  end

  @impl true
  def demand(%Q.GetRunnerCapacityDemand{} = query) do
    read(fn ->
      case {
        valid_platform_runner_context?(query.platform_context),
        validate_pool_release(query.runner_pool, query.required_runner_release_id)
      } do
        {true, :ok} ->
          case Repo.get_by(Demand,
                 runner_pool: query.runner_pool,
                 required_runner_release_id: query.required_runner_release_id
               ) do
            %Demand{healthy: true} = demand -> to_demand(demand)
            %Demand{} -> {:error, Error.new(:unavailable, "runner capacity demand is stale")}
            nil -> {:error, Error.new(:unavailable, "runner capacity demand is unavailable")}
          end

        _invalid ->
          {:error, Error.new(:invalid, "invalid runner demand query")}
      end
    end)
  end

  defp update_task!(task, command, attrs) do
    {1, _} =
      Repo.update_all(task_query(task),
        set: attrs ++ [last_command_id: command.command_id, updated_at: command.occurred_at]
      )

    update_demand_for_status_change!(task, Keyword.get(attrs, :status), command.occurred_at)

    fetch_task!(task.workspace_id, task.task_id)
  end

  defp transition_values!(task, %C.TransitionRunnerTask{transition: :preparing}),
    do: allowed_transition!(task, ~w(assigned), "preparing", task.assignment_expires_at)

  defp transition_values!(task, %C.TransitionRunnerTask{transition: :running}),
    do: allowed_transition!(task, ~w(assigned preparing), "running", task.assignment_expires_at)

  defp transition_values!(task, %C.TransitionRunnerTask{transition: :renew} = command) do
    allowed_transition!(
      task,
      @active_statuses,
      task.status,
      DateTime.add(command.occurred_at, command.lease_duration_ms, :millisecond)
    )
  end

  defp allowed_transition!(task, allowed, status, expires_at) do
    if task.status in allowed,
      do: {status, expires_at},
      else: Repo.rollback(Error.new(:conflict, "invalid runner task transition"))
  end

  defp terminal_status!(:succeeded), do: "succeeded"
  defp terminal_status!(:failed), do: "failed"
  defp terminal_status!(:cancelled), do: "cancelled"
  defp terminal_status!(:unknown), do: "unknown"

  defp fenced_task!(command) do
    validate_fence!(command)
    task = lock_task!(command.workspace_context.workspace_id, command.task_id)

    if task.assigned_runner_instance_id == command.runner_instance_id and
         task.assigned_runner_session_generation == command.runner_session_generation and
         task.assignment_generation == command.assignment_generation do
      task
    else
      Repo.rollback(Error.new(:conflict, "stale runner task assignment"))
    end
  end

  defp lock_task!(workspace_id, task_id) do
    Repo.one!(
      from(task in RunnerTask,
        where: task.workspace_id == ^workspace_id and task.task_id == ^task_id,
        lock: "FOR UPDATE"
      )
    )
  end

  defp active_runner_task(command) do
    Repo.one(
      from(task in RunnerTask,
        where:
          task.assigned_runner_instance_id == ^command.runner_instance_id and
            task.assigned_runner_session_generation == ^command.runner_session_generation and
            task.status in ^@active_statuses,
        order_by: [asc: task.workspace_id, asc: task.task_id],
        limit: 1,
        lock: "FOR UPDATE"
      )
    )
  end

  defp active_runner_matches_claim?(task, command) do
    task.runner_pool == command.runner_pool and
      task.required_runner_release_id == command.required_runner_release_id and
      task_kind!(task.task_kind) in command.supported_task_kinds and
      (is_nil(task.required_capability) or
         task.required_capability in command.capabilities)
  end

  defp lock_runner_claim_key!(runner_instance_id, session_generation) do
    lock_key =
      "runner-claim:#{byte_size(runner_instance_id)}:#{runner_instance_id}:#{session_generation}"

    SQL.query!(Repo, "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))", [lock_key])
    :ok
  end

  defp fetch_task!(workspace_id, task_id),
    do: Repo.get_by!(RunnerTask, workspace_id: workspace_id, task_id: task_id)

  defp task_query(task),
    do:
      from(row in RunnerTask,
        where: row.workspace_id == ^task.workspace_id and row.task_id == ^task.task_id
      )

  defp update_demand_for_status_change!(_task, nil, _occurred_at), do: :ok

  defp update_demand_for_status_change!(task, status, _occurred_at)
       when status == task.status,
       do: :ok

  defp update_demand_for_status_change!(task, status, _occurred_at)
       when task.status in @active_statuses and status in @active_statuses,
       do: :ok

  defp update_demand_for_status_change!(task, "queued", occurred_at)
       when task.status in @active_statuses do
    move_active_to_queued_demand!(
      task.runner_pool,
      task.required_runner_release_id,
      task.enqueued_at,
      occurred_at
    )
  end

  defp update_demand_for_status_change!(task, status, occurred_at)
       when task.status == "queued" and status in @terminal_statuses do
    remove_queued_demand!(
      task.runner_pool,
      task.required_runner_release_id,
      occurred_at
    )
  end

  defp update_demand_for_status_change!(task, status, occurred_at)
       when task.status in @active_statuses and status in @terminal_statuses do
    remove_active_demand!(
      task.runner_pool,
      task.required_runner_release_id,
      occurred_at
    )
  end

  defp update_demand_for_status_change!(task, status, _occurred_at) do
    Repo.rollback(
      Error.new(
        :internal,
        "unsupported runner capacity transition #{task.status} -> #{status}"
      )
    )
  end

  defp add_queued_demand!(runner_pool, release_id, enqueued_at, occurred_at) do
    lock_demand_key!(runner_pool, release_id)

    attrs = %{
      runner_pool: runner_pool,
      required_runner_release_id: release_id,
      outstanding_count: 1,
      queued_count: 1,
      active_count: 0,
      oldest_queued_at: enqueued_at,
      version: 1,
      healthy: true,
      updated_at: occurred_at
    }

    case Repo.insert_all(Demand, [attrs], on_conflict: :nothing) do
      {1, _} ->
        :ok

      {0, _} ->
        demand = lock_healthy_demand!(runner_pool, release_id)
        oldest = oldest(demand.oldest_queued_at, enqueued_at)

        update_demand!(
          demand,
          [outstanding_count: 1, queued_count: 1, version: 1],
          oldest_queued_at: oldest,
          updated_at: occurred_at
        )
    end
  end

  defp move_queued_to_active!(runner_pool, release_id, occurred_at) do
    demand = lock_healthy_demand!(runner_pool, release_id)
    next_oldest = oldest_queued_at(runner_pool, release_id)

    update_demand!(
      demand,
      [queued_count: -1, active_count: 1, version: 1],
      oldest_queued_at: next_oldest,
      updated_at: occurred_at
    )
  end

  defp move_active_to_queued_demand!(
         runner_pool,
         release_id,
         enqueued_at,
         occurred_at
       ) do
    demand = lock_healthy_demand!(runner_pool, release_id)
    oldest = oldest(demand.oldest_queued_at, enqueued_at)

    update_demand!(
      demand,
      [queued_count: 1, active_count: -1, version: 1],
      oldest_queued_at: oldest,
      updated_at: occurred_at
    )
  end

  defp remove_queued_demand!(runner_pool, release_id, occurred_at) do
    demand = lock_healthy_demand!(runner_pool, release_id)
    next_oldest = oldest_queued_at(runner_pool, release_id)

    update_demand!(
      demand,
      [outstanding_count: -1, queued_count: -1, version: 1],
      oldest_queued_at: next_oldest,
      updated_at: occurred_at
    )
  end

  defp remove_active_demand!(runner_pool, release_id, occurred_at) do
    demand = lock_healthy_demand!(runner_pool, release_id)

    update_demand!(
      demand,
      [outstanding_count: -1, active_count: -1, version: 1],
      updated_at: occurred_at
    )
  end

  defp lock_healthy_demand!(runner_pool, release_id) do
    demand =
      Repo.one(
        from(row in Demand,
          where:
            row.runner_pool == ^runner_pool and row.required_runner_release_id == ^release_id,
          lock: "FOR UPDATE"
        )
      )

    case demand do
      %Demand{healthy: true} -> demand
      %Demand{} -> Repo.rollback(Error.new(:unavailable, "runner capacity demand is stale"))
      nil -> Repo.rollback(Error.new(:unavailable, "runner capacity demand is unavailable"))
    end
  end

  defp update_demand!(demand, increments, values) do
    {1, _} =
      Repo.update_all(
        from(row in Demand,
          where:
            row.runner_pool == ^demand.runner_pool and
              row.required_runner_release_id == ^demand.required_runner_release_id
        ),
        inc: increments,
        set: values
      )

    :ok
  end

  defp oldest_queued_at(runner_pool, release_id) do
    Repo.one!(
      from(task in RunnerTask,
        where:
          task.runner_pool == ^runner_pool and
            task.required_runner_release_id == ^release_id and task.status == "queued",
        select: min(task.enqueued_at)
      )
    )
  end

  defp oldest(nil, value), do: value
  defp oldest(value, nil), do: value

  defp oldest(left, right),
    do: if(DateTime.compare(left, right) == :gt, do: right, else: left)

  defp audit_demand!(runner_pool, release_id, occurred_at) do
    lock_demand_key!(runner_pool, release_id)
    demand = lock_demand(runner_pool, release_id)
    actual = demand_counts(runner_pool, release_id)

    case demand do
      nil ->
        healthy? = actual.outstanding_count == 0
        insert_demand!(runner_pool, release_id, actual, healthy?, occurred_at)

      %Demand{healthy: true} = current ->
        if demand_matches?(current, actual) do
          current
        else
          {1, _} =
            Repo.update_all(demand_query(runner_pool, release_id),
              inc: [version: 1],
              set: [healthy: false, updated_at: occurred_at]
            )

          Repo.get_by!(Demand,
            runner_pool: runner_pool,
            required_runner_release_id: release_id
          )
        end

      %Demand{} = current ->
        current
    end
  end

  defp rebuild_demand!(runner_pool, release_id, occurred_at) do
    lock_demand_key!(runner_pool, release_id)
    lock_demand(runner_pool, release_id)
    actual = demand_counts(runner_pool, release_id)

    attrs = %{
      runner_pool: runner_pool,
      required_runner_release_id: release_id,
      outstanding_count: actual.outstanding_count,
      queued_count: actual.queued_count,
      active_count: actual.active_count,
      oldest_queued_at: actual.oldest_queued_at,
      version: 1,
      healthy: true,
      updated_at: occurred_at
    }

    Repo.insert_all(Demand, [attrs],
      conflict_target: [:runner_pool, :required_runner_release_id],
      on_conflict: [
        set: [
          outstanding_count: attrs.outstanding_count,
          queued_count: attrs.queued_count,
          active_count: attrs.active_count,
          oldest_queued_at: attrs.oldest_queued_at,
          healthy: true,
          updated_at: occurred_at
        ],
        inc: [version: 1]
      ]
    )

    Repo.get_by!(Demand,
      runner_pool: runner_pool,
      required_runner_release_id: release_id
    )
  end

  defp lock_demand(runner_pool, release_id) do
    Repo.one(
      from(row in Demand,
        where: row.runner_pool == ^runner_pool and row.required_runner_release_id == ^release_id,
        lock: "FOR UPDATE"
      )
    )
  end

  defp demand_counts(runner_pool, release_id) do
    queued =
      Repo.one!(
        from(task in RunnerTask,
          where:
            task.runner_pool == ^runner_pool and
              task.required_runner_release_id == ^release_id and task.status == "queued",
          select: %{count: count(task.task_id), oldest: min(task.enqueued_at)}
        )
      )

    active =
      Repo.one!(
        from(task in RunnerTask,
          where:
            task.runner_pool == ^runner_pool and
              task.required_runner_release_id == ^release_id and task.status in ^@active_statuses,
          select: count(task.task_id)
        )
      )

    %{
      outstanding_count: queued.count + active,
      queued_count: queued.count,
      active_count: active,
      oldest_queued_at: queued.oldest
    }
  end

  defp demand_matches?(demand, actual) do
    demand.outstanding_count == actual.outstanding_count and
      demand.queued_count == actual.queued_count and
      demand.active_count == actual.active_count and
      demand.oldest_queued_at == actual.oldest_queued_at
  end

  defp insert_demand!(
         runner_pool,
         release_id,
         actual,
         healthy?,
         occurred_at
       ) do
    Repo.insert_all(Demand, [
      %{
        runner_pool: runner_pool,
        required_runner_release_id: release_id,
        outstanding_count: actual.outstanding_count,
        queued_count: actual.queued_count,
        active_count: actual.active_count,
        oldest_queued_at: actual.oldest_queued_at,
        version: 1,
        healthy: healthy?,
        updated_at: occurred_at
      }
    ])

    Repo.get_by!(Demand,
      runner_pool: runner_pool,
      required_runner_release_id: release_id
    )
  end

  defp demand_query(runner_pool, release_id) do
    from(row in Demand,
      where: row.runner_pool == ^runner_pool and row.required_runner_release_id == ^release_id
    )
  end

  defp lock_demand_key!(runner_pool, release_id) do
    lock_key =
      Enum.map_join([runner_pool, release_id], "|", fn value ->
        "#{byte_size(value)}:#{value}"
      end)

    SQL.query!(Repo, "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))", [lock_key])
    :ok
  end

  defp validate_enqueue!(command) do
    with :ok <- bounded_id(command.task_id),
         true <- Regex.match?(~r/^rt_[A-Za-z0-9_-]{1,252}$/, command.task_id),
         :ok <- bounded_id(command.domain_identity),
         :ok <- Favn.RunnerPool.validate_runtime(command.runner_pool),
         :ok <- Favn.Contracts.RunnerReleaseBinding.validate(command.required_runner_release_id),
         true <- command.task_kind in Favn.Contracts.RunnerTask.task_kinds(),
         true <- command.retry_class in Favn.Contracts.RunnerTask.retry_classes(),
         true <-
           Favn.Contracts.RunnerTask.valid_initial_retry_class?(
             command.task_kind,
             command.retry_class
           ),
         true <- is_map(command.payload),
         true <- is_map(command.orchestration_context),
         true <- is_binary(command.payload_hash) and byte_size(command.payload_hash) == 32,
         {:ok, _decoded} <- Codec.decode_payload(command.task_kind, command.payload),
         {:ok, _context} <- Codec.decode_orchestration_context(command.orchestration_context),
         {:ok, expected_hash} <- Codec.payload_hash(command.payload),
         true <- expected_hash == command.payload_hash,
         :ok <- optional_bounded_id(command.run_id),
         :ok <- optional_bounded_id(command.operation_id),
         :ok <- optional_bounded_id(command.asset_step_id),
         :ok <- optional_bounded_id(command.required_capability),
         :ok <- valid_deadline(command.occurred_at, command.deadline_at) do
      :ok
    else
      _other -> Repo.rollback(Error.new(:invalid, "invalid runner task enqueue command"))
    end
  end

  defp same_enqueued_task?(task, command) do
    task.domain_identity == command.domain_identity and
      task.task_kind == Atom.to_string(command.task_kind) and
      task.run_id == command.run_id and
      task.operation_id == command.operation_id and
      task.asset_step_id == command.asset_step_id and
      task.runner_pool == command.runner_pool and
      task.required_runner_release_id == command.required_runner_release_id and
      task.required_capability == command.required_capability and
      task.retry_class == Atom.to_string(command.retry_class) and
      task.enqueued_at == command.occurred_at and
      task.deadline_at == command.deadline_at and
      task.payload_version == 13 and
      task.payload_hash == command.payload_hash and
      task.orchestration_context == command.orchestration_context
  end

  defp validate_claim!(command) do
    with true <- valid_platform_runner_context?(command.platform_context),
         :ok <- validate_pool_release!(command.runner_pool, command.required_runner_release_id),
         :ok <- bounded_id(command.runner_instance_id),
         true <-
           is_integer(command.runner_session_generation) and
             command.runner_session_generation > 0,
         :ok <- validate_supported_task_kinds(command.supported_task_kinds),
         :ok <- validate_capabilities(command.capabilities),
         true <- is_integer(command.lease_duration_ms) and command.lease_duration_ms > 0 do
      :ok
    else
      _other -> Repo.rollback(Error.new(:invalid, "invalid runner task claim command"))
    end
  end

  defp validate_supported_task_kinds(values) when is_list(values) and values != [] do
    if Enum.all?(values, &(&1 in Favn.Contracts.RunnerTask.task_kinds())) and
         length(values) == length(Enum.uniq(values)),
       do: :ok,
       else: :error
  end

  defp validate_supported_task_kinds(_values), do: :error

  defp validate_capabilities(values) when is_list(values) and length(values) <= 64 do
    if Enum.all?(
         values,
         &(is_binary(&1) and Regex.match?(~r/^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/, &1))
       ) and length(values) == length(Enum.uniq(values)),
       do: :ok,
       else: :error
  end

  defp validate_capabilities(_values), do: :error

  defp validate_transition!(%C.TransitionRunnerTask{transition: transition} = command)
       when transition in [:preparing, :running] do
    if is_nil(command.lease_duration_ms) or
         (is_integer(command.lease_duration_ms) and command.lease_duration_ms > 0),
       do: :ok,
       else: Repo.rollback(Error.new(:invalid, "invalid runner task transition"))
  end

  defp validate_transition!(%C.TransitionRunnerTask{
         transition: :renew,
         lease_duration_ms: lease_duration_ms
       })
       when is_integer(lease_duration_ms) and lease_duration_ms > 0,
       do: :ok

  defp validate_transition!(_command),
    do: Repo.rollback(Error.new(:invalid, "invalid runner task transition"))

  defp validate_recovery!(command) do
    with true <- valid_platform_runner_context?(command.platform_context),
         :ok <- bounded_id(command.owner_id),
         true <- is_integer(command.limit) and command.limit in 1..500,
         true <- is_integer(command.lease_duration_ms) and command.lease_duration_ms > 0 do
      :ok
    else
      _other -> Repo.rollback(Error.new(:invalid, "invalid runner task recovery command"))
    end
  end

  defp validate_fence!(command) do
    with :ok <- bounded_id(command.task_id),
         :ok <- bounded_id(command.runner_instance_id),
         true <-
           is_integer(command.runner_session_generation) and
             command.runner_session_generation >= 0,
         true <- is_integer(command.assignment_generation) and command.assignment_generation > 0 do
      :ok
    else
      _other -> Repo.rollback(Error.new(:invalid, "invalid runner task assignment fence"))
    end
  end

  defp validate_log_batch!(command) do
    with :ok <- bounded_id(command.batch_id),
         true <- is_integer(command.sequence) and command.sequence >= 0,
         true <- is_list(command.entries),
         true <- is_binary(command.payload_hash) and byte_size(command.payload_hash) == 32,
         {:ok, expected_hash} <-
           Favn.Contracts.RunnerTask.PersistenceCodec.hash_term(command.entries),
         true <- expected_hash == command.payload_hash do
      :ok
    else
      _other -> Repo.rollback(Error.new(:invalid, "invalid runner task log batch"))
    end
  end

  defp validate_completion!(task, command) do
    task_kind = task_kind!(task.task_kind)

    with true <- command.outcome in Favn.Contracts.RunnerTask.terminal_outcomes(),
         true <- command.retry_class in Favn.Contracts.RunnerTask.retry_classes(),
         true <- is_integer(command.result_version) and command.result_version >= 0,
         {:ok, _decoded} <- Codec.decode_result(task_kind, command.outcome, command.result),
         :ok <-
           Favn.Contracts.RunnerTask.validate_terminal_retry(
             task_kind,
             command.outcome,
             command.retry_class,
             command.error
           ) do
      :ok
    else
      _other -> Repo.rollback(Error.new(:invalid, "invalid runner task completion"))
    end
  end

  defp proven_safe_to_requeue?(%RunnerTask{status: status})
       when status in ~w(assigned preparing),
       do: true

  defp proven_safe_to_requeue?(%RunnerTask{
         status: "running",
         task_kind: kind,
         retry_class: "safe_to_retry"
       })
       when kind in ~w(relation_inspection generation_capabilities generation_marker_read),
       do: true

  defp proven_safe_to_requeue?(_task), do: false

  defp persisted_error(nil), do: nil

  defp persisted_error(reason) do
    reason
    |> RunnerError.normalize()
    |> Map.from_struct()
    |> CanonicalJSON.encode()
    |> case do
      {:ok, encoded} ->
        case Jason.decode(encoded) do
          {:ok, value} -> value
          {:error, _reason} -> %{"message" => "Runner error", "redacted?" => true}
        end

      {:error, _reason} ->
        %{"message" => "Runner error", "redacted?" => true}
    end
  end

  defp valid_deadline(_occurred_at, nil), do: :ok

  defp valid_deadline(%DateTime{} = occurred_at, %DateTime{} = deadline_at) do
    if DateTime.compare(deadline_at, occurred_at) in [:eq, :gt], do: :ok, else: :error
  end

  defp valid_deadline(_occurred_at, _deadline_at), do: :error

  defp validate_pool_release!(runner_pool, release_id) do
    case validate_pool_release(runner_pool, release_id) do
      :ok -> :ok
      :error -> Repo.rollback(Error.new(:invalid, "invalid runner pool release binding"))
    end
  end

  defp validate_platform_runner_context!(context) do
    if valid_platform_runner_context?(context),
      do: :ok,
      else: Repo.rollback(Error.new(:invalid, "invalid platform runner task authority"))
  end

  defp valid_platform_runner_context?(
         %FavnOrchestrator.Persistence.PlatformContext{roles: roles} = context
       ) do
    FavnOrchestrator.Persistence.PlatformContext.valid?(context) and
      Enum.any?(roles, &(&1 in [:platform_operator, :platform_admin]))
  end

  defp valid_platform_runner_context?(_context), do: false

  defp validate_pool_release(runner_pool, release_id) do
    with :ok <- Favn.RunnerPool.validate_runtime(runner_pool),
         :ok <- Favn.Contracts.RunnerReleaseBinding.validate(release_id) do
      :ok
    else
      _other -> :error
    end
  end

  defp valid_page_query?(query) do
    valid_statuses? =
      query.statuses == :all or
        (is_list(query.statuses) and
           Enum.all?(query.statuses, &(&1 in Map.values(@status_by_string))))

    bounded_id(query.run_id) == :ok and is_integer(query.limit) and query.limit in 1..500 and
      valid_statuses? and valid_page_cursor?(query.cursor)
  end

  defp valid_page_cursor?(nil), do: true

  defp valid_page_cursor?({%DateTime{}, task_id}),
    do: bounded_id(task_id) == :ok

  defp valid_page_cursor?(_cursor), do: false

  defp bounded_id(value) when is_binary(value) and byte_size(value) in 1..255, do: :ok
  defp bounded_id(_value), do: :error

  defp optional_bounded_id(nil), do: :ok
  defp optional_bounded_id(value), do: bounded_id(value)

  defp idempotent_transact(command, operation, fun) do
    transact(fn ->
      scope_id = command_scope(command)

      unless bounded_id(command.command_id) == :ok do
        Repo.rollback(Error.new(:invalid, "invalid runner task command identity"))
      end

      request_hash = command_hash(command)

      case Repo.insert_all(
             RunnerTaskCommand,
             [
               %{
                 scope_id: scope_id,
                 command_id: command.command_id,
                 operation: operation,
                 request_hash: request_hash,
                 result: %{"kind" => "pending"},
                 inserted_at: command.occurred_at
               }
             ],
             on_conflict: :nothing
           ) do
        {1, _} ->
          result = fun.()
          receipt = encode_command_result(result)

          {1, _} =
            Repo.update_all(
              from(row in RunnerTaskCommand,
                where: row.scope_id == ^scope_id and row.command_id == ^command.command_id
              ),
              set: [result: receipt]
            )

          result

        {0, _} ->
          receipt =
            Repo.get_by!(RunnerTaskCommand,
              scope_id: scope_id,
              command_id: command.command_id
            )

          if receipt.operation == operation and receipt.request_hash == request_hash do
            decode_command_result!(receipt.result)
          else
            Repo.rollback(Error.new(:conflict, "runner task command identity was reused"))
          end
      end
    end)
  end

  defp command_hash(command),
    do: :crypto.hash(:sha256, :erlang.term_to_binary(command, [:deterministic]))

  defp command_scope(%{workspace_context: %{workspace_id: workspace_id}}),
    do: "workspace:" <> workspace_id

  defp command_scope(%{platform_context: _context}), do: "platform:runner_tasks"

  defp encode_command_result(%RunnerTask{workspace_id: workspace_id, task_id: task_id}),
    do: %{"kind" => "task", "workspace_id" => workspace_id, "task_id" => task_id}

  defp encode_command_result(%Demand{} = demand),
    do: %{
      "kind" => "demand",
      "runner_pool" => demand.runner_pool,
      "required_runner_release_id" => demand.required_runner_release_id
    }

  defp encode_command_result(tasks) when is_list(tasks),
    do: %{
      "kind" => "tasks",
      "task_keys" =>
        Enum.map(tasks, &%{"workspace_id" => &1.workspace_id, "task_id" => &1.task_id})
    }

  defp encode_command_result(nil), do: %{"kind" => "none"}
  defp encode_command_result(:persisted), do: %{"kind" => "atom", "value" => "persisted"}

  defp encode_command_result(:already_persisted),
    do: %{"kind" => "atom", "value" => "already_persisted"}

  defp decode_command_result!(%{
         "kind" => "task",
         "workspace_id" => workspace_id,
         "task_id" => task_id
       }),
       do: fetch_task!(workspace_id, task_id)

  defp decode_command_result!(%{
         "kind" => "demand",
         "runner_pool" => runner_pool,
         "required_runner_release_id" => release_id
       }),
       do:
         Repo.get_by!(Demand,
           runner_pool: runner_pool,
           required_runner_release_id: release_id
         )

  defp decode_command_result!(%{"kind" => "tasks", "task_keys" => task_keys}),
    do:
      Enum.map(
        task_keys,
        &(fetch_task!(&1["workspace_id"], &1["task_id"]) |> to_result())
      )

  defp decode_command_result!(%{"kind" => "none"}), do: nil

  defp decode_command_result!(%{"kind" => "atom", "value" => "persisted"}),
    do: :persisted

  defp decode_command_result!(%{
         "kind" => "atom",
         "value" => "already_persisted"
       }),
       do: :already_persisted

  defp decode_command_result!(_receipt),
    do: Repo.rollback(Error.new(:internal, "runner task command receipt is invalid"))

  defp transact(fun) do
    case Repo.transaction(fun) do
      {:ok, %RunnerTask{} = task} -> {:ok, to_result(task)}
      {:ok, %Demand{} = demand} -> {:ok, to_demand(demand)}
      {:ok, tasks} when is_list(tasks) -> {:ok, tasks}
      {:ok, result} -> {:ok, result}
      {:error, %Error{} = error} -> {:error, error}
      {:error, reason} -> {:error, ErrorMapper.map(reason)}
    end
  rescue
    error -> {:error, ErrorMapper.map(error)}
  end

  defp read(fun) do
    case fun.() do
      {:error, %Error{} = error} -> {:error, error}
      value -> {:ok, value}
    end
  rescue
    error -> {:error, ErrorMapper.map(error)}
  catch
    :exit, reason -> {:error, ErrorMapper.map(reason)}
  end

  defp to_result(%RunnerTask{} = task) do
    task_kind = task_kind!(task.task_kind)
    payload = decode_payload!(task_kind, task.payload)
    orchestration_context = decode_orchestration_context!(task.orchestration_context)
    result = decode_result!(task_kind, Map.fetch!(@status_by_string, task.status), task.result)

    task
    |> Map.from_struct()
    |> Map.take(Map.keys(%RunnerTaskResult{}))
    |> Map.put(:task_kind, task_kind)
    |> Map.put(:payload, payload)
    |> Map.put(:orchestration_context, orchestration_context)
    |> Map.put(:result, result)
    |> Map.update!(:retry_class, &Map.fetch!(@retry_class_by_string, &1))
    |> Map.update!(:status, &Map.fetch!(@status_by_string, &1))
    |> Map.update!(:runtime_input_resolution_status, fn
      nil -> nil
      "resolved" -> :resolved
      "failed" -> :failed
    end)
    |> then(&struct!(RunnerTaskResult, &1))
  end

  defp task_kind!(value), do: Map.fetch!(@task_kind_by_string, value)

  defp decode_payload!(task_kind, envelope) do
    case Codec.decode_payload(task_kind, envelope) do
      {:ok, payload} -> payload
      {:error, reason} -> raise "invalid persisted runner task payload: #{inspect(reason)}"
    end
  end

  defp decode_orchestration_context!(envelope) do
    case Codec.decode_orchestration_context(envelope) do
      {:ok, context} -> context
      {:error, reason} -> raise "invalid persisted runner task context: #{inspect(reason)}"
    end
  end

  defp decode_result!(_task_kind, status, nil)
       when status in [:queued, :assigned, :preparing, :running, :cancelling],
       do: nil

  defp decode_result!(task_kind, status, envelope)
       when status in [:succeeded, :failed, :cancelled, :unknown] do
    outcome =
      case status do
        :succeeded -> :succeeded
        :failed -> :failed
        :cancelled -> :cancelled
        :unknown -> :unknown
      end

    case Codec.decode_result(task_kind, outcome, envelope) do
      {:ok, result} -> result
      {:error, reason} -> raise "invalid persisted runner task result: #{inspect(reason)}"
    end
  end

  defp to_demand(%Demand{} = demand) do
    %RunnerCapacityDemand{
      runner_pool: demand.runner_pool,
      required_runner_release_id: demand.required_runner_release_id,
      outstanding_count: demand.outstanding_count,
      queued_count: demand.queued_count,
      active_count: demand.active_count,
      oldest_queued_at: demand.oldest_queued_at,
      version: demand.version,
      updated_at: demand.updated_at,
      healthy?: demand.healthy
    }
  end
end
