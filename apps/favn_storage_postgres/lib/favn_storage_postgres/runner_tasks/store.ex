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
  alias FavnOrchestrator.Persistence.Results.RunnerCapacityHealth
  alias FavnOrchestrator.Persistence.Results.RunnerReleaseDrain
  alias FavnOrchestrator.Persistence.Results.RunnerTask, as: RunnerTaskResult
  alias FavnOrchestrator.Storage.JsonSafe
  alias Favn.Contracts.RunnerError
  alias FavnStoragePostgres.CanonicalJSON
  alias FavnStoragePostgres.ErrorMapper
  alias FavnStoragePostgres.Repo
  alias FavnStoragePostgres.RunnerTasks.Codec
  alias FavnStoragePostgres.Runs.Store, as: RunsStore
  alias FavnStoragePostgres.Schemas.RunnerCapacityDemand, as: Demand
  alias FavnStoragePostgres.Schemas.RebuildPlanAction
  alias FavnStoragePostgres.Schemas.Run
  alias FavnStoragePostgres.Schemas.RunnerTask
  alias FavnStoragePostgres.Schemas.RunnerTaskCommand
  alias FavnStoragePostgres.Schemas.RunnerTaskCommandTask
  alias FavnStoragePostgres.Schemas.RunnerTaskLogBatch
  alias FavnStoragePostgres.Schemas.RunnerTaskOutcome
  alias FavnStoragePostgres.Schemas.RunnerTaskRuntimeInputError

  @active_statuses ~w(assigned preparing running cancelling)
  @terminal_statuses ~w(succeeded failed cancelled unknown)
  @terminal_result_statuses [:succeeded, :failed, :cancelled, :unknown]
  @receipt_retention_ms :timer.hours(24) * 7
  @maximum_future_clock_skew_ms :timer.minutes(5)
  @receipt_prune_limit 100
  @immutable_task_result_fields [
    :workspace_id,
    :task_id,
    :domain_identity,
    :task_kind,
    :run_id,
    :operation_id,
    :asset_step_id,
    :runner_pool,
    :required_runner_release_id,
    :required_capability,
    :enqueued_at,
    :deadline_at,
    :payload_version,
    :payload,
    :payload_hash,
    :orchestration_context,
    :inserted_at
  ]
  @mutable_task_result_fields [
    :retry_class,
    :status,
    :assigned_runner_instance_id,
    :assigned_runner_session_generation,
    :assignment_generation,
    :assignment_expires_at,
    :cancellation_requested_at,
    :cancellation_acknowledged_at,
    :runtime_input_resolution_id,
    :runtime_input_resolution_status,
    :runtime_input_payload_fingerprint,
    :runtime_inputs_resolved_at,
    :result_version,
    :terminal_at
  ]
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
                  task.deadline_at > ^command.occurred_at and
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

      cond do
        is_nil(task.runtime_input_resolution_status) ->
          update_task!(
            task,
            command,
            attrs ++
              [
                runtime_input_resolution_id: command.resolution_id,
                runtime_inputs_resolved_at: command.occurred_at
              ]
          )

        task.runtime_input_resolution_id == command.resolution_id and
          task.runtime_input_resolution_status == persisted_status and
          task.runtime_input_payload_fingerprint == persisted_fingerprint and
            task.runtime_input_error == persisted_resolution_error ->
          task

        task.runtime_input_resolution_status == "failed" ->
          # A failed resolution pins nothing durable, so the currently fenced
          # assignment may replace it with its own outcome. Without this, a
          # task whose first attempt failed resolution could never record a
          # second attempt's result and would wedge until its lease expired.
          update_task!(
            task,
            command,
            attrs ++
              [
                runtime_input_resolution_id: command.resolution_id,
                runtime_inputs_resolved_at: command.occurred_at
              ]
          )

        true ->
          Repo.rollback(Error.new(:conflict, "runner task runtime input result conflict"))
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
        entries: Enum.map(command.entries, &JsonSafe.data/1),
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

  @doc false
  def cancel_operation_in_transaction(workspace_id, operation_id, occurred_at, command_id)
      when is_binary(workspace_id) and is_binary(operation_id) and
             is_struct(occurred_at, DateTime) and is_binary(command_id) do
    command = %{command_id: command_id, occurred_at: occurred_at}

    from(task in RunnerTask,
      where:
        task.workspace_id == ^workspace_id and task.operation_id == ^operation_id and
          task.status not in ^@terminal_statuses,
      order_by: [asc: task.task_id],
      lock: "FOR UPDATE"
    )
    |> Repo.all()
    |> Enum.each(fn task ->
      attrs =
        if task.status == "queued" do
          [
            status: "cancelled",
            cancellation_requested_at: occurred_at,
            terminal_at: occurred_at
          ]
        else
          [status: "cancelling", cancellation_requested_at: occurred_at]
        end

      update_task!(task, command, attrs)
    end)

    :ok
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
  def retry(%C.RetryRunnerTask{} = command) do
    idempotent_transact(command, "retry", fn ->
      unless bounded_id(command.task_id) == :ok and
               is_integer(command.expected_assignment_generation) and
               command.expected_assignment_generation > 0 and
               is_integer(command.expected_result_version) and
               command.expected_result_version >= 0 do
        Repo.rollback(Error.new(:invalid, "invalid runner task retry command"))
      end

      task = lock_task!(command.workspace_context.workspace_id, command.task_id)

      unless task.status == "failed" and task.retry_class == "safe_to_retry" and
               task.assignment_generation == command.expected_assignment_generation and
               task.result_version == command.expected_result_version do
        Repo.rollback(Error.new(:conflict, "runner task is not a retryable terminal failure"))
      end

      update_task!(task, command,
        status: "queued",
        assigned_runner_instance_id: nil,
        assigned_runner_session_generation: nil,
        assignment_expires_at: nil,
        cancellation_requested_at: nil,
        cancellation_acknowledged_at: nil,
        runtime_input_resolution_id: nil,
        runtime_input_resolution_status: nil,
        runtime_input_payload_fingerprint: nil,
        runtime_input_error: nil,
        runtime_inputs_resolved_at: nil,
        result_version: nil,
        result: nil,
        error: nil,
        terminal_at: nil
      )
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
  def page_workspace(%Q.PageWorkspaceRunnerTasks{} = query) do
    read(fn ->
      if valid_workspace_page_query?(query) do
        statuses =
          case query.statuses do
            :all -> nil
            values when is_list(values) -> Enum.map(values, &Atom.to_string/1)
          end

        base =
          from(task in RunnerTask,
            where: task.workspace_id == ^query.workspace_context.workspace_id,
            order_by: [desc: task.inserted_at, desc: task.task_id],
            limit: ^query.limit
          )

        base =
          case query.cursor do
            nil ->
              base

            {%DateTime{} = inserted_at, task_id} ->
              from(task in base,
                where:
                  task.inserted_at < ^inserted_at or
                    (task.inserted_at == ^inserted_at and task.task_id < ^task_id)
              )
          end

        base = if statuses, do: from(task in base, where: task.status in ^statuses), else: base
        base |> Repo.all() |> Enum.map(&to_operator_result/1)
      else
        {:error, Error.new(:invalid, "invalid workspace runner task page query")}
      end
    end)
  end

  @impl true
  def demand(%Q.GetRunnerCapacityDemand{} = query) do
    read(fn ->
      case {
        valid_platform_runner_read_context?(query.platform_context),
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

  @impl true
  def ensure_demand(%C.EnsureRunnerCapacityDemand{} = command) do
    transact(fn ->
      validate_platform_runner_context!(command.platform_context)
      validate_pool_release!(command.runner_pool, command.required_runner_release_id)

      row = %{
        runner_pool: command.runner_pool,
        required_runner_release_id: command.required_runner_release_id,
        outstanding_count: 0,
        queued_count: 0,
        active_count: 0,
        oldest_queued_at: nil,
        version: 0,
        healthy: true,
        updated_at: command.occurred_at
      }

      Repo.insert_all(Demand, [row], on_conflict: :nothing)

      Repo.get_by!(Demand,
        runner_pool: command.runner_pool,
        required_runner_release_id: command.required_runner_release_id
      )
    end)
  end

  @impl true
  def list_demands(%Q.ListRunnerCapacityDemands{} = query) do
    read(fn ->
      if valid_platform_runner_read_context?(query.platform_context) and
           is_integer(query.limit) and query.limit in 1..1_024 do
        Demand
        |> order_by([demand], asc: demand.runner_pool, asc: demand.required_runner_release_id)
        |> limit(^query.limit)
        |> Repo.all()
        |> Enum.map(&to_demand/1)
      else
        {:error, Error.new(:invalid, "invalid runner demand list query")}
      end
    end)
  end

  @impl true
  def release_drain(%Q.GetRunnerReleaseDrain{} = query) do
    read(fn ->
      with true <- valid_platform_runner_read_context?(query.platform_context),
           :ok <-
             validate_pool_release(query.runner_pool, query.required_runner_release_id),
           %Demand{} = demand <-
             Repo.get_by(Demand,
               runner_pool: query.runner_pool,
               required_runner_release_id: query.required_runner_release_id
             ),
           true <- demand.healthy do
        active_run_count =
          active_run_blocker_count(query.runner_pool, query.required_runner_release_id)

        pending_operation_count =
          pending_operation_blocker_count(
            query.runner_pool,
            query.required_runner_release_id
          )

        to_release_drain(demand, active_run_count, pending_operation_count)
      else
        false -> {:error, Error.new(:invalid, "invalid runner release drain query")}
        nil -> {:error, Error.new(:unavailable, "runner release drain is unavailable")}
        %Demand{} -> {:error, Error.new(:unavailable, "runner release drain is stale")}
        {:error, _reason} -> {:error, Error.new(:invalid, "invalid runner release drain query")}
      end
    end)
  end

  @impl true
  def capacity_health(%Q.GetRunnerCapacityHealth{} = query) do
    read(fn ->
      if valid_platform_runner_read_context?(query.platform_context) do
        %{rows: [[partition_count, unhealthy_partition_count]]} =
          SQL.query!(
            Repo,
            """
            SELECT count(*)::bigint,
                   count(*) FILTER (WHERE NOT healthy)::bigint
            FROM favn_control.runner_capacity_demands
            """,
            []
          )

        %RunnerCapacityHealth{
          partition_count: partition_count,
          unhealthy_partition_count: unhealthy_partition_count
        }
      else
        {:error, Error.new(:invalid, "invalid runner capacity health query")}
      end
    end)
  end

  @impl true
  def list_release_drains(%Q.ListRunnerReleaseDrains{} = query) do
    read(fn ->
      if valid_platform_runner_read_context?(query.platform_context) and
           is_integer(query.limit) and query.limit in 1..1_024 do
        demands =
          Demand
          |> order_by([demand], asc: demand.runner_pool, asc: demand.required_runner_release_id)
          |> limit(^query.limit)
          |> Repo.all()

        active_runs = active_run_blocker_counts()
        pending_operations = pending_operation_blocker_counts()

        Enum.map(demands, fn demand ->
          partition = {demand.runner_pool, demand.required_runner_release_id}

          to_release_drain(
            demand,
            Map.get(active_runs, partition, 0),
            Map.get(pending_operations, partition, 0)
          )
        end)
      else
        {:error, Error.new(:invalid, "invalid runner release drain list query")}
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
      # :fenced (not :conflict) so runners can distinguish "this assignment
      # was fenced away, abandon it" from content conflicts they must not
      # blindly retry.
      Repo.rollback(Error.new(:fenced, "stale runner task assignment"))
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

  defp update_demand_for_status_change!(task, "queued", occurred_at)
       when task.status in @terminal_statuses do
    add_queued_demand!(
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

  defp active_run_blocker_count(runner_pool, release_id) do
    Run
    |> where(
      [run],
      run.status in ["pending", "running"] and
        fragment(
          """
          ?->'runner_releases'->>? = ?
          """,
          run.snapshot,
          ^runner_pool,
          ^release_id
        )
    )
    |> select([run], count(run.run_id))
    |> Repo.one()
  end

  defp pending_operation_blocker_count(runner_pool, release_id) do
    RebuildPlanAction
    |> where(
      [action],
      action.runner_pool == ^runner_pool and
        action.required_runner_release_id == ^release_id and
        action.status in ["planned", "running"]
    )
    |> select([action], count(action.target_id))
    |> Repo.one()
  end

  defp active_run_blocker_counts do
    sql = """
    SELECT binding.runner_pool, binding.required_runner_release_id, count(*)::bigint
    FROM favn_control.runs AS run
    CROSS JOIN LATERAL jsonb_each_text(run.snapshot->'runner_releases')
      AS binding(runner_pool, required_runner_release_id)
    WHERE run.status IN ('pending', 'running')
    GROUP BY binding.runner_pool, binding.required_runner_release_id
    """

    {:ok, %{rows: rows}} = SQL.query(Repo, sql, [])
    Map.new(rows, fn [pool, release_id, count] -> {{pool, release_id}, count} end)
  end

  defp pending_operation_blocker_counts do
    RebuildPlanAction
    |> where([action], action.status in ["planned", "running"])
    |> where(
      [action],
      not is_nil(action.runner_pool) and not is_nil(action.required_runner_release_id)
    )
    |> group_by([action], [action.runner_pool, action.required_runner_release_id])
    |> select(
      [action],
      {action.runner_pool, action.required_runner_release_id, count(action.target_id)}
    )
    |> Repo.all()
    |> Map.new(fn {pool, release_id, count} -> {{pool, release_id}, count} end)
  end

  defp to_release_drain(demand, active_run_count, pending_operation_count) do
    blocker_count = demand.outstanding_count + active_run_count + pending_operation_count

    %RunnerReleaseDrain{
      runner_pool: demand.runner_pool,
      required_runner_release_id: demand.required_runner_release_id,
      outstanding_task_count: demand.outstanding_count,
      active_run_count: active_run_count,
      pending_operation_count: pending_operation_count,
      blocker_count: blocker_count,
      updated_at: demand.updated_at,
      healthy?: demand.healthy,
      durable_drained?: demand.healthy and blocker_count == 0
    }
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
    task.task_id == command.task_id and
      task.domain_identity == command.domain_identity and
      task.task_kind == Atom.to_string(command.task_kind) and
      task.run_id == command.run_id and
      task.operation_id == command.operation_id and
      task.asset_step_id == command.asset_step_id and
      task.runner_pool == command.runner_pool and
      task.required_runner_release_id == command.required_runner_release_id and
      task.required_capability == command.required_capability and
      task.retry_class == Atom.to_string(command.retry_class) and
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

    with :ok <- validate_completion_deadline(task, command),
         true <- command.outcome in Favn.Contracts.RunnerTask.terminal_outcomes(),
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

  defp validate_completion_deadline(
         %RunnerTask{task_kind: "relation_inspection", deadline_at: deadline_at, status: status},
         command
       ) do
    if status != "cancelling" and DateTime.compare(command.occurred_at, deadline_at) != :gt,
      do: :ok,
      else: :error
  end

  defp validate_completion_deadline(_task, _command), do: :ok

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

  defp valid_platform_runner_read_context?(
         %FavnOrchestrator.Persistence.PlatformContext{roles: roles} = context
       ) do
    FavnOrchestrator.Persistence.PlatformContext.valid?(context) and
      Enum.any?(roles, &(&1 in [:platform_reader, :platform_operator, :platform_admin]))
  end

  defp valid_platform_runner_read_context?(_context), do: false

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

  defp valid_workspace_page_query?(query) do
    valid_statuses? =
      query.statuses == :all or
        (is_list(query.statuses) and
           Enum.all?(query.statuses, &(&1 in Map.values(@status_by_string))))

    is_integer(query.limit) and query.limit in 1..200 and valid_statuses? and
      valid_page_cursor?(query.cursor)
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

      now = database_now!()
      validate_command_window!(command.issued_at, now)
      command = canonicalize_enqueue_issued_at!(command, operation, scope_id)
      prune_command_receipts!(now)
      validate_command_window!(command.issued_at, now)
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
                 issued_at: command.issued_at,
                 inserted_at: now
               }
             ],
             on_conflict: :nothing
           ) do
        {1, _} ->
          result = fun.()

          receipt =
            encode_command_result(result, operation, scope_id, command.command_id, now)

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
            decode_command_result!(receipt, operation)
          else
            Repo.rollback(Error.new(:conflict, "runner task command identity was reused"))
          end
      end
    end)
  end

  defp command_hash(command) do
    command
    |> Map.put(:occurred_at, nil)
    |> :erlang.term_to_binary([:deterministic])
    |> then(&:crypto.hash(:sha256, &1))
  end

  defp command_scope(%{workspace_context: %{workspace_id: workspace_id}}),
    do: "workspace:" <> workspace_id

  defp command_scope(%{platform_context: _context}), do: "platform:runner_tasks"

  defp canonicalize_enqueue_issued_at!(
         %C.EnqueueRunnerTask{} = command,
         "enqueue",
         scope_id
       ) do
    workspace_id = command.workspace_context.workspace_id
    lock_enqueue_identity!(workspace_id, command.task_id)

    issued_at =
      case Repo.get_by(RunnerTaskCommand,
             scope_id: scope_id,
             command_id: command.command_id
           ) do
        %RunnerTaskCommand{issued_at: issued_at} ->
          issued_at

        nil ->
          case Repo.get_by(RunnerTask,
                 workspace_id: workspace_id,
                 task_id: command.task_id
               ) do
            %RunnerTask{enqueued_at: enqueued_at} -> enqueued_at
            nil -> command.issued_at
          end
      end

    %{command | issued_at: issued_at}
  end

  defp canonicalize_enqueue_issued_at!(command, _operation, _scope_id), do: command

  defp lock_enqueue_identity!(workspace_id, task_id) do
    lock_key = "runner-task-enqueue|#{byte_size(workspace_id)}:#{workspace_id}|#{task_id}"
    SQL.query!(Repo, "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))", [lock_key])
    :ok
  end

  defp encode_command_result(result, operation, scope_id, command_id, now) do
    result = normalize_command_result(result)

    unless valid_command_result?(operation, result) do
      Repo.rollback(Error.new(:internal, "runner task command returned an invalid result"))
    end

    case result do
      %RunnerTaskResult{} = task ->
        persist_task_snapshots!([task], scope_id, command_id, now)
        %{"kind" => "task_snapshots_v1", "count" => 1}

      tasks when is_list(tasks) ->
        persist_task_snapshots!(tasks, scope_id, command_id, now)
        %{"kind" => "task_snapshots_v1", "count" => length(tasks)}

      %RunnerCapacityDemand{} = demand ->
        encode_demand_receipt(demand)

      nil ->
        %{"kind" => "none"}

      atom when atom in [:persisted, :already_persisted] ->
        %{"kind" => "atom", "value" => Atom.to_string(atom)}
    end
  end

  defp decode_command_result!(
         %RunnerTaskCommand{
           scope_id: scope_id,
           command_id: command_id,
           result: %{"kind" => "task_snapshots_v1", "count" => count}
         },
         operation
       )
       when is_integer(count) and count >= 0 do
    results = load_task_snapshots!(scope_id, command_id, count)

    case {operation, results} do
      {"recover_expired", results} -> results
      {_single, [task]} -> task
      _invalid -> Repo.rollback(Error.new(:internal, "runner task command receipt is invalid"))
    end
  end

  defp decode_command_result!(
         %RunnerTaskCommand{result: %{"kind" => "demand_v1"} = receipt},
         "reconcile_demand"
       ),
       do: decode_demand_receipt!(receipt)

  defp decode_command_result!(%RunnerTaskCommand{result: %{"kind" => "none"}}, "claim"),
    do: nil

  defp decode_command_result!(
         %RunnerTaskCommand{result: %{"kind" => "atom", "value" => "persisted"}},
         "append_log_batch"
       ),
       do: :persisted

  defp decode_command_result!(
         %RunnerTaskCommand{result: %{"kind" => "atom", "value" => "already_persisted"}},
         "append_log_batch"
       ),
       do: :already_persisted

  defp decode_command_result!(_receipt, _operation),
    do: Repo.rollback(Error.new(:internal, "runner task command receipt is invalid"))

  defp persist_task_snapshots!(tasks, scope_id, command_id, now) do
    rows =
      tasks
      |> Enum.with_index()
      |> Enum.map(fn {%RunnerTaskResult{} = task, ordinal} ->
        row = fetch_task!(task.workspace_id, task.task_id)
        persisted = to_result(row)

        unless persisted == task do
          Repo.rollback(Error.new(:internal, "runner task receipt source changed in transaction"))
        end

        {snapshot, runtime_input_resolution_id} = task_snapshot(task, row, now)

        %{
          scope_id: scope_id,
          command_id: command_id,
          ordinal: ordinal,
          workspace_id: task.workspace_id,
          task_id: task.task_id,
          outcome_assignment_generation:
            if(is_nil(snapshot.outcome_hash), do: nil, else: task.assignment_generation),
          runtime_input_resolution_id: runtime_input_resolution_id,
          snapshot: :erlang.term_to_binary(snapshot, [:deterministic])
        }
      end)

    case Repo.insert_all(RunnerTaskCommandTask, rows) do
      {count, _} when count == length(rows) ->
        :ok

      _invalid ->
        Repo.rollback(Error.new(:internal, "runner task receipt snapshots were not stored"))
    end
  end

  defp task_snapshot(task, row, now) do
    outcome_hash =
      if task.status in @terminal_result_statuses do
        persist_task_outcome!(task, row, now)
      end

    {runtime_input_resolution_id, runtime_input_error_hash} =
      persist_runtime_input_error!(task, row, now)

    {%{
       version: 1,
       immutable_hash: result_hash(Map.take(task, @immutable_task_result_fields)),
       mutable: Map.take(task, @mutable_task_result_fields),
       outcome_hash: outcome_hash,
       runtime_input_error_hash: runtime_input_error_hash
     }, runtime_input_resolution_id}
  end

  defp persist_task_outcome!(task, row, now) do
    hash = result_hash({task.result_version, row.result, row.error})

    Repo.insert_all(
      RunnerTaskOutcome,
      [
        %{
          workspace_id: task.workspace_id,
          task_id: task.task_id,
          assignment_generation: task.assignment_generation,
          result_version: task.result_version,
          result: row.result,
          error: row.error,
          result_hash: hash,
          inserted_at: now
        }
      ],
      on_conflict: :nothing
    )

    outcome =
      Repo.get_by!(RunnerTaskOutcome,
        workspace_id: task.workspace_id,
        task_id: task.task_id,
        assignment_generation: task.assignment_generation
      )

    if outcome.result_hash == hash do
      hash
    else
      Repo.rollback(Error.new(:conflict, "runner task terminal outcome history conflict"))
    end
  end

  defp persist_runtime_input_error!(_task, %{runtime_input_error: nil}, _now),
    do: {nil, nil}

  defp persist_runtime_input_error!(task, row, now) do
    resolution_id = task.runtime_input_resolution_id

    unless is_binary(resolution_id) do
      Repo.rollback(Error.new(:internal, "runner task runtime-input error has no resolution"))
    end

    hash = result_hash(row.runtime_input_error)

    Repo.insert_all(
      RunnerTaskRuntimeInputError,
      [
        %{
          workspace_id: task.workspace_id,
          task_id: task.task_id,
          resolution_id: resolution_id,
          error: row.runtime_input_error,
          error_hash: hash,
          inserted_at: now
        }
      ],
      on_conflict: :nothing
    )

    outcome =
      Repo.get_by!(RunnerTaskRuntimeInputError,
        workspace_id: task.workspace_id,
        task_id: task.task_id,
        resolution_id: resolution_id
      )

    if outcome.error_hash == hash do
      {resolution_id, hash}
    else
      Repo.rollback(Error.new(:conflict, "runner task runtime-input error history conflict"))
    end
  end

  defp load_task_snapshots!(scope_id, command_id, expected_count) do
    rows =
      Repo.all(
        from(snapshot in RunnerTaskCommandTask,
          where: snapshot.scope_id == ^scope_id and snapshot.command_id == ^command_id,
          order_by: [asc: snapshot.ordinal]
        )
      )

    expected_ordinals =
      if expected_count == 0, do: [], else: Enum.to_list(0..(expected_count - 1))

    unless length(rows) == expected_count and
             Enum.map(rows, & &1.ordinal) == expected_ordinals do
      Repo.rollback(Error.new(:internal, "runner task command snapshot count is invalid"))
    end

    Enum.map(rows, &rehydrate_task_snapshot!/1)
  end

  defp rehydrate_task_snapshot!(row) do
    snapshot = decode_task_snapshot!(row.snapshot)
    current_row = fetch_task!(row.workspace_id, row.task_id)
    current = to_result(current_row)
    immutable = Map.take(current, @immutable_task_result_fields)

    unless result_hash(immutable) == snapshot.immutable_hash do
      Repo.rollback(Error.new(:conflict, "runner task immutable receipt fields changed"))
    end

    runtime_input_error = load_runtime_input_error!(row, snapshot)

    {result, error} =
      load_task_outcome!(row, snapshot, current.task_kind, snapshot.mutable.status)

    immutable
    |> Map.merge(snapshot.mutable)
    |> Map.put(:runtime_input_error, runtime_input_error)
    |> Map.put(:result, result)
    |> Map.put(:error, error)
    |> then(&struct!(RunnerTaskResult, &1))
  end

  defp decode_task_snapshot!(binary) do
    case :erlang.binary_to_term(binary, [:safe]) do
      %{
        version: 1,
        immutable_hash: immutable_hash,
        mutable: mutable,
        outcome_hash: outcome_hash,
        runtime_input_error_hash: runtime_input_error_hash
      } = snapshot
      when is_binary(immutable_hash) and is_map(mutable) and
             (is_nil(outcome_hash) or is_binary(outcome_hash)) and
             (is_nil(runtime_input_error_hash) or is_binary(runtime_input_error_hash)) ->
        snapshot

      _invalid ->
        Repo.rollback(Error.new(:internal, "runner task command snapshot is invalid"))
    end
  rescue
    _error -> Repo.rollback(Error.new(:internal, "runner task command snapshot is invalid"))
  end

  defp load_task_outcome!(_row, %{outcome_hash: nil}, _task_kind, _status), do: {nil, nil}

  defp load_task_outcome!(row, snapshot, task_kind, status) do
    outcome =
      Repo.get_by!(RunnerTaskOutcome,
        workspace_id: row.workspace_id,
        task_id: row.task_id,
        assignment_generation: snapshot.mutable.assignment_generation
      )

    unless outcome.result_hash == snapshot.outcome_hash and
             result_hash({outcome.result_version, outcome.result, outcome.error}) ==
               snapshot.outcome_hash do
      Repo.rollback(Error.new(:conflict, "runner task terminal receipt outcome changed"))
    end

    {decode_result!(task_kind, status, outcome.result), outcome.error}
  end

  defp load_runtime_input_error!(_row, %{runtime_input_error_hash: nil}), do: nil

  defp load_runtime_input_error!(row, snapshot) do
    outcome =
      Repo.get_by!(RunnerTaskRuntimeInputError,
        workspace_id: row.workspace_id,
        task_id: row.task_id,
        resolution_id: row.runtime_input_resolution_id
      )

    unless outcome.error_hash == snapshot.runtime_input_error_hash and
             result_hash(outcome.error) == snapshot.runtime_input_error_hash do
      Repo.rollback(Error.new(:conflict, "runner task runtime-input receipt changed"))
    end

    outcome.error
  end

  defp result_hash(value),
    do: :crypto.hash(:sha256, :erlang.term_to_binary(value, [:deterministic]))

  defp encode_demand_receipt(demand) do
    %{
      "kind" => "demand_v1",
      "runner_pool" => demand.runner_pool,
      "required_runner_release_id" => demand.required_runner_release_id,
      "outstanding_count" => demand.outstanding_count,
      "queued_count" => demand.queued_count,
      "active_count" => demand.active_count,
      "oldest_queued_at" => encode_optional_datetime(demand.oldest_queued_at),
      "version" => demand.version,
      "updated_at" => DateTime.to_iso8601(demand.updated_at),
      "healthy" => demand.healthy?
    }
  end

  defp decode_demand_receipt!(receipt) do
    struct!(RunnerCapacityDemand,
      runner_pool: receipt["runner_pool"],
      required_runner_release_id: receipt["required_runner_release_id"],
      outstanding_count: receipt["outstanding_count"],
      queued_count: receipt["queued_count"],
      active_count: receipt["active_count"],
      oldest_queued_at: decode_optional_datetime!(receipt["oldest_queued_at"]),
      version: receipt["version"],
      updated_at: decode_datetime!(receipt["updated_at"]),
      healthy?: receipt["healthy"]
    )
  rescue
    _error -> Repo.rollback(Error.new(:internal, "runner task demand receipt is invalid"))
  end

  defp encode_optional_datetime(nil), do: nil
  defp encode_optional_datetime(%DateTime{} = datetime), do: DateTime.to_iso8601(datetime)

  defp decode_optional_datetime!(nil), do: nil
  defp decode_optional_datetime!(value), do: decode_datetime!(value)

  defp decode_datetime!(value) when is_binary(value) do
    case DateTime.from_iso8601(value) do
      {:ok, datetime, 0} -> datetime
      _invalid -> raise ArgumentError, "invalid receipt datetime"
    end
  end

  defp normalize_command_result(%RunnerTask{} = task), do: to_result(task)
  defp normalize_command_result(%Demand{} = demand), do: to_demand(demand)
  defp normalize_command_result(result), do: result

  defp valid_command_result?("enqueue", %RunnerTaskResult{}), do: true
  defp valid_command_result?("claim", result), do: is_nil(result) or task_result?(result)
  defp valid_command_result?("transition", result), do: task_result?(result)
  defp valid_command_result?("runtime_inputs", result), do: task_result?(result)

  defp valid_command_result?("append_log_batch", result),
    do: result in [:persisted, :already_persisted]

  defp valid_command_result?("complete", result), do: task_result?(result)
  defp valid_command_result?("request_cancellation", result), do: task_result?(result)
  defp valid_command_result?("acknowledge_cancellation", result), do: task_result?(result)
  defp valid_command_result?("release", result), do: task_result?(result)
  defp valid_command_result?("retry", result), do: task_result?(result)

  defp valid_command_result?("recover_expired", results) when is_list(results),
    do: Enum.all?(results, &task_result?/1)

  defp valid_command_result?("reconcile_demand", %RunnerCapacityDemand{}), do: true
  defp valid_command_result?(_operation, _result), do: false

  defp task_result?(%RunnerTaskResult{}), do: true
  defp task_result?(_result), do: false

  defp validate_command_window!(%DateTime{} = issued_at, now) do
    oldest = DateTime.add(now, -@receipt_retention_ms, :millisecond)
    newest = DateTime.add(now, @maximum_future_clock_skew_ms, :millisecond)

    if DateTime.compare(issued_at, oldest) == :lt or
         DateTime.compare(issued_at, newest) == :gt do
      Repo.rollback(Error.new(:invalid, "runner task command is outside the idempotency window"))
    end
  end

  defp validate_command_window!(_issued_at, _now),
    do: Repo.rollback(Error.new(:invalid, "invalid runner task command issued-at timestamp"))

  defp prune_command_receipts!(now) do
    cutoff = DateTime.add(now, -@receipt_retention_ms, :millisecond)

    SQL.query!(
      Repo,
      """
      DELETE FROM favn_control.runner_task_commands
      WHERE ctid IN (
        SELECT ctid
        FROM favn_control.runner_task_commands
        WHERE inserted_at < $1
        ORDER BY inserted_at
        LIMIT $2
        FOR UPDATE SKIP LOCKED
      )
      """,
      [cutoff, @receipt_prune_limit]
    )

    SQL.query!(
      Repo,
      """
      DELETE FROM favn_control.runner_task_outcomes
      WHERE ctid IN (
        SELECT outcome.ctid
        FROM favn_control.runner_task_outcomes AS outcome
        WHERE outcome.inserted_at < $1
          AND NOT EXISTS (
            SELECT 1
            FROM favn_control.runner_task_command_tasks AS snapshot
            WHERE snapshot.outcome_assignment_generation IS NOT NULL
              AND snapshot.workspace_id = outcome.workspace_id
              AND snapshot.task_id = outcome.task_id
              AND snapshot.outcome_assignment_generation = outcome.assignment_generation
          )
        ORDER BY outcome.inserted_at
        LIMIT $2
        FOR UPDATE SKIP LOCKED
      )
      """,
      [cutoff, @receipt_prune_limit]
    )

    SQL.query!(
      Repo,
      """
      DELETE FROM favn_control.runner_task_runtime_input_errors
      WHERE ctid IN (
        SELECT outcome.ctid
        FROM favn_control.runner_task_runtime_input_errors AS outcome
        WHERE outcome.inserted_at < $1
          AND NOT EXISTS (
            SELECT 1
            FROM favn_control.runner_task_command_tasks AS snapshot
            WHERE snapshot.runtime_input_resolution_id IS NOT NULL
              AND snapshot.workspace_id = outcome.workspace_id
              AND snapshot.task_id = outcome.task_id
              AND snapshot.runtime_input_resolution_id = outcome.resolution_id
          )
        ORDER BY outcome.inserted_at
        LIMIT $2
        FOR UPDATE SKIP LOCKED
      )
      """,
      [cutoff, @receipt_prune_limit]
    )
  end

  defp database_now! do
    %{rows: [[now]]} = SQL.query!(Repo, "SELECT clock_timestamp()", [])
    now
  end

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

  defp to_operator_result(%RunnerTask{} = task) do
    task
    |> Map.from_struct()
    |> Map.take(Map.keys(%RunnerTaskResult{}))
    |> Map.put(:task_kind, task_kind!(task.task_kind))
    |> Map.put(:retry_class, Map.fetch!(@retry_class_by_string, task.retry_class))
    |> Map.put(:status, Map.fetch!(@status_by_string, task.status))
    |> Map.put(:payload, nil)
    |> Map.put(:orchestration_context, nil)
    |> Map.put(:result, nil)
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
