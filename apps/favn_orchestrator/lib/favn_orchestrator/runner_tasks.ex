defmodule FavnOrchestrator.RunnerTasks do
  @moduledoc """
  Typed control-plane facade for durable runner tasks.

  Live BEAM sessions are advisory. Every assignment, transition, log batch, and
  terminal result is accepted only through the fenced persistence store.
  """

  alias Favn.Contracts.RunnerTask.Assignment
  alias Favn.Contracts.RunnerTask.CancellationAck
  alias Favn.Contracts.RunnerTask.ClaimRequest
  alias Favn.Contracts.RunnerTask.LeaseRenewal
  alias Favn.Contracts.RunnerTask.LogAck
  alias Favn.Contracts.RunnerTask.LogBatch
  alias Favn.Contracts.RunnerTask.NoWork
  alias Favn.Contracts.RunnerTask.PersistenceCodec
  alias Favn.Contracts.RunnerTask.Result
  alias Favn.Contracts.RunnerTask.ResultAck
  alias Favn.Contracts.RunnerTask.RuntimeInputsAck
  alias Favn.Contracts.RunnerTask.RuntimeInputsResolved
  alias Favn.Contracts.RunnerTask.Started
  alias FavnOrchestrator.Persistence
  alias FavnOrchestrator.Persistence.Commands, as: C
  alias FavnOrchestrator.Persistence.Queries, as: Q
  alias FavnOrchestrator.Persistence.SystemContext
  alias FavnOrchestrator.CancellationOutcome
  alias FavnOrchestrator.RunnerQueueCoordinator
  alias FavnOrchestrator.RunnerRegistry
  alias FavnOrchestrator.RunnerTaskResultRouter

  @default_lease_ms 30_000
  @empty_wait_ms 30_000
  @max_wake_race_retries 2
  @cancellation_ack_wait_ms 1_000
  @cancellation_poll_ms 20

  def enqueue(%C.EnqueueRunnerTask{} = command) do
    with {:ok, task} <- store().enqueue(command) do
      if task.status == :queued do
        RunnerQueueCoordinator.notify(
          task.runner_pool,
          task.required_runner_release_id,
          1
        )
      end

      {:ok, task}
    end
  end

  @doc false
  def retry_safe(%FavnOrchestrator.Persistence.WorkspaceContext{} = context, task) do
    with {:ok, retried} <-
           store().retry(%C.RetryRunnerTask{
             workspace_context: context,
             command_id:
               "retry:#{task.task_id}:#{task.assignment_generation}:#{task.result_version}",
             task_id: task.task_id,
             expected_assignment_generation: task.assignment_generation,
             expected_result_version: task.result_version,
             issued_at: task.terminal_at,
             occurred_at: DateTime.utc_now()
           }) do
      RunnerQueueCoordinator.notify(
        retried.runner_pool,
        retried.required_runner_release_id,
        1
      )

      {:ok, retried}
    end
  end

  def fetch_manifest(%Assignment{} = assignment) do
    with :ok <- Assignment.validate(assignment),
         {:ok, task} <-
           store().get(%Q.GetRunnerTask{
             workspace_context: workspace_context(assignment.workspace_id, :runner_task_manifest),
             task_id: assignment.task_id
           }),
         true <- assignment_matches_task?(assignment, task),
         {:ok, manifest_version_id, manifest_content_hash, required_runner_release_id} <-
           manifest_identity(task.task_kind, task.payload),
         true <-
           is_nil(required_runner_release_id) or
             required_runner_release_id == assignment.required_runner_release_id,
         {:ok, version} <-
           FavnOrchestrator.ManifestStore.get_manifest(
             SystemContext.platform(:runner_task_manifest),
             manifest_version_id
           ),
         true <- version.content_hash == manifest_content_hash do
      {:ok, version}
    else
      false -> {:error, :stale_runner_task_assignment}
      _other -> {:error, :runner_task_manifest_unavailable}
    end
  end

  def fetch(workspace_id, task_id) when is_binary(workspace_id) and is_binary(task_id) do
    store().get(%Q.GetRunnerTask{
      workspace_context: workspace_context(workspace_id, :runner_task_fetch),
      task_id: task_id
    })
  end

  @doc false
  def verify_registration_resume(%Favn.Contracts.RunnerTask.Registration{
        active_assignment: nil
      }),
      do: :ok

  def verify_registration_resume(%Favn.Contracts.RunnerTask.Registration{} = registration) do
    assignment = registration.active_assignment

    case assignment do
      %{workspace_id: workspace_id, task_id: task_id, assignment_generation: generation}
      when is_binary(workspace_id) and is_binary(task_id) and is_integer(generation) ->
        case fetch(workspace_id, task_id) do
          {:ok, task} ->
            if task.status in [:assigned, :preparing, :running, :cancelling] and
                 task.assigned_runner_instance_id == registration.runner_instance_id and
                 task.assigned_runner_session_generation ==
                   registration.runner_session_generation and
                 task.assignment_generation == generation and
                 task.runner_pool == registration.runner_pool and
                 task.required_runner_release_id == registration.required_runner_release_id do
              :ok
            else
              {:error, :stale_runner_task_resume}
            end

          {:error, %{kind: :not_found}} ->
            {:error, :stale_runner_task_resume}

          {:error, reason} ->
            {:error, {:runner_task_resume_verification_failed, reason}}
        end

      _invalid ->
        {:error, :stale_runner_task_resume}
    end
  end

  def claim(%ClaimRequest{} = request) do
    with :ok <- ClaimRequest.validate(request),
         {:ok, claim_state, session} <- RunnerRegistry.begin_claim(request) do
      case claim_state do
        :start -> do_claim(request, session, 0)
        {:duplicate, :in_flight} -> {:error, :claim_in_flight}
        {:duplicate, outcome} -> {:ok, outcome}
      end
    end
  end

  def started(%Started{} = message) do
    with :ok <- Started.validate(message),
         {:ok, task} <-
           store().transition(%C.TransitionRunnerTask{
             workspace_context: workspace_context(message.workspace_id, :runner_task_started),
             command_id: command_id("started", message),
             task_id: message.task_id,
             runner_instance_id: message.runner_instance_id,
             runner_session_generation: message.runner_session_generation,
             assignment_generation: message.assignment_generation,
             transition: :running,
             issued_at: message.issued_at,
             occurred_at: message.occurred_at
           }) do
      maybe_mark_busy(message, task)
      emit_started(task)

      {:ok, task}
    end
  end

  def renew(%LeaseRenewal{} = message) do
    with :ok <- LeaseRenewal.validate(message) do
      duration = lease_ms()
      occurred_at = DateTime.add(message.lease_expires_at, -duration, :millisecond)

      store().transition(%C.TransitionRunnerTask{
        workspace_context: workspace_context(message.workspace_id, :runner_task_renew),
        command_id: renewal_command_id(message),
        task_id: message.task_id,
        runner_instance_id: message.runner_instance_id,
        runner_session_generation: message.runner_session_generation,
        assignment_generation: message.assignment_generation,
        transition: :renew,
        lease_duration_ms: duration,
        issued_at: occurred_at,
        occurred_at: occurred_at
      })
    end
  end

  def runtime_inputs_resolved(%RuntimeInputsResolved{} = message) do
    with :ok <- RuntimeInputsResolved.validate(message),
         {:ok, pin, fingerprint} <- runtime_input_pin(message),
         {:ok, _task} <-
           store().persist_runtime_inputs(%C.PersistRunnerTaskRuntimeInputs{
             workspace_context:
               workspace_context(message.workspace_id, :runner_task_runtime_inputs),
             command_id: command_id("runtime-inputs", message),
             task_id: message.task_id,
             runner_instance_id: message.runner_instance_id,
             runner_session_generation: message.runner_session_generation,
             assignment_generation: message.assignment_generation,
             resolution_id: message.resolution_id,
             status: message.status,
             payload_fingerprint: fingerprint,
             runtime_input_pin: pin,
             error: message.error,
             issued_at: message.issued_at,
             occurred_at: DateTime.utc_now()
           }) do
      {:ok,
       %RuntimeInputsAck{
         workspace_id: message.workspace_id,
         task_id: message.task_id,
         runner_instance_id: message.runner_instance_id,
         runner_session_generation: message.runner_session_generation,
         assignment_generation: message.assignment_generation,
         resolution_id: message.resolution_id,
         payload_fingerprint: fingerprint,
         status: :persisted
       }}
    end
  end

  def append_logs(%LogBatch{} = message) do
    with :ok <- LogBatch.validate(message),
         {:ok, hash} <- PersistenceCodec.hash_term(message.entries),
         {:ok, _persisted} <-
           store().append_log_batch(%C.AppendRunnerTaskLogBatch{
             workspace_context: workspace_context(message.workspace_id, :runner_task_logs),
             command_id:
               "log:#{message.task_id}:#{message.assignment_generation}:#{message.batch_id}",
             task_id: message.task_id,
             runner_instance_id: message.runner_instance_id,
             runner_session_generation: message.runner_session_generation,
             assignment_generation: message.assignment_generation,
             batch_id: message.batch_id,
             sequence: message.sequence,
             entries: message.entries,
             payload_hash: hash,
             issued_at: message.issued_at,
             occurred_at: DateTime.utc_now()
           }) do
      {:ok,
       %LogAck{
         workspace_id: message.workspace_id,
         task_id: message.task_id,
         runner_instance_id: message.runner_instance_id,
         runner_session_generation: message.runner_session_generation,
         assignment_generation: message.assignment_generation,
         batch_id: message.batch_id,
         sequence: message.sequence
       }}
    end
  end

  def complete(%Result{} = message) do
    with :ok <- Result.validate(message),
         {:ok, encoded_result} <-
           PersistenceCodec.encode_result(message.task_kind, message.outcome, message.result),
         {:ok, task} <-
           store().complete(%C.CompleteRunnerTask{
             workspace_context: workspace_context(message.workspace_id, :runner_task_result),
             command_id:
               "result:#{message.task_id}:#{message.assignment_generation}:#{message.result_version}",
             task_id: message.task_id,
             runner_instance_id: message.runner_instance_id,
             runner_session_generation: message.runner_session_generation,
             assignment_generation: message.assignment_generation,
             result_version: message.result_version,
             outcome: message.outcome,
             retry_class: message.retry_class,
             result: encoded_result,
             error: message.error,
             issued_at: message.finished_at,
             occurred_at: message.finished_at
           }) do
      maybe_mark_idle(message)

      RunnerTaskResultRouter.notify(task)

      {:ok,
       %ResultAck{
         workspace_id: message.workspace_id,
         task_id: message.task_id,
         runner_instance_id: message.runner_instance_id,
         runner_session_generation: message.runner_session_generation,
         assignment_generation: message.assignment_generation,
         result_version: message.result_version,
         status: :persisted
       }}
    end
  end

  def request_cancellation(workspace_id, task_id, reason) do
    now = DateTime.utc_now()

    with {:ok, task} <-
           store().request_cancellation(%C.RequestRunnerTaskCancellation{
             workspace_context: workspace_context(workspace_id, :runner_task_cancellation),
             command_id: "cancel:#{task_id}:#{System.unique_integer([:positive, :monotonic])}",
             task_id: task_id,
             reason: reason,
             issued_at: now,
             occurred_at: now
           }) do
      delivery = maybe_send_cancellation(task, reason, now)
      if task.status == :cancelled, do: RunnerTaskResultRouter.notify(task)

      cancellation_outcome(task, delivery)
    else
      {:error, reason} ->
        CancellationOutcome.from_runner_result(task_id, {:error, reason})
    end
  end

  def acknowledge_cancellation(%CancellationAck{status: :observed} = message) do
    with :ok <- CancellationAck.validate(message),
         {:ok, _task} <-
           store().acknowledge_cancellation(%C.AcknowledgeRunnerTaskCancellation{
             workspace_context:
               workspace_context(message.workspace_id, :runner_task_cancellation_ack),
             command_id: command_id("cancel-ack", message),
             task_id: message.task_id,
             runner_instance_id: message.runner_instance_id,
             runner_session_generation: message.runner_session_generation,
             assignment_generation: message.assignment_generation,
             issued_at: message.issued_at,
             occurred_at: message.acknowledged_at
           }) do
      {:ok, message}
    end
  end

  def acknowledge_cancellation(%CancellationAck{} = message) do
    with :ok <- CancellationAck.validate(message) do
      {:ok, message}
    end
  end

  defp do_claim(request, session, attempt) do
    observed =
      RunnerQueueCoordinator.generation(
        request.runner_pool,
        request.required_runner_release_id
      )

    command = %C.ClaimRunnerTask{
      platform_context: platform_context(:runner_task_claim),
      command_id: store_claim_id(request.command_id, attempt),
      runner_instance_id: request.runner_instance_id,
      runner_session_generation: request.runner_session_generation,
      runner_pool: request.runner_pool,
      required_runner_release_id: request.required_runner_release_id,
      supported_task_kinds: request.supported_task_kinds,
      capabilities: request.capabilities,
      lease_duration_ms: lease_ms(),
      issued_at: request.issued_at,
      occurred_at: DateTime.utc_now()
    }

    case store().claim(command) do
      {:ok, nil} ->
        wait_or_retry(request, session, observed, attempt)

      {:ok, task} ->
        assignment = assignment(request, task)
        {:ok, _session} = RunnerRegistry.finish_claim(request, assignment)
        {:ok, assignment}

      {:error, reason} ->
        _ = RunnerRegistry.finish_claim(request, no_work(request, :wait, 0))
        {:error, reason}
    end
  end

  defp wait_or_retry(request, session, observed, attempt)
       when attempt < @max_wake_race_retries do
    case RunnerQueueCoordinator.wait(
           request.runner_pool,
           request.required_runner_release_id,
           observed,
           session
         ) do
      :retry -> do_claim(request, session, attempt + 1)
      :waiting -> finish_empty_claim(request)
    end
  end

  defp wait_or_retry(request, _session, _observed, _attempt), do: finish_empty_claim(request)

  defp finish_empty_claim(request) do
    wait_ms =
      case FavnOrchestrator.RunnerPools.fetch(
             FavnOrchestrator.RuntimeConfig.runner_pools(),
             request.runner_pool
           ) do
        {:ok, %{mode: :elastic, idle_grace_ms: idle_grace_ms}} -> idle_grace_ms
        {:ok, %{mode: :resident}} -> @empty_wait_ms
        {:error, _reason} -> @empty_wait_ms
      end

    no_work = no_work(request, :wait, wait_ms)
    {:ok, _session} = RunnerRegistry.finish_claim(request, no_work)
    {:ok, no_work}
  end

  defp assignment(request, task) do
    %Assignment{
      command_id: request.command_id,
      workspace_id: task.workspace_id,
      task_id: task.task_id,
      task_kind: task.task_kind,
      runner_instance_id: request.runner_instance_id,
      runner_session_generation: request.runner_session_generation,
      assignment_generation: task.assignment_generation,
      runner_pool: task.runner_pool,
      required_runner_release_id: task.required_runner_release_id,
      assigned_at: DateTime.add(task.assignment_expires_at, -lease_ms(), :millisecond),
      lease_expires_at: task.assignment_expires_at,
      retry_class: task.retry_class,
      payload: task.payload
    }
  end

  defp no_work(request, action, wait_ms) do
    %NoWork{
      command_id: request.command_id,
      runner_instance_id: request.runner_instance_id,
      runner_session_generation: request.runner_session_generation,
      action: action,
      wait_ms: wait_ms
    }
  end

  defp runtime_input_pin(%RuntimeInputsResolved{status: :resolved} = message) do
    with {:ok, pin} <-
           FavnOrchestrator.RuntimeInputPins.pin_for_resolution(
             message.workspace_id,
             message.task_id,
             message.runtime_inputs
           ) do
      case Base.decode16(pin.payload_fingerprint, case: :mixed) do
        {:ok, fingerprint} when byte_size(fingerprint) == 32 -> {:ok, pin, fingerprint}
        _other -> {:error, :invalid_runtime_input_payload_fingerprint}
      end
    end
  end

  defp runtime_input_pin(%RuntimeInputsResolved{status: :failed}), do: {:ok, nil, nil}

  defp store, do: Persistence.stores().runner_tasks

  defp lease_ms,
    do: Application.get_env(:favn_orchestrator, :runner_task_lease_ms, @default_lease_ms)

  defp platform_context(purpose),
    do: SystemContext.platform(purpose, roles: [:platform_operator])

  defp workspace_context(workspace_id, purpose),
    do: SystemContext.workspace(workspace_id, purpose)

  defp store_claim_id(command_id, attempt), do: "#{command_id}:#{attempt}"

  defp command_id(prefix, message),
    do: "#{prefix}:#{message.task_id}:#{message.assignment_generation}"

  defp renewal_command_id(message) do
    digest =
      :crypto.hash(
        :sha256,
        [
          message.task_id,
          0,
          Integer.to_string(message.assignment_generation),
          0,
          DateTime.to_iso8601(message.lease_expires_at)
        ]
      )
      |> Base.url_encode64(padding: false)

    "renew:" <> digest
  end

  defp assignment_key(task),
    do: %{
      workspace_id: task.workspace_id,
      task_id: task.task_id,
      assignment_generation: task.assignment_generation
    }

  defp assignment_matches_task?(assignment, task) do
    task.workspace_id == assignment.workspace_id and task.task_id == assignment.task_id and
      task.assigned_runner_instance_id == assignment.runner_instance_id and
      task.assigned_runner_session_generation == assignment.runner_session_generation and
      task.assignment_generation == assignment.assignment_generation and
      task.runner_pool == assignment.runner_pool and
      task.required_runner_release_id == assignment.required_runner_release_id and
      task.status in [:assigned, :preparing, :running, :cancelling]
  end

  defp manifest_identity(:asset_attempt, %Favn.Contracts.RunnerWork{} = work),
    do:
      {:ok, work.manifest_version_id, work.manifest_content_hash, work.required_runner_release_id}

  defp manifest_identity(
         :relation_inspection,
         %Favn.Contracts.RelationInspectionRequest{} = request
       ),
       do:
         {:ok, request.manifest_version_id, request.manifest_content_hash,
          request.required_runner_release_id}

  defp manifest_identity(
         kind,
         %{manifest: %Favn.Manifest.Version{} = version}
       )
       when kind in [:generation_capabilities, :generation_marker_read],
       do: {:ok, version.manifest_version_id, version.content_hash, nil}

  defp manifest_identity(
         :generation_reconcile,
         %Favn.Contracts.GenerationReconciliationRequest{activation: activation}
       ),
       do: manifest_identity(:generation_activate, activation)

  defp manifest_identity(
         kind,
         %{manifest_version_id: manifest_version_id, manifest_content_hash: content_hash} =
           request
       )
       when kind in [
              :generation_marker_initialize,
              :generation_activate,
              :generation_discard
            ],
       do: {:ok, manifest_version_id, content_hash, request.required_runner_release_id}

  defp manifest_identity(kind, payload),
    do: {:error, {:invalid_runner_task_manifest_identity, kind, payload}}

  defp maybe_send_cancellation(
         %{
           status: status,
           assigned_runner_instance_id: runner_id,
           assigned_runner_session_generation: session_generation
         } = task,
         reason,
         now
       )
       when status in [:assigned, :preparing, :running, :cancelling] do
    case runner_session(runner_id) do
      {:ok, %{session_generation: ^session_generation, agent_pid: agent_pid}} ->
        message = %Favn.Contracts.RunnerTask.Cancellation{
          workspace_id: task.workspace_id,
          task_id: task.task_id,
          runner_instance_id: runner_id,
          runner_session_generation: session_generation,
          assignment_generation: task.assignment_generation,
          command_id: "cancel:#{task.task_id}",
          reason: reason,
          requested_at: now
        }

        send(agent_pid, {:favn_runner_task, message})
        :sent

      _other ->
        :unavailable
    end
  end

  defp maybe_send_cancellation(_task, _reason, _now), do: :terminal

  defp cancellation_outcome(%{status: :cancelled} = task, _delivery),
    do: cancellation_outcome(task, :acknowledged, :cancelled)

  defp cancellation_outcome(%{status: status} = task, _delivery)
       when status in [:succeeded, :failed, :unknown],
       do: cancellation_outcome(task, :already_completed, status)

  defp cancellation_outcome(task, :sent) do
    deadline = System.monotonic_time(:millisecond) + cancellation_ack_wait_ms()
    await_cancellation_ack(task, deadline)
  end

  defp cancellation_outcome(task, _delivery),
    do: cancellation_outcome(task, :requested, task.status)

  defp await_cancellation_ack(task, deadline) do
    case fetch(task.workspace_id, task.task_id) do
      {:ok, %{cancellation_acknowledged_at: %DateTime{}} = acknowledged} ->
        cancellation_outcome(acknowledged, :acknowledged, acknowledged.status)

      {:ok, %{status: :cancelled} = cancelled} ->
        cancellation_outcome(cancelled, :acknowledged, :cancelled)

      {:ok, %{status: status} = completed} when status in [:succeeded, :failed, :unknown] ->
        cancellation_outcome(completed, :already_completed, status)

      _result ->
        if System.monotonic_time(:millisecond) < deadline do
          Process.sleep(@cancellation_poll_ms)
          await_cancellation_ack(task, deadline)
        else
          cancellation_outcome(task, :requested, task.status)
        end
    end
  end

  defp cancellation_outcome(task, status, runner_status) do
    %CancellationOutcome{
      task_id: task.task_id,
      status: status,
      runner_status: runner_status
    }
  end

  defp cancellation_ack_wait_ms do
    Application.get_env(
      :favn_orchestrator,
      :runner_task_cancellation_ack_wait_ms,
      @cancellation_ack_wait_ms
    )
  end

  defp maybe_mark_busy(message, task) do
    if Process.whereis(RunnerRegistry) do
      RunnerRegistry.mark_busy(
        message.runner_instance_id,
        message.runner_session_generation,
        assignment_key(task)
      )
    else
      :ok
    end
  end

  defp maybe_mark_idle(message) do
    if Process.whereis(RunnerRegistry) do
      RunnerRegistry.mark_idle(
        message.runner_instance_id,
        message.runner_session_generation
      )
    else
      :ok
    end
  end

  defp emit_started(task) do
    :telemetry.execute(
      [:favn, :runner_task, :started],
      %{count: 1},
      %{
        workspace_id: task.workspace_id,
        task_id: task.task_id,
        runner_pool: task.runner_pool,
        required_runner_release_id: task.required_runner_release_id,
        runner_instance_id: task.assigned_runner_instance_id,
        assignment_generation: task.assignment_generation
      }
    )
  end

  defp runner_session(runner_id) do
    if Process.whereis(RunnerRegistry),
      do: RunnerRegistry.fetch(runner_id),
      else: {:error, :runner_registry_unavailable}
  end
end
