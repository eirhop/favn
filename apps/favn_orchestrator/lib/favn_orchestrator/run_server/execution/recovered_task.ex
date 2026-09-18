defmodule FavnOrchestrator.RunServer.Execution.RecoveredTask do
  @moduledoc false

  alias Favn.Contracts.RunnerWork
  alias FavnOrchestrator.CancellationOutcome
  alias FavnOrchestrator.ExecutionAdmission
  alias FavnOrchestrator.RunnerTasks
  alias FavnOrchestrator.RunServer.Execution.StageEntry
  alias FavnOrchestrator.RunState
  alias FavnOrchestrator.Storage.JsonSafe

  @terminal [:succeeded, :failed, :cancelled, :unknown]

  @doc false
  @spec await_failure_reason(term()) :: map()
  def await_failure_reason(reason) do
    reason |> JsonSafe.error() |> Map.take(~w(kind type message reason redacted truncated))
  end

  @identity_fields [:task_id, :payload_hash, :orchestration_context_hash]
  @receipt_fields [:assignment_generation, :result_version, :status]

  @doc false
  def evidence(task) do
    fields =
      if task.status in @terminal, do: @identity_fields ++ @receipt_fields, else: @identity_fields

    Map.take(task, fields)
  end

  @doc false
  def verify_evidence(task, saved), do: Map.take(task, Map.keys(saved)) == saved

  @doc false
  @spec validate_terminal_read(map(), map()) :: :ok | {:error, atom()}
  def validate_terminal_read(%{data_state: :available, payload: %RunnerWork{}} = task, saved) do
    valid_result? =
      match?(%Favn.Contracts.RunnerResult{}, task.result) or
        (task.status in [:failed, :cancelled, :unknown] and is_nil(task.result))

    if verify_evidence(task, saved) and valid_result?,
      do: :ok,
      else: {:error, :recovered_terminal_evidence_mismatch}
  end

  def validate_terminal_read(_task, _saved), do: {:error, :recovered_terminal_data_unavailable}

  @spec entry(RunState.t(), map()) ::
          {:ok, StageEntry.t()} | {:error, term()}
  def entry(
        %RunState{id: run_id},
        %{
          task_id: task_id,
          payload:
            %RunnerWork{
              asset_step_id: asset_step_id,
              attempt: attempt,
              stage: stage
            } = work
        } = task
      )
      when is_binary(task_id) and is_binary(asset_step_id) and is_integer(attempt) and
             attempt > 0 and is_integer(stage) and stage >= 0 do
    context = if is_map(task.orchestration_context), do: task.orchestration_context, else: %{}
    asset_ref = RunnerWork.asset_ref(work)
    node_key = RunnerWork.node_key(work)

    if is_tuple(asset_ref) and is_tuple(node_key) do
      {:ok,
       %{
         run_id: run_id,
         asset_step_id: asset_step_id,
         asset_ref: asset_ref,
         node_key: node_key,
         window: RunnerWork.window(work),
         task_id: task_id,
         deadline_at: work.deadline_at,
         assignment_generation: task.assignment_generation,
         runner_pool: task.runner_pool,
         required_runner_release_id: task.required_runner_release_id,
         decision: Map.get(context, :decision, %{}),
         attempt: attempt,
         stage: stage,
         lease: nil,
         materialization_claim: Map.get(context, :materialization_claim),
         execution_pool: RunnerWork.execution_pool(work),
         resource_circuit_permits: Map.get(context, :resource_circuit_permits, []),
         freshness_key: Map.get(context, :freshness_key)
       }
       |> then(fn entry ->
         if Map.get(task, :recovery_outcome),
           do: Map.put(entry, :recovered_outcome, task.recovery_outcome),
           else: entry
       end)}
    else
      {:error, {:invalid_recovered_runner_task, task_id}}
    end
  end

  def entry(_run, task) when is_map(task),
    do: {:error, {:invalid_recovered_runner_task, Map.get(task, :task_id)}}

  def entry(_run, _task), do: {:error, {:invalid_recovered_runner_task, nil}}

  @spec reconcile(RunState.t(), map(), map()) :: {:ok, map()} | {:error, term()}
  def reconcile(run, %{payload: %RunnerWork{} = work} = task, entry) do
    cond do
      work.run_id != run.id or work.manifest_version_id != run.manifest_version_id or
          work.manifest_content_hash != run.manifest_content_hash ->
        {:error, :recovered_task_identity_mismatch}

      task.status in @terminal ->
        with :ok <- ExecutionAdmission.release_completed(run, task.task_id),
             do:
               {:ok,
                entry
                |> Map.put(:lease, nil)
                |> Map.put(:terminal_task?, true)
                |> Map.put(:terminal_evidence, evidence(task))}

      not is_struct(work.deadline_at, DateTime) ->
        {:error, :recovered_task_deadline_missing}

      DateTime.compare(work.deadline_at, DateTime.utc_now()) != :gt ->
        cancel_expired(run, task, entry)

      RunState.execution_mode(run) == :pipeline ->
        with {:ok, lease} <- ExecutionAdmission.adopt(run, entry),
             do: {:ok, Map.put(entry, :lease, lease)}

      true ->
        {:ok, entry}
    end
  end

  def reconcile(_run, _task, _entry), do: {:error, :recovered_task_data_unavailable}

  @doc false
  def settlement(%{recovered_outcome: event}, result) do
    status =
      case to_string(event.event_type) do
        "step_finished" -> :ok
        "step_failed" -> :error
        "step_timed_out" -> :timed_out
        "step_cancelled" -> :cancelled
      end

    {error, value} =
      cond do
        not is_nil(field(event.data, :result_status)) -> {result.error, result}
        status == :timed_out -> {:timeout, :timeout}
        true -> {field(event.data, :error), field(event.data, :error)}
      end

    {status,
     field(event.data, :retryable?) == true and field(event.data, :retry_exhausted?) != true,
     error, value}
  end

  def settlement(_entry, result) do
    status =
      FavnOrchestrator.RunServer.Execution.StepAttemptLifecycle.map_runner_status(result.status)

    {_event, retryable?} =
      FavnOrchestrator.RunServer.Execution.StepAttemptLifecycle.step_outcome(status)

    {status,
     retryable? and
       FavnOrchestrator.RunServer.Execution.StepAttemptLifecycle.runner_result_retryable?(result),
     result.error, result}
  end

  defp field(data, key), do: Map.get(data, key, Map.get(data, Atom.to_string(key)))

  defp cancel_expired(run, task, entry) do
    outcome =
      RunnerTasks.request_cancellation(run.workspace_id, task.task_id, %{
        kind: :original_deadline_expired
      })

    if CancellationOutcome.confirmed?(outcome) do
      case RunnerTasks.fetch(run.workspace_id, task.task_id) do
        {:ok, %{status: status} = saved} when status in @terminal -> reconcile(run, saved, entry)
        {:ok, _pending} -> {:error, {:recovered_task_cancellation_pending, task.task_id}}
        {:error, reason} -> {:error, reason}
      end
    else
      {:error, {:recovered_task_cancellation_unconfirmed, outcome}}
    end
  end
end
