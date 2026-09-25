defmodule FavnOrchestrator.RunServer.Execution.Restore do
  @moduledoc """
  Reads recovery evidence in bounded pages between run-server callbacks.

  The event reducer keeps compact facts for each planned node and a bounded
  slice of display results. Task payloads are read only for unsettled work.
  This module never submits an asset or interprets a failed read as absence.
  """

  alias FavnOrchestrator.RunServer.Execution.{
    RecoveredTask,
    PipelineTaskContinuation,
    PipelineFreshnessCheckpoint
  }

  alias FavnOrchestrator.RunServer.Persistence
  alias Favn.Contracts.RunnerWork
  alias Favn.Contracts.RunnerResult
  alias Favn.Run.NodeResult
  alias FavnOrchestrator.RunServer.Execution.ResultBuilder
  alias FavnOrchestrator.RunServer.Execution.ResultSanitizer
  alias FavnOrchestrator.Persistence.SystemContext
  alias FavnOrchestrator.RunnerTasks
  alias FavnOrchestrator.Runs
  alias FavnOrchestrator.RunServer.Execution.ActiveTaskSet
  alias FavnOrchestrator.RunServer.Execution.RecoveryProgress
  alias FavnOrchestrator.RunServer.Execution.RunExecutionState

  @spec start(RunExecutionState.t()) :: RunExecutionState.t()
  def start(state),
    do: %{state | recovery: %{progress: RecoveryProgress.new(state.run), phase: :events}}

  @spec next(RunExecutionState.t()) ::
          {:more, RunExecutionState.t()}
          | {:ready, RunExecutionState.t(), RecoveryProgress.t(), [map()]}
          | {:error, term()}
  def next(%{recovery: %{phase: :events, progress: progress}} = state) do
    context = SystemContext.workspace(state.run.workspace_id, :run_worker)

    with {:ok, page} <-
           Runs.page_events(context, state.run.id, after_sequence: progress.sequence, limit: 50),
         events <- Enum.take_while(page.items, &(&1.sequence <= state.run.event_seq)),
         true <- events != [] or progress.sequence == state.run.event_seq,
         {:ok, progress} <- RecoveryProgress.fold(progress, events) do
      if progress.sequence == state.run.event_seq do
        tasks =
          progress.steps |> Map.values() |> Enum.filter(&(&1.phase in [:submitted, :outcome]))

        ids = MapSet.new(tasks, &Map.fetch!(&1, :task_id))

        missing =
          Enum.reject(ActiveTaskSet.active_runner_task_ids(state.run), &MapSet.member?(ids, &1))

        if missing == [] do
          {:more,
           %{state | recovery: %{progress: progress, phase: :tasks, pending: tasks, tasks: []}}}
        else
          {:error, {:recovery_active_task_reference_mismatch, missing}}
        end
      else
        {:more, %{state | recovery: %{state.recovery | progress: progress}}}
      end
    else
      false -> {:error, :recovery_event_history_incomplete}
      {:error, _} = error -> error
    end
  end

  def next(%{recovery: %{phase: :tasks, pending: [step | rest]} = recovery} = state) do
    with {:ok, task} <- RunnerTasks.fetch(state.run.workspace_id, step.task_id),
         :ok <- validate_task(state, step, task),
         {:ok, outcome} <- outcome(state, step),
         :ok <- validate_outcome(step, task, outcome),
         :ok <- validate_checkpoint(state, task),
         {:ok, entry} <- RecoveredTask.entry(state.run, task) do
      entry =
        entry
        |> Map.put(:recovery_evidence, RecoveredTask.evidence(task))
        |> Map.put(:recovery_pending?, true)

      entry =
        if outcome, do: Map.put(entry, :recovered_outcome, compact_outcome(outcome)), else: entry

      {:more, %{state | recovery: %{recovery | pending: rest, tasks: [entry | recovery.tasks]}}}
    end
  end

  def next(%{recovery: %{phase: :tasks, pending: []} = recovery} = state) do
    {:more,
     %{
       state
       | recovery:
           Map.merge(recovery, %{
             phase: :details,
             pending: recovery.progress.details,
             details: [],
             asset_results: []
           })
     }}
  end

  def next(%{recovery: %{phase: :details, pending: [{id, sequence} | rest]} = recovery} = state) do
    with {:ok, event} <- event(state, sequence),
         {:ok, assets} <- retained_assets(state, id, event) do
      {:more,
       %{
         state
         | recovery: %{
             recovery
             | pending: rest,
               details: [{id, event} | recovery.details],
               asset_results: ResultBuilder.retain_asset_results(recovery.asset_results ++ assets)
           }
       }}
    end
  end

  def next(%{recovery: %{phase: :details, pending: []} = recovery} = state) do
    progress = recovery.progress

    with :ok <- validate_position(state, progress),
         {:ok, failure} <- failure(state, progress.failure) do
      run = restore_results(state.run, progress, Enum.reverse(recovery.details))

      {:ready,
       %{
         state
         | run: run,
           recovery: nil,
           accumulated_results: recovery.asset_results
       }, %{progress | failure: failure}, Enum.reverse(recovery.tasks)}
    end
  end

  defp validate_position(%{mode: :pipeline, freshness_checkpoint: checkpoint}, progress) do
    case {checkpoint, progress.position} do
      {nil, nil} ->
        :ok

      {%{} = ref, %{} = position} ->
        if ref.sequence == progress.position_sequence and
             ref.stage == field(position, :index) and ref.attempt == field(position, :attempt),
           do: :ok,
           else: {:error, :recovery_position_checkpoint_mismatch}

      _ ->
        {:error, :recovery_position_checkpoint_mismatch}
    end
  end

  defp validate_position(_state, _progress), do: :ok

  defp compact_outcome(event) do
    %{
      event_type: event.event_type,
      occurred_at: event.occurred_at,
      data:
        Map.take(event.data, ~w(error result_status retryable? retry_exhausted? retry_after_ms))
    }
  end

  defp validate_checkpoint(%{mode: :pipeline} = state, task) do
    if Persistence.externally_cancelled?(state.run) do
      :ok
    else
      reference = state.freshness_checkpoint
      context = task.orchestration_context

      if is_map(reference) and task.payload.stage == reference.stage and
           task.payload.attempt == reference.attempt and PipelineTaskContinuation.valid?(context) and
           PipelineFreshnessCheckpoint.matches?(
             reference,
             PipelineTaskContinuation.checkpoint(context)
           ), do: :ok, else: {:error, :runner_task_freshness_checkpoint_mismatch}
    end
  end

  defp validate_checkpoint(_state, _task), do: :ok

  defp validate_task(state, step, %{task_id: id, payload: %RunnerWork{} = work} = task) do
    node = Map.fetch!(state.run.plan.nodes, step.node_key)

    if id == step.task_id and work.run_id == state.run.id and
         work.manifest_version_id == state.run.manifest_version_id and
         work.manifest_content_hash == state.run.manifest_content_hash and
         work.asset_step_id ==
           FavnOrchestrator.AssetStepIdentity.asset_step_id(state.run.id, step.node_key, node.ref) and
         work.stage == step.stage and work.attempt == step.attempt and
         RunnerWork.node_key(work) == step.node_key and RunnerWork.asset_ref(work) == node.ref and
         (step.phase != :outcome or task.status in [:succeeded, :failed, :cancelled, :unknown]) do
      :ok
    else
      {:error, :recovered_task_identity_mismatch}
    end
  end

  defp validate_task(_state, _step, _task), do: {:error, :recovered_task_data_unavailable}

  @doc false
  def validate_outcome(%{phase: :outcome, status: status} = step, task, outcome) do
    compatible? =
      case status do
        :ok ->
          task.status == :succeeded and match?(%RunnerResult{status: :ok}, task.result)

        :error ->
          task.status in [:failed, :unknown] or
            (task.status == :cancelled and is_nil(field(outcome.data, :result_status)))

        :cancelled ->
          task.status == :cancelled

        :timed_out ->
          task.status in [:failed, :cancelled, :unknown]
      end

    if compatible? and (not step.retry_allowed? or task.retry_class == :safe_to_retry),
      do: :ok,
      else: {:error, :recovered_task_outcome_mismatch}
  end

  def validate_outcome(_step, _task, _outcome), do: :ok

  defp retained_assets(state, id, event) do
    step = Map.fetch!(state.recovery.progress.steps, id)

    task_id =
      field(event.data, :runner_task_id) ||
        field(field(event.data, :node_result) || %{}, :runner_task_id)

    pending? = step.phase == :outcome and step.outcome_sequence == event.sequence

    if is_nil(task_id) or pending? do
      {:ok, []}
    else
      evidence =
        Map.merge(step, %{task_id: task_id, attempt: field(event.data, :attempt), phase: :outcome})

      with {:ok, task} <- RunnerTasks.fetch(state.run.workspace_id, task_id),
           :ok <- validate_task(state, evidence, task) do
        case task do
          %{result: %RunnerResult{} = result} ->
            {:ok, ResultSanitizer.sanitize_asset_results(result.asset_results)}

          %{status: status} when status in [:failed, :cancelled, :unknown] ->
            {:ok, []}

          _ ->
            {:error, :recovered_terminal_result_missing}
        end
      end
    end
  end

  defp failure(_state, nil), do: {:ok, nil}

  defp failure(state, failure) do
    with {:ok, event} <- event(state, failure.sequence),
         do:
           {:ok,
            Map.put(failure, :error, field(event.data, :error) || field(event.data, :reason))}
  end

  defp event(state, sequence) do
    context = SystemContext.workspace(state.run.workspace_id, :run_worker)

    case Runs.page_events(context, state.run.id, after_sequence: sequence - 1, limit: 1) do
      {:ok, %{items: [%{sequence: ^sequence} = event]}} -> {:ok, event}
      {:ok, _} -> {:error, :recovery_event_missing}
      error -> error
    end
  end

  defp outcome(state, %{phase: :outcome, outcome_sequence: sequence}) do
    context = SystemContext.workspace(state.run.workspace_id, :run_worker)

    case Runs.page_events(context, state.run.id, after_sequence: sequence - 1, limit: 1) do
      {:ok, %{items: [%{sequence: ^sequence} = event]}} -> {:ok, event}
      {:ok, _} -> {:error, :recovery_outcome_missing}
      error -> error
    end
  end

  defp outcome(_state, _step), do: {:ok, nil}

  @doc false
  def restore_results(run, progress, details) do
    results =
      Enum.map(details, fn {id, event} ->
        step = Map.fetch!(progress.steps, id)
        node = Map.fetch!(run.plan.nodes, step.node_key)
        data = field(event.data, :node_result) || %{}

        fields =
          Map.new(Map.from_struct(%NodeResult{}), fn {key, default} ->
            {key, field(data, key, default)}
          end)

        fields =
          Map.merge(fields, %{
            node_key: step.node_key,
            ref: node.ref,
            window: node.window,
            stage: node.stage,
            status: outcome_status(event),
            attempt_count: field(data, :attempt_count, field(event.data, :attempt, 1)),
            runner_pool: FavnOrchestrator.RunnerPoolSelection.for_node(run, step.node_key),
            execution_pool: Map.get(node, :execution_pool),
            asset_step_id: id
          })

        fields =
          Enum.reduce([:started_at, :finished_at], fields, fn key, acc ->
            Map.put(acc, key, datetime(field(data, key)) || event.occurred_at)
          end)

        NodeResult.new(fields)
      end)

    retention = %{
      node_result_count: progress.result_count,
      retained_node_result_count: length(results),
      truncated: progress.result_count > length(results)
    }

    %{
      run
      | result: %{
          node_results: results,
          asset_results: [],
          metadata: %{result_retention: retention}
        }
    }
  end

  defp outcome_status(event) do
    case to_string(event.event_type) do
      "step_finished" -> :ok
      "step_failed" -> :error
      "step_timed_out" -> :timed_out
      "step_cancelled" -> :cancelled
      "step_skipped_fresh" -> :skipped_fresh
      "step_blocked" -> :blocked
    end
  end

  defp datetime(%DateTime{} = value), do: value

  defp datetime(value) when is_binary(value) do
    case DateTime.from_iso8601(value) do
      {:ok, time, 0} -> time
      _ -> nil
    end
  end

  defp datetime(_), do: nil

  defp field(map, key, default \\ nil),
    do: Map.get(map, key, Map.get(map, Atom.to_string(key), default))
end
