defmodule FavnOrchestrator.RunServer.FailureCleanup do
  @moduledoc """
  Reconciles only existing work after an immutable failed execution outcome.

  The run server owns this continuation. Each operation runs in one registered
  helper, returns to the coordinator, and retains durable evidence on interruption.
  Pending cleanup is selected independently of executable runs. Unknown writes
  retain target protection while every other task is drained.
  """
  alias Favn.Manifest.Index
  alias Favn.Contracts.{RunnerResult, RunnerWork, RunnerError}

  alias FavnOrchestrator.{
    ExecutionAdmission,
    InitialTargetGenerationReconciler,
    ResourceCircuits,
    RunnerTasks,
    Runs,
    RunState
  }

  alias FavnOrchestrator.Persistence, as: Stores
  alias FavnOrchestrator.Persistence.Commands.ReleaseFailedRunResources
  alias FavnOrchestrator.Persistence.Queries.PageRunRunnerTasks
  alias FavnOrchestrator.Persistence.SystemContext
  alias FavnOrchestrator.RunServer.{Persistence, PersistenceRetry}

  alias FavnOrchestrator.RunServer.Execution.{
    FreshnessContext,
    PipelineFreshnessCheckpoint,
    RecoveredTask,
    RecoveryProgress,
    Restore,
    RunExecutionState,
    Sequential,
    StageAttemptState,
    StageResult
  }

  @enforce_keys [:run, :version, :index, :progress]
  defstruct @enforce_keys ++
              [
                phase: :tasks,
                history_end: nil,
                progress_dirty?: false,
                cursor: nil,
                tasks: [],
                entry: nil,
                result: nil,
                pending: nil,
                freshness: nil,
                details: [],
                detail_ids: [],
                waiting?: false,
                draining?: true,
                unresolved: [],
                unresolved_count: 0
              ]

  @type t :: %__MODULE__{}
  @terminal [:succeeded, :failed, :cancelled, :unknown]
  @read_kinds [:relation_inspection, :generation_capabilities, :generation_marker_read]

  @doc "Identifies the durable failed-cleanup lifecycle without reopening execution."
  @spec pending?(RunState.t()) :: boolean()
  def pending?(run),
    do: run.status == :error and get_in(run.metadata, ["failure_cleanup", "state"]) == "pending"

  @doc "Persists failure and cleanup intent together under the existing owner fence."
  @spec fail(RunState.t(), term()) :: {:ok, RunState.t()} | {:error, term()}
  def fail(run, reason) do
    with {:ok, latest} <- Runs.get(context(run), run.id),
         false <- RunState.finalized?(latest) do
      code = reason_code(reason)

      cleanup = %{
        "version" => 1,
        "state" => "pending",
        "reason_code" => code,
        "started_at" => DateTime.to_iso8601(DateTime.utc_now())
      }

      failed =
        latest
        |> RunState.with_storage_fence(run.storage_owner_id, run.storage_fencing_token)
        |> RunState.transition(
          status: :error,
          error:
            latest.error ||
              %{type: :recovery_exhausted, reason_code: code, phase: reason_phase(reason)},
          metadata:
            latest.metadata
            |> Map.put(:terminal_event_type, :run_failed)
            |> Map.put("failure_cleanup", cleanup)
        )

      case Persistence.persist_run_step(failed, :run_failed, %{
             status: :error,
             error: failed.error,
             cleanup: cleanup
           }) do
        :ok -> {:ok, failed}
        error -> error
      end
    else
      true -> {:error, :run_already_terminal}
      error -> error
    end
  end

  @doc "Builds cleanup state from the pinned manifest; no asset work is admitted."
  @spec new(RunState.t(), Favn.Manifest.Version.t()) :: {:ok, t()} | {:error, term()}
  def new(run, version) do
    with true <- pending?(run), {:ok, index} <- Index.build_from_version(version) do
      {:ok,
       %__MODULE__{
         run: run,
         version: version,
         index: index,
         progress: RecoveryProgress.new(run),
         history_end: run.event_seq,
         unresolved: Enum.reverse(get_in(run.metadata, ["failure_cleanup", "unresolved"]) || []),
         unresolved_count: get_in(run.metadata, ["failure_cleanup", "unresolved_count"]) || 0
       }}
    else
      false -> {:error, :cleanup_not_pending}
      error -> error
    end
  end

  @doc "Describes the next bounded read or fenced settlement operation."
  @spec operation(t()) :: tuple()
  def operation(%{progress_dirty?: true} = s) do
    info =
      Map.merge(s.run.metadata["failure_cleanup"], %{
        "unresolved_count" => s.unresolved_count,
        "unresolved" => Enum.reverse(s.unresolved)
      })

    run = RunState.transition(s.run, metadata: Map.put(s.run.metadata, "failure_cleanup", info))
    {:progress, run, info}
  end

  def operation(%{phase: :events} = s),
    do: {:events, context(s.run), s.run.id, s.progress.sequence}

  def operation(%{phase: :details, detail_ids: [{id, seq} | _]} = s),
    do: {:detail, context(s.run), s.run.id, id, seq}

  def operation(%{phase: :details, detail_ids: []} = s),
    do: {:restore_results, s.run, s.progress, Enum.reverse(s.details)}

  def operation(%{phase: :checkpoint} = s), do: {:checkpoint, s.run, s.index}

  def operation(%{phase: :tasks, tasks: []} = s),
    do:
      {:tasks,
       %PageRunRunnerTasks{
         workspace_context: context(s.run),
         run_id: s.run.id,
         cursor: s.cursor,
         limit: 20
       }}

  def operation(%{phase: :tasks, tasks: [task | _], draining?: true} = s),
    do: {:drain_task, s.run, task.task_id}

  def operation(%{phase: :tasks, tasks: [task | _]} = s), do: {:task, s.run, task.task_id}

  def operation(%{phase: :outcome} = s),
    do:
      {:outcome, context(s.run), s.run.id,
       s.progress.steps[s.entry.asset_step_id].outcome_sequence}

  def operation(%{phase: :settle} = s) do
    if RunState.execution_mode(s.run) == :sequential,
      do: {:settle_sequential, s.run, s.version, s.index, s.entry, s.result},
      else:
        {:settle, StageAttemptState.new(s.run, [], [s.entry], [], MapSet.new()), s.entry,
         s.result}
  end

  def operation(%{phase: :generation} = s),
    do: {:generation, s.pending.entry, s.run.storage_fencing_token}

  def operation(%{phase: :finish_step} = s),
    do: {:finish_step, StageAttemptState.new(s.run, [], [], [], MapSet.new()), s.pending}

  def operation(%{phase: :release_unresolved} = s), do: {:release_unresolved, s.run, s.entry}

  def operation(%{phase: :resources} = s),
    do:
      {:resources,
       %ReleaseFailedRunResources{
         workspace_context: context(s.run),
         run_id: s.run.id,
         owner_id: s.run.storage_owner_id,
         fencing_token: s.run.storage_fencing_token,
         limit: 100
       }}

  def operation(%{phase: :finish} = s) do
    status = if s.unresolved_count > 0, do: "attention", else: "complete"

    info =
      Map.merge(s.run.metadata["failure_cleanup"], %{
        "state" => status,
        "unresolved_count" => s.unresolved_count,
        "unresolved" => Enum.reverse(s.unresolved)
      })

    run =
      RunState.transition(s.run,
        runner_task_id: nil,
        metadata:
          s.run.metadata
          |> Map.put("failure_cleanup", info)
          |> Map.put(:active_runner_task_ids, [])
      )

    {:finish, run,
     if(status == "complete", do: :run_cleanup_completed, else: :run_cleanup_attention), info}
  end

  @doc "Executes one described operation; it owns no coordinator scheduling state."
  @spec perform(tuple()) :: term()
  def perform({:events, context, id, sequence}),
    do: Runs.page_events(context, id, after_sequence: sequence, limit: 50)

  def perform({:detail, context, run_id, _id, seq}),
    do: Runs.page_events(context, run_id, after_sequence: seq - 1, limit: 1)

  def perform({:restore_results, run, progress, details}),
    do: {:ok, Restore.restore_results(run, progress, details)}

  def perform({:checkpoint, run, index}) do
    if RunState.execution_mode(run) == :sequential,
      do: {:ok, nil},
      else: load_checkpoint(run, index)
  end

  def perform({:tasks, query}), do: Stores.stores().runner_tasks.page_run(query)

  def perform({:drain_task, run, id}) do
    with {:ok, task} <- RunnerTasks.fetch(run.workspace_id, id) do
      cond do
        task.run_id != run.id ->
          {:error, :cleanup_task_identity_mismatch}

        task.status in @terminal ->
          case release_terminal_capacity(run, task) do
            :ok ->
              {:ok, :settled}

            {:error, reason} ->
              if PersistenceRetry.recovery_required?(reason) or fenced?(reason),
                do: {:error, reason},
                else: {:ok, {:unresolved, "terminal_task_resources_unresolved", id}}
          end

        true ->
          RunnerTasks.request_cancellation(run.workspace_id, id, %{kind: :failed_run_cleanup},
            wait_for_ack: false
          )

          {:ok, :waiting}
      end
    end
  end

  def perform({:task, run, id}) do
    with {:ok, task} <- RunnerTasks.fetch(run.workspace_id, id) do
      cond do
        task.run_id != run.id ->
          {:error, :cleanup_task_identity_mismatch}

        task.status not in @terminal ->
          RunnerTasks.request_cancellation(run.workspace_id, id, %{kind: :failed_run_cleanup},
            wait_for_ack: false
          )

          {:ok, :waiting}

        task.task_kind == :asset_attempt ->
          with {:ok, entry} <- RecoveredTask.entry(run, task),
               true <-
                 task.payload.run_id == run.id and
                   task.payload.manifest_version_id == run.manifest_version_id and
                   task.payload.manifest_content_hash == run.manifest_content_hash,
               :ok <- ExecutionAdmission.release_completed(run, id) do
            if uncertain_write?(task) do
              with :ok <- release_permits(run, entry),
                   do: {:ok, {:unresolved, "unknown_write", id}}
            else
              {:ok, {:asset, task, entry}}
            end
          else
            false -> {:error, :cleanup_task_identity_mismatch}
            error -> error
          end

        uncertain_write?(task) and task.task_kind not in @read_kinds ->
          {:ok, {:unresolved, "unknown_helper_write", id}}

        true ->
          {:ok, :settled}
      end
    end
  end

  def perform({:outcome, context, id, sequence}),
    do: Runs.page_events(context, id, after_sequence: sequence - 1, limit: 1)

  def perform({:settle_sequential, run, version, index, entry, result}),
    do:
      Sequential.handle_result(
        RunExecutionState.new(run, version,
          manifest_index: index,
          mode: :sequential,
          manifest_lease_id: nil
        ),
        entry,
        result
      )

  def perform({:settle, stage, entry, result}),
    do: StageResult.process(stage, entry, result, %{stage: entry.stage, attempt: entry.attempt})

  def perform({:generation, entry, generation}),
    do:
      InitialTargetGenerationReconciler.reconcile(entry,
        cleanup?: true,
        cleanup_generation: generation,
        timeout_ms: 300_000
      )

  def perform({:finish_step, stage, pending}),
    do: StageResult.finish_post_step(stage, pending, :ok)

  def perform({:release_unresolved, run, entry}), do: release_permits(run, entry)
  def perform({:resources, command}), do: Stores.stores().admission.release_failed_run(command)

  def perform({:progress, run, info}),
    do: Persistence.persist_run_step(run, :run_cleanup_progress, info)

  def perform({:finish, run, event, data}), do: Persistence.persist_run_step(run, event, data)

  @doc "Applies a matching operation reply in the coordinator."
  @spec apply_result(t(), tuple(), term()) ::
          {:cont, t()} | {:done, t()} | {:retry, term()} | {:error, term()}
  def apply_result(s, {:events, _, _, _}, {:ok, page}) do
    events = Enum.take_while(page.items, &(&1.sequence <= s.history_end))

    with true <- events != [] or s.progress.sequence == s.history_end,
         {:ok, progress} <- RecoveryProgress.fold(s.progress, events) do
      {:cont,
       %{
         s
         | progress: progress,
           phase: if(progress.sequence == s.history_end, do: :details, else: :events),
           detail_ids: progress.details
       }}
    else
      _ -> {:cont, %{unresolved(s, "cleanup_history_incomplete", nil) | phase: :resources}}
    end
  end

  def apply_result(s, {:detail, _, _, id, seq}, {:ok, %{items: [%{sequence: seq} = event]}}),
    do: {:cont, %{s | details: [{id, event} | s.details], detail_ids: tl(s.detail_ids)}}

  def apply_result(s, {:restore_results, _, _, _}, {:ok, run}),
    do: {:cont, %{s | run: run, phase: :checkpoint, details: []}}

  def apply_result(s, {:checkpoint, _, _}, {:ok, freshness}),
    do: {:cont, %{s | phase: :tasks, freshness: freshness}}

  def apply_result(%{waiting?: true}, {:tasks, _}, {:ok, []}),
    do: {:retry, :cleanup_tasks_pending}

  def apply_result(%{draining?: true} = s, {:tasks, _}, {:ok, []}),
    do: {:cont, %{s | draining?: false, cursor: nil, phase: :events}}

  def apply_result(s, {:tasks, _}, {:ok, []}), do: {:cont, %{s | phase: :resources}}

  def apply_result(s, {:tasks, _}, {:ok, tasks}) do
    last = List.last(tasks)
    {:cont, %{s | tasks: tasks, cursor: {last.enqueued_at, last.task_id}}}
  end

  def apply_result(s, {:drain_task, _, _}, {:ok, :settled}), do: {:cont, next_task(s)}

  def apply_result(s, {:drain_task, _, _}, {:ok, {:unresolved, code, id}}),
    do: {:cont, s |> unresolved(code, id) |> next_task()}

  def apply_result(s, {:drain_task, _, _}, {:ok, :waiting}),
    do: {:cont, next_task(%{s | waiting?: true})}

  def apply_result(s, {:drain_task, _, id}, {:error, reason}) do
    if PersistenceRetry.recovery_required?(reason) or fenced?(reason),
      do: {:retry, reason},
      else:
        {:cont,
         s |> unresolved("task_drain_unresolved", id) |> Map.put(:waiting?, true) |> next_task()}
  end

  def apply_result(s, {:task, _, _}, {:ok, :waiting}),
    do: {:cont, next_task(%{s | waiting?: true})}

  def apply_result(s, {:task, _, _}, {:ok, :settled}), do: {:cont, next_task(s)}

  def apply_result(s, {:task, _, _}, {:ok, {:unresolved, code, id}}),
    do: {:cont, s |> unresolved(code, id) |> next_task()}

  def apply_result(s, {:task, _, _}, {:ok, {:asset, task, entry}}) do
    step = s.progress.steps[entry.asset_step_id]

    cond do
      is_nil(step) ->
        {:cont, unresolved_entry(s, entry, "task_without_step")}

      Map.get(step, :task_id) != task.task_id or step.phase == :settled ->
        {:cont, %{s | entry: entry, phase: :release_unresolved}}

      is_nil(s.freshness) and RunState.execution_mode(s.run) == :pipeline ->
        {:cont, unresolved_entry(s, entry, "cleanup_context_unavailable")}

      not valid_task?(s, step, task, entry) ->
        {:cont, unresolved_entry(s, entry, "task_identity_mismatch")}

      true ->
        entry =
          Map.merge(entry, %{
            version: s.version,
            manifest_index: s.index,
            freshness_context: s.freshness,
            cleanup_task_outcome: %{
              status: task.status,
              retry_class: task.retry_class,
              result:
                if(match?(%RunnerResult{}, task.result),
                  do: %RunnerResult{status: task.result.status}
                )
            }
          })

        {:cont,
         %{
           s
           | entry: entry,
             result: result(task),
             phase: if(step.phase == :outcome, do: :outcome, else: :settle)
         }}
    end
  end

  def apply_result(s, {:outcome, _, _, seq}, {:ok, %{items: [%{sequence: seq} = event]}}) do
    step = s.progress.steps[s.entry.asset_step_id]

    case Restore.validate_outcome(step, s.entry.cleanup_task_outcome, event) do
      :ok -> {:cont, %{s | entry: Map.put(s.entry, :recovered_outcome, event), phase: :settle}}
      {:error, _} -> {:cont, unresolved_entry(s, s.entry, "task_outcome_mismatch")}
    end
  end

  def apply_result(s, {:settle_sequential, _, _, _, _, _}, {:terminal, run}),
    do: {:cont, next_task(%{s | run: run})}

  def apply_result(_s, {:settle_sequential, _, _, _, _, _}, {:persist_retry, _, _, reason}),
    do: {:retry, reason}

  def apply_result(s, {kind, _, _, _}, result) when kind == :settle, do: settlement(s, result)
  def apply_result(s, {:finish_step, _, _}, result), do: settlement(s, result)
  def apply_result(s, {:generation, _, _}, :ok), do: {:cont, %{s | phase: :finish_step}}

  def apply_result(s, {:generation, _, _}, {:error, reason}) do
    if PersistenceRetry.recovery_required?(reason),
      do: {:cont, next_task(%{s | waiting?: true})},
      else:
        {:cont,
         %{
           unresolved(s, "generation_evidence_unresolved", s.entry.task_id)
           | phase: :release_unresolved
         }}
  end

  def apply_result(s, {:release_unresolved, _, _}, :ok), do: {:cont, next_task(s)}

  def apply_result(s, {:resources, _}, {:ok, release}) do
    done? = release.released_lease_ids == [] and release.expired_waiter_ids == []
    {:cont, %{s | phase: if(done?, do: :finish, else: :resources)}}
  end

  def apply_result(s, {:finish, run, _, _}, :ok), do: {:done, %{s | run: run}}

  def apply_result(s, {kind, _, _}, {:error, reason}) when kind == :checkpoint do
    if PersistenceRetry.recovery_required?(reason) or fenced?(reason),
      do: {:retry, reason},
      else: {:cont, %{unresolved(s, "cleanup_context_unavailable", nil) | phase: :tasks}}
  end

  def apply_result(s, {:task, _, id}, {:error, reason}) do
    cond do
      fenced?(reason) -> {:retry, reason}
      PersistenceRetry.recovery_required?(reason) -> {:cont, next_task(%{s | waiting?: true})}
      true -> {:cont, s |> unresolved("task_evidence_unresolved", id) |> next_task()}
    end
  end

  def apply_result(
        s,
        {:finish, _run, :run_cleanup_completed, _},
        {:error,
         %FavnOrchestrator.Persistence.Error{
           details: %{reason_code: "cleanup_write_hold_unresolved"}
         }}
      ),
      do: {:cont, %{unresolved(s, "write_hold_unresolved", nil) | phase: :finish}}

  def apply_result(s, {:progress, run, _}, :ok),
    do: {:cont, %{s | run: run, progress_dirty?: false}}

  def apply_result(s, {:events, _, _, _}, {:error, reason}) do
    if PersistenceRetry.recovery_required?(reason) or fenced?(reason),
      do: {:retry, reason},
      else: {:cont, %{unresolved(s, "cleanup_history_unreadable", nil) | phase: :resources}}
  end

  def apply_result(s, {:detail, _, _, id, _}, reply) do
    case reply do
      {:error, reason}
      when is_struct(reason, FavnOrchestrator.Persistence.Error) and
             (reason.retryable? or reason.kind in [:fenced, :timeout, :unavailable]) ->
        {:retry, reason}

      _ ->
        {:cont, %{unresolved(s, "cleanup_detail_unavailable", id) | detail_ids: tl(s.detail_ids)}}
    end
  end

  def apply_result(s, {:outcome, _, _, _}, reply) do
    case reply do
      {:error, reason}
      when is_struct(reason, FavnOrchestrator.Persistence.Error) and
             (reason.retryable? or reason.kind in [:fenced, :timeout, :unavailable]) ->
        {:retry, reason}

      _ ->
        {:cont, unresolved_entry(s, s.entry, "cleanup_outcome_unavailable")}
    end
  end

  def apply_result(_s, _op, {:error, reason}), do: {:retry, reason}
  def apply_result(_s, _op, other), do: {:error, {:invalid_cleanup_reply, other}}

  @doc false
  def uncertain_write?(task) do
    error = task.error || %{}

    never_assigned? =
      task.status == :cancelled and Map.get(task, :assignment_generation) == 0 and
        is_nil(Map.get(task, :assigned_at)) and is_nil(Map.get(task, :result)) and
        is_nil(task.error)

    not never_assigned? and
      (task.status == :unknown or
         (task.status in [:failed, :cancelled] and
            (task.retry_class in [:unknown_do_not_retry, :reconcile_before_retry] or
               (is_map(error) and
                  (Map.get(error, :outcome) || Map.get(error, "outcome")) in [:unknown, "unknown"]))))
  end

  defp fenced?(:fenced), do: true
  defp fenced?(%FavnOrchestrator.Persistence.Error{kind: :fenced}), do: true
  defp fenced?(_), do: false

  defp valid_task?(s, step, task, entry) do
    node = s.run.plan.nodes[step.node_key]

    node && entry.node_key == step.node_key && entry.asset_ref == node.ref &&
      entry.asset_step_id ==
        FavnOrchestrator.AssetStepIdentity.asset_step_id(s.run.id, step.node_key, node.ref) &&
      entry.stage == step.stage && entry.attempt == step.attempt &&
      RecoveredTask.validate_terminal_read(task, RecoveredTask.evidence(task)) == :ok &&
      (not match?(%RunnerResult{}, task.result) or
         FavnOrchestrator.RunnerIdentityVerifier.verify_result(
           task.required_runner_release_id,
           task.result
         ) == :ok)
  end

  defp settlement(s, {:cont, stage}), do: {:cont, next_task(%{s | run: stage.run})}

  defp settlement(s, {:post_step_pending, stage, pending}),
    do: {:cont, %{s | run: stage.run, pending: pending, phase: :generation}}

  defp settlement(_s, {:persist_retry, _retry, reason}), do: {:retry, reason}

  defp settlement(s, {:recovery_required, run, reason}) do
    if PersistenceRetry.recovery_required?(reason) or fenced?(reason),
      do: {:retry, reason},
      else: {:cont, unresolved_entry(%{s | run: run}, s.entry, "step_settlement_unresolved")}
  end

  defp settlement(_s, other), do: {:error, {:invalid_cleanup_settlement, other}}

  defp next_task(s),
    do: %{s | tasks: tl(s.tasks), phase: :tasks, entry: nil, result: nil, pending: nil}

  defp unresolved_entry(s, entry, code),
    do: %{unresolved(s, code, entry.task_id) | entry: entry, phase: :release_unresolved}

  defp unresolved(s, code, id) do
    reason = %{"reason_code" => code, "task_id" => id}

    if reason in s.unresolved do
      s
    else
      %{
        s
        | unresolved_count: s.unresolved_count + 1,
          progress_dirty?: true,
          unresolved: Enum.take([reason | s.unresolved], 32)
      }
    end
  end

  defp release_terminal_capacity(run, %{task_kind: :asset_attempt} = task) do
    with :ok <- ExecutionAdmission.release_completed(run, task.task_id),
         {:ok, entry} <- RecoveredTask.entry(run, task),
         do: release_permits(run, entry)
  end

  defp release_terminal_capacity(_run, _task), do: :ok

  defp release_permits(run, entry) do
    case ResourceCircuits.release(run, entry.resource_circuit_permits) do
      :ok -> :ok
      error -> error
    end
  end

  defp result(%{result: %RunnerResult{} = result}), do: {:ok, result}

  defp result(%{payload: %RunnerWork{} = work, status: status, error: error}) do
    {:ok,
     %RunnerResult{
       run_id: work.run_id,
       manifest_version_id: work.manifest_version_id,
       manifest_content_hash: work.manifest_content_hash,
       required_runner_release_id: work.required_runner_release_id,
       status: if(status == :cancelled, do: :cancelled, else: :error),
       asset_results: [],
       error: if(error, do: RunnerError.normalize(error)),
       metadata: RunnerWork.lifecycle_metadata(work)
     }}
  end

  defp load_checkpoint(run, index) do
    case PipelineFreshnessCheckpoint.load(run, index) do
      {:ok, {freshness, _}} -> {:ok, freshness}
      {:ok, nil} -> FreshnessContext.initialize(run, index)
      error -> error
    end
  end

  defp context(run), do: SystemContext.workspace(run.workspace_id, :failed_run_cleanup)
  defp reason_phase(reason) when is_atom(reason), do: Atom.to_string(reason)
  defp reason_phase(reason) when is_binary(reason), do: String.slice(reason, 0, 128)
  defp reason_phase(reason) when is_tuple(reason), do: reason_phase(elem(reason, 0))
  defp reason_phase(_), do: "recovery"
  defp reason_code({:registration_retry_exhausted, _}), do: "registration_retry_exhausted"
  defp reason_code(_), do: "automatic_recovery_exhausted"
end
