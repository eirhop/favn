defmodule FavnOrchestrator.RunServer.Execution.Sequential do
  @moduledoc """
  Executes and settles one sequential run attempt at a time.

  The module owns sequential dispatch, durable ownership, retry scheduling, and
  aggregate result construction. Await worker mechanics remain in the run-server
  coordinator and are requested through an `:await` directive.
  """

  alias FavnOrchestrator.RunServer.Execution.RecoveredTask
  alias Favn.Contracts.RunnerError
  alias Favn.Contracts.RunnerResult
  alias Favn.Contracts.RunnerWork
  alias FavnOrchestrator.Persistence.Commands.AdmitRunnerTask
  alias FavnOrchestrator.Projector
  alias FavnOrchestrator.Runs
  alias FavnOrchestrator.RunServer.Execution.AdmissionIntent
  alias FavnOrchestrator.AssetRunnerTasks
  alias FavnOrchestrator.MaterializationClaims
  alias FavnOrchestrator.AssetStepIdentity
  alias FavnOrchestrator.ExecutionPackages
  alias FavnOrchestrator.Persistence.SystemContext
  alias FavnOrchestrator.RunServer.Execution.ResultBuilder
  alias FavnOrchestrator.RunServer.Execution.ResultSanitizer
  alias FavnOrchestrator.RunServer.Execution.PreSubmitFailure
  alias FavnOrchestrator.RunServer.Execution.RunExecutionState
  alias FavnOrchestrator.RunServer.Execution.ActiveTaskSet
  alias FavnOrchestrator.RunServer.Execution.StepAttemptLifecycle
  alias FavnOrchestrator.RunServer.Persistence
  alias FavnOrchestrator.RunServer.PersistenceRetry
  alias FavnOrchestrator.RunServer.Snapshots
  alias FavnOrchestrator.RunState

  @type directive ::
          {:await, RunExecutionState.t(), map()}
          | {:cont, RunExecutionState.t()}
          | {:retry_timer, RunExecutionState.t(), map()}
          | {:terminal, RunState.t()}
          | {:persist_retry, RunExecutionState.t(), PersistenceRetry.t(), term()}

  @doc "Continues a sequential run from its current index."
  @spec continue(RunExecutionState.t()) :: directive()
  def continue(%RunExecutionState{} = state) do
    cond do
      Persistence.externally_cancelled?(state.run) ->
        {:terminal, Snapshots.cancelled_terminal(state.run, state.accumulated_results)}

      state.sequential_index >= length(state.sequential_refs) ->
        {:terminal,
         Snapshots.snapshot_update(state.run,
           status: :ok,
           error: nil,
           runner_task_id: nil,
           result:
             ResultBuilder.pipeline_result(
               state.run,
               :ok,
               ResultBuilder.sort_asset_results(state.run, state.accumulated_results)
             )
         )}

      true ->
        {asset_ref, node_key, stage} = Enum.at(state.sequential_refs, state.sequential_index)
        submit_attempt(state, asset_ref, node_key, stage, 1)
    end
  end

  @doc "Settles one sequential runner await result."
  @spec handle_result(RunExecutionState.t(), map(), term()) :: directive()
  def handle_result(%RunExecutionState{} = state, entry, {:ok, %RunnerResult{} = result}) do
    result = ResultSanitizer.sanitize(result)
    asset_results = ResultSanitizer.sanitize_asset_results(result.asset_results)

    {step_status, retryable?, outcome_error, post_step_value} =
      RecoveredTask.settlement(entry, result)

    {event_type, _retryable?} = StepAttemptLifecycle.step_outcome(step_status)
    metadata = ResultSanitizer.merge_metadata(state.run.metadata, result.metadata)

    retry_delay =
      if not failed_cleanup?(state.run) and retryable? and
           StepAttemptLifecycle.retry_allowed?(state.run, entry.node_key, entry.attempt) do
        if entry[:recovered_outcome],
          do: Map.get(entry.recovered_outcome.data, "retry_after_ms"),
          else:
            StepAttemptLifecycle.retry_delay_ms(state.run, entry.node_key, entry.attempt, result)
      end

    attrs = [status: step_status, runner_task_id: nil, error: outcome_error, metadata: metadata]

    step_finished =
      if entry[:recovered_outcome],
        do: Snapshots.snapshot_update(state.run, attrs),
        else: RunState.transition(state.run, attrs)

    data = %{
      asset_ref: entry.asset_ref,
      result_status: result.status,
      runner_task_id: entry.task_id,
      error: outcome_error,
      node_key: entry.node_key,
      asset_step_id: entry.asset_step_id,
      window: entry.window,
      stage: entry.stage,
      attempt: entry.attempt,
      max_attempts: StepAttemptLifecycle.retry_policy(state.run, entry.node_key).max_attempts,
      retryable?: retryable?,
      retry_after_ms: retry_delay,
      retry_exhausted?:
        retryable? and
          not StepAttemptLifecycle.retry_allowed?(state.run, entry.node_key, entry.attempt),
      asset_results: asset_results
    }

    resume = %{
      kind: :step_result,
      run: step_finished,
      entry: entry,
      status: step_status,
      retryable?: retryable?,
      failure: post_step_value,
      retry_after_ms: retry_delay,
      asset_results: asset_results
    }

    if entry[:recovered_outcome],
      do: resume_persisted(state, resume),
      else: persist_or_retry(state, step_finished, event_type, data, resume)
  end

  def handle_result(%RunExecutionState{} = state, entry, {:error, :timeout}) do
    state =
      cancel_work(state, [entry.task_id], %{
        kind: :await_timeout,
        asset_ref: entry.asset_ref,
        stage: entry.stage,
        attempt: entry.attempt
      })

    timeout_state =
      RunState.transition(state.run,
        status: :timed_out,
        runner_task_id: nil,
        error: :timeout
      )

    data = %{
      asset_ref: entry.asset_ref,
      error: :timeout,
      node_key: entry.node_key,
      asset_step_id: entry.asset_step_id,
      window: entry.window,
      stage: entry.stage,
      attempt: entry.attempt,
      max_attempts: StepAttemptLifecycle.retry_policy(state.run, entry.node_key).max_attempts,
      asset_results: []
    }

    resume = %{
      kind: :step_result,
      run: timeout_state,
      entry: entry,
      status: :timed_out,
      retryable?: false,
      failure: :timeout,
      asset_results: []
    }

    persist_or_retry(state, timeout_state, :step_timed_out, data, resume)
  end

  def handle_result(%RunExecutionState{} = state, entry, {:error, reason}) do
    reason = RecoveredTask.await_failure_reason(reason)

    state =
      cancel_work(state, [entry.task_id], %{
        kind: :await_error,
        asset_ref: entry.asset_ref,
        stage: entry.stage,
        attempt: entry.attempt,
        error: reason
      })

    failed =
      RunState.transition(state.run,
        status: :error,
        runner_task_id: nil,
        error: reason
      )

    data = %{
      asset_ref: entry.asset_ref,
      error: reason,
      node_key: entry.node_key,
      asset_step_id: entry.asset_step_id,
      window: entry.window,
      stage: entry.stage,
      attempt: entry.attempt,
      max_attempts: StepAttemptLifecycle.retry_policy(state.run, entry.node_key).max_attempts,
      asset_results: []
    }

    resume = %{
      kind: :step_result,
      run: failed,
      entry: entry,
      status: :error,
      retryable?: false,
      failure: reason,
      asset_results: []
    }

    persist_or_retry(state, failed, :step_failed, data, resume)
  end

  @doc false
  @spec resume_persisted(RunExecutionState.t(), map()) :: directive()
  def resume_persisted(%RunExecutionState{} = state, %{kind: :step_result} = resume) do
    settled = RunState.transition(resume.run, [])

    retryable? =
      not failed_cleanup?(settled) and resume.retryable? and
        StepAttemptLifecycle.retry_allowed?(settled, resume.entry.node_key, resume.entry.attempt)

    data = %{
      asset_ref: resume.entry.asset_ref,
      asset_step_id: resume.entry.asset_step_id,
      node_key: resume.entry.node_key,
      runner_task_id: resume.entry.task_id,
      stage: resume.entry.stage,
      attempt: resume.entry.attempt,
      status: resume.status,
      error: resume.run.error,
      retryable?: retryable?,
      retry_after_ms: Map.get(resume, :retry_after_ms)
    }

    persist_or_retry(state, settled, :step_settled, data, %{
      resume
      | kind: :settled,
        run: settled,
        retryable?: retryable?
    })
  end

  def resume_persisted(%RunExecutionState{} = state, %{kind: :settled} = resume) do
    state = %{state | run: resume.run}

    cond do
      failed_cleanup?(state.run) ->
        {:terminal, state.run}

      Persistence.externally_cancelled?(state.run) ->
        {:terminal,
         Snapshots.cancelled_terminal(
           state.run,
           resume.asset_results ++ state.accumulated_results
         )}

      resume.status == :ok ->
        continue(%{
          state
          | sequential_index: state.sequential_index + 1,
            accumulated_results:
              resume.asset_results
              |> Enum.reverse(state.accumulated_results)
              |> ResultBuilder.retain_asset_results()
        })

      resume.retryable? ->
        lifecycle =
          StepAttemptLifecycle.new(
            state.run,
            state.version,
            resume.entry.node_key,
            resume.entry.stage,
            resume.entry.attempt
          )

        retry = StepAttemptLifecycle.retry(lifecycle, Map.get(resume, :failure))

        schedule_retry(state, %{
          retry
          | retry_after_ms: Map.get(resume, :retry_after_ms) || retry.retry_after_ms
        })

      true ->
        terminalize_error(state, resume.asset_results)
    end
  end

  def resume_persisted(%RunExecutionState{} = state, %{kind: :schedule_retry} = resume) do
    schedule_retry_timer(%{state | run: resume.run}, resume.retry)
  end

  def resume_persisted(%RunExecutionState{} = state, %{kind: :pre_submit_failure} = resume) do
    state = %{state | run: resume.run}

    if resume.retryable? do
      maybe_schedule_retry(
        state,
        resume.asset_ref,
        resume.node_key,
        resume.stage,
        resume.attempt,
        [],
        resume.reason
      )
    else
      terminalize_error(state, [])
    end
  end

  @doc "Resubmits a sequential attempt after its retry timer fires."
  @spec resume_retry(RunExecutionState.t(), map()) :: directive()
  def resume_retry(%RunExecutionState{} = state, retry) do
    deadline = Map.get(retry, :admission_deadline_ms)

    run =
      if is_integer(deadline),
        do: state.run,
        else:
          state.run
          |> Map.put(:metadata, clear_retry_state(state.run.metadata))
          |> RunState.with_snapshot_hash()

    submit_attempt(
      %{state | run: run},
      retry.asset_ref,
      retry.node_key,
      retry.stage,
      retry.next_attempt,
      deadline
    )
  end

  @doc false
  @spec restore_retry(RunExecutionState.t(), map()) :: directive()
  def restore_retry(state, step) do
    lifecycle =
      StepAttemptLifecycle.new(state.run, state.version, step.node_key, step.stage, step.attempt)

    retry = StepAttemptLifecycle.retry(lifecycle)

    delay =
      if step[:retry_at],
        do: max(DateTime.diff(step.retry_at, DateTime.utc_now(), :millisecond), 0),
        else: 0

    schedule_retry(state, %{retry | retry_after_ms: delay})
  end

  @doc false
  def restore_intent(state, step) do
    node = Map.fetch!(state.run.plan.nodes, step.node_key)
    submit_attempt(state, node.ref, step.node_key, step.stage, step.attempt)
  end

  @doc "Returns sequential work refs with their plan stage."
  @spec refs(RunState.t()) :: [{Favn.Ref.t(), Favn.Plan.node_key(), non_neg_integer()}]
  def refs(%RunState{plan: %Favn.Plan{} = plan} = run_state) do
    node_keys =
      case plan.target_node_keys do
        [_ | _] = target_node_keys -> target_node_keys
        _node_keys -> [{run_state.asset_ref, nil}]
      end

    Enum.map(node_keys, fn node_key ->
      asset_ref = node_asset_ref(plan, node_key)
      {asset_ref, node_key, stage_from_plan(plan, node_key, 0)}
    end)
  end

  def refs(%RunState{} = run_state),
    do: [{run_state.asset_ref, {run_state.asset_ref, nil}, 0}]

  defp failed_cleanup?(run),
    do: run.status == :error and is_map(run.metadata["failure_cleanup"])

  defp submit_attempt(
         %RunExecutionState{} = state,
         asset_ref,
         node_key,
         stage,
         attempt,
         admission_deadline_ms \\ nil
       ) do
    with {:ok, %{work: work} = lifecycle} <-
           state.run
           |> StepAttemptLifecycle.new(state.version, node_key, stage, attempt)
           |> StepAttemptLifecycle.build_work(state.manifest_index),
         {:ok, work} <-
           StepAttemptLifecycle.attach_publication(
             work,
             state.manifest_index,
             state.run.workspace_id
           ),
         work <-
           work
           |> StepAttemptLifecycle.attach_deadline(state.run)
           |> Map.put(:manifest_lease_id, state.manifest_lease_id) do
      work =
        if is_integer(admission_deadline_ms),
          do: %{
            work
            | deadline_at: DateTime.from_unix!(admission_deadline_ms * 1_000, :microsecond)
          },
          else: work

      prepare_intent(state, lifecycle, work)
    else
      {:error, reason} ->
        persist_pre_submit_failure(
          state,
          asset_ref,
          node_key,
          stage,
          attempt,
          PreSubmitFailure.normalize(reason)
        )
    end
  end

  defp prepare_intent(state, lifecycle, work) do
    case AdmissionIntent.load(state.run, work, state.version) do
      {:ok, nil} ->
        with {:ok, intent} <-
               AdmissionIntent.new(
                 state.run,
                 work,
                 %{kind: :sequential, materialization_claim: nil},
                 DateTime.utc_now()
               ),
             {:ok, metadata} <- AdmissionIntent.put(state.run.metadata, intent) do
          running = RunState.transition(state.run, metadata: metadata)

          pause =
            sequential_pause(state.run, lifecycle, work, nil, :intent)
            |> Map.merge(%{intent: intent, intent_run: running})

          retry =
            PersistenceRetry.new(
              running,
              :step_intended,
              intent_data(lifecycle, work, intent),
              {:sequential_operation, pause}
            )

          persist_operation(state, retry, pause)
        else
          {:error, reason} -> {:recovery_required, state, reason}
        end

      {:ok, intent} ->
        admit_intent(state, lifecycle, %{work | deadline_at: intent.deadline_at}, intent)

      {:error, reason} ->
        {:recovery_required, state, reason}
    end
  end

  defp admit_intent(state, lifecycle, work, intent) do
    if DateTime.compare(work.deadline_at, DateTime.utc_now()) != :gt do
      fail_before_enqueue(state, lifecycle, work, nil, :target_write_admission_timeout)
    else
      context = SystemContext.workspace(state.run.workspace_id, :execution_package_fetch)

      case ExecutionPackages.attach(
             context,
             state.run.deployment_id,
             work,
             state.version,
             state.manifest_index
           ) do
        {:ok, work} ->
          prepare_admission(state, lifecycle, work, intent)

        {:error, %FavnOrchestrator.Persistence.Error{kind: kind, retryable?: retryable?} = reason}
        when retryable? or kind in [:timeout, :unavailable] ->
          {:recovery_required, state, reason}

        {:error, reason} ->
          fail_before_enqueue(state, lifecycle, work, nil, reason)
      end
    end
  end

  defp prepare_admission(state, lifecycle, work, intent) do
    with {:ok, claim} <- MaterializationClaims.prepare_sequential(state.run, work),
         {:ok, enqueue, work} <-
           AssetRunnerTasks.prepare(
             state.run,
             work,
             lifecycle.node_key,
             lifecycle.attempt,
             intent.context
           ),
         {:ok, metadata} <- AdmissionIntent.clear(state.run.metadata, intent) do
      admitted =
        RunState.transition(state.run,
          runner_task_id: nil,
          metadata:
            metadata
            |> clear_retry_state()
            |> Map.merge(RunnerWork.lifecycle_metadata(work))
            |> Map.update(
              :active_runner_task_ids,
              [intent.task_id],
              &Enum.uniq(&1 ++ [intent.task_id])
            )
        )

      durable = RunState.for_step_persistence(admitted)

      event =
        Projector.run_event(
          durable,
          attempt_start_event(lifecycle.attempt),
          intent_data(lifecycle, work, intent)
        )

      {:ok, transition} =
        Runs.prepare_commit(enqueue.workspace_context, durable, event,
          owner_id: admitted.storage_owner_id,
          fencing_token: admitted.storage_fencing_token
        )

      command = %AdmitRunnerTask{
        intent: intent,
        enqueue: enqueue,
        transition: transition,
        claim: claim
      }

      pause =
        sequential_pause(state.run, lifecycle, work, nil, :admission)
        |> Map.merge(%{intent: intent, admitted_run: admitted})

      retry =
        PersistenceRetry.command(
          state.run,
          :runner_admission,
          command,
          intent_data(lifecycle, work, intent),
          {:sequential_operation, pause}
        )

      persist_operation(state, retry, pause)
    else
      {:error, reason} -> fail_before_enqueue(state, lifecycle, work, nil, reason)
    end
  end

  defp intent_data(lifecycle, work, intent) do
    {:ok, fingerprint} = AdmissionIntent.fingerprint(intent)

    %{
      asset_ref: lifecycle.asset_ref,
      runner_task_id: intent.task_id,
      node_key: lifecycle.node_key,
      asset_step_id: work.asset_step_id,
      window: RunnerWork.window(work),
      stage: lifecycle.stage,
      attempt: lifecycle.attempt,
      max_attempts: lifecycle.max_attempts,
      admission_intent_hash: fingerprint
    }
  end

  defp handle_acquired_claim(state, lifecycle, work, result) do
    case result do
      {:error, %{details: %{reason_code: "target_write_in_progress"}}} ->
        deadline = work.deadline_at || DateTime.add(DateTime.utc_now(), 300_000, :millisecond)

        retry =
          lifecycle
          |> StepAttemptLifecycle.retry()
          |> Map.merge(%{
            next_attempt: lifecycle.attempt,
            retry_after_ms: 250,
            admission_deadline_ms: DateTime.to_unix(deadline, :millisecond)
          })

        if get_in(state.run.metadata, [:retry_state, :retry, :admission_deadline_ms]),
          do: schedule_retry_timer(state, retry),
          else: schedule_retry(state, retry)

      {:error, reason} ->
        persist_pre_submit_failure(
          state,
          lifecycle.asset_ref,
          lifecycle.node_key,
          lifecycle.stage,
          lifecycle.attempt,
          reason,
          work.asset_step_id
        )
    end
  end

  defp sequential_pause(run, lifecycle, work, claim, phase),
    do: %{
      kind: :sequential,
      phase: phase,
      lifecycle: lifecycle,
      work: work,
      claim: claim,
      ctx: %{current_run: run, materialization_claim: claim},
      entries: []
    }

  defp persist_operation(state, retry, pause) do
    case PersistenceRetry.persist(retry) do
      :ok ->
        resume_operation(state, pause, :ok)

      {:ok, result} ->
        resume_operation(state, pause, result)

      {:error, %{details: %{reason_code: "target_write_in_progress"}}} = result ->
        handle_acquired_claim(state, pause.lifecycle, pause.work, result)

      {:error, reason} when reason in [:fenced, :external_cancel, :cancellation_race] ->
        {:persist_retry, %{state | paused_admission: pause}, retry, reason}

      {:error, reason} ->
        if PersistenceRetry.recovery_required?(reason) do
          {:persist_retry, %{state | paused_admission: pause}, retry, reason}
        else
          fail_before_enqueue(state, pause.lifecycle, pause.work, nil, reason)
        end
    end
  end

  @doc false
  def reject_operation(state, %{phase: :admission} = pause, reason) do
    state = %{state | run: pause.ctx.current_run, paused_admission: nil}
    handle_acquired_claim(state, pause.lifecycle, pause.work, {:error, reason})
  end

  @doc false
  @spec adopt_operation(RunExecutionState.t(), map(), term()) :: RunExecutionState.t()
  def adopt_operation(state, %{phase: :intent} = pause, :ok) do
    pause = %{pause | ctx: %{pause.ctx | current_run: pause.intent_run}}
    %{state | run: pause.intent_run, paused_admission: pause}
  end

  def adopt_operation(state, %{phase: :admission} = pause, %{status: :admitted} = result) do
    entry =
      sequential_entry(state, pause.lifecycle, pause.work, result.task)
      |> Map.put(:materialization_claim, result.context.materialization_claim)

    pause = Map.put(pause, :submitted_entry, entry)

    %{
      state
      | run:
          if(state.run.event_seq > pause.admitted_run.event_seq,
            do: state.run,
            else: pause.admitted_run
          ),
        paused_admission: pause
    }
    |> RunExecutionState.add_work(entry)
  end

  def adopt_operation(state, _pause, _result), do: state

  @doc false
  @spec resume_operation(RunExecutionState.t(), map(), term()) :: directive()
  def resume_operation(state, %{phase: :intent} = pause, :ok) do
    state = %{state | run: pause.intent_run, paused_admission: nil}
    admit_intent(state, pause.lifecycle, pause.work, pause.intent)
  end

  def resume_operation(state, %{phase: :admission} = pause, %{status: :admitted} = result) do
    run =
      if state.run.event_seq > pause.admitted_run.event_seq,
        do: state.run,
        else: pause.admitted_run

    state = %{state | run: run, paused_admission: nil}

    {:await, state, entry} =
      accept_enqueued_attempt(
        state,
        pause.lifecycle,
        pause.work,
        result.context.materialization_claim,
        result.task
      )

    if result.replayed? do
      case FavnOrchestrator.RunServer.Execution.RecoveredTask.reconcile(run, result.task, entry) do
        {:ok, entry} -> {:await, RunExecutionState.add_work(state, entry), entry}
        {:error, reason} -> {:recovery_required, state, reason}
      end
    else
      {:await, state, entry}
    end
  end

  def resume_operation(state, %{phase: :admission} = pause, %{status: :already_claimed}) do
    handle_acquired_claim(
      %{state | paused_admission: nil},
      pause.lifecycle,
      pause.work,
      {:error, %{details: %{reason_code: "target_write_in_progress"}}}
    )
  end

  @doc false
  @spec cleanup_paused(RunExecutionState.t()) :: RunExecutionState.t()
  def cleanup_paused(%{paused_admission: %{kind: :sequential, submitted_entry: _}} = state),
    do: %{state | paused_admission: nil}

  def cleanup_paused(%{paused_admission: %{kind: :sequential}} = state),
    do: %{state | paused_admission: nil}

  defp accept_enqueued_attempt(state, lifecycle, work, claim, task) do
    entry =
      sequential_entry(state, lifecycle, work, task) |> Map.put(:materialization_claim, claim)

    {:await, RunExecutionState.add_work(state, entry), entry}
  end

  defp fail_before_enqueue(%RunExecutionState{} = state, lifecycle, work, claim, reason) do
    pause = sequential_pause(state.run, lifecycle, work, claim, :started)
    state = cleanup_paused(struct(state, paused_admission: pause))
    {:ok, intent} = AdmissionIntent.load(state.run, work, state.version)

    state =
      if intent do
        {:ok, metadata} = AdmissionIntent.clear(state.run.metadata, intent)
        %{state | run: Snapshots.snapshot_update(state.run, metadata: metadata)}
      else
        state
      end

    persist_pre_submit_failure(
      state,
      lifecycle.asset_ref,
      lifecycle.node_key,
      lifecycle.stage,
      lifecycle.attempt,
      reason,
      work.asset_step_id
    )
  end

  defp persist_pre_submit_failure(
         state,
         asset_ref,
         node_key,
         stage,
         attempt,
         reason,
         asset_step_id \\ nil
       ) do
    status = if reason == :target_write_admission_timeout, do: :timed_out, else: :error
    failed = RunState.transition(state.run, status: status, runner_task_id: nil, error: reason)

    data = %{
      asset_ref: asset_ref,
      error: reason,
      node_key: node_key,
      asset_step_id:
        asset_step_id || AssetStepIdentity.asset_step_id(state.run.id, node_key, asset_ref),
      window: planned_window(state.run, node_key),
      stage: stage,
      attempt: attempt,
      max_attempts: StepAttemptLifecycle.retry_policy(state.run, node_key).max_attempts
    }

    resume = %{
      kind: :pre_submit_failure,
      run: failed,
      asset_ref: asset_ref,
      node_key: node_key,
      stage: stage,
      attempt: attempt,
      reason: reason,
      retryable?: safe_retryable?(reason)
    }

    event = if status == :timed_out, do: :step_timed_out, else: :step_failed
    persist_or_retry(state, failed, event, data, resume)
  end

  defp sequential_entry(state, lifecycle, work, task) do
    %{
      run_id: state.run.id,
      asset_step_id: work.asset_step_id,
      asset_ref: lifecycle.asset_ref,
      node_key: lifecycle.node_key,
      window: RunnerWork.window(work),
      task_id: task.task_id,
      deadline_at: work.deadline_at,
      assignment_generation: task.assignment_generation,
      runner_pool: task.runner_pool,
      required_runner_release_id: task.required_runner_release_id,
      stage: lifecycle.stage,
      attempt: lifecycle.attempt,
      execution_pool: RunnerWork.execution_pool(work)
    }
  end

  defp maybe_schedule_retry(state, _asset_ref, node_key, stage, attempt, step_results, failure) do
    if StepAttemptLifecycle.retry_allowed?(state.run, node_key, attempt) do
      lifecycle = StepAttemptLifecycle.new(state.run, state.version, node_key, stage, attempt)
      schedule_retry(state, StepAttemptLifecycle.retry(lifecycle, failure))
    else
      terminalize_error(state, step_results)
    end
  end

  defp schedule_retry(state, retry) do
    admission? = is_integer(Map.get(retry, :admission_deadline_ms))
    next_retry_at = System.system_time(:millisecond) + retry.retry_after_ms

    retry_state = %{
      kind: :sequential,
      retry: Map.drop(retry, [:retry_policy]),
      sequential_index: state.sequential_index,
      next_retry_at: next_retry_at
    }

    retrying =
      RunState.transition(state.run,
        status: :running,
        error: nil,
        runner_task_id: nil,
        metadata:
          Map.merge(state.run.metadata, %{
            retrying: not admission?,
            next_attempt: retry.next_attempt,
            retry_state: retry_state,
            next_retry_at: next_retry_at
          })
      )

    persist_or_retry(
      state,
      retrying,
      if(admission?, do: :step_queued, else: :step_retry_scheduled),
      StepAttemptLifecycle.retry_event_payload(retry)
      |> Map.put(:next_retry_at, next_retry_at),
      %{kind: :schedule_retry, run: retrying, retry: retry}
    )
  end

  defp schedule_retry_timer(state, retry), do: {:retry_timer, state, retry}

  defp clear_retry_state(metadata) do
    metadata
    |> Map.drop([:retry_state, "retry_state", :next_retry_at, "next_retry_at"])
    |> Map.put(:retrying, false)
  end

  defp safe_retryable?(%RunnerError{retryable?: true, outcome: :safe_failure}), do: true
  defp safe_retryable?(_reason), do: false

  defp attempt_start_event(attempt) when attempt > 1, do: :step_retry_started
  defp attempt_start_event(_attempt), do: :step_started

  defp planned_window(%RunState{plan: %Favn.Plan{nodes: nodes}}, node_key) do
    nodes
    |> Map.get(node_key, %{})
    |> Map.get(:window)
  end

  defp planned_window(%RunState{}, _node_key), do: nil

  defp persist_or_retry(state, run, event_type, data, resume) do
    retry = PersistenceRetry.new(run, event_type, data, {:sequential, resume})

    case PersistenceRetry.persist(retry) do
      :ok -> resume_persisted(state, resume)
      {:error, :external_cancel} -> {:terminal, Snapshots.cancelled_snapshot(state.run)}
      {:error, reason} -> {:persist_retry, state, retry, reason}
    end
  end

  defp terminalize_error(state, step_results) do
    all_results =
      state.run
      |> ResultBuilder.sort_asset_results(
        step_results
        |> ResultSanitizer.sanitize_asset_results()
        |> Kernel.++(state.accumulated_results)
        |> ResultBuilder.retain_asset_results()
      )

    {:terminal,
     Snapshots.snapshot_update(state.run,
       runner_task_id: nil,
       result: ResultBuilder.pipeline_result(state.run, state.run.status, all_results)
     )}
  end

  defp cancel_work(%RunExecutionState{} = state, task_ids, reason) do
    work_set = ActiveTaskSet.retain_task_ids(state.work_set, task_ids)

    {run, work_set} =
      ActiveTaskSet.cancel_all(state.run, work_set, reason)

    %{state | run: run, work_set: work_set}
  end

  defp node_asset_ref(%Favn.Plan{nodes: nodes}, node_key) do
    case Map.fetch(nodes, node_key) do
      {:ok, node} -> node.ref
      :error -> elem(node_key, 0)
    end
  end

  defp stage_from_plan(%Favn.Plan{nodes: nodes}, node_key, fallback_stage) do
    case Map.get(nodes, node_key) do
      %{stage: stage} when is_integer(stage) and stage >= 0 -> stage
      _node -> fallback_stage
    end
  end
end
