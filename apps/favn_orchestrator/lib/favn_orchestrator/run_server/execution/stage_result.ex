defmodule FavnOrchestrator.RunServer.Execution.StageResult do
  @moduledoc """
  Settles runner outcomes for one pipeline stage attempt.

  A stage result is durable only after its run event, node result, freshness
  evidence, materialization claim, and execution ownership have been updated.
  Failed siblings are remembered while already-submitted work in the same stage
  drains to a known outcome.

  A successful node whose claim pins an uninitialized persisted generation
  settles in two parts. Its step outcome and claim completion are persisted
  synchronously, then `process/4` returns `{:post_step_pending, state, pending}`
  so the caller can run `InitialTargetGenerationReconciler.reconcile/1` in a
  worker. `finish_post_step/3` completes the settlement from the worker's reply
  using the current stage state. The node's result is already appended to the
  stage run and its task removed from the pending set at that point, so sibling
  settlements use the correct sequence while the worker is running.
  """

  alias Favn.Contracts.RunnerResult
  alias FavnOrchestrator.CancellationOutcome
  alias FavnOrchestrator.Freshness.StateWriter
  alias FavnOrchestrator.InitialTargetGenerationReconciler
  alias FavnOrchestrator.MaterializationClaims
  alias FavnOrchestrator.ResourceCircuits
  alias FavnOrchestrator.RunServer.Cancellation
  alias FavnOrchestrator.RunServer.Execution.ResultBuilder
  alias FavnOrchestrator.RunServer.Execution.ResultSanitizer
  alias FavnOrchestrator.RunServer.Execution.RecoveryPosition
  alias FavnOrchestrator.RunServer.Execution.ActiveTaskSet
  alias FavnOrchestrator.RunServer.Execution.StageAttemptState
  alias FavnOrchestrator.RunServer.Execution.StepAttemptLifecycle
  alias FavnOrchestrator.RunServer.Persistence
  alias FavnOrchestrator.RunServer.PersistenceRetry
  alias FavnOrchestrator.RunServer.Snapshots
  alias FavnOrchestrator.RunState

  @type settlement_context :: %{
          required(:stage) => non_neg_integer(),
          required(:attempt) => pos_integer()
        }

  @typedoc """
  Settlement continuation for a node whose reconciliation runs in a worker.

  `entry` carries the pinned version, manifest index, and freshness context the
  reconciler needs. `post_step_value` is the successful runner result.
  """
  @type post_step_pending :: %{
          required(:entry) => map(),
          required(:stage) => non_neg_integer(),
          required(:attempt) => pos_integer(),
          required(:post_step_value) => RunnerResult.t()
        }

  @type settlement_result ::
          {:cont, StageAttemptState.t()}
          | {:halt, {:error, RunState.t(), [term()], [Favn.Plan.node_key()]}}
          | {:persist_retry, PersistenceRetry.t(), term()}
          | {:post_step_pending, StageAttemptState.t(), post_step_pending()}

  @doc "Builds a timed-out terminal result for work still waiting on admission."
  @spec timeout_deferred(StageAttemptState.t()) ::
          {:error, RunState.t(), [term()], [Favn.Plan.node_key()]}
  def timeout_deferred(%StageAttemptState{run: %RunState{} = run_state} = state) do
    results = StageAttemptState.settled_results(state)

    timed_out =
      Snapshots.snapshot_update(run_state,
        status: :timed_out,
        error: :timeout,
        runner_task_id: nil
      )

    {:error, timed_out, results, StageAttemptState.attempted_node_keys(state)}
  end

  @doc "Settles one completed runner await within its stage attempt."
  @spec process(StageAttemptState.t(), map(), term(), settlement_context()) :: settlement_result()
  def process(
        %StageAttemptState{
          run: current_run,
          results: _current_results
        } = state,
        entry,
        await_result,
        %{stage: stage, attempt: attempt}
      ) do
    cancellation = Persistence.cancellation_state(current_run)

    if cancellation == :cancelled or
         (cancellation == :requested and not match?({:ok, %RunnerResult{}}, await_result)) do
      cancelled =
        cancel_task_ids(
          current_run,
          ActiveTaskSet.active_runner_task_ids(current_run),
          %{kind: :external_cancel}
        )

      :ok = ActiveTaskSet.fail_entry_claim(entry, :external_cancel)

      results = StageAttemptState.settled_results(state)

      {:halt,
       {:error, Snapshots.cancelled_terminal(cancelled, results), results,
        StageAttemptState.attempted_node_keys(state)}}
    else
      case process_one_result(current_run, entry, await_result, %{
             stage: stage,
             attempt: attempt
           }) do
        {:persist_retry, %PersistenceRetry{} = retry, reason} ->
          {:persist_retry, retry, reason}

        finished ->
          settle_finished_step(state, finished, entry, %{stage: stage, attempt: attempt})
      end
    end
  end

  @doc false
  @spec resume_persisted(StageAttemptState.t(), map()) :: settlement_result()
  def resume_persisted(%StageAttemptState{} = state, %{kind: :step} = resume) do
    settle_finished_step(state, finish_persisted_step(resume), resume.entry, resume)
  end

  def resume_persisted(%StageAttemptState{}, %{kind: :stage_state, state: next_state}),
    do: {:cont, next_state}

  @doc """
  Completes a node settlement deferred by `{:post_step_pending, _, pending}`.

  `result` is the reconciler's reply, or an error built from a worker exit.
  The node's result is not appended again. On `:ok` the node settles as it
  would have synchronously; any error settles it as
  `post_step_persistence_failed` with the bounded reason. Resource-circuit
  settlement and result recording run exactly once, here.
  """
  @spec finish_post_step(StageAttemptState.t(), post_step_pending(), :ok | {:error, term()}) ::
          settlement_result()
  def finish_post_step(%StageAttemptState{run: current_run} = state, pending, result) do
    {next_run, outcome} = complete_post_step(current_run, pending, result)

    settle_processed_result(
      state,
      next_run,
      outcome,
      [],
      pending.entry,
      %{stage: pending.stage, attempt: pending.attempt}
    )
  end

  defp complete_post_step(%RunState{} = step_state, pending, :ok) do
    case persist_terminal_resource_outcome(
           step_state,
           pending.entry,
           :ok,
           pending.post_step_value
         ) do
      :ok -> {step_state, :ok}
      {:error, reason} -> {post_step_persistence_failure(step_state, reason), :error}
    end
  end

  defp complete_post_step(%RunState{} = step_state, _pending, {:error, reason}),
    do: {post_step_persistence_failure(step_state, reason), :error}

  defp settle_finished_step(state, {:settled, next_run, outcome, step_results}, entry, context) do
    settle_processed_result(state, next_run, outcome, step_results, entry, context)
  end

  defp settle_finished_step(state, {:post_step_pending, step_state, pending}, entry, _context) do
    next_state =
      StageAttemptState.record_result(
        state,
        step_state,
        Enum.reverse(pending.step_results, state.results),
        state.retry_refs,
        state.retry_delays,
        state.terminal_failure,
        MapSet.delete(state.pending_ids, entry.task_id)
      )

    {:post_step_pending, next_state, Map.delete(pending, :step_results)}
  end

  defp settle_processed_result(
         state,
         next_run,
         outcome,
         step_results,
         entry,
         %{stage: stage, attempt: attempt}
       ) do
    status = ResultBuilder.latest_node_status(next_run, entry.node_key) || :error
    state = StageAttemptState.put_node_status(state, entry.node_key, status)

    reduce_outcome(
      outcome,
      %{
        state: state,
        run: next_run,
        results: Enum.reverse(step_results, state.results),
        retry_refs: state.retry_refs,
        retry_delays: state.retry_delays,
        terminal_failure: state.terminal_failure,
        pending_ids: MapSet.delete(state.pending_ids, entry.task_id)
      },
      %{entry: entry, stage: stage, attempt: attempt}
    )
  end

  @doc "Returns the terminal or retryable result of a fully drained stage attempt."
  @spec finalize(StageAttemptState.t()) ::
          {:ok, RunState.t(), [term()], [Favn.Plan.node_key()], [Favn.Plan.node_key()], map()}
          | {:error, RunState.t(), [term()], [Favn.Plan.node_key()], map()}
  def finalize(
        %StageAttemptState{
          run: next_run,
          results: next_results,
          retry_refs: retry_refs,
          terminal_failure: nil,
          node_statuses: node_statuses
        } = state
      ) do
    {:ok, next_run, Enum.reverse(next_results), Enum.reverse(retry_refs),
     StageAttemptState.attempted_node_keys(state), node_statuses}
  end

  def finalize(
        %StageAttemptState{
          run: next_run,
          results: next_results,
          terminal_failure: terminal_failure,
          node_statuses: node_statuses
        } = state
      ) do
    failed_run = failed_terminal_state(next_run, terminal_failure)

    {:error, failed_run, Enum.reverse(next_results), StageAttemptState.attempted_node_keys(state),
     node_statuses}
  end

  defp process_one_result(
         %RunState{} = run_state,
         %{asset_ref: asset_ref} = entry,
         {:ok, %RunnerResult{} = result},
         %{stage: stage, attempt: attempt}
       ) do
    result = ResultSanitizer.sanitize(result)
    asset_results = result.asset_results
    step_status = StepAttemptLifecycle.map_runner_status(result.status)
    {event_type, retryable?} = StepAttemptLifecycle.step_outcome(step_status)
    retryable? = retryable? and StepAttemptLifecycle.runner_result_retryable?(result)

    node_result =
      ResultBuilder.execution_result(
        run_state,
        entry,
        stage,
        attempt,
        step_status,
        asset_results
      )

    step_state =
      RunState.transition(run_state,
        status: step_status,
        error: result.error,
        metadata: ResultSanitizer.merge_metadata(run_state.metadata, result.metadata),
        runner_task_id: nil
      )
      |> RecoveryPosition.record_outcome(stage, attempt)

    data = %{
      asset_ref: asset_ref,
      result_status: result.status,
      error: result.error,
      node_key: Map.get(entry, :node_key),
      asset_step_id: Map.get(entry, :asset_step_id),
      stage: stage,
      attempt: attempt,
      max_attempts:
        StepAttemptLifecycle.retry_policy(run_state, Map.fetch!(entry, :node_key)).max_attempts,
      retryable?: retryable?,
      retry_exhausted?:
        retryable? and
          not StepAttemptLifecycle.retry_allowed?(run_state, entry.node_key, attempt),
      execution_pool: Map.get(entry, :execution_pool),
      node_result: node_result
    }

    resume = %{
      kind: :step,
      run: step_state,
      original_run: run_state,
      entry: entry,
      stage: stage,
      attempt: attempt,
      status: step_status,
      retryable?: retryable?,
      asset_results: asset_results,
      node_result: node_result,
      post_step_value: result
    }

    persist_step(event_type, data, resume)
  end

  defp process_one_result(
         %RunState{} = run_state,
         %{asset_ref: asset_ref, task_id: task_id} = entry,
         {:error, :timeout},
         %{stage: stage, attempt: attempt}
       ) do
    cleared =
      cancel_task_ids(
        run_state,
        [task_id],
        %{kind: :await_timeout, asset_ref: asset_ref, stage: stage, attempt: attempt}
      )

    node_result =
      ResultBuilder.execution_result(cleared, entry, stage, attempt, :timed_out, [])

    step_state =
      RunState.transition(cleared,
        status: :timed_out,
        error: :timeout,
        runner_task_id: nil
      )
      |> RecoveryPosition.record_outcome(stage, attempt)

    data = %{
      asset_ref: asset_ref,
      error: :timeout,
      node_key: Map.get(entry, :node_key),
      asset_step_id: Map.get(entry, :asset_step_id),
      stage: stage,
      attempt: attempt,
      max_attempts:
        StepAttemptLifecycle.retry_policy(run_state, Map.fetch!(entry, :node_key)).max_attempts,
      execution_pool: Map.get(entry, :execution_pool),
      node_result: node_result
    }

    resume = %{
      kind: :step,
      run: step_state,
      original_run: run_state,
      entry: entry,
      stage: stage,
      attempt: attempt,
      status: :timed_out,
      retryable?: false,
      asset_results: [],
      node_result: node_result,
      post_step_value: :timeout
    }

    persist_step(:step_timed_out, data, resume)
  end

  defp process_one_result(
         %RunState{} = run_state,
         %{asset_ref: asset_ref, task_id: task_id} = entry,
         {:error, reason},
         %{stage: stage, attempt: attempt}
       ) do
    cleared =
      cancel_task_ids(
        run_state,
        [task_id],
        %{
          kind: :await_error,
          asset_ref: asset_ref,
          stage: stage,
          attempt: attempt,
          error: reason
        }
      )

    node_result = ResultBuilder.execution_result(cleared, entry, stage, attempt, :error, [])

    step_state =
      RunState.transition(cleared,
        status: :error,
        error: reason,
        runner_task_id: nil
      )
      |> RecoveryPosition.record_outcome(stage, attempt)

    data = %{
      asset_ref: asset_ref,
      error: reason,
      node_key: Map.get(entry, :node_key),
      asset_step_id: Map.get(entry, :asset_step_id),
      stage: stage,
      attempt: attempt,
      max_attempts:
        StepAttemptLifecycle.retry_policy(run_state, Map.fetch!(entry, :node_key)).max_attempts,
      execution_pool: Map.get(entry, :execution_pool),
      node_result: node_result
    }

    resume = %{
      kind: :step,
      run: step_state,
      original_run: run_state,
      entry: entry,
      stage: stage,
      attempt: attempt,
      status: :error,
      retryable?: false,
      asset_results: [],
      node_result: node_result,
      post_step_value: reason
    }

    persist_step(:step_failed, data, resume)
  end

  defp persist_step(event_type, data, resume) do
    retry = PersistenceRetry.new(resume.run, event_type, data, {:pipeline, resume})

    case PersistenceRetry.persist(retry) do
      :ok ->
        finish_persisted_step(resume)

      {:error, :external_cancel} ->
        {run, outcome, results} =
          return_external_cancel(resume.original_run, resume.asset_results)

        {:settled, run, outcome, results}

      {:error, reason} ->
        {:persist_retry, retry, reason}
    end
  end

  defp finish_persisted_step(resume) do
    step_state = ResultBuilder.append_node_result(resume.run, resume.node_result)

    case persist_post_step_state(
           step_state,
           resume.entry,
           resume.status,
           resume.post_step_value
         ) do
      :ok ->
        outcome =
          cond do
            resume.status == :ok ->
              :ok

            resume.retryable? and not Persistence.externally_cancelled?(step_state) and
                StepAttemptLifecycle.retry_allowed?(
                  step_state,
                  resume.entry.node_key,
                  resume.attempt
                ) ->
              {:retry,
               StepAttemptLifecycle.retry_delay_ms(
                 step_state,
                 resume.entry.node_key,
                 resume.attempt,
                 resume.post_step_value
               )}

            true ->
              :error
          end

        case persist_terminal_resource_outcome(
               step_state,
               resume.entry,
               outcome,
               resume.post_step_value
             ) do
          :ok ->
            {:settled, step_state, outcome, resume.asset_results}

          {:error, reason} ->
            {:settled, post_step_persistence_failure(step_state, reason), :error,
             resume.asset_results}
        end

      :post_step_pending ->
        if Persistence.externally_cancelled?(step_state) do
          step_state = %{
            step_state
            | metadata: Map.put(step_state.metadata, "cancellation_needs_attention", true)
          }

          case persist_terminal_resource_outcome(
                 step_state,
                 resume.entry,
                 :ok,
                 resume.post_step_value
               ) do
            :ok ->
              {:settled, step_state, :ok, resume.asset_results}

            {:error, reason} ->
              {:settled, post_step_persistence_failure(step_state, reason), :error,
               resume.asset_results}
          end
        else
          {:post_step_pending, step_state,
           %{
             entry: resume.entry,
             stage: resume.stage,
             attempt: resume.attempt,
             post_step_value: resume.post_step_value,
             step_results: resume.asset_results
           }}
        end

      {:error, reason} ->
        {:settled, post_step_persistence_failure(step_state, reason), :error,
         resume.asset_results}
    end
  end

  defp persist_terminal_resource_outcome(step_state, entry, :ok, value),
    do: ResourceCircuits.settle(step_state, entry, :ok, value)

  defp persist_terminal_resource_outcome(step_state, entry, :error, value),
    do: ResourceCircuits.settle(step_state, entry, :error, value)

  defp persist_terminal_resource_outcome(_step_state, _entry, {:retry, _delay_ms}, _value),
    do: :ok

  # Runner-backed reconciliation never runs here: it would block the run process
  # past its ownership lease. The caller runs it in a worker instead.
  defp persist_post_step_state(%RunState{} = step_state, entry, :ok, %RunnerResult{} = result) do
    with {:ok, freshness_state} <- record_freshness(step_state, entry, :ok),
         :ok <-
           MaterializationClaims.complete(
             Map.get(entry, :materialization_claim),
             result,
             freshness_state
           ) do
      if InitialTargetGenerationReconciler.applicable?(entry), do: :post_step_pending, else: :ok
    else
      {:error, reason} -> {:error, reason}
    end
  end

  defp persist_post_step_state(%RunState{} = _step_state, entry, status, failure_reason) do
    with :ok <- MaterializationClaims.fail_entry(entry, {status, failure_reason}) do
      :ok
    else
      {:error, reason} -> {:error, reason}
    end
  end

  defp record_freshness(
         %RunState{} = run_state,
         %{version: version, node_key: node_key, freshness_context: freshness_context} = entry,
         :ok
       ) do
    {:ok,
     StateWriter.build_success_state(
       run_state,
       version,
       node_key,
       entry.decision,
       freshness_context
     )}
  end

  defp reduce_outcome(
         :ok,
         %{state: state} = settlement,
         _context
       ) do
    {:cont, record_result(state, settlement)}
  end

  defp reduce_outcome(
         {:retry, retry_delay_ms},
         %{
           state: state,
           retry_refs: retry_refs,
           retry_delays: retry_delays
         } = settlement,
         %{entry: entry}
       ) do
    next_retry_refs = [entry.node_key | retry_refs]
    next_retry_delays = Map.put(retry_delays, entry.node_key, retry_delay_ms)

    {:cont,
     record_result(state, %{
       settlement
       | retry_refs: next_retry_refs,
         retry_delays: next_retry_delays
     })}
  end

  defp reduce_outcome(
         :error,
         %{state: state, run: %RunState{} = next_run, results: next_results} = settlement,
         %{entry: entry, stage: stage, attempt: attempt}
       ) do
    case remember_failure(
           next_run,
           settlement.terminal_failure,
           entry,
           stage,
           attempt,
           settlement.pending_ids
         ) do
      {:ok, failure_run, next_terminal_failure} ->
        {:cont,
         record_result(state, %{
           settlement
           | run: failure_run,
             terminal_failure: next_terminal_failure
         })}

      {:error, cancelled} ->
        results = Enum.reverse(next_results)

        {:halt,
         {:error, Snapshots.cancelled_terminal(cancelled, results), results,
          StageAttemptState.attempted_node_keys(state)}}

      {:persist_retry, failure_run, next_terminal_failure, retry, reason} ->
        next_state =
          record_result(state, %{
            settlement
            | run: failure_run,
              terminal_failure: next_terminal_failure
          })

        retry = %{retry | resume: {:pipeline, %{kind: :stage_state, state: next_state}}}
        {:persist_retry, retry, reason}
    end
  end

  defp record_result(state, settlement) do
    StageAttemptState.record_result(
      state,
      settlement.run,
      settlement.results,
      settlement.retry_refs,
      settlement.retry_delays,
      settlement.terminal_failure,
      settlement.pending_ids
    )
  end

  defp remember_failure(run_state, terminal_failure, entry, stage, attempt, pending_ids)

  defp remember_failure(%RunState{} = run_state, nil, entry, stage, attempt, pending_ids) do
    terminal_failure = %{status: run_state.status, error: run_state.error}

    if MapSet.size(pending_ids) == 0 do
      {:ok, run_state, terminal_failure}
    else
      pending_task_ids = MapSet.to_list(pending_ids)

      metadata =
        Map.put(run_state.metadata, :stage_draining_after_failure, %{
          stage: stage,
          attempt: attempt,
          failed_asset_ref: entry.asset_ref,
          pending_task_ids: pending_task_ids
        })

      draining =
        RunState.transition(run_state,
          status: :running,
          runner_task_id: List.first(pending_task_ids),
          metadata: metadata
        )

      data = %{
        stage: stage,
        attempt: attempt,
        failed_asset_ref: entry.asset_ref,
        pending_task_ids: pending_task_ids
      }

      retry = PersistenceRetry.new(draining, :stage_draining_after_failure, data, nil)

      case PersistenceRetry.persist(retry) do
        :ok -> {:ok, draining, terminal_failure}
        {:error, :external_cancel} -> {:error, Snapshots.cancelled_snapshot(draining)}
        {:error, reason} -> {:persist_retry, draining, terminal_failure, retry, reason}
      end
    end
  end

  defp remember_failure(%RunState{} = run_state, terminal_failure, _, _, _, _),
    do: {:ok, run_state, terminal_failure}

  defp failed_terminal_state(%RunState{} = run_state, %{status: status, error: error}) do
    Snapshots.snapshot_update(run_state,
      status: status,
      error: error,
      runner_task_id: nil
    )
  end

  defp post_step_persistence_failure(%RunState{} = step_state, reason) do
    Snapshots.snapshot_update(step_state,
      status: :error,
      error: %{
        type: :post_step_persistence_failed,
        message: "Post-step reconciliation or persistence failed after the asset write succeeded",
        reason: reason
      },
      runner_task_id: nil
    )
  end

  defp cancel_task_ids(
         %RunState{} = run_state,
         task_ids,
         reason
       )
       when is_list(task_ids) do
    cancel_results = Cancellation.dispatch_runner_tasks(run_state, task_ids, reason)

    confirmed_ids =
      cancel_results
      |> Enum.filter(&CancellationOutcome.confirmed?/1)
      |> Enum.map(& &1.task_id)

    run_state
    |> put_cancel_outcomes(cancel_results)
    |> clear_inflight_tasks(confirmed_ids)
  end

  defp clear_inflight_tasks(%RunState{} = run_state, task_ids) do
    rejected = MapSet.new(task_ids)

    ids =
      run_state
      |> ActiveTaskSet.active_runner_task_ids()
      |> Enum.reject(&MapSet.member?(rejected, &1))

    Snapshots.snapshot_update(run_state,
      metadata:
        run_state.metadata
        |> Map.delete(:active_runner_task_ids)
        |> Map.delete("active_runner_task_ids")
        |> Map.put(:active_runner_task_ids, ids),
      runner_task_id: nil
    )
  end

  defp put_cancel_outcomes(%RunState{} = run_state, outcomes) do
    metadata =
      Map.put(
        run_state.metadata,
        :cancel_outcomes,
        Enum.map(outcomes, &CancellationOutcome.to_map/1)
      )

    Snapshots.snapshot_update(run_state, metadata: metadata)
  end

  defp return_external_cancel(%RunState{} = run_state, step_results) do
    {Snapshots.cancelled_snapshot(run_state), :error, step_results}
  end
end
