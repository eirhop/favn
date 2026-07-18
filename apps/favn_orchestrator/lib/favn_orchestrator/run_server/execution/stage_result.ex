defmodule FavnOrchestrator.RunServer.Execution.StageResult do
  @moduledoc """
  Settles runner outcomes for one pipeline stage attempt.

  A stage result is durable only after its run event, node result, freshness
  evidence, materialization claim, and execution ownership have been updated.
  Failed siblings are remembered while already-submitted work in the same stage
  drains to a known outcome.
  """

  alias Favn.Contracts.RunnerResult
  alias FavnOrchestrator.Freshness.StateWriter
  alias FavnOrchestrator.MaterializationClaims
  alias FavnOrchestrator.RunExecutionOwnership
  alias FavnOrchestrator.RunServer.Cancellation
  alias FavnOrchestrator.RunServer.Execution.ResultBuilder
  alias FavnOrchestrator.RunServer.Execution.ResultSanitizer
  alias FavnOrchestrator.RunServer.Execution.RunWorkSet
  alias FavnOrchestrator.RunServer.Execution.StageAttemptState
  alias FavnOrchestrator.RunServer.Execution.StepAttemptLifecycle
  alias FavnOrchestrator.RunServer.Persistence
  alias FavnOrchestrator.RunServer.PersistenceRetry
  alias FavnOrchestrator.RunServer.Snapshots
  alias FavnOrchestrator.RunState

  @type settlement_context :: %{
          required(:stage) => non_neg_integer(),
          required(:attempt) => pos_integer(),
          required(:runner_client) => module(),
          required(:runner_opts) => keyword()
        }

  @type settlement_result ::
          {:cont, StageAttemptState.t()}
          | {:halt, {:error, RunState.t(), [term()], [Favn.Plan.node_key()]}}
          | {:persist_retry, PersistenceRetry.t(), term()}

  @doc "Builds a timed-out terminal result for work still waiting on admission."
  @spec timeout_deferred(StageAttemptState.t()) ::
          {:error, RunState.t(), [term()], [Favn.Plan.node_key()]}
  def timeout_deferred(%StageAttemptState{run: %RunState{} = run_state} = state) do
    results = StageAttemptState.settled_results(state)

    timed_out =
      RunState.transition(run_state,
        status: :timed_out,
        error: :timeout,
        runner_execution_id: nil
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
        %{stage: stage, attempt: attempt, runner_client: runner_client, runner_opts: runner_opts}
      ) do
    if Persistence.externally_cancelled?(current_run) do
      cancelled =
        cancel_execution_ids(
          current_run,
          RunWorkSet.inflight_execution_ids(current_run),
          %{kind: :external_cancel},
          runner_client,
          runner_opts
        )

      :ok = RunWorkSet.fail_entry_claim(entry, :external_cancel)

      results = StageAttemptState.settled_results(state)

      {:halt,
       {:error, Snapshots.cancelled_terminal(cancelled, results), results,
        StageAttemptState.attempted_node_keys(state)}}
    else
      case process_one_result(current_run, entry, await_result, %{
             stage: stage,
             attempt: attempt,
             runner_client: runner_client,
             runner_opts: runner_opts
           }) do
        {:settled, next_run, outcome, step_results} ->
          settle_processed_result(
            state,
            next_run,
            outcome,
            step_results,
            entry,
            %{stage: stage, attempt: attempt}
          )

        {:persist_retry, %PersistenceRetry{} = retry, reason} ->
          {:persist_retry, retry, reason}
      end
    end
  end

  @doc false
  @spec resume_persisted(StageAttemptState.t(), map()) :: settlement_result()
  def resume_persisted(%StageAttemptState{} = state, %{kind: :step} = resume) do
    {next_run, outcome, step_results} = finish_persisted_step(resume)

    settle_processed_result(
      state,
      next_run,
      outcome,
      step_results,
      resume.entry,
      resume
    )
  end

  def resume_persisted(%StageAttemptState{}, %{kind: :stage_state, state: next_state}),
    do: {:cont, next_state}

  defp settle_processed_result(
         state,
         next_run,
         outcome,
         step_results,
         entry,
         %{stage: stage, attempt: attempt}
       ) do
    reduce_outcome(
      outcome,
      %{
        state: state,
        run: next_run,
        results: Enum.reverse(step_results, state.results),
        retry_refs: state.retry_refs,
        retry_delays: state.retry_delays,
        terminal_failure: state.terminal_failure,
        pending_ids: MapSet.delete(state.pending_ids, entry.execution_id)
      },
      %{entry: entry, stage: stage, attempt: attempt}
    )
  end

  @doc "Returns the terminal or retryable result of a fully drained stage attempt."
  @spec finalize(StageAttemptState.t()) ::
          {:ok, RunState.t(), [term()], [Favn.Plan.node_key()], [Favn.Plan.node_key()]}
          | {:error, RunState.t(), [term()], [Favn.Plan.node_key()]}
  def finalize(%StageAttemptState{
        run: next_run,
        results: next_results,
        retry_refs: retry_refs,
        terminal_failure: nil,
        attempted_node_keys: attempted_node_keys
      }) do
    {:ok, next_run, Enum.reverse(next_results), Enum.reverse(retry_refs), attempted_node_keys}
  end

  def finalize(%StageAttemptState{
        run: next_run,
        results: next_results,
        terminal_failure: terminal_failure,
        attempted_node_keys: attempted_node_keys
      }) do
    failed_run = failed_terminal_state(next_run, terminal_failure)
    {:error, failed_run, Enum.reverse(next_results), attempted_node_keys}
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
        runner_execution_id: nil
      )

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
         %{asset_ref: asset_ref, execution_id: execution_id} = entry,
         {:error, :timeout},
         %{stage: stage, attempt: attempt, runner_client: runner_client, runner_opts: runner_opts}
       ) do
    cleared =
      cancel_execution_ids(
        run_state,
        [execution_id],
        %{kind: :await_timeout, asset_ref: asset_ref, stage: stage, attempt: attempt},
        runner_client,
        runner_opts
      )

    node_result =
      ResultBuilder.execution_result(cleared, entry, stage, attempt, :timed_out, [])

    step_state =
      RunState.transition(cleared,
        status: :timed_out,
        error: :timeout,
        runner_execution_id: nil
      )

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
         %{asset_ref: asset_ref, execution_id: execution_id} = entry,
         {:error, reason},
         %{stage: stage, attempt: attempt, runner_client: runner_client, runner_opts: runner_opts}
       ) do
    cleared =
      cancel_execution_ids(
        run_state,
        [execution_id],
        %{
          kind: :await_error,
          asset_ref: asset_ref,
          stage: stage,
          attempt: attempt,
          error: reason
        },
        runner_client,
        runner_opts
      )

    node_result = ResultBuilder.execution_result(cleared, entry, stage, attempt, :error, [])

    step_state =
      RunState.transition(cleared,
        status: :error,
        error: reason,
        runner_execution_id: nil
      )

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
        {run, outcome, results} = finish_persisted_step(resume)
        {:settled, run, outcome, results}

      {:error, :external_cancel} ->
        {run, outcome, results} =
          return_external_cancel(resume.original_run, resume.asset_results)

        {:settled, run, outcome, results}

      {:error, reason} ->
        {:persist_retry, retry, reason}
    end
  end

  defp finish_persisted_step(resume) do
    _ =
      RunExecutionOwnership.complete_execution(
        resume.original_run,
        resume.entry.execution_id
      )

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

            resume.retryable? and
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

        {step_state, outcome, resume.asset_results}

      {:error, reason} ->
        {post_step_persistence_failure(step_state, reason), :error, resume.asset_results}
    end
  end

  defp persist_post_step_state(%RunState{} = step_state, entry, :ok, %RunnerResult{} = result) do
    with {:ok, freshness_state} <- record_freshness(step_state, entry, :ok),
         :ok <-
           MaterializationClaims.complete(
             Map.get(entry, :materialization_claim),
             result,
             freshness_state
           ) do
      :ok
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

  defp record_freshness(%RunState{} = run_state, entry, :ok) do
    {:ok,
     StateWriter.build_success_state(
       run_state,
       Map.fetch!(entry, :version),
       Map.fetch!(entry, :node_key),
       Map.get(entry, :decision, %{}),
       Map.fetch!(entry, :freshness_context)
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
           terminal_failure: terminal_failure,
           retry_refs: retry_refs,
           retry_delays: retry_delays
         } = settlement,
         %{entry: entry}
       ) do
    next_retry_refs =
      if is_nil(terminal_failure), do: [entry.node_key | retry_refs], else: retry_refs

    next_retry_delays =
      if is_nil(terminal_failure),
        do: Map.put(retry_delays, entry.node_key, retry_delay_ms),
        else: retry_delays

    {:cont,
     record_result(state, %{
       settlement
       | retry_refs: next_retry_refs,
         retry_delays: next_retry_delays
     })}
  end

  defp reduce_outcome(
         :error,
         %{state: state, run: %RunState{status: :cancelled} = next_run, results: next_results},
         _context
       ) do
    results = Enum.reverse(next_results)

    {:halt,
     {:error, Snapshots.cancelled_terminal(next_run, results), results,
      StageAttemptState.attempted_node_keys(state)}}
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
      pending_execution_ids = MapSet.to_list(pending_ids)

      metadata =
        Map.put(run_state.metadata, :stage_draining_after_failure, %{
          stage: stage,
          attempt: attempt,
          failed_asset_ref: entry.asset_ref,
          pending_execution_ids: pending_execution_ids
        })

      draining =
        RunState.transition(run_state,
          status: :running,
          runner_execution_id: List.first(pending_execution_ids),
          metadata: metadata
        )

      data = %{
        stage: stage,
        attempt: attempt,
        failed_asset_ref: entry.asset_ref,
        pending_execution_ids: pending_execution_ids
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
      runner_execution_id: nil
    )
  end

  defp post_step_persistence_failure(%RunState{} = step_state, reason) do
    RunState.transition(step_state,
      status: :error,
      error: %{type: :post_step_persistence_failed, reason: reason},
      runner_execution_id: nil
    )
  end

  defp cancel_execution_ids(
         %RunState{} = run_state,
         execution_ids,
         reason,
         runner_client,
         runner_opts
       )
       when is_list(execution_ids) do
    cancel_results =
      Cancellation.dispatch_runner_work(
        run_state,
        execution_ids,
        reason,
        runner_client,
        runner_opts
      )

    _ = RunExecutionOwnership.persist_cancel_outcomes(run_state, cancel_results, reason)
    clear_inflight_executions(run_state, Enum.map(cancel_results, & &1.execution_id))
  end

  defp clear_inflight_executions(%RunState{} = run_state, execution_ids) do
    rejected = MapSet.new(execution_ids)

    ids =
      run_state
      |> RunWorkSet.inflight_execution_ids()
      |> Enum.reject(&MapSet.member?(rejected, &1))

    Snapshots.snapshot_update(run_state,
      metadata: Map.put(run_state.metadata, :in_flight_execution_ids, ids),
      runner_execution_id: List.first(ids)
    )
  end

  defp return_external_cancel(%RunState{} = run_state, step_results) do
    {Snapshots.cancelled_snapshot(run_state), :error, step_results}
  end
end
