defmodule FavnOrchestrator.RunServer.Execution.Sequential do
  @moduledoc """
  Executes and settles one sequential run attempt at a time.

  The module owns sequential dispatch, durable ownership, retry scheduling, and
  aggregate result construction. Await worker mechanics remain in the run-server
  coordinator and are requested through an `:await` directive.
  """

  alias Favn.Contracts.RunnerResult
  alias Favn.Contracts.RunnerWork
  alias FavnOrchestrator.AssetStepIdentity
  alias FavnOrchestrator.RunExecutionOwnership
  alias FavnOrchestrator.RunnerClientValidator
  alias FavnOrchestrator.RunServer.Execution.ResultBuilder
  alias FavnOrchestrator.RunServer.Execution.ResultSanitizer
  alias FavnOrchestrator.RunServer.Execution.RunExecutionState
  alias FavnOrchestrator.RunServer.Execution.RunWorkSet
  alias FavnOrchestrator.RunServer.Execution.StepAttemptLifecycle
  alias FavnOrchestrator.RunServer.Persistence
  alias FavnOrchestrator.RunServer.PersistenceRetry
  alias FavnOrchestrator.RunServer.Snapshots
  alias FavnOrchestrator.RunState

  @type directive ::
          {:await, RunExecutionState.t(), map()}
          | {:cont, RunExecutionState.t()}
          | {:terminal, RunState.t()}
          | {:persist_retry, RunExecutionState.t(), PersistenceRetry.t(), term()}

  @doc "Continues a sequential run from its current index."
  @spec continue(RunExecutionState.t()) :: directive()
  def continue(%RunExecutionState{} = state) do
    if state.sequential_index >= length(state.sequential_refs) do
      {:terminal,
       Snapshots.snapshot_update(state.run,
         status: :ok,
         error: nil,
         runner_execution_id: nil,
         result: %{
           status: :ok,
           asset_results: ResultBuilder.sort_asset_results(state.run, state.accumulated_results),
           metadata: state.run.metadata
         }
       )}
    else
      {asset_ref, node_key, stage} = Enum.at(state.sequential_refs, state.sequential_index)
      submit_attempt(state, asset_ref, node_key, stage, 1)
    end
  end

  @doc "Settles one sequential runner await result."
  @spec handle_result(RunExecutionState.t(), map(), term()) :: directive()
  def handle_result(%RunExecutionState{} = state, entry, {:ok, %RunnerResult{} = result}) do
    result = ResultSanitizer.sanitize(result)
    asset_results = ResultSanitizer.sanitize_asset_results(result.asset_results)
    step_status = StepAttemptLifecycle.map_runner_status(result.status)
    {event_type, retryable?} = StepAttemptLifecycle.step_outcome(step_status)
    retryable? = retryable? and StepAttemptLifecycle.runner_result_retryable?(result)
    metadata = ResultSanitizer.merge_metadata(state.run.metadata, result.metadata)

    step_finished =
      RunState.transition(state.run,
        status: step_status,
        runner_execution_id: nil,
        error: result.error,
        metadata: metadata
      )

    data = %{
      asset_ref: entry.asset_ref,
      result_status: result.status,
      error: result.error,
      node_key: entry.node_key,
      asset_step_id: entry.asset_step_id,
      stage: entry.stage,
      attempt: entry.attempt,
      max_attempts: state.run.max_attempts,
      asset_results: asset_results
    }

    resume = %{
      kind: :step_result,
      run: step_finished,
      entry: entry,
      status: step_status,
      retryable?: retryable?,
      asset_results: asset_results
    }

    persist_or_retry(state, step_finished, event_type, data, resume)
  end

  def handle_result(%RunExecutionState{} = state, entry, {:error, :timeout}) do
    state =
      cancel_work(state, [entry.execution_id], %{
        kind: :await_timeout,
        asset_ref: entry.asset_ref,
        stage: entry.stage,
        attempt: entry.attempt
      })

    timeout_state =
      RunState.transition(state.run,
        status: :timed_out,
        runner_execution_id: nil,
        error: :timeout
      )

    data = %{
      asset_ref: entry.asset_ref,
      error: :timeout,
      node_key: entry.node_key,
      asset_step_id: entry.asset_step_id,
      stage: entry.stage,
      attempt: entry.attempt,
      max_attempts: state.run.max_attempts,
      asset_results: []
    }

    resume = %{
      kind: :step_result,
      run: timeout_state,
      entry: entry,
      status: :timed_out,
      retryable?: true,
      asset_results: []
    }

    persist_or_retry(state, timeout_state, :step_timed_out, data, resume)
  end

  def handle_result(%RunExecutionState{} = state, entry, {:error, reason}) do
    state =
      cancel_work(state, [entry.execution_id], %{
        kind: :await_error,
        asset_ref: entry.asset_ref,
        stage: entry.stage,
        attempt: entry.attempt,
        error: reason
      })

    failed =
      RunState.transition(state.run,
        status: :error,
        runner_execution_id: nil,
        error: reason
      )

    data = %{
      asset_ref: entry.asset_ref,
      error: reason,
      node_key: entry.node_key,
      asset_step_id: entry.asset_step_id,
      stage: entry.stage,
      attempt: entry.attempt,
      max_attempts: state.run.max_attempts,
      asset_results: []
    }

    resume = %{
      kind: :step_result,
      run: failed,
      entry: entry,
      status: :error,
      retryable?: true,
      asset_results: []
    }

    persist_or_retry(state, failed, :step_failed, data, resume)
  end

  @doc false
  @spec resume_persisted(RunExecutionState.t(), map()) :: directive()
  def resume_persisted(%RunExecutionState{} = state, %{kind: :step_result} = resume) do
    _ = RunExecutionOwnership.complete_execution(state.run.id, resume.entry.execution_id)
    state = %{state | run: resume.run}

    cond do
      resume.status == :ok ->
        continue(%{
          state
          | sequential_index: state.sequential_index + 1,
            accumulated_results: Enum.reverse(resume.asset_results, state.accumulated_results)
        })

      resume.retryable? ->
        maybe_schedule_retry(
          state,
          resume.entry.asset_ref,
          resume.entry.stage,
          resume.entry.attempt,
          resume.asset_results
        )

      true ->
        terminalize_error(state, resume.asset_results)
    end
  end

  def resume_persisted(%RunExecutionState{} = state, %{kind: :schedule_retry} = resume) do
    schedule_retry_timer(%{state | run: resume.run}, resume.retry)
  end

  @doc "Resubmits a sequential attempt after its retry timer fires."
  @spec resume_retry(RunExecutionState.t(), map()) :: directive()
  def resume_retry(%RunExecutionState{} = state, retry) do
    submit_attempt(state, retry.asset_ref, retry.node_key, retry.stage, retry.next_attempt)
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

  defp submit_attempt(%RunExecutionState{} = state, asset_ref, node_key, stage, attempt) do
    with :ok <- RunnerClientValidator.validate(state.runner_client),
         :ok <- state.runner_client.register_manifest(state.version, state.runner_opts),
         {:ok, %{work: work} = lifecycle} <-
           state.run
           |> StepAttemptLifecycle.new(state.version, node_key, stage, attempt)
           |> StepAttemptLifecycle.build_work(),
         ownership <-
           RunExecutionOwnership.new(state.run,
             asset_step_id: work.asset_step_id,
             node_key: node_key,
             asset_ref: asset_ref,
             stage: stage,
             attempt: attempt,
             execution_pool: RunnerWork.execution_pool(work),
             deadline_at: run_deadline_at(state.run)
           ),
         work <- attach_ownership_metadata(work, ownership),
         :ok <- RunExecutionOwnership.persist(ownership) do
      dispatch_attempt(state, lifecycle, work, ownership)
    else
      {:error, reason} ->
        persist_pre_submit_failure(state, asset_ref, node_key, stage, attempt, reason)
    end
  end

  defp dispatch_attempt(state, lifecycle, work, ownership) do
    case state.runner_client.submit_work(work, state.runner_opts) do
      {:ok, execution_id} ->
        submitted_ownership = RunExecutionOwnership.submitted(ownership, execution_id)

        case persist_submitted_ownership_snapshot(submitted_ownership) do
          :ok ->
            start_submitted_attempt(state, lifecycle, work, submitted_ownership, execution_id)

          {:error, :external_cancel} ->
            fail_submitted_attempt(state, submitted_ownership, execution_id, :external_cancel)

          {:error, reason} ->
            fail_submitted_attempt(state, submitted_ownership, execution_id, reason)
        end

      {:error, reason} ->
        _ =
          RunExecutionOwnership.mark_dispatch_failed(state.run.id, ownership.ownership_id, reason)

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

  defp fail_submitted_attempt(state, ownership, execution_id, reason) do
    state =
      cancel_work(state, [execution_id], %{
        kind:
          if(reason == :external_cancel,
            do: :external_cancel,
            else: :step_submitted_persist_failed
          ),
        error: reason
      })

    persist_submit_persist_failure_outcome(ownership, state.run, execution_id, reason)

    if reason == :external_cancel do
      {:terminal, Snapshots.cancelled_snapshot(state.run)}
    else
      {:terminal,
       RunState.transition(state.run, status: :error, runner_execution_id: nil, error: reason)}
    end
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
    failed =
      RunState.transition(state.run, status: :error, runner_execution_id: nil, error: reason)

    case Persistence.persist_run_step(failed, :step_failed, %{
           asset_ref: asset_ref,
           error: reason,
           node_key: node_key,
           asset_step_id:
             asset_step_id || AssetStepIdentity.asset_step_id(state.run.id, node_key, asset_ref),
           stage: stage,
           attempt: attempt,
           max_attempts: state.run.max_attempts
         }) do
      :ok -> maybe_schedule_retry(%{state | run: failed}, asset_ref, stage, attempt, [])
      {:error, :external_cancel} -> {:terminal, Snapshots.cancelled_snapshot(failed)}
      {:error, _persist_reason} -> {:terminal, failed}
    end
  end

  defp start_submitted_attempt(state, lifecycle, work, ownership, execution_id) do
    entry = sequential_entry(state, lifecycle, work, execution_id)
    started_ownership = RunExecutionOwnership.started(ownership)

    case RunExecutionOwnership.persist(started_ownership) do
      :ok ->
        running =
          RunState.transition(state.run,
            runner_execution_id: execution_id,
            metadata: Map.merge(state.run.metadata, RunnerWork.lifecycle_metadata(work))
          )

        state = %{state | run: running} |> RunExecutionState.add_work(entry)

        case Persistence.persist_run_step(state.run, :step_started, %{
               asset_ref: lifecycle.asset_ref,
               runner_execution_id: execution_id,
               node_key: lifecycle.node_key,
               asset_step_id: work.asset_step_id,
               stage: lifecycle.stage,
               attempt: lifecycle.attempt,
               max_attempts: state.run.max_attempts
             }) do
          :ok ->
            {:await, state, entry}

          {:error, :external_cancel} ->
            state =
              cancel_work(state, [execution_id], %{
                kind: :external_cancel,
                asset_ref: lifecycle.asset_ref,
                stage: lifecycle.stage,
                attempt: lifecycle.attempt
              })

            {:terminal, Snapshots.cancelled_snapshot(state.run)}

          {:error, reason} ->
            state =
              cancel_work(state, [execution_id], %{
                kind: :step_started_persist_failed,
                error: reason
              })

            {:terminal,
             RunState.transition(state.run,
               status: :error,
               runner_execution_id: nil,
               error: reason
             )}
        end

      {:error, reason} ->
        state =
          cancel_work(state, [execution_id], %{
            kind: :step_started_ownership_persist_failed,
            error: reason
          })

        {:terminal,
         RunState.transition(state.run, status: :error, runner_execution_id: nil, error: reason)}
    end
  end

  defp sequential_entry(state, lifecycle, work, execution_id) do
    %{
      run_id: state.run.id,
      asset_step_id: work.asset_step_id,
      asset_ref: lifecycle.asset_ref,
      node_key: lifecycle.node_key,
      execution_id: execution_id,
      runner_execution_id: execution_id,
      stage: lifecycle.stage,
      attempt: lifecycle.attempt,
      execution_pool: RunnerWork.execution_pool(work)
    }
  end

  defp maybe_schedule_retry(state, asset_ref, stage, attempt, step_results) do
    if attempt < state.run.max_attempts do
      node_key = Map.get(state.run.metadata, :node_key) || {asset_ref, nil}
      lifecycle = StepAttemptLifecycle.new(state.run, state.version, node_key, stage, attempt)

      case StepAttemptLifecycle.schedule_retry(lifecycle) do
        {:ok, retry} -> schedule_retry(state, retry)
        :terminal -> terminalize_error(state, step_results)
      end
    else
      terminalize_error(state, step_results)
    end
  end

  defp schedule_retry(state, retry) do
    retrying =
      RunState.transition(state.run,
        status: :running,
        error: nil,
        runner_execution_id: nil,
        metadata:
          Map.merge(state.run.metadata, %{retrying: true, next_attempt: retry.next_attempt})
      )

    persist_or_retry(
      state,
      retrying,
      :step_retry_scheduled,
      StepAttemptLifecycle.retry_event_payload(retry),
      %{kind: :schedule_retry, run: retrying, retry: retry}
    )
  end

  defp schedule_retry_timer(state, retry) do
    timer_token = make_ref()
    timer_ref = Process.send_after(self(), {:retry_attempt, timer_token}, retry.retry_after_ms)

    {:cont,
     RunExecutionState.put_retry_timer(
       state,
       timer_token,
       timer_ref,
       retry
     )}
  end

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
        ResultSanitizer.sanitize_asset_results(step_results) ++ state.accumulated_results
      )

    {:terminal,
     Snapshots.snapshot_update(state.run,
       runner_execution_id: nil,
       result: %{
         status: state.run.status,
         asset_results: all_results,
         metadata: state.run.metadata
       }
     )}
  end

  defp cancel_work(%RunExecutionState{} = state, execution_ids, reason) do
    work_set =
      Enum.reduce(execution_ids, state.work_set, fn execution_id, acc ->
        case Map.get(acc.entries, execution_id) do
          nil ->
            RunWorkSet.add_entry(acc, %{
              execution_id: execution_id,
              runner_execution_id: execution_id
            })

          _entry ->
            acc
        end
      end)

    {run, work_set} =
      RunWorkSet.cancel_all(state.run, work_set, reason, state.runner_client, state.runner_opts)

    %{state | run: run, work_set: work_set}
  end

  defp persist_submitted_ownership_snapshot(%RunExecutionOwnership{} = ownership) do
    if Persistence.externally_cancelled?(ownership.run_id) do
      {:error, :external_cancel}
    else
      RunExecutionOwnership.persist(ownership)
    end
  end

  defp attach_ownership_metadata(%RunnerWork{} = work, %RunExecutionOwnership{} = ownership) do
    metadata =
      work.metadata
      |> Map.put(:ownership_id, ownership.ownership_id)
      |> Map.put(:dispatch_id, ownership.dispatch_id)
      |> Map.put(:deadline_at, ownership.deadline_at)

    %{work | metadata: metadata}
  end

  defp persist_submit_persist_failure_outcome(ownership, run_state, execution_id, reason) do
    result = cancel_outcome_for(run_state, execution_id)
    _ = RunExecutionOwnership.mark_submit_persist_failed(ownership, result, reason)
    :ok
  end

  defp cancel_outcome_for(%RunState{metadata: metadata}, execution_id) when is_map(metadata) do
    metadata
    |> Map.get(:cancel_outcomes, Map.get(metadata, "cancel_outcomes", []))
    |> Enum.find(&(Map.get(&1, :execution_id, Map.get(&1, "execution_id")) == execution_id))
  end

  defp cancel_outcome_for(%RunState{}, _execution_id), do: nil

  defp run_deadline_at(%RunState{timeout_ms: timeout_ms})
       when is_integer(timeout_ms) and timeout_ms > 0,
       do: DateTime.add(DateTime.utc_now(), timeout_ms, :millisecond)

  defp run_deadline_at(%RunState{}), do: nil

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
