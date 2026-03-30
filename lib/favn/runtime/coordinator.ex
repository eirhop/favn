defmodule Favn.Runtime.Coordinator do
  @moduledoc """
  Run-scoped coordinator process.

  Owns lifecycle mutation, step readiness, dispatch, transition application,
  persistence, and event emission.
  """

  use GenServer

  alias Favn.Run.Context
  alias Favn.Runtime.Executor.Local

  require Logger
  alias Favn.Runtime.Projector
  alias Favn.Runtime.State
  alias Favn.Runtime.Transitions.Run, as: RunTransitions
  alias Favn.Runtime.Transitions.Step, as: StepTransitions

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) when is_list(opts) do
    GenServer.start_link(__MODULE__, opts)
  end

  @impl true
  def init(opts) do
    {:ok, %{state: Keyword.fetch!(opts, :state)}}
  end

  @impl true
  def handle_cast(:start_run, %{state: state} = data) do
    with {:ok, state} <- apply_run_transition(state, :start),
         {:ok, state} <- maybe_schedule_timeout(state),
         {:ok, state} <- emit_step_ready_for_sources(state),
         {:ok, state} <- dispatch_ready_work(state),
         {:ok, state} <- maybe_finalize_terminal(state) do
      {:noreply, %{data | state: state}}
    else
      {:error, reason} ->
        {:stop, reason, data}
    end
  end

  @impl true
  def handle_cast({:cancel_run, reason}, %{state: state} = data) when is_map(reason) do
    with {:ok, state} <- request_cancellation(state, reason),
         {:ok, state} <- maybe_finalize_terminal(state) do
      {:noreply, %{data | state: state}}
    else
      {:error, reason} -> {:stop, reason, data}
    end
  end

  @impl true
  def handle_info({:executor_step_result, exec_ref, ref, result}, %{state: state} = data) do
    with {:ok, state} <- handle_step_result(state, exec_ref, ref, result),
         {:ok, state} <- dispatch_ready_work(state),
         {:ok, state} <- maybe_finalize_terminal(state) do
      {:noreply, %{data | state: state}}
    else
      {:error, reason} ->
        {:stop, reason, data}
    end
  end

  @impl true
  def handle_info({:retry_due, ref, attempt}, %{state: state} = data) do
    with {:ok, state} <- clear_retry_timer(state, ref),
         {:ok, state} <- maybe_make_retry_ready(state, ref, attempt),
         {:ok, state} <- dispatch_ready_work(state),
         {:ok, state} <- maybe_finalize_terminal(state) do
      {:noreply, %{data | state: state}}
    else
      {:error, reason} -> {:stop, reason, data}
    end
  end

  @impl true
  def handle_info(:run_deadline_reached, %{state: state} = data) do
    with {:ok, state} <- request_timeout(state),
         {:ok, state} <- maybe_finalize_terminal(state) do
      {:noreply, %{data | state: state}}
    else
      {:error, reason} -> {:stop, reason, data}
    end
  end

  @impl true
  def handle_info({:DOWN, monitor_ref, :process, _pid, reason}, %{state: state} = data) do
    case Map.fetch(state.exec_refs_by_monitor, monitor_ref) do
      {:ok, exec_ref} ->
        if MapSet.member?(state.completed_exec_refs, exec_ref) do
          {:noreply, %{data | state: clear_monitor(state, exec_ref, monitor_ref)}}
        else
          if abnormal_executor_exit_reason?(reason) do
            with {:ok, state} <-
                   handle_step_result(
                     state,
                     exec_ref,
                     nil,
                     {:error, %{kind: :exit, reason: reason, stacktrace: []}}
                   ),
                 {:ok, state} <- dispatch_ready_work(state),
                 {:ok, state} <- maybe_finalize_terminal(state) do
              {:noreply, %{data | state: state}}
            else
              {:error, reason} -> {:stop, reason, data}
            end
          else
            {:noreply, %{data | state: clear_monitor(state, exec_ref, monitor_ref)}}
          end
        end

      :error ->
        {:noreply, data}
    end
  end

  @impl true
  def handle_info(_msg, data), do: {:noreply, data}

  defp abnormal_executor_exit_reason?(reason) do
    reason not in [:normal, :shutdown, {:shutdown, :normal}]
  end

  defp dispatch_ready_work(%State{} = state) do
    if dispatch_allowed?(state) and capacity(state) > 0 do
      do_dispatch(state)
    else
      {:ok, state}
    end
  end

  defp do_dispatch(%State{} = state) do
    cond do
      not dispatch_allowed?(state) ->
        {:ok, state}

      capacity(state) <= 0 ->
        {:ok, state}

      true ->
        case pop_next_ready(state) do
          {:ok, ref, next_state} ->
            with {:ok, next_state} <- start_step_execution(next_state, ref) do
              do_dispatch(next_state)
            end

          :none ->
            {:ok, state}
        end
    end
  end

  defp start_step_execution(%State{} = state, ref) do
    with {:ok, state} <- apply_step_transition(state, &StepTransitions.start_step(&1, ref)),
         {:ok, asset} <- Favn.Registry.get_asset(ref),
         {:ok, deps} <- dependency_outputs(state, ref),
         {:ok, handle} <-
           executor_module().start_step(asset, build_context(state, ref), deps, self(), ref) do
      {:ok, put_execution_handle(state, ref, handle)}
    else
      {:error, reason} ->
        normalized = %{kind: :error, reason: reason, stacktrace: [], class: :executor_error}
        handle_step_error(state, ref, normalized)
    end
  end

  defp handle_step_result(
         %State{} = state,
         exec_ref,
         maybe_ref,
         {:ok, %{output: output, meta: meta}}
       ) do
    case take_execution(state, exec_ref, maybe_ref) do
      {:ok, ref, state} ->
        apply_step_transition(state, &StepTransitions.complete_success(&1, ref, output, meta))

      {:ignore, state} ->
        {:ok, state}
    end
  end

  defp handle_step_result(%State{} = state, exec_ref, maybe_ref, {:error, error})
       when is_map(error) do
    case take_execution(state, exec_ref, maybe_ref) do
      {:ok, ref, state} ->
        handle_step_error(state, ref, error)

      {:ignore, state} ->
        {:ok, state}
    end
  end

  defp handle_step_error(%State{run_status: :cancelling} = state, ref, _error) do
    apply_step_transition(
      state,
      &StepTransitions.complete_cancelled(&1, ref, %{kind: :run_cancelled})
    )
  end

  defp handle_step_error(%State{run_status: :timing_out} = state, ref, _error) do
    apply_step_transition(
      state,
      &StepTransitions.complete_timed_out(&1, ref, %{kind: :run_timed_out})
    )
  end

  defp handle_step_error(%State{} = state, ref, error) do
    step = Map.fetch!(state.steps, ref)
    classification = classify_error(error)
    retryable? = retryable?(state.retry_policy, classification)
    exhausted? = step.attempt >= step.max_attempts
    payload = enrich_error(error, classification)

    if retryable? and not exhausted? do
      with {:ok, state} <- emit_retryable_step_failed(state, ref, step, payload),
           {:ok, state} <-
             apply_step_transition(
               state,
               &StepTransitions.schedule_retry(&1, ref, payload, state.retry_policy.delay_ms)
             ),
           {:ok, state} <- schedule_retry_timer(state, ref) do
        {:ok, state}
      end
    else
      with {:ok, state} <-
             apply_step_transition(
               state,
               &StepTransitions.complete_failure(&1, ref, payload,
                 retryable?: retryable?,
                 exhausted?: true
               )
             ),
           {:ok, state} <- maybe_emit_retry_exhausted(state, ref, step, retryable?),
           {:ok, state} <- close_admission_with_failure(state, ref, payload[:reason]) do
        {:ok, state}
      end
    end
  end

  defp maybe_finalize_terminal(%State{run_status: :running} = state) do
    cond do
      map_size(state.inflight_execs) > 0 ->
        {:ok, state}

      retries_pending?(state) ->
        {:ok, state}

      all_targets_success?(state) ->
        apply_run_transition(state, :mark_success)

      true ->
        reason = state.run_error || %{reason: :run_did_not_reach_targets}

        with {:ok, state} <- close_admission(state),
             {:ok, state} <- apply_run_transition(state, {:mark_failed, reason}),
             {:ok, state} <- maybe_finalize_unresolved(state) do
          {:ok, state}
        end
    end
  end

  defp maybe_finalize_terminal(%State{run_status: :failed} = state) do
    if map_size(state.inflight_execs) == 0 do
      maybe_finalize_unresolved(state)
    else
      {:ok, state}
    end
  end

  defp maybe_finalize_terminal(%State{run_status: :cancelling} = state) do
    if map_size(state.inflight_execs) == 0 do
      with {:ok, state} <- maybe_finalize_unresolved(state, :cancelled),
           {:ok, state} <- apply_run_transition(state, :mark_cancelled) do
        {:ok, state}
      end
    else
      {:ok, state}
    end
  end

  defp maybe_finalize_terminal(%State{run_status: :timing_out} = state) do
    if map_size(state.inflight_execs) == 0 do
      with {:ok, state} <- maybe_finalize_unresolved(state, :timed_out),
           {:ok, state} <- apply_run_transition(state, :mark_timed_out) do
        {:ok, state}
      end
    else
      {:ok, state}
    end
  end

  defp maybe_finalize_terminal(%State{} = state), do: {:ok, state}

  defp maybe_finalize_unresolved(%State{} = state, replacement_status \\ :skipped) do
    if unresolved_steps?(state) do
      apply_finalize_unresolved(state, replacement_status)
    else
      {:ok, state}
    end
  end

  defp close_admission_with_failure(%State{} = state, ref, reason) do
    with {:ok, state} <- close_admission(state),
         {:ok, state} <-
           maybe_mark_run_failed(state, %{ref: ref, stage: stage_for(state, ref), reason: reason}) do
      {:ok, state}
    end
  end

  defp maybe_mark_run_failed(%State{run_status: :running} = state, reason),
    do: apply_run_transition(state, {:mark_failed, reason})

  defp maybe_mark_run_failed(%State{} = state, _reason), do: {:ok, state}

  defp close_admission(%State{} = state), do: {:ok, %{state | admission_open?: false}}

  defp request_cancellation(%State{run_status: :pending} = state, reason) do
    with {:ok, state} <- apply_run_transition(state, :start),
         {:ok, state} <- request_cancellation(state, reason) do
      {:ok, state}
    end
  end

  defp request_cancellation(%State{run_status: :running} = state, reason) do
    with {:ok, state} <- apply_run_transition(state, :request_cancel),
         {:ok, state} <- close_admission(state),
         {:ok, state} <- interrupt_inflight(state, Map.put(reason, :kind, :run_cancelled)) do
      {:ok, state}
    end
  end

  defp request_cancellation(%State{run_status: :cancelling} = state, _reason), do: {:ok, state}
  defp request_cancellation(%State{} = state, _reason), do: {:ok, state}

  defp request_timeout(%State{run_status: :running} = state) do
    with {:ok, state} <- apply_run_transition(state, :request_timeout),
         {:ok, state} <- close_admission(state),
         {:ok, state} <- interrupt_inflight(state, %{kind: :run_timed_out}) do
      {:ok, state}
    end
  end

  defp request_timeout(%State{} = state), do: {:ok, state}

  defp interrupt_inflight(%State{} = state, reason) do
    result =
      Enum.reduce_while(state.inflight_execs, :ok, fn {exec_ref, exec_info}, :ok ->
        handle = %{exec_ref: exec_ref, monitor_ref: exec_info.monitor_ref, pid: exec_info.pid}

        case executor_module().cancel_step(handle, reason) do
          :ok -> {:cont, :ok}
          {:error, err} -> {:halt, {:error, err}}
        end
      end)

    case result do
      :ok -> {:ok, state}
      {:error, reason} -> {:error, {:executor_cancel_failed, reason}}
    end
  end

  defp take_execution(%State{} = state, exec_ref, maybe_ref) do
    case Map.pop(state.inflight_execs, exec_ref) do
      {nil, _} ->
        Logger.debug("Ignoring stale executor result for unknown exec_ref=#{inspect(exec_ref)}")
        {:ignore, state}

      {%{ref: tracked_ref, monitor_ref: monitor_ref}, inflight} ->
        if maybe_ref != nil and maybe_ref != tracked_ref do
          Logger.warning(
            "Executor returned mismatched step ref; using tracked ref. exec_ref=#{inspect(exec_ref)} tracked_ref=#{inspect(tracked_ref)} received_ref=#{inspect(maybe_ref)}"
          )
        end

        next_state =
          state
          |> Map.put(:inflight_execs, inflight)
          |> Map.put(:exec_refs_by_monitor, Map.delete(state.exec_refs_by_monitor, monitor_ref))
          |> Map.put(:completed_exec_refs, MapSet.put(state.completed_exec_refs, exec_ref))

        {:ok, tracked_ref, next_state}
    end
  end

  defp clear_retry_timer(%State{} = state, ref) do
    {:ok, %{state | retry_timers: Map.delete(state.retry_timers, ref)}}
  end

  defp maybe_make_retry_ready(%State{} = state, ref, attempt) do
    step = Map.fetch!(state.steps, ref)

    cond do
      state.run_status != :running ->
        {:ok, state}

      step.status != :retrying ->
        {:ok, state}

      step.attempt != attempt ->
        {:ok, state}

      true ->
        apply_step_transition(state, &StepTransitions.requeue_retry(&1, ref))
    end
  end

  defp schedule_retry_timer(%State{} = state, ref) do
    step = Map.fetch!(state.steps, ref)
    delay_ms = state.retry_policy.delay_ms

    if delay_ms == 0 do
      apply_step_transition(state, &StepTransitions.requeue_retry(&1, ref))
    else
      timer_ref = Process.send_after(self(), {:retry_due, ref, step.attempt}, delay_ms)
      next_state = %{state | retry_timers: Map.put(state.retry_timers, ref, timer_ref)}
      persist_snapshot(next_state)
    end
  end

  defp emit_retryable_step_failed(%State{} = state, ref, step, error) do
    payload = %{
      attempt: step.attempt,
      max_attempts: step.max_attempts,
      remaining_attempts: max(step.max_attempts - step.attempt, 0),
      retryable: true,
      exhausted: false,
      final: false,
      class: error[:class],
      reason: error[:reason]
    }

    emit_events(state, [{:step_failed, ref, payload}])
  end

  defp emit_retry_exhausted(%State{} = state, ref, step) do
    payload = %{
      attempt: step.attempt,
      max_attempts: step.max_attempts,
      exhausted: true
    }

    emit_events(state, [{:step_retry_exhausted, ref, payload}])
  end

  defp maybe_emit_retry_exhausted(%State{} = state, _ref, _step, false), do: {:ok, state}

  defp maybe_emit_retry_exhausted(%State{} = state, ref, step, true),
    do: emit_retry_exhausted(state, ref, step)

  defp retries_pending?(%State{} = state) do
    Enum.any?(state.steps, fn {_ref, step} -> step.status == :retrying end)
  end

  defp clear_monitor(%State{} = state, exec_ref, monitor_ref) do
    %{
      state
      | exec_refs_by_monitor: Map.delete(state.exec_refs_by_monitor, monitor_ref),
        completed_exec_refs: MapSet.put(state.completed_exec_refs, exec_ref)
    }
  end

  defp put_execution_handle(%State{} = state, ref, %{
         exec_ref: exec_ref,
         monitor_ref: monitor_ref,
         pid: pid
       }) do
    info = %{ref: ref, monitor_ref: monitor_ref, pid: pid}

    %{
      state
      | inflight_execs: Map.put(state.inflight_execs, exec_ref, info),
        exec_refs_by_monitor: Map.put(state.exec_refs_by_monitor, monitor_ref, exec_ref)
    }
  end

  defp apply_run_transition(%State{} = state, command) do
    with {:ok, state, events} <- RunTransitions.apply(state, command),
         {:ok, state} <- emit_events(state, events),
         {:ok, state} <- persist_snapshot(state) do
      {:ok, state}
    end
  end

  defp apply_step_transition(%State{} = state, transition_fun) do
    with {:ok, state, events} <- transition_fun.(state),
         {:ok, state} <- emit_events(state, events),
         {:ok, state} <- persist_snapshot(state) do
      {:ok, state}
    end
  end

  defp apply_finalize_unresolved(%State{} = state, replacement_status) do
    {state, events} = StepTransitions.finalize_unresolved(state, replacement_status)

    with {:ok, state} <- emit_events(state, events),
         {:ok, state} <- persist_snapshot(state) do
      {:ok, state}
    end
  end

  defp emit_step_ready_for_sources(%State{} = state) do
    source_refs =
      state.steps
      |> Enum.filter(fn {_ref, step} -> step.status == :ready end)
      |> Enum.map(&elem(&1, 0))
      |> Enum.sort()

    events = Enum.map(source_refs, &{:step_ready, &1})
    emit_events(%{state | ready_queue: source_refs}, events)
  end

  defp emit_events(%State{} = state, events) when is_list(events) do
    state =
      Enum.reduce(events, state, fn event, acc ->
        {event_name, attrs} = event_attrs(acc, event)
        seq = acc.event_seq + 1

        _ =
          Favn.Runtime.Events.publish_run_event(
            acc.run_id,
            event_name,
            Map.merge(attrs, %{seq: seq})
          )

        %{acc | event_seq: seq}
      end)

    {:ok, state}
  end

  defp maybe_schedule_timeout(%State{timeout_ms: nil} = state), do: {:ok, state}

  defp maybe_schedule_timeout(%State{timeout_ms: timeout_ms} = state)
       when is_integer(timeout_ms) and timeout_ms > 0 do
    timer_ref = Process.send_after(self(), :run_deadline_reached, timeout_ms)
    deadline_at = DateTime.add(state.started_at || DateTime.utc_now(), timeout_ms, :millisecond)
    {:ok, %{state | timeout_timer_ref: timer_ref, deadline_at: deadline_at}}
  end

  defp event_attrs(%State{} = state, {event_name, ref}) do
    stage = stage_for(state, ref)
    {event_name, %{ref: ref, stage: stage}}
  end

  defp event_attrs(%State{} = state, {event_name, ref, payload}) when is_map(payload) do
    stage = stage_for(state, ref)
    {event_name, %{ref: ref, stage: stage, payload: payload}}
  end

  defp event_attrs(%State{} = state, event_name) when is_atom(event_name) do
    payload =
      case event_name do
        :run_failed -> %{error: state.run_error, terminal_reason: state.run_terminal_reason}
        :run_cancel_requested -> %{terminal_reason: state.run_terminal_reason}
        :run_cancelled -> %{terminal_reason: state.run_terminal_reason}
        :run_timeout_triggered -> %{terminal_reason: state.run_terminal_reason}
        :run_timed_out -> %{terminal_reason: state.run_terminal_reason}
        _ -> %{}
      end

    {event_name, %{payload: payload}}
  end

  defp persist_snapshot(%State{} = state) do
    case state |> Projector.to_public_run() |> Favn.Storage.put_run() do
      :ok -> {:ok, state}
      {:error, reason} -> {:error, {:storage_persist_failed, reason}}
    end
  end

  defp dependency_outputs(%State{} = state, ref) do
    upstream = state.steps |> Map.fetch!(ref) |> Map.get(:upstream)

    Enum.reduce_while(upstream, {:ok, %{}}, fn dep_ref, {:ok, acc} ->
      case Map.fetch(state.outputs, dep_ref) do
        {:ok, value} -> {:cont, {:ok, Map.put(acc, dep_ref, value)}}
        :error -> {:halt, {:error, {:missing_dependency_output, dep_ref}}}
      end
    end)
  end

  defp build_context(%State{} = state, ref) do
    step = Map.fetch!(state.steps, ref)

    %Context{
      run_id: state.run_id,
      target_refs: state.target_refs,
      current_ref: ref,
      params: state.params,
      run_started_at: state.started_at,
      stage: step.stage,
      attempt: step.attempt,
      max_attempts: step.max_attempts
    }
  end

  defp stage_for(%State{} = state, ref), do: state.steps |> Map.fetch!(ref) |> Map.get(:stage)

  defp pop_next_ready(%State{ready_queue: []}), do: :none

  defp pop_next_ready(%State{ready_queue: [ref | rest]} = state),
    do: {:ok, ref, %{state | ready_queue: rest}}

  defp all_targets_success?(%State{} = state) do
    Enum.all?(state.target_refs, fn ref ->
      state.steps |> Map.fetch!(ref) |> Map.get(:status) == :success
    end)
  end

  defp unresolved_steps?(%State{} = state) do
    Enum.any?(state.steps, fn {_ref, step} -> step.status in [:pending, :ready, :retrying] end)
  end

  defp dispatch_allowed?(%State{} = state),
    do: state.run_status == :running and state.admission_open?

  defp capacity(%State{} = state),
    do: max(state.max_concurrency - map_size(state.inflight_execs), 0)

  defp executor_module do
    Application.get_env(:favn, :runtime_executor, Local)
  end

  defp classify_error(%{class: class}) when is_atom(class), do: class
  defp classify_error(%{kind: :throw}), do: :throw
  defp classify_error(%{kind: :exit}), do: :exit
  defp classify_error(%{kind: :error, reason: reason}) when is_struct(reason), do: :exception
  defp classify_error(%{kind: :error}), do: :error_return
  defp classify_error(_), do: :error_return

  defp retryable?(policy, class), do: class in policy.retry_on

  defp enrich_error(error, class), do: Map.put(error, :class, class)
end
