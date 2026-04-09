defmodule Favn.Runtime.Coordinator do
  @moduledoc """
  Run-scoped coordinator process.

  Owns lifecycle mutation, step readiness, dispatch, transition application,
  persistence, and event emission.
  """

  use GenServer

  alias Favn.Run.Context
  alias Favn.Runtime.Events
  alias Favn.Runtime.Executor.Local

  require Logger
  alias Favn.Runtime.Projector
  alias Favn.Runtime.State
  alias Favn.Runtime.Telemetry
  alias Favn.Runtime.Transitions.Run, as: RunTransitions
  alias Favn.Runtime.Transitions.Step, as: StepTransitions

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) when is_list(opts) do
    GenServer.start_link(__MODULE__, opts)
  end

  @impl true
  def init(opts) do
    state = Keyword.fetch!(opts, :state)
    Logger.metadata(run_id: state.run_id)
    {:ok, %{state: state}}
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
      {:error, {:invalid_step_transition, status, event}} ->
        Logger.warning(
          "Ignoring executor result that produced invalid transition status=#{inspect(status)} event=#{inspect(event)} exec_ref=#{inspect(exec_ref)} node_key=#{inspect(ref)}"
        )

        {:noreply, data}

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
        if Map.has_key?(state.completed_exec_refs, exec_ref) do
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
    started_ms = System.monotonic_time(:millisecond)
    ready_before = length(state.ready_queue)
    inflight_before = map_size(state.inflight_execs)

    result =
      if dispatch_allowed?(state) and capacity(state) > 0 do
        do_dispatch(state)
      else
        {:ok, state}
      end

    case result do
      {:ok, next_state} ->
        dispatched = max(map_size(next_state.inflight_execs) - inflight_before, 0)
        duration_ms = System.monotonic_time(:millisecond) - started_ms

        _ =
          Telemetry.emit_operation(:coordinator, :dispatch, duration_ms, %{
            run_id: state.run_id,
            result: :ok,
            ready_before: ready_before,
            inflight_before: inflight_before,
            dispatched_count: dispatched
          })

        {:ok, next_state}

      {:error, _reason} = error ->
        duration_ms = System.monotonic_time(:millisecond) - started_ms

        _ =
          Telemetry.emit_operation(:coordinator, :dispatch, duration_ms, %{
            run_id: state.run_id,
            result: :error
          })

        error
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
          {:ok, node_key, next_state} ->
            with {:ok, next_state} <- start_step_execution(next_state, node_key) do
              do_dispatch(next_state)
            end

          :none ->
            {:ok, state}
        end
    end
  end

  defp start_step_execution(%State{} = state, node_key) do
    with {:ok, state} <- apply_step_transition(state, &StepTransitions.start_step(&1, node_key)),
         step <- Map.fetch!(state.steps, node_key),
         {:ok, asset} <- Favn.Assets.Registry.get_asset(step.ref),
         {:ok, handle} <- start_executor_step(state, node_key, asset) do
      {:ok, put_execution_handle(state, node_key, handle)}
    else
      {:error, reason} ->
        normalized = %{kind: :error, reason: reason, stacktrace: [], class: :executor_error}
        handle_step_error(state, node_key, normalized)
    end
  end

  defp handle_step_result(
         %State{} = state,
         exec_ref,
         maybe_ref,
         {:ok, meta}
       ) do
    case take_execution(state, exec_ref, maybe_ref) do
      {:ok, node_key, state} ->
        complete_step_transition(
          state,
          fn next_state ->
            apply_step_transition(
              next_state,
              &StepTransitions.complete_success(&1, node_key, meta)
            )
          end,
          exec_ref,
          node_key,
          :success
        )

      {:ignore, state} ->
        {:ok, state}
    end
  end

  defp handle_step_result(%State{} = state, exec_ref, maybe_ref, {:error, error})
       when is_map(error) do
    case take_execution(state, exec_ref, maybe_ref) do
      {:ok, node_key, state} ->
        complete_step_transition(
          state,
          fn next_state -> handle_step_error(next_state, node_key, error) end,
          exec_ref,
          node_key,
          :failure
        )

      {:ignore, state} ->
        {:ok, state}
    end
  end

  defp complete_step_transition(%State{} = state, transition_fun, exec_ref, node_key, outcome) do
    case transition_fun.(state) do
      {:ok, %State{} = next_state} ->
        {:ok, next_state}

      {:error, {:invalid_step_transition, status, _event}} ->
        Logger.warning(
          "Ignoring late executor #{outcome} for node_key=#{inspect(node_key)} exec_ref=#{inspect(exec_ref)} from_status=#{inspect(status)}"
        )

        {:ok, state}

      {:error, _reason} = error ->
        error
    end
  end

  defp handle_step_error(%State{run_status: :cancelling} = state, node_key, _error) do
    apply_step_transition_allow_stale(
      state,
      &StepTransitions.complete_cancelled(&1, node_key, %{kind: :run_cancelled}),
      node_key,
      :complete_cancelled
    )
  end

  defp handle_step_error(%State{run_status: :timing_out} = state, node_key, _error) do
    apply_step_transition_allow_stale(
      state,
      &StepTransitions.complete_timed_out(&1, node_key, %{kind: :run_timed_out}),
      node_key,
      :complete_timed_out
    )
  end

  defp handle_step_error(%State{} = state, node_key, error) do
    step = Map.fetch!(state.steps, node_key)
    classification = classify_error(error)
    retryable? = retryable?(state.retry_policy, classification)
    exhausted? = step.attempt >= step.max_attempts
    payload = enrich_error(error, classification)

    if retryable? and not exhausted? do
      with {:ok, state} <-
             apply_step_transition(
               state,
               &StepTransitions.schedule_retry(&1, node_key, payload, state.retry_policy.delay_ms)
             ),
           {:ok, state} <- emit_retryable_step_failed(state, node_key, payload) do
        schedule_retry_timer(state, node_key)
      else
        error -> error
      end
    else
      with {:ok, state} <-
             apply_step_transition_allow_stale(
               state,
               &StepTransitions.complete_failure(&1, node_key, payload,
                 retryable?: retryable?,
                 exhausted?: true
               ),
               node_key,
               :complete_failure
             ),
           {:ok, state} <- maybe_emit_retry_exhausted(state, node_key, step, retryable?) do
        close_admission_with_failure(state, node_key, payload[:reason])
      else
        error -> error
      end
    end
  end

  defp apply_step_transition_allow_stale(%State{} = state, transition_fun, node_key, command) do
    case apply_step_transition(state, transition_fun) do
      {:ok, %State{} = next_state} ->
        {:ok, next_state}

      {:error, {:invalid_step_transition, status, _event}} ->
        Logger.warning(
          "Ignoring stale step transition command=#{inspect(command)} node_key=#{inspect(node_key)} from_status=#{inspect(status)}"
        )

        {:ok, state}

      {:error, _reason} = error ->
        error
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
             {:ok, state} <- apply_run_transition(state, {:mark_failed, reason}) do
          maybe_finalize_unresolved(state)
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
      with {:ok, state} <- maybe_finalize_unresolved(state, :cancelled) do
        apply_run_transition(state, :mark_cancelled)
      end
    else
      {:ok, state}
    end
  end

  defp maybe_finalize_terminal(%State{run_status: :timing_out} = state) do
    if map_size(state.inflight_execs) == 0 do
      with {:ok, state} <- maybe_finalize_unresolved(state, :timed_out) do
        apply_run_transition(state, :mark_timed_out)
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

  defp close_admission_with_failure(%State{} = state, node_key, reason) do
    step = Map.fetch!(state.steps, node_key)

    with {:ok, state} <- close_admission(state) do
      maybe_mark_run_failed(state, %{
        ref: step.ref,
        stage: stage_for_key(state, node_key),
        reason: reason
      })
    end
  end

  defp maybe_mark_run_failed(%State{run_status: :running} = state, reason),
    do: apply_run_transition(state, {:mark_failed, reason})

  defp maybe_mark_run_failed(%State{} = state, _reason), do: {:ok, state}

  defp close_admission(%State{admission_open?: false} = state), do: {:ok, state}

  defp close_admission(%State{} = state) do
    started_ms = System.monotonic_time(:millisecond)
    next_state = %{state | admission_open?: false}
    duration_ms = System.monotonic_time(:millisecond) - started_ms

    _ =
      Telemetry.emit_operation(:coordinator, :admission, duration_ms, %{
        run_id: state.run_id,
        result: :closed,
        inflight_count: map_size(state.inflight_execs),
        ready_count: length(state.ready_queue)
      })

    {:ok, next_state}
  end

  defp request_cancellation(%State{run_status: :pending} = state, reason) do
    with {:ok, state} <- apply_run_transition(state, :start) do
      request_cancellation(state, reason)
    end
  end

  defp request_cancellation(%State{run_status: :running} = state, reason) do
    with {:ok, state} <- apply_run_transition(state, :request_cancel),
         {:ok, state} <- close_admission(state) do
      interrupt_inflight(state, Map.put(reason, :kind, :run_cancelled))
    end
  end

  defp request_cancellation(%State{run_status: :cancelling} = state, _reason), do: {:ok, state}
  defp request_cancellation(%State{} = state, _reason), do: {:ok, state}

  defp request_timeout(%State{run_status: :running} = state) do
    with {:ok, state} <- apply_run_transition(state, :request_timeout),
         {:ok, state} <- close_admission(state) do
      interrupt_inflight(state, %{kind: :run_timed_out})
    end
  end

  defp request_timeout(%State{} = state), do: {:ok, state}

  defp interrupt_inflight(%State{} = state, reason) do
    result =
      Enum.reduce_while(state.inflight_execs, :ok, fn {exec_ref, exec_info}, :ok ->
        handle = %{exec_ref: exec_ref, monitor_ref: exec_info.monitor_ref, pid: exec_info.pid}
        node_key = exec_info.node_key
        step = Map.fetch!(state.steps, node_key)
        started_ms = System.monotonic_time(:millisecond)
        Logger.metadata(ref: inspect(step.ref), stage: step.stage, attempt: step.attempt)

        case safe_cancel_executor_step(handle, reason) do
          :ok ->
            duration_ms = System.monotonic_time(:millisecond) - started_ms

            _ =
              Telemetry.emit_operation(:executor, :cancel_step, duration_ms, %{
                run_id: state.run_id,
                ref: step.ref,
                stage: step.stage,
                attempt: step.attempt,
                result: :ok
              })

            {:cont, :ok}

          {:error, err, class, kind} ->
            duration_ms = System.monotonic_time(:millisecond) - started_ms

            _ =
              Telemetry.emit_operation(:executor, :cancel_step, duration_ms, %{
                run_id: state.run_id,
                ref: step.ref,
                stage: step.stage,
                attempt: step.attempt,
                result: :error,
                error_class: class,
                error_kind: kind
              })

            {:halt, {:error, err}}
        end
      end)

    case result do
      :ok -> {:ok, state}
      {:error, reason} -> {:error, {:executor_cancel_failed, reason}}
    end
  end

  defp take_execution(%State{} = state, exec_ref, maybe_node_key) do
    case Map.pop(state.inflight_execs, exec_ref) do
      {nil, _} ->
        Logger.debug("Ignoring stale executor result for unknown exec_ref=#{inspect(exec_ref)}")
        {:ignore, state}

      {%{node_key: tracked_node_key, monitor_ref: monitor_ref}, inflight} ->
        if maybe_node_key != nil and maybe_node_key != tracked_node_key do
          Logger.warning(
            "Executor returned mismatched step key; using tracked key. exec_ref=#{inspect(exec_ref)} tracked_key=#{inspect(tracked_node_key)} received_key=#{inspect(maybe_node_key)}"
          )
        end

        next_state =
          state
          |> Map.put(:inflight_execs, inflight)
          |> Map.put(:exec_refs_by_monitor, Map.delete(state.exec_refs_by_monitor, monitor_ref))
          |> Map.put(:completed_exec_refs, Map.put(state.completed_exec_refs, exec_ref, true))

        {:ok, tracked_node_key, next_state}
    end
  end

  defp clear_retry_timer(%State{} = state, node_key) do
    {:ok, %{state | retry_timers: Map.delete(state.retry_timers, node_key)}}
  end

  defp maybe_make_retry_ready(%State{} = state, node_key, attempt) do
    step = Map.fetch!(state.steps, node_key)

    cond do
      state.run_status != :running ->
        {:ok, state}

      step.status != :retrying ->
        {:ok, state}

      step.attempt != attempt ->
        {:ok, state}

      true ->
        apply_step_transition(state, &StepTransitions.requeue_retry(&1, node_key))
    end
  end

  defp schedule_retry_timer(%State{} = state, node_key) do
    step = Map.fetch!(state.steps, node_key)
    delay_ms = state.retry_policy.delay_ms

    if delay_ms == 0 do
      apply_step_transition(state, &StepTransitions.requeue_retry(&1, node_key))
    else
      timer_ref = Process.send_after(self(), {:retry_due, node_key, step.attempt}, delay_ms)
      next_state = %{state | retry_timers: Map.put(state.retry_timers, node_key, timer_ref)}
      persist_snapshot(next_state)
    end
  end

  defp emit_retryable_step_failed(%State{} = state, node_key, error) do
    step = Map.fetch!(state.steps, node_key)

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

    emit_events(state, [{:step_failed, node_key, payload}])
  end

  defp emit_retry_exhausted(%State{} = state, node_key, step) do
    payload = %{
      attempt: step.attempt,
      max_attempts: step.max_attempts,
      exhausted: true
    }

    emit_events(state, [{:step_retry_exhausted, node_key, payload}])
  end

  defp maybe_emit_retry_exhausted(%State{} = state, _node_key, _step, false), do: {:ok, state}

  defp maybe_emit_retry_exhausted(%State{} = state, node_key, step, true),
    do: emit_retry_exhausted(state, node_key, step)

  defp retries_pending?(%State{} = state) do
    Enum.any?(state.steps, fn {_ref, step} -> step.status == :retrying end)
  end

  defp clear_monitor(%State{} = state, exec_ref, monitor_ref) do
    %{
      state
      | exec_refs_by_monitor: Map.delete(state.exec_refs_by_monitor, monitor_ref),
        completed_exec_refs: Map.put(state.completed_exec_refs, exec_ref, true)
    }
  end

  defp put_execution_handle(%State{} = state, node_key, %{
         exec_ref: exec_ref,
         monitor_ref: monitor_ref,
         pid: pid
       }) do
    info = %{node_key: node_key, monitor_ref: monitor_ref, pid: pid}

    %{
      state
      | inflight_execs: Map.put(state.inflight_execs, exec_ref, info),
        exec_refs_by_monitor: Map.put(state.exec_refs_by_monitor, monitor_ref, exec_ref)
    }
  end

  defp apply_run_transition(%State{} = state, command) do
    with {:ok, state, events} <- RunTransitions.apply(state, command),
         {:ok, state} <- emit_events(state, events) do
      persist_snapshot(state)
    end
  end

  defp apply_step_transition(%State{} = state, transition_fun) do
    with {:ok, state, events} <- transition_fun.(state),
         {:ok, state} <- emit_events(state, events) do
      persist_snapshot(state)
    end
  end

  defp apply_finalize_unresolved(%State{} = state, replacement_status) do
    {state, events} = StepTransitions.finalize_unresolved(state, replacement_status)

    with {:ok, state} <- emit_events(state, events) do
      persist_snapshot(state)
    end
  end

  defp emit_step_ready_for_sources(%State{} = state) do
    source_keys =
      state.steps
      |> Enum.filter(fn {_node_key, step} -> step.status == :ready end)
      |> Enum.map(&elem(&1, 0))
      |> Enum.sort()

    events =
      Enum.map(source_keys, fn node_key ->
        {:step_ready, node_key}
      end)

    emit_events(%{state | ready_queue: source_keys}, events)
  end

  defp emit_events(%State{} = state, events) when is_list(events) do
    state =
      Enum.reduce(events, state, fn event, acc ->
        {event_name, attrs} = event_attrs(acc, event)
        seq = acc.event_seq + 1
        attrs = Map.merge(attrs, %{run_id: acc.run_id, seq: seq})

        _ = Telemetry.emit_runtime_event(event_name, attrs)

        _ =
          Events.publish_run_event(
            acc.run_id,
            event_name,
            attrs
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

  defp event_attrs(%State{} = state, {event_name, node_key}) do
    step = Map.fetch!(state.steps, node_key)
    stage = step.stage

    {event_name,
     %{
       entity: :step,
       status: step.status,
       ref: step.ref,
       stage: stage,
       data: %{
         duration_ms: step.duration_ms,
         attempt: step.attempt,
         max_attempts: step.max_attempts,
         node_key: node_key
       }
     }}
  end

  defp event_attrs(%State{} = state, {event_name, node_key, payload}) when is_map(payload) do
    step = Map.fetch!(state.steps, node_key)
    stage = step.stage
    error = step.error || %{}
    payload = payload_with_error_metadata(payload, error)

    {event_name,
     %{
       entity: :step,
       status: step.status,
       ref: step.ref,
       stage: stage,
       data:
         Map.merge(payload, %{
           duration_ms: step.duration_ms,
           attempt: step.attempt,
           max_attempts: step.max_attempts,
           node_key: node_key
         })
     }}
  end

  defp event_attrs(%State{} = state, event_name) when is_atom(event_name) do
    run_duration_ms = run_duration_ms(state)

    data =
      case event_name do
        :run_finished ->
          %{duration_ms: run_duration_ms}

        :run_failed ->
          %{
            duration_ms: run_duration_ms,
            error: state.run_error,
            terminal_reason: state.run_terminal_reason,
            error_class: :run_failed,
            error_kind: :error
          }

        :run_cancel_requested ->
          %{terminal_reason: state.run_terminal_reason}

        :run_cancelled ->
          %{terminal_reason: state.run_terminal_reason, duration_ms: run_duration_ms}

        :run_timeout_triggered ->
          %{terminal_reason: state.run_terminal_reason, duration_ms: run_duration_ms}

        :run_timed_out ->
          %{terminal_reason: state.run_terminal_reason, duration_ms: run_duration_ms}

        _ ->
          %{}
      end

    {event_name, %{entity: :run, status: state.run_status, data: data}}
  end

  defp payload_with_error_metadata(payload, error) do
    payload
    |> maybe_put_error_field(:error_kind, Map.get(error, :kind))
    |> maybe_put_error_field(:error_class, Map.get(error, :class))
  end

  defp maybe_put_error_field(payload, _key, nil), do: payload
  defp maybe_put_error_field(payload, key, value), do: Map.put_new(payload, key, value)

  defp persist_snapshot(%State{} = state) do
    case state |> Projector.to_public_run() |> Favn.Storage.put_run() do
      :ok -> {:ok, state}
      {:error, reason} -> {:error, {:storage_persist_failed, reason}}
    end
  end

  defp build_context(%State{} = state, node_key) do
    step = Map.fetch!(state.steps, node_key)

    %Context{
      run_id: state.run_id,
      target_refs: state.target_refs,
      current_ref: step.ref,
      params: state.params,
      window: step.runtime_window,
      pipeline: state.pipeline_context,
      run_started_at: state.started_at,
      stage: step.stage,
      attempt: step.attempt,
      max_attempts: step.max_attempts
    }
  end

  defp stage_for_key(%State{} = state, node_key),
    do: state.steps |> Map.fetch!(node_key) |> Map.get(:stage)

  defp pop_next_ready(%State{ready_queue: []}), do: :none

  defp pop_next_ready(%State{ready_queue: [node_key | rest]} = state),
    do: {:ok, node_key, %{state | ready_queue: rest}}

  defp all_targets_success?(%State{} = state) do
    Enum.all?(target_node_keys(state), fn node_key ->
      Map.fetch!(state.steps, node_key).status == :success
    end)
  end

  defp target_node_keys(%State{} = state) do
    if is_list(state.target_node_keys) and state.target_node_keys != [] do
      state.target_node_keys
    else
      target_ref_set = MapSet.new(state.target_refs)

      state.plan.nodes
      |> Enum.filter(fn {_node_key, node} -> MapSet.member?(target_ref_set, node.ref) end)
      |> Enum.map(&elem(&1, 0))
      |> Enum.sort()
    end
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

  defp start_executor_step(%State{} = state, node_key, asset) do
    step = Map.fetch!(state.steps, node_key)
    ctx = build_context(state, node_key)
    Logger.metadata(ref: inspect(step.ref), stage: step.stage, attempt: step.attempt)
    started_ms = System.monotonic_time(:millisecond)

    case safe_start_executor_step(asset, ctx, node_key) do
      {:ok, handle} ->
        duration_ms = System.monotonic_time(:millisecond) - started_ms

        _ =
          Telemetry.emit_operation(:executor, :start_step, duration_ms, %{
            run_id: state.run_id,
            ref: step.ref,
            stage: step.stage,
            attempt: step.attempt,
            result: :ok
          })

        {:ok, handle}

      {:error, reason, class, kind} ->
        duration_ms = System.monotonic_time(:millisecond) - started_ms

        _ =
          Telemetry.emit_operation(:executor, :start_step, duration_ms, %{
            run_id: state.run_id,
            ref: step.ref,
            stage: step.stage,
            attempt: step.attempt,
            result: :error,
            error_class: class,
            error_kind: kind
          })

        {:error, reason}
    end
  end

  defp safe_start_executor_step(asset, ctx, node_key) do
    executor_module().start_step(asset, ctx, self(), node_key)
    |> unwrap_executor_result()
  rescue
    error -> {:error, error, :executor_start_raise, :error}
  catch
    :exit, reason -> {:error, reason, :executor_start_exit, :exit}
    :throw, reason -> {:error, reason, :executor_start_throw, :throw}
  end

  defp safe_cancel_executor_step(handle, reason) do
    case executor_module().cancel_step(handle, reason) do
      :ok -> :ok
      {:error, err} -> {:error, err, :executor_cancel_error, :error}
    end
  rescue
    error -> {:error, error, :executor_cancel_raise, :error}
  catch
    :exit, reason -> {:error, reason, :executor_cancel_exit, :exit}
    :throw, reason -> {:error, reason, :executor_cancel_throw, :throw}
  end

  defp unwrap_executor_result({:ok, handle}), do: {:ok, handle}

  defp unwrap_executor_result({:error, reason}),
    do: {:error, reason, :executor_start_error, :error}

  defp unwrap_executor_result(other), do: {:error, other, :executor_start_invalid_return, :error}

  defp run_duration_ms(%State{} = state) do
    case state.started_at do
      %DateTime{} = started_at ->
        ended_at = state.finished_at || DateTime.utc_now()
        max(DateTime.diff(ended_at, started_at, :millisecond), 0)

      _ ->
        nil
    end
  end
end
