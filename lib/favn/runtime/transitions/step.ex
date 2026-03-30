defmodule Favn.Runtime.Transitions.Step do
  @moduledoc """
  Pure per-step transitions and deterministic readiness updates.
  """

  alias Favn.Runtime.State
  alias Favn.Runtime.StepState

  @type event :: {atom(), Favn.asset_ref()} | {atom(), Favn.asset_ref(), map()}
  @type transition_error :: {:invalid_step_transition, StepState.status(), atom()}

  @spec mark_ready(State.t(), Favn.asset_ref()) ::
          {:ok, State.t(), [event()]} | {:error, transition_error()}
  def mark_ready(%State{} = state, ref) do
    with {:ok, step} <- fetch_step(state, ref),
         :ok <- require_status(step, :pending, :mark_ready) do
      next_step = %{step | status: :ready}

      next_state =
        state
        |> put_step(next_step)
        |> enqueue_ready(ref)

      {:ok, next_state, [{:step_ready, ref}]}
    end
  end

  @spec schedule_retry(State.t(), Favn.asset_ref(), map(), non_neg_integer()) ::
          {:ok, State.t(), [event()]} | {:error, transition_error()}
  def schedule_retry(%State{} = state, ref, error, delay_ms)
      when is_map(error) and is_integer(delay_ms) and delay_ms >= 0 do
    with {:ok, step} <- fetch_step(state, ref),
         :ok <- require_status(step, :running, :schedule_retry) do
      now = DateTime.utc_now()
      duration_ms = DateTime.diff(now, step.started_at || now, :millisecond)
      next_attempt_at = DateTime.add(now, delay_ms, :millisecond)
      attempt = max(step.attempt, 1)

      attempt_result = %{
        attempt: attempt,
        exec_ref: nil,
        started_at: step.started_at || now,
        finished_at: now,
        duration_ms: max(duration_ms, 0),
        status: :error,
        output: nil,
        meta: %{},
        error: error
      }

      next_step = %{
        step
        | status: :retrying,
          started_at: nil,
          finished_at: nil,
          duration_ms: nil,
          output: nil,
          meta: %{},
          error: error,
          next_retry_at: next_attempt_at,
          attempts: step.attempts ++ [attempt_result]
      }

      payload = %{
        attempt: attempt,
        max_attempts: step.max_attempts,
        remaining_attempts: max(step.max_attempts - attempt, 0),
        delay_ms: delay_ms,
        next_attempt_at: next_attempt_at
      }

      next_state =
        state
        |> put_step(next_step)
        |> clear_running(ref)

      {:ok, next_state, [{:step_retry_scheduled, ref, payload}]}
    end
  end

  @spec requeue_retry(State.t(), Favn.asset_ref()) ::
          {:ok, State.t(), [event()]} | {:error, transition_error()}
  def requeue_retry(%State{} = state, ref) do
    with {:ok, step} <- fetch_step(state, ref),
         :ok <- require_status(step, :retrying, :requeue_retry) do
      next_step = %{step | status: :ready, next_retry_at: nil}

      next_state =
        state
        |> put_step(next_step)
        |> enqueue_ready(ref)

      {:ok, next_state, [{:step_ready, ref}]}
    end
  end

  @spec start_step(State.t(), Favn.asset_ref()) ::
          {:ok, State.t(), [event()]} | {:error, transition_error()}
  def start_step(%State{} = state, ref) do
    with {:ok, step} <- fetch_step(state, ref),
         :ok <- require_status(step, :ready, :start_step) do
      now = DateTime.utc_now()
      attempt = step.attempt + 1

      next_step = %{step | status: :running, started_at: now, attempt: attempt}

      next_state =
        state
        |> put_step(next_step)
        |> remove_ready(ref)
        |> put_running(ref)

      {:ok, next_state,
       [
         {:step_started, ref,
          %{
            attempt: attempt,
            max_attempts: step.max_attempts,
            remaining_attempts: step.max_attempts - attempt
          }}
       ]}
    end
  end

  @spec complete_success(State.t(), Favn.asset_ref(), term(), map()) ::
          {:ok, State.t(), [event()]} | {:error, transition_error()}
  def complete_success(%State{} = state, ref, output, meta) when is_map(meta) do
    with {:ok, step} <- fetch_step(state, ref),
         :ok <- require_status(step, :running, :complete_success) do
      now = DateTime.utc_now()
      duration_ms = DateTime.diff(now, step.started_at || now, :millisecond)

      attempt_result = %{
        attempt: step.attempt,
        exec_ref: nil,
        started_at: step.started_at || now,
        finished_at: now,
        duration_ms: max(duration_ms, 0),
        status: :ok,
        output: output,
        meta: meta,
        error: nil
      }

      next_step = %{
        step
        | status: :success,
          finished_at: now,
          duration_ms: max(duration_ms, 0),
          output: output,
          meta: meta,
          error: nil,
          next_retry_at: nil,
          attempts: step.attempts ++ [attempt_result]
      }

      state =
        state
        |> put_step(next_step)
        |> clear_running(ref)
        |> put_completed(ref)
        |> put_output(ref, output)

      {state, ready_events} = unlock_downstream(state, ref)

      {:ok, state, [{:step_finished, ref, %{attempt: step.attempt}} | ready_events]}
    end
  end

  @spec complete_failure(State.t(), Favn.asset_ref(), map(), keyword()) ::
          {:ok, State.t(), [event()]} | {:error, transition_error()}
  def complete_failure(%State{} = state, ref, error, opts \\ [])
      when is_map(error) and is_list(opts) do
    with {:ok, step} <- fetch_step(state, ref),
         :ok <- require_status(step, :running, :complete_failure) do
      now = DateTime.utc_now()
      duration_ms = DateTime.diff(now, step.started_at || now, :millisecond)
      retryable? = Keyword.get(opts, :retryable?, false)
      exhausted? = Keyword.get(opts, :exhausted?, true)

      attempt_result = %{
        attempt: step.attempt,
        exec_ref: nil,
        started_at: step.started_at || now,
        finished_at: now,
        duration_ms: max(duration_ms, 0),
        status: :error,
        output: nil,
        meta: %{},
        error: error
      }

      next_step = %{
        step
        | status: :failed,
          finished_at: now,
          duration_ms: max(duration_ms, 0),
          output: nil,
          error: error,
          next_retry_at: nil,
          attempts: step.attempts ++ [attempt_result]
      }

      payload = %{
        attempt: step.attempt,
        max_attempts: step.max_attempts,
        remaining_attempts: max(step.max_attempts - step.attempt, 0),
        retryable: retryable?,
        exhausted: exhausted?,
        final: true
      }

      next_state =
        state
        |> put_step(next_step)
        |> clear_running(ref)
        |> put_completed(ref)

      {:ok, next_state, [{:step_failed, ref, payload}]}
    end
  end

  @spec complete_cancelled(State.t(), Favn.asset_ref(), map()) ::
          {:ok, State.t(), [event()]} | {:error, transition_error()}
  def complete_cancelled(%State{} = state, ref, reason) when is_map(reason) do
    with {:ok, step} <- fetch_step(state, ref),
         :ok <- require_status(step, :running, :complete_cancelled) do
      now = DateTime.utc_now()
      duration_ms = DateTime.diff(now, step.started_at || now, :millisecond)

      attempt_result = %{
        attempt: step.attempt,
        exec_ref: nil,
        started_at: step.started_at || now,
        finished_at: now,
        duration_ms: max(duration_ms, 0),
        status: :cancelled,
        output: nil,
        meta: %{},
        error: nil
      }

      next_step = %{
        step
        | status: :cancelled,
          finished_at: now,
          duration_ms: max(duration_ms, 0),
          output: nil,
          error: nil,
          terminal_reason: reason,
          attempts: step.attempts ++ [attempt_result]
      }

      next_state =
        state
        |> put_step(next_step)
        |> clear_running(ref)
        |> put_completed(ref)

      {:ok, next_state, [{:step_cancelled, ref, %{attempt: step.attempt}}]}
    end
  end

  @spec complete_timed_out(State.t(), Favn.asset_ref(), map()) ::
          {:ok, State.t(), [event()]} | {:error, transition_error()}
  def complete_timed_out(%State{} = state, ref, reason) when is_map(reason) do
    with {:ok, step} <- fetch_step(state, ref),
         :ok <- require_status(step, :running, :complete_timed_out) do
      now = DateTime.utc_now()
      duration_ms = DateTime.diff(now, step.started_at || now, :millisecond)

      attempt_result = %{
        attempt: step.attempt,
        exec_ref: nil,
        started_at: step.started_at || now,
        finished_at: now,
        duration_ms: max(duration_ms, 0),
        status: :timed_out,
        output: nil,
        meta: %{},
        error: nil
      }

      next_step = %{
        step
        | status: :timed_out,
          finished_at: now,
          duration_ms: max(duration_ms, 0),
          output: nil,
          error: nil,
          terminal_reason: reason,
          attempts: step.attempts ++ [attempt_result]
      }

      next_state =
        state
        |> put_step(next_step)
        |> clear_running(ref)
        |> put_completed(ref)

      {:ok, next_state, [{:step_timed_out, ref, %{attempt: step.attempt}}]}
    end
  end

  @doc """
  Finalize unresolved steps deterministically after run failure/cancellation.
  """
  @spec finalize_unresolved(State.t(), :skipped | :cancelled | :timed_out) ::
          {State.t(), [event()]}
  def finalize_unresolved(%State{} = state, replacement_status)
      when replacement_status in [:skipped, :cancelled, :timed_out] do
    {next_steps, events} =
      Enum.reduce(state.steps, {%{}, []}, fn {ref, step}, {acc_steps, acc_events} ->
        case step.status do
          status when status in [:pending, :ready, :retrying] ->
            event = event_for(replacement_status)
            reason = terminal_reason_for(replacement_status)

            {Map.put(acc_steps, ref, %{step | status: replacement_status, terminal_reason: reason}),
             [{event, ref} | acc_events]}

          _ ->
            {Map.put(acc_steps, ref, step), acc_events}
        end
      end)

    {%{state | steps: next_steps, ready_queue: []}, Enum.reverse(events)}
  end

  defp event_for(:skipped), do: :step_skipped
  defp event_for(:cancelled), do: :step_cancelled
  defp event_for(:timed_out), do: :step_timed_out
  defp terminal_reason_for(:skipped), do: %{kind: :skipped}
  defp terminal_reason_for(:cancelled), do: %{kind: :cancelled}
  defp terminal_reason_for(:timed_out), do: %{kind: :timed_out}

  defp fetch_step(%State{steps: steps}, ref) do
    case Map.fetch(steps, ref) do
      {:ok, step} -> {:ok, step}
      :error -> {:error, {:invalid_step_transition, :missing, :unknown_step}}
    end
  end

  defp require_status(%StepState{status: expected}, expected, _action), do: :ok

  defp require_status(%StepState{status: status}, _expected, action),
    do: {:error, {:invalid_step_transition, status, action}}

  defp put_step(%State{} = state, %StepState{} = step),
    do: %{state | steps: Map.put(state.steps, step.ref, step)}

  defp enqueue_ready(%State{} = state, ref),
    do: %{state | ready_queue: state.ready_queue ++ [ref]}

  defp remove_ready(%State{} = state, ref),
    do: %{state | ready_queue: Enum.reject(state.ready_queue, &(&1 == ref))}

  defp put_running(%State{} = state, ref),
    do: %{state | running_steps: MapSet.put(state.running_steps, ref)}

  defp clear_running(%State{} = state, ref),
    do: %{state | running_steps: MapSet.delete(state.running_steps, ref)}

  defp put_completed(%State{} = state, ref),
    do: %{state | completed_steps: MapSet.put(state.completed_steps, ref)}

  defp put_output(%State{} = state, ref, output),
    do: %{state | outputs: Map.put(state.outputs, ref, output)}

  defp unlock_downstream(%State{} = state, ref) do
    step = Map.fetch!(state.steps, ref)

    ready_refs =
      step.downstream
      |> Enum.uniq()
      |> Enum.filter(fn downstream_ref ->
        downstream = Map.fetch!(state.steps, downstream_ref)
        downstream.status == :pending and all_upstream_success?(state, downstream.upstream)
      end)
      |> Enum.sort()

    Enum.reduce(ready_refs, {state, []}, fn downstream_ref, {acc, events} ->
      {:ok, next_acc, next_events} = mark_ready(acc, downstream_ref)
      {next_acc, events ++ next_events}
    end)
  end

  defp all_upstream_success?(%State{} = state, upstream_refs) do
    Enum.all?(upstream_refs, fn upstream_ref ->
      state.steps |> Map.fetch!(upstream_ref) |> Map.get(:status) == :success
    end)
  end
end
