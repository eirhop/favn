defmodule Favn.Runtime.Transitions.Step do
  @moduledoc """
  Pure per-step transitions and deterministic readiness updates.
  """

  alias Favn.Runtime.State
  alias Favn.Runtime.StepState

  @type event :: {atom(), Favn.asset_ref()}
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

  @spec start_step(State.t(), Favn.asset_ref()) ::
          {:ok, State.t(), [event()]} | {:error, transition_error()}
  def start_step(%State{} = state, ref) do
    with {:ok, step} <- fetch_step(state, ref),
         :ok <- require_status(step, :ready, :start_step) do
      now = DateTime.utc_now()

      next_step = %{step | status: :running, started_at: now}

      next_state =
        state
        |> put_step(next_step)
        |> remove_ready(ref)
        |> put_running(ref)

      {:ok, next_state, [{:step_started, ref}]}
    end
  end

  @spec complete_success(State.t(), Favn.asset_ref(), term(), map()) ::
          {:ok, State.t(), [event()]} | {:error, transition_error()}
  def complete_success(%State{} = state, ref, output, meta) when is_map(meta) do
    with {:ok, step} <- fetch_step(state, ref),
         :ok <- require_status(step, :running, :complete_success) do
      now = DateTime.utc_now()
      duration_ms = DateTime.diff(now, step.started_at || now, :millisecond)

      next_step = %{
        step
        | status: :success,
          finished_at: now,
          duration_ms: max(duration_ms, 0),
          output: output,
          meta: meta,
          error: nil
      }

      state =
        state
        |> put_step(next_step)
        |> clear_running(ref)
        |> put_completed(ref)
        |> put_output(ref, output)

      {state, ready_events} = unlock_downstream(state, ref)

      {:ok, state, [{:step_finished, ref} | ready_events]}
    end
  end

  @spec complete_failure(State.t(), Favn.asset_ref(), map()) ::
          {:ok, State.t(), [event()]} | {:error, transition_error()}
  def complete_failure(%State{} = state, ref, error) when is_map(error) do
    with {:ok, step} <- fetch_step(state, ref),
         :ok <- require_status(step, :running, :complete_failure) do
      now = DateTime.utc_now()
      duration_ms = DateTime.diff(now, step.started_at || now, :millisecond)

      next_step = %{
        step
        | status: :failed,
          finished_at: now,
          duration_ms: max(duration_ms, 0),
          output: nil,
          error: error
      }

      next_state =
        state
        |> put_step(next_step)
        |> clear_running(ref)
        |> put_completed(ref)

      {:ok, next_state, [{:step_failed, ref}]}
    end
  end

  @doc """
  Finalize unresolved steps deterministically after run failure/cancellation.
  """
  @spec finalize_unresolved(State.t(), :skipped | :cancelled) :: {State.t(), [event()]}
  def finalize_unresolved(%State{} = state, replacement_status)
      when replacement_status in [:skipped, :cancelled] do
    {next_steps, events} =
      Enum.reduce(state.steps, {%{}, []}, fn {ref, step}, {acc_steps, acc_events} ->
        case step.status do
          status when status in [:pending, :ready] ->
            event = event_for(replacement_status)

            {Map.put(acc_steps, ref, %{step | status: replacement_status}),
             [{event, ref} | acc_events]}

          _ ->
            {Map.put(acc_steps, ref, step), acc_events}
        end
      end)

    {%{state | steps: next_steps, ready_queue: []}, Enum.reverse(events)}
  end

  defp event_for(:skipped), do: :step_skipped
  defp event_for(:cancelled), do: :step_cancelled

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

    Enum.reduce(step.downstream, {state, []}, fn downstream_ref, {acc, events} ->
      downstream = Map.fetch!(acc.steps, downstream_ref)

      if downstream.status == :pending and all_upstream_success?(acc, downstream.upstream) do
        {:ok, next_acc, next_events} = mark_ready(acc, downstream_ref)
        {next_acc, events ++ next_events}
      else
        {acc, events}
      end
    end)
  end

  defp all_upstream_success?(%State{} = state, upstream_refs) do
    Enum.all?(upstream_refs, fn upstream_ref ->
      state.steps |> Map.fetch!(upstream_ref) |> Map.get(:status) == :success
    end)
  end
end
