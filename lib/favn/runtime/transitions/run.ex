defmodule Favn.Runtime.Transitions.Run do
  @moduledoc """
  Pure run-level state transitions.
  """

  alias Favn.Runtime.State

  @type transition_error :: {:invalid_run_transition, State.run_status(), atom()}

  @spec apply(
          State.t(),
          :start | :mark_success | {:mark_failed, term()} | :request_cancel | :mark_cancelled
        ) ::
          {:ok, State.t(), [atom()]} | {:error, transition_error()}
  def apply(%State{run_status: :pending} = state, :start) do
    now = DateTime.utc_now()
    {:ok, %{state | run_status: :running, started_at: now}, [:run_started]}
  end

  def apply(%State{run_status: :running} = state, :mark_success) do
    now = DateTime.utc_now()
    {:ok, %{state | run_status: :success, finished_at: now}, [:run_finished]}
  end

  def apply(%State{run_status: :running} = state, {:mark_failed, reason}) do
    now = DateTime.utc_now()
    {:ok, %{state | run_status: :failed, finished_at: now, run_error: reason}, [:run_failed]}
  end

  def apply(%State{run_status: :running} = state, :request_cancel) do
    {:ok, %{state | run_status: :cancelling, cancel_requested_at: DateTime.utc_now()},
     [:run_cancel_requested]}
  end

  def apply(%State{run_status: :cancelling} = state, :mark_cancelled) do
    now = DateTime.utc_now()
    {:ok, %{state | run_status: :cancelled, finished_at: now}, [:run_cancelled]}
  end

  def apply(%State{run_status: status}, action) do
    {:error, {:invalid_run_transition, status, normalize_action(action)}}
  end

  defp normalize_action({action, _}), do: action
  defp normalize_action(action), do: action
end
