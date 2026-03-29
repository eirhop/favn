defmodule Favn.Runtime.Coordinator do
  @moduledoc """
  Run-scoped coordinator process.

  Owns lifecycle mutation, step readiness, dispatch, transition application,
  persistence, and event emission.
  """

  use GenServer

  alias Favn.Run.Context
  alias Favn.Runtime.Executor.Local
  alias Favn.Runtime.Projector
  alias Favn.Runtime.State
  alias Favn.Runtime.Transitions.Run, as: RunTransitions
  alias Favn.Runtime.Transitions.Step, as: StepTransitions

  @executor Application.compile_env(:favn, :runtime_executor, Local)

  @type run_result :: {:ok, Favn.Run.t()} | {:error, Favn.Run.t() | term()}

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) when is_list(opts) do
    GenServer.start_link(__MODULE__, opts)
  end

  @impl true
  def init(opts) do
    send(self(), :start_run)
    {:ok, %{state: Keyword.fetch!(opts, :state)}}
  end

  @impl true
  def handle_info(:start_run, %{state: state} = data) do
    _result = start_and_execute(state)
    {:stop, :normal, data}
  end

  @impl true
  def handle_info(msg, data) when msg != :start_run do
    {:noreply, data}
  end

  @spec start_and_execute(State.t()) :: run_result()
  defp start_and_execute(state) do
    with {:ok, state} <- apply_run_transition(state, :start),
         {:ok, state} <- emit_step_ready_for_sources(state),
         {:ok, state} <- execute_until_terminal(state) do
      public_run = Projector.to_public_run(state)
      if public_run.status == :ok, do: {:ok, public_run}, else: {:error, public_run}
    else
      {:error, reason} ->
        {:error, reason}
    end
  end

  defp execute_until_terminal(%State{run_status: :running} = state) do
    case pop_next_ready(state) do
      {:ok, ref, next_state} ->
        with {:ok, next_state} <- run_step(next_state, ref) do
          execute_until_terminal(next_state)
        end

      :none ->
        finalize_terminal(state)
    end
  end

  defp execute_until_terminal(%State{} = state), do: {:ok, state}

  defp run_step(%State{} = state, ref) do
    with {:ok, state} <- apply_step_transition(state, &StepTransitions.start_step(&1, ref)),
         {:ok, asset} <- Favn.Registry.get_asset(ref),
         {:ok, deps} <- dependency_outputs(state, ref) do
      ctx = build_context(state, ref)

      case @executor.execute_step(asset, ctx, deps) do
        {:ok, %{output: output, meta: meta}} ->
          apply_step_transition(state, &StepTransitions.complete_success(&1, ref, output, meta))

        {:error, error} ->
          with {:ok, state} <-
                 apply_step_transition(state, &StepTransitions.complete_failure(&1, ref, error)),
               {:ok, state} <-
                 apply_run_transition(
                   state,
                   {:mark_failed, %{ref: ref, stage: stage_for(state, ref), reason: error.reason}}
                 ),
               {:ok, state} <- apply_finalize_unresolved(state, :skipped) do
            {:ok, state}
          end
      end
    else
      {:error, reason} ->
        normalized = %{kind: :error, reason: reason, stacktrace: []}

        with {:ok, state} <-
               apply_step_transition(
                 state,
                 &StepTransitions.complete_failure(&1, ref, normalized)
               ),
             {:ok, state} <-
               apply_run_transition(
                 state,
                 {:mark_failed, %{ref: ref, stage: stage_for(state, ref), reason: reason}}
               ),
             {:ok, state} <- apply_finalize_unresolved(state, :skipped) do
          {:ok, state}
        end
    end
  end

  defp finalize_terminal(%State{} = state) do
    if all_targets_success?(state) do
      apply_run_transition(state, :mark_success)
    else
      reason = state.run_error || %{reason: :run_did_not_reach_targets}

      with {:ok, state} <- apply_run_transition(state, {:mark_failed, reason}),
           {:ok, state} <- apply_finalize_unresolved(state, :skipped) do
        {:ok, state}
      end
    end
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

  defp event_attrs(%State{} = state, {event_name, ref}) do
    stage = stage_for(state, ref)
    {event_name, %{ref: ref, stage: stage}}
  end

  defp event_attrs(%State{} = state, event_name) when is_atom(event_name) do
    payload = if event_name == :run_failed, do: %{error: state.run_error}, else: %{}
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
    %Context{
      run_id: state.run_id,
      target_refs: state.target_refs,
      current_ref: ref,
      params: state.params,
      run_started_at: state.started_at,
      stage: stage_for(state, ref)
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
end
