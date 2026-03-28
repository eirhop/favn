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
  alias Favn.Runtime.StepState
  alias Favn.Runtime.Transitions.Run, as: RunTransitions
  alias Favn.Runtime.Transitions.Step, as: StepTransitions

  @executor Application.compile_env(:favn, :runtime_executor, Local)

  @spec run_sync(Favn.asset_ref(), keyword()) ::
          {:ok, Favn.Run.t()} | {:error, Favn.Run.t() | term()}
  def run_sync(target_ref, opts) when is_list(opts) do
    dependencies = Keyword.get(opts, :dependencies, :all)
    params = Keyword.get(opts, :params, %{})

    with :ok <- validate_params(params),
         {:ok, plan} <- Favn.plan_run(target_ref, dependencies: dependencies) do
      state = %State{
        run_id: new_run_id(),
        target_refs: plan.target_refs,
        plan: plan,
        params: params,
        steps: build_steps(plan)
      }

      case GenServer.start_link(__MODULE__, state: state, caller: self()) do
        {:ok, pid} ->
          ref = Process.monitor(pid)

          receive do
            {:favn_coordinator_result, ^pid, result} ->
              Process.demonitor(ref, [:flush])
              result

            {:DOWN, ^ref, :process, ^pid, reason} ->
              {:error, {:coordinator_down, reason}}
          after
            30_000 ->
              Process.demonitor(ref, [:flush])
              {:error, :coordinator_timeout}
          end

        {:error, reason} ->
          {:error, reason}
      end
    end
  end

  @impl true
  def init(opts) do
    state = opts |> Keyword.fetch!(:state) |> persist_snapshot()
    caller = Keyword.fetch!(opts, :caller)
    send(self(), :start_run)
    {:ok, %{state: state, caller: caller}}
  end

  @impl true
  def handle_info(:start_run, %{state: state} = data) do
    {:ok, state, run_events} = RunTransitions.apply(state, :start)

    state =
      state
      |> persist_snapshot()
      |> emit_events(run_events)
      |> seed_initial_ready()
      |> execute_until_terminal()

    run = Projector.to_public_run(state)
    result = if run.status == :ok, do: {:ok, run}, else: {:error, run}

    send(data.caller, {:favn_coordinator_result, self(), result})
    {:stop, :normal, %{data | state: state}}
  end

  defp validate_params(params) when is_map(params), do: :ok
  defp validate_params(_), do: {:error, :invalid_run_params}

  defp build_steps(plan) do
    Enum.reduce(plan.nodes, %{}, fn {ref, node}, acc ->
      status = if node.upstream == [], do: :ready, else: :pending

      step = %StepState{
        ref: ref,
        stage: node.stage,
        upstream: node.upstream,
        downstream: node.downstream,
        status: status
      }

      Map.put(acc, ref, step)
    end)
  end

  defp seed_initial_ready(%State{} = state) do
    ready_refs =
      state.steps
      |> Enum.filter(fn {_ref, step} -> step.status == :ready end)
      |> Enum.map(&elem(&1, 0))
      |> Enum.sort()

    %{state | ready_queue: ready_refs}
  end

  defp execute_until_terminal(%State{run_status: :running} = state) do
    case pop_next_ready(state) do
      {:ok, ref, next_state} ->
        next_state |> run_step(ref) |> execute_until_terminal()

      :none ->
        if all_targets_success?(state) do
          {:ok, state, run_events} = RunTransitions.apply(state, :mark_success)
          state |> persist_snapshot() |> emit_events(run_events)
        else
          reason = infer_failure_reason(state)
          {:ok, state, run_events} = RunTransitions.apply(state, {:mark_failed, reason})
          {state, step_events} = StepTransitions.finalize_unresolved(state, :skipped)
          state |> persist_snapshot() |> emit_events(step_events ++ run_events)
        end
    end
  end

  defp execute_until_terminal(state), do: state

  defp run_step(%State{} = state, ref) do
    {:ok, state, start_events} = StepTransitions.start_step(state, ref)
    state = state |> persist_snapshot() |> emit_events(start_events)

    with {:ok, asset} <- Favn.Registry.get_asset(ref),
         {:ok, deps} <- dependency_outputs(state, ref) do
      ctx = build_context(state, ref)

      case @executor.execute_step(asset, ctx, deps) do
        {:ok, %{output: output, meta: meta}} ->
          {:ok, state, events} = StepTransitions.complete_success(state, ref, output, meta)
          state |> persist_snapshot() |> emit_events(events)

        {:error, error} ->
          {:ok, state, events} = StepTransitions.complete_failure(state, ref, error)
          reason = %{ref: ref, stage: Map.fetch!(state.steps, ref).stage, reason: error.reason}
          {:ok, state, run_events} = RunTransitions.apply(state, {:mark_failed, reason})
          {state, skipped_events} = StepTransitions.finalize_unresolved(state, :skipped)
          state |> persist_snapshot() |> emit_events(events ++ skipped_events ++ run_events)
      end
    else
      {:error, reason} ->
        normalized = %{kind: :error, reason: reason, stacktrace: []}
        {:ok, state, events} = StepTransitions.complete_failure(state, ref, normalized)
        run_reason = %{ref: ref, stage: Map.fetch!(state.steps, ref).stage, reason: reason}
        {:ok, state, run_events} = RunTransitions.apply(state, {:mark_failed, run_reason})
        {state, skipped_events} = StepTransitions.finalize_unresolved(state, :skipped)
        state |> persist_snapshot() |> emit_events(events ++ skipped_events ++ run_events)
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
      run_started_at: state.started_at || DateTime.utc_now(),
      stage: state.steps |> Map.fetch!(ref) |> Map.get(:stage)
    }
  end

  defp pop_next_ready(%State{ready_queue: []}), do: :none

  defp pop_next_ready(%State{ready_queue: [ref | rest]} = state),
    do: {:ok, ref, %{state | ready_queue: rest}}

  defp all_targets_success?(%State{} = state) do
    Enum.all?(state.target_refs, fn ref ->
      state.steps |> Map.fetch!(ref) |> Map.get(:status) == :success
    end)
  end

  defp infer_failure_reason(%State{} = state),
    do: state.run_error || %{reason: :run_did_not_reach_targets}

  defp emit_events(%State{} = state, events) when is_list(events) do
    Enum.reduce(events, state, fn event, acc ->
      seq = acc.event_seq + 1

      _ =
        Favn.Runtime.Events.publish_run_event(acc.run_id, event, %{
          seq: seq,
          payload: event_payload(acc, event)
        })

      %{acc | event_seq: seq}
    end)
  end

  defp event_payload(%State{} = state, :run_failed), do: %{error: state.run_error}
  defp event_payload(%State{}, _event), do: %{}

  defp persist_snapshot(%State{} = state) do
    _ = state |> Projector.to_public_run() |> Favn.Storage.put_run()
    state
  end

  defp new_run_id do
    binary = :crypto.strong_rand_bytes(16)
    <<a::32, b::16, c::16, d::16, e::48>> = binary

    c = Bitwise.bor(Bitwise.band(c, 0x0FFF), 0x4000)
    d = Bitwise.bor(Bitwise.band(d, 0x3FFF), 0x8000)

    Enum.join(
      [
        a |> Integer.to_string(16) |> String.pad_leading(8, "0"),
        b |> Integer.to_string(16) |> String.pad_leading(4, "0"),
        c |> Integer.to_string(16) |> String.pad_leading(4, "0"),
        d |> Integer.to_string(16) |> String.pad_leading(4, "0"),
        e |> Integer.to_string(16) |> String.pad_leading(12, "0")
      ],
      "-"
    )
  end
end
