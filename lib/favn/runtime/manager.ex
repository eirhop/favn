defmodule Favn.Runtime.Manager do
  @moduledoc """
  Async run submission manager.

  The manager owns run admission, initial snapshot persistence, and run process
  startup under `Favn.Runtime.RunSupervisor`.
  """

  use GenServer

  alias Favn.Runtime.Projector
  alias Favn.Runtime.State

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @spec submit_run(Favn.asset_ref(), keyword()) :: {:ok, Favn.run_id()} | {:error, term()}
  def submit_run(target_ref, opts \\ []) when is_list(opts) do
    GenServer.call(__MODULE__, {:submit_run, target_ref, opts}, :infinity)
  end

  @spec cancel_run(Favn.run_id()) ::
          {:ok, :cancelling | :cancelled | :already_terminal}
          | {:error, :not_found | :invalid_run_id | term()}
  def cancel_run(run_id) when is_binary(run_id) do
    GenServer.call(__MODULE__, {:cancel_run, run_id}, :infinity)
  end

  def cancel_run(_run_id), do: {:error, :invalid_run_id}

  @impl true
  def init(_opts), do: {:ok, %{run_monitors: %{}, run_pids: %{}}}

  @impl true
  def handle_call({:submit_run, target_ref, opts}, _from, state) do
    dependencies = Keyword.get(opts, :dependencies, :all)
    params = Keyword.get(opts, :params, %{})
    max_concurrency = resolve_max_concurrency(opts)
    timeout_ms = Keyword.get(opts, :timeout_ms)

    with :ok <- validate_params(params),
         :ok <- validate_max_concurrency(max_concurrency),
         :ok <- validate_timeout_ms(timeout_ms),
         {:ok, plan} <- Favn.plan_run(target_ref, dependencies: dependencies),
         runtime_state <- build_runtime_state(plan, params, max_concurrency, timeout_ms),
         {:ok, pid} <- start_run_coordinator(runtime_state),
         :ok <- persist_initial_snapshot(runtime_state),
         :ok <- emit_run_created(runtime_state),
         :ok <- kickoff_run(pid) do
      ref = Process.monitor(pid)

      next_state =
        state
        |> put_in([:run_monitors, ref], runtime_state.run_id)
        |> put_in([:run_pids, runtime_state.run_id], pid)

      {:reply, {:ok, runtime_state.run_id}, next_state}
    else
      {:start_failed_after_spawn, pid, reason} ->
        _ = DynamicSupervisor.terminate_child(Favn.Runtime.RunSupervisor, pid)
        {:reply, {:error, reason}, state}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  @impl true
  def handle_call({:cancel_run, run_id}, _from, state) do
    reply =
      case Favn.Storage.get_run(run_id) do
        {:ok, %{status: :cancelled}} ->
          {:ok, :cancelled}

        {:ok, %{status: :running}} ->
          case Map.fetch(state.run_pids, run_id) do
            {:ok, pid} ->
              GenServer.cast(pid, {:cancel_run, %{requested_by: :api}})
              {:ok, :cancelling}

            :error ->
              {:ok, :cancelling}
          end

        {:ok, _run} ->
          {:ok, :already_terminal}

        {:error, :not_found} ->
          {:error, :not_found}

        {:error, reason} ->
          {:error, reason}
      end

    {:reply, reply, state}
  end

  @impl true
  def handle_info({:DOWN, ref, :process, _pid, reason}, state) do
    {run_id, remaining} = Map.pop(state.run_monitors, ref)
    run_pids = if run_id, do: Map.delete(state.run_pids, run_id), else: state.run_pids

    if run_id do
      maybe_finalize_crashed_run(run_id, reason)
    end

    {:noreply, %{state | run_monitors: remaining, run_pids: run_pids}}
  end

  defp build_runtime_state(plan, params, max_concurrency, timeout_ms) do
    %State{
      run_id: new_run_id(),
      target_refs: plan.target_refs,
      plan: plan,
      params: params,
      max_concurrency: max_concurrency,
      timeout_ms: timeout_ms,
      event_seq: 1,
      steps: build_steps(plan)
    }
  end

  defp build_steps(plan) do
    Enum.reduce(plan.nodes, %{}, fn {ref, node}, acc ->
      status = if node.upstream == [], do: :ready, else: :pending

      step = %Favn.Runtime.StepState{
        ref: ref,
        stage: node.stage,
        upstream: node.upstream,
        downstream: node.downstream,
        status: status
      }

      Map.put(acc, ref, step)
    end)
  end

  defp persist_initial_snapshot(%State{} = runtime_state) do
    case runtime_state |> Projector.to_public_run() |> Favn.Storage.put_run() do
      :ok -> :ok
      {:error, reason} -> {:error, {:storage_persist_failed, reason}}
    end
  end

  defp emit_run_created(%State{} = runtime_state) do
    Favn.Runtime.Events.publish_run_event(runtime_state.run_id, :run_created, %{seq: 1})
    :ok
  end

  defp kickoff_run(pid) when is_pid(pid) do
    GenServer.cast(pid, :start_run)
    :ok
  rescue
    error ->
      {:start_failed_after_spawn, pid, error}
  end

  defp start_run_coordinator(%State{} = runtime_state) do
    child_spec = %{
      id: {Favn.Runtime.Coordinator, runtime_state.run_id},
      start: {Favn.Runtime.Coordinator, :start_link, [[state: runtime_state]]},
      restart: :temporary,
      shutdown: 5000,
      type: :worker
    }

    case DynamicSupervisor.start_child(Favn.Runtime.RunSupervisor, child_spec) do
      {:ok, pid} -> {:ok, pid}
      {:error, reason} -> {:error, reason}
    end
  end

  defp maybe_finalize_crashed_run(run_id, reason) do
    with {:ok, run} <- Favn.Storage.get_run(run_id),
         true <- run.status in [:running] do
      failed =
        %{
          run
          | status: :error,
            event_seq: run.event_seq + 1,
            finished_at: DateTime.utc_now(),
            error: {:run_process_crash, reason}
        }

      case Favn.Storage.put_run(failed) do
        :ok ->
          _ =
            Favn.Runtime.Events.publish_run_event(run_id, :run_failed, %{
              seq: failed.event_seq,
              payload: %{error: failed.error}
            })

          :ok

        {:error, _reason} ->
          :ok
      end

      :ok
    else
      _ -> :ok
    end
  end

  defp validate_params(params) when is_map(params), do: :ok
  defp validate_params(_), do: {:error, :invalid_run_params}

  defp validate_max_concurrency(value) when is_integer(value) and value > 0, do: :ok
  defp validate_max_concurrency(_), do: {:error, :invalid_max_concurrency}

  defp validate_timeout_ms(nil), do: :ok
  defp validate_timeout_ms(value) when is_integer(value) and value > 0, do: :ok
  defp validate_timeout_ms(_), do: {:error, :invalid_timeout_ms}

  defp resolve_max_concurrency(opts) do
    Keyword.get(opts, :max_concurrency, Application.get_env(:favn, :runtime_max_concurrency, 1))
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
