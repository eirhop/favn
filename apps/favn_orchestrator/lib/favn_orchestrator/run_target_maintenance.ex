defmodule FavnOrchestrator.RunTargetMaintenance do
  @moduledoc false
  use GenServer
  alias FavnOrchestrator.{Persistence, RunHelper, RunLeaseKeeper}

  defstruct [
    :context,
    :ownership,
    :manager,
    :worker,
    :operation,
    :started,
    :timer,
    :initial_deadline,
    watches: %{},
    closed?: false,
    ready?: false
  ]

  def start_link(opts), do: GenServer.start_link(__MODULE__, opts, name: name(opts[:ownership]))

  def register(run, task_id) do
    GenServer.call(name(run), {:register, task_id}, 1_000)
  rescue
    ArgumentError -> {:error, :target_maintenance_unavailable}
  catch
    :exit, _ -> {:error, :target_maintenance_unavailable}
  end

  def permit(run, timeout), do: GenServer.call(name(run), :permit, timeout)

  def admission_result(run, task_id, {:ok, %{status: status}}) do
    GenServer.cast(name(run), {:acquisition_result, task_id, status})
  end

  def admission_result(_, _, _), do: :ok

  @impl true
  def handle_cast({:acquisition_result, task, :admitted}, state) do
    {:noreply, update_watch(state, task, &Map.put(&1, :acquisition_deadline, nil))}
  end

  def handle_cast({:acquisition_result, task, _}, state) do
    if expired?(state),
      do: check_deadlines(state),
      else: {:noreply, %{state | watches: Map.delete(state.watches, task)}}
  end

  defp name(authority),
    do:
      {:via, Registry,
       {FavnOrchestrator.RunLeaseRegistry, {:targets, RunLeaseKeeper.name(authority)}}}

  @impl true
  def init(opts) do
    send(self(), :renew)
    Process.send_after(self(), :check, 1_000)
    {:ok, struct!(__MODULE__, Map.put(Map.new(opts), :initial_deadline, now() + 5_000))}
  end

  @impl true
  def handle_call(:permit, _from, state) do
    allowed = state.ready? and not state.closed? and not expired?(state)
    {:reply, if(allowed, do: :ok, else: {:error, :target_maintenance_degraded}), state}
  end

  def handle_call({:register, task_id}, _from, state) do
    cond do
      state.closed? or expired?(state) ->
        {:reply, {:error, :target_maintenance_degraded}, state}

      Map.has_key?(state.watches, task_id) ->
        {:reply, {:ok, {self(), state.watches[task_id].reference}}, state}

      map_size(state.watches) >= 512 ->
        {:reply, {:error, :target_maintenance_capacity}, state}

      true ->
        reference = make_ref()

        watch = %{
          reference: reference,
          renewal_deadline: now() + 40_000,
          checkout_deadline: now() + 3_000,
          acquisition_deadline: now() + 20_000
        }

        Process.send_after(self(), :deadline_check, 3_000)
        Process.send_after(self(), :deadline_check, 20_000)

        {:reply, {:ok, {self(), reference}},
         %{state | watches: Map.put(state.watches, task_id, watch)}}
    end
  end

  @impl true
  def handle_info({:acquisition_checked_out, reference}, state) do
    watches =
      Map.new(state.watches, fn {task, watch} ->
        if watch.reference == reference and not watch_expired?(watch),
          do: {task, %{watch | checkout_deadline: nil}},
          else: {task, watch}
      end)

    {:noreply, %{state | watches: watches}}
  end

  def handle_info(:renew, %{worker: nil, closed?: false} = state) do
    started = now()

    operation =
      state.operation ||
        "target-renew:" <> Base.url_encode64(:crypto.strong_rand_bytes(18), padding: false)

    task =
      RunHelper.async(state.ownership, fn ->
        Persistence.stores().run_ownership.maintain_targets(
          state.context,
          state.ownership,
          Map.keys(state.watches),
          operation
        )
      end)

    timer = Process.send_after(self(), {:timeout, task.ref}, 5_000)
    {:noreply, %{state | worker: task, operation: operation, started: started, timer: timer}}
  end

  def handle_info({ref, {:ok, receipts}}, %{worker: %{ref: ref}} = state) do
    Process.demonitor(ref, [:flush])
    Process.cancel_timer(state.timer)

    if state.closed? or expired?(state) do
      send(state.manager, {:target_maintenance_lost, self(), state.ownership})
      {:noreply, %{state | worker: nil, closed?: true}}
    else
      watches =
        Enum.reduce(receipts, state.watches, fn
          {task, :terminal}, acc ->
            Map.delete(acc, task)

          {task, %{expires_at: expiry, database_observed_at: observed}}, acc ->
            watch = %{
              reference: nil,
              checkout_deadline: nil,
              acquisition_deadline: nil,
              renewal_deadline:
                state.started + max(DateTime.diff(expiry, observed, :millisecond), 0) - 20_000
            }

            Map.put(acc, task, watch)
        end)

      if not state.ready? and not state.closed?,
        do: send(state.manager, {:target_maintenance_ready, self()})

      Process.send_after(self(), :renew, max(state.started + 5_000 - now(), 0))
      {:noreply, %{state | watches: watches, worker: nil, operation: nil, ready?: true}}
    end
  end

  def handle_info(
        {ref, {:error, %{details: %{reason_code: "target_lease_lost"}}}},
        %{worker: %{ref: ref}} = state
      ) do
    Process.demonitor(ref, [:flush])
    Process.cancel_timer(state.timer)

    send(
      state.manager,
      {:target_maintenance_needs_attention, self(), :original_target_lease_lost}
    )

    {:noreply, %{state | worker: nil, closed?: true}}
  end

  def handle_info({ref, {:error, %{kind: :fenced}}}, %{worker: %{ref: ref}} = state) do
    Process.demonitor(ref, [:flush])
    Process.cancel_timer(state.timer)
    send(state.manager, {:target_maintenance_lost, self(), state.ownership})
    {:noreply, %{state | worker: nil, closed?: true}}
  end

  def handle_info({ref, _error}, %{worker: %{ref: ref}} = state) do
    Process.demonitor(ref, [:flush])
    Process.cancel_timer(state.timer)
    Process.send_after(self(), :renew, 500)
    {:noreply, %{state | worker: nil}}
  end

  def handle_info({:timeout, ref}, %{worker: %{ref: ref, pid: pid}} = state) do
    Process.exit(pid, :kill)
    {:noreply, state}
  end

  def handle_info({:DOWN, ref, :process, _, _}, %{worker: %{ref: ref}} = state) do
    Process.send_after(self(), :renew, 500)
    {:noreply, %{state | worker: nil}}
  end

  def handle_info(:check, state) do
    Process.send_after(self(), :check, 1_000)
    check_deadlines(state)
  end

  def handle_info(:deadline_check, state), do: check_deadlines(state)
  def handle_info(_, state), do: {:noreply, state}

  defp check_deadlines(state) do
    expired = expired?(state)

    if expired and not state.closed? do
      send(state.manager, {:target_maintenance_lost, self(), state.ownership})
    end

    {:noreply, %{state | closed?: state.closed? or expired}}
  end

  defp expired?(state),
    do:
      (not state.ready? and now() >= state.initial_deadline) or
        Enum.any?(state.watches, fn {_, watch} -> watch_expired?(watch) end)

  defp watch_expired?(watch),
    do:
      Enum.any?(
        [watch.renewal_deadline, watch.checkout_deadline, watch.acquisition_deadline],
        &(is_integer(&1) and now() >= &1)
      )

  defp update_watch(state, task, fun) do
    case state.watches[task] do
      nil ->
        state

      watch ->
        if watch_expired?(watch),
          do: state,
          else: %{state | watches: Map.put(state.watches, task, fun.(watch))}
    end
  end

  defp now, do: System.monotonic_time(:millisecond)
end
