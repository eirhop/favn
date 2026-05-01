defmodule Favn.SQL.Admission.Limiter do
  @moduledoc false

  use GenServer

  @initial_state %{limits: %{}, holders: %{}, queues: %{}, monitors: %{}}

  @type scope :: term()

  @spec acquire(scope(), pos_integer()) :: :ok | {:error, :admission_timeout}
  @spec acquire(scope(), pos_integer(), pos_integer() | :infinity) ::
          :ok | {:error, :admission_timeout}
  def acquire(scope, limit, timeout_ms \\ :infinity)

  def acquire(scope, limit, timeout_ms)
      when is_integer(limit) and limit > 0 and
             (timeout_ms == :infinity or (is_integer(timeout_ms) and timeout_ms > 0)) do
    ensure_started()
    GenServer.call(__MODULE__, {:acquire, scope, limit, timeout_ms}, :infinity)
  end

  @spec release(scope()) :: :ok
  def release(scope) do
    if Process.whereis(__MODULE__) do
      GenServer.cast(__MODULE__, {:release, scope, self()})
    end

    :ok
  end

  @spec reset() :: :ok
  def reset do
    ensure_started()
    GenServer.call(__MODULE__, :reset)
  end

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, %{}, Keyword.put_new(opts, :name, __MODULE__))
  end

  @impl true
  def init(_opts) do
    {:ok, initial_state()}
  end

  @impl true
  def handle_call({:acquire, scope, limit, timeout_ms}, {pid, _tag} = from, state) do
    state = put_in(state, [:limits, scope], limit)

    if available?(state, scope) do
      {:reply, :ok, add_holder(state, scope, pid)}
    else
      {:noreply, enqueue_waiter(state, scope, from, timeout_ms)}
    end
  end

  def handle_call(:reset, _from, state) do
    Enum.each(Map.keys(state.monitors), &Process.demonitor(&1, [:flush]))
    {:reply, :ok, initial_state()}
  end

  @impl true
  def handle_cast({:release, scope, pid}, state) do
    {:noreply, state |> remove_holder(scope, pid) |> drain(scope)}
  end

  @impl true
  def handle_info({:DOWN, monitor_ref, :process, _pid, _reason}, state) do
    case Map.pop(state.monitors, monitor_ref) do
      {nil, monitors} ->
        {:noreply, %{state | monitors: monitors}}

      {{:holder, scope, pid}, monitors} ->
        {:noreply,
         %{state | monitors: monitors}
         |> remove_holder(scope, pid, monitor_ref)
         |> drain(scope)}

      {{:waiter, scope, _from}, monitors} ->
        {:noreply, %{state | monitors: monitors} |> remove_waiter(scope, monitor_ref)}
    end
  end

  def handle_info({:admission_timeout, monitor_ref}, state) do
    case Map.pop(state.monitors, monitor_ref) do
      {nil, monitors} ->
        {:noreply, %{state | monitors: monitors}}

      {{:waiter, scope, from}, monitors} ->
        Process.demonitor(monitor_ref, [:flush])
        GenServer.reply(from, {:error, :admission_timeout})

        {:noreply,
         %{state | monitors: monitors}
         |> remove_waiter(scope, monitor_ref)}

      {{:holder, scope, pid}, monitors} ->
        {:noreply, %{state | monitors: Map.put(monitors, monitor_ref, {:holder, scope, pid})}}
    end
  end

  defp initial_state, do: @initial_state

  defp available?(state, scope) do
    active_count(state, scope) < Map.fetch!(state.limits, scope)
  end

  defp active_count(state, scope), do: state.holders |> Map.get(scope, []) |> length()

  defp add_holder(state, scope, pid) do
    monitor_ref = Process.monitor(pid)
    holder = %{pid: pid, monitor_ref: monitor_ref}

    state
    |> update_in([:holders, scope], fn holders -> [holder | holders || []] end)
    |> put_in([:monitors, monitor_ref], {:holder, scope, pid})
  end

  defp remove_holder(state, scope, pid) do
    case pop_first_holder(state.holders |> Map.get(scope, []), &(&1.pid == pid)) do
      {nil, _holders} ->
        state

      {%{monitor_ref: monitor_ref}, holders} ->
        Process.demonitor(monitor_ref, [:flush])

        state
        |> put_scope_holders(scope, holders)
        |> update_in([:monitors], &Map.delete(&1, monitor_ref))
    end
  end

  defp remove_holder(state, scope, pid, monitor_ref) do
    holders =
      state.holders
      |> Map.get(scope, [])
      |> Enum.reject(&(&1.pid == pid and &1.monitor_ref == monitor_ref))

    put_scope_holders(state, scope, holders)
  end

  defp pop_first_holder([], _predicate), do: {nil, []}

  defp pop_first_holder([holder | rest], predicate) do
    if predicate.(holder) do
      {holder, rest}
    else
      {found, holders} = pop_first_holder(rest, predicate)
      {found, [holder | holders]}
    end
  end

  defp put_scope_holders(state, scope, []),
    do: %{state | holders: Map.delete(state.holders, scope)}

  defp put_scope_holders(state, scope, holders), do: put_in(state, [:holders, scope], holders)

  defp enqueue_waiter(state, scope, {pid, _tag} = from, timeout_ms) do
    monitor_ref = Process.monitor(pid)
    timer_ref = schedule_timeout(monitor_ref, timeout_ms)
    waiter = %{from: from, pid: pid, monitor_ref: monitor_ref, timer_ref: timer_ref}

    state
    |> update_in([:queues, scope], fn queue -> :queue.in(waiter, queue || :queue.new()) end)
    |> put_in([:monitors, monitor_ref], {:waiter, scope, from})
  end

  defp schedule_timeout(_monitor_ref, :infinity), do: nil

  defp schedule_timeout(monitor_ref, timeout_ms) do
    Process.send_after(self(), {:admission_timeout, monitor_ref}, timeout_ms)
  end

  defp remove_waiter(state, scope, monitor_ref) do
    queue = Map.get(state.queues, scope, :queue.new())

    {waiter, waiters} = pop_first_waiter(:queue.to_list(queue), monitor_ref)
    cancel_timeout(waiter)
    next_queue = :queue.from_list(waiters)

    put_scope_queue(state, scope, next_queue)
  end

  defp pop_first_waiter([], _monitor_ref), do: {nil, []}

  defp pop_first_waiter([waiter | rest], monitor_ref) do
    if waiter.monitor_ref == monitor_ref do
      {waiter, rest}
    else
      {found, waiters} = pop_first_waiter(rest, monitor_ref)
      {found, [waiter | waiters]}
    end
  end

  defp cancel_timeout(%{timer_ref: timer_ref}) when is_reference(timer_ref) do
    Process.cancel_timer(timer_ref)
    :ok
  end

  defp cancel_timeout(_waiter), do: :ok

  defp drain(state, scope) do
    queue = Map.get(state.queues, scope, :queue.new())

    cond do
      :queue.is_empty(queue) ->
        state

      available?(state, scope) ->
        {{:value, waiter}, next_queue} = :queue.out(queue)
        Process.demonitor(waiter.monitor_ref, [:flush])
        cancel_timeout(waiter)
        GenServer.reply(waiter.from, :ok)

        state
        |> put_scope_queue(scope, next_queue)
        |> update_in([:monitors], &Map.delete(&1, waiter.monitor_ref))
        |> add_holder(scope, waiter.pid)
        |> drain(scope)

      true ->
        state
    end
  end

  defp put_scope_queue(state, scope, queue) do
    if :queue.is_empty(queue) do
      %{state | queues: Map.delete(state.queues, scope)}
    else
      put_in(state, [:queues, scope], queue)
    end
  end

  defp ensure_started do
    if Process.whereis(__MODULE__) do
      :ok
    else
      case Application.ensure_all_started(:favn_sql_runtime) do
        {:ok, _apps} -> ensure_started_after_application_start()
        {:error, _reason} -> start_unlinked()
      end
    end
  end

  defp ensure_started_after_application_start do
    if Process.whereis(__MODULE__), do: :ok, else: start_unlinked()
  end

  defp start_unlinked do
    case GenServer.start(__MODULE__, %{}, name: __MODULE__) do
      {:ok, _pid} -> :ok
      {:error, {:already_started, _pid}} -> :ok
    end
  end
end
