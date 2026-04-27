defmodule Favn.SQL.Admission.Limiter do
  @moduledoc false

  use GenServer

  @spec acquire(term(), pos_integer()) :: :ok
  def acquire(scope, limit) when is_integer(limit) and limit > 0 do
    ensure_started()
    GenServer.call(__MODULE__, {:acquire, scope, limit}, :infinity)
  end

  @spec release(term()) :: :ok
  def release(scope) do
    if Process.whereis(__MODULE__) do
      GenServer.cast(__MODULE__, {:release, scope})
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
  def init(_opts), do: {:ok, %{limits: %{}, active: %{}, queues: %{}}}

  @impl true
  def handle_call({:acquire, scope, limit}, from, state) do
    state = put_in(state, [:limits, scope], limit)

    if available?(state, scope) do
      {:reply, :ok, increment_active(state, scope)}
    else
      {:noreply, enqueue(state, scope, from)}
    end
  end

  def handle_call(:reset, _from, _state), do: {:reply, :ok, %{limits: %{}, active: %{}, queues: %{}}}

  @impl true
  def handle_cast({:release, scope}, state) do
    {:noreply, state |> decrement_active(scope) |> drain(scope)}
  end

  defp available?(state, scope) do
    Map.get(state.active, scope, 0) < Map.fetch!(state.limits, scope)
  end

  defp increment_active(state, scope) do
    update_in(state, [:active, scope], fn active -> (active || 0) + 1 end)
  end

  defp decrement_active(state, scope) do
    update_in(state, [:active, scope], fn active -> max((active || 0) - 1, 0) end)
  end

  defp enqueue(state, scope, from) do
    update_in(state, [:queues, scope], fn queue -> :queue.in(from, queue || :queue.new()) end)
  end

  defp drain(state, scope) do
    queue = Map.get(state.queues, scope, :queue.new())

    cond do
      :queue.is_empty(queue) ->
        state

      available?(state, scope) ->
        {{:value, from}, next_queue} = :queue.out(queue)
        GenServer.reply(from, :ok)

        state
        |> put_in([:queues, scope], next_queue)
        |> increment_active(scope)
        |> drain(scope)

      true ->
        state
    end
  end

  defp ensure_started do
    case GenServer.start_link(__MODULE__, %{}, name: __MODULE__) do
      {:ok, _pid} -> :ok
      {:error, {:already_started, _pid}} -> :ok
    end
  end
end
