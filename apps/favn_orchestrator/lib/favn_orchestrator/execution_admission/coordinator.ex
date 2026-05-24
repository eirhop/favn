defmodule FavnOrchestrator.ExecutionAdmission.Coordinator do
  @moduledoc """
  BEAM-local wakeup delivery for persisted execution admission waiters.

  The coordinator sends hints only. Storage-backed execution leases remain the
  authority for whether runner work may start.
  """

  use GenServer

  alias FavnOrchestrator.ExecutionAdmission.Waiter
  alias FavnOrchestrator.Storage
  alias FavnOrchestrator.Storage.ExecutionLeaseCodec

  @name __MODULE__
  @candidate_limit 100

  @type state :: %{
          subscribers: %{optional(String.t()) => map()},
          monitors: %{optional(reference()) => MapSet.t(String.t())}
        }

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []),
    do: GenServer.start_link(__MODULE__, %{}, Keyword.put_new(opts, :name, @name))

  @spec register(Waiter.t(), pid()) :: :ok
  def register(%Waiter{} = waiter, owner \\ self()) when is_pid(owner) do
    call_if_running({:register, waiter, owner}, :ok)
  end

  @spec cancel(String.t()) :: :ok
  def cancel(waiter_id) when is_binary(waiter_id), do: call_if_running({:cancel, waiter_id}, :ok)

  @spec notify_scopes([map()]) :: :ok
  def notify_scopes(scopes) when is_list(scopes), do: cast_if_running({:notify_scopes, scopes})

  @impl true
  def init(_args), do: {:ok, %{subscribers: %{}, monitors: %{}}}

  @impl true
  def handle_call({:register, %Waiter{} = waiter, owner}, _from, state) do
    {monitor_ref, state} = monitor_owner(state, waiter.waiter_id, owner)

    subscriber = %{
      owner: owner,
      monitor_ref: monitor_ref,
      wake_generation: waiter.wake_generation,
      blocked_scope: waiter.blocked_scope
    }

    {:reply, :ok,
     %{state | subscribers: Map.put(state.subscribers, waiter.waiter_id, subscriber)}}
  end

  def handle_call({:cancel, waiter_id}, _from, state) do
    {:reply, :ok, remove_subscriber(state, waiter_id)}
  end

  @impl true
  def handle_cast({:notify_scopes, scopes}, state) do
    scopes = unique_scopes(scopes)

    with [_ | _] <- scopes,
         {:ok, _expired} <- Storage.expire_execution_admission_waiters(DateTime.utc_now()) do
      Enum.each(scopes, &wake_scope(&1, state))
    end

    {:noreply, state}
  end

  @impl true
  def handle_info({:DOWN, monitor_ref, :process, _pid, _reason}, state) do
    waiter_ids = Map.get(state.monitors, monitor_ref, MapSet.new())

    state =
      Enum.reduce(waiter_ids, state, fn waiter_id, acc ->
        :ok = Storage.delete_execution_admission_waiter(waiter_id)
        %{acc | subscribers: Map.delete(acc.subscribers, waiter_id)}
      end)

    {:noreply, %{state | monitors: Map.delete(state.monitors, monitor_ref)}}
  end

  defp monitor_owner(state, waiter_id, owner) do
    state = remove_subscriber(state, waiter_id)
    monitor_ref = Process.monitor(owner)

    monitors =
      Map.update(state.monitors, monitor_ref, MapSet.new([waiter_id]), &MapSet.put(&1, waiter_id))

    {monitor_ref, %{state | monitors: monitors}}
  end

  defp remove_subscriber(state, waiter_id) do
    case Map.pop(state.subscribers, waiter_id) do
      {nil, subscribers} ->
        %{state | subscribers: subscribers}

      {%{monitor_ref: monitor_ref}, subscribers} ->
        Process.demonitor(monitor_ref, [:flush])

        waiter_ids =
          state.monitors |> Map.get(monitor_ref, MapSet.new()) |> MapSet.delete(waiter_id)

        monitors =
          if MapSet.size(waiter_ids) == 0 do
            Map.delete(state.monitors, monitor_ref)
          else
            Map.put(state.monitors, monitor_ref, waiter_ids)
          end

        %{state | subscribers: subscribers, monitors: monitors}
    end
  end

  defp wake_scope(scope, state) do
    with {:ok, waiters} <-
           Storage.list_execution_admission_waiters_for_scope(scope, limit: @candidate_limit),
         %Waiter{} = waiter <- first_registered_waiter(waiters, state.subscribers) do
      subscriber = Map.fetch!(state.subscribers, waiter.waiter_id)

      send(
        subscriber.owner,
        {:execution_admission_wakeup, waiter.waiter_id, waiter.wake_generation}
      )
    else
      _other -> :ok
    end
  end

  defp unique_scopes(scopes) do
    scopes
    |> Enum.reduce(%{}, fn scope, acc ->
      case ExecutionLeaseCodec.normalize_scope(scope) do
        {:ok, normalized} ->
          Map.put_new(acc, ExecutionLeaseCodec.scope_identity(normalized), normalized)

        {:error, _reason} ->
          acc
      end
    end)
    |> Map.values()
  end

  defp first_registered_waiter(waiters, subscribers) do
    Enum.find(waiters, &Map.has_key?(subscribers, &1.waiter_id))
  end

  defp call_if_running(message, fallback) do
    case Process.whereis(@name) do
      nil -> fallback
      _pid -> GenServer.call(@name, message)
    end
  end

  defp cast_if_running(message) do
    case Process.whereis(@name) do
      nil -> :ok
      _pid -> GenServer.cast(@name, message)
    end
  end
end
