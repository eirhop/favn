defmodule FavnOrchestrator.ExecutionAdmission.Coordinator do
  @moduledoc """
  BEAM-local wakeup delivery for persisted execution admission waiters.

  The coordinator sends hints only. Storage-backed execution leases remain the
  authority for whether runner work may start.
  """

  use GenServer

  require Logger

  alias FavnOrchestrator.ExecutionAdmission.Waiter
  alias FavnOrchestrator.Persistence
  alias FavnOrchestrator.Persistence.Commands.ExpireAdmission
  alias FavnOrchestrator.Persistence.SystemContext
  alias FavnOrchestrator.Redaction
  alias FavnOrchestrator.Storage.ExecutionLeaseCodec

  @name __MODULE__
  @call_timeout_ms 5_000
  @poll_interval_ms 30_000

  @type state :: %{
          subscribers: %{optional(String.t()) => map()},
          monitors: %{optional(reference()) => MapSet.t(String.t())}
        }

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []),
    do: GenServer.start_link(__MODULE__, %{}, Keyword.put_new(opts, :name, @name))

  @spec register(Waiter.t(), pid()) :: :ok | {:error, term()}
  def register(%Waiter{} = waiter, owner \\ self()) when is_pid(owner) do
    required_call({:register, waiter, owner})
  end

  @spec cancel(String.t()) :: :ok
  def cancel(waiter_id) when is_binary(waiter_id), do: call_if_running({:cancel, waiter_id}, :ok)

  @spec notify_scopes([map()]) :: :ok
  def notify_scopes(scopes) when is_list(scopes), do: cast_if_running({:notify_scopes, scopes})

  @doc false
  @spec storage_changed() :: :ok
  def storage_changed, do: cast_if_running(:storage_changed)

  @impl true
  def init(_args) do
    schedule_poll(0)
    {:ok, %{subscribers: %{}, monitors: %{}}}
  end

  @impl true
  def handle_call({:register, %Waiter{} = waiter, owner}, _from, state) do
    {monitor_ref, state} = monitor_owner(state, waiter.waiter_id, owner)

    subscriber = %{
      workspace_id: waiter.workspace_id,
      owner: owner,
      monitor_ref: monitor_ref,
      wake_generation: waiter.wake_generation,
      blocked_scope: waiter.blocked_scope,
      inserted_at: waiter.inserted_at,
      deadline_at: waiter.deadline_at
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

    if scopes != [] do
      maybe_expire_waiters()
      Enum.each(scopes, &wake_scope(&1, state))
    end

    {:noreply, state}
  end

  def handle_cast(:storage_changed, state) do
    wake_all(state)
    {:noreply, state}
  end

  @impl true
  def handle_info({:DOWN, monitor_ref, :process, _pid, _reason}, state) do
    waiter_ids = Map.get(state.monitors, monitor_ref, MapSet.new())

    state =
      Enum.reduce(waiter_ids, state, fn waiter_id, acc ->
        %{acc | subscribers: Map.delete(acc.subscribers, waiter_id)}
      end)

    {:noreply, %{state | monitors: Map.delete(state.monitors, monitor_ref)}}
  end

  def handle_info(:poll_persisted_waiters, state) do
    now = DateTime.utc_now()
    maybe_expire_waiters()

    Enum.each(state.subscribers, fn {waiter_id, subscriber} ->
      if waiter_live?(subscriber, now) do
        send(
          subscriber.owner,
          {:execution_admission_wakeup, waiter_id, subscriber.wake_generation}
        )
      end
    end)

    schedule_poll()
    {:noreply, state}
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
    case first_matching_subscriber(scope, state.subscribers, DateTime.utc_now()) do
      {waiter_id, subscriber} ->
        send(
          subscriber.owner,
          {:execution_admission_wakeup, waiter_id, subscriber.wake_generation}
        )

      nil ->
        :ok
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

  defp first_matching_subscriber(scope, subscribers, now) do
    identity = ExecutionLeaseCodec.scope_identity(scope)

    subscribers
    |> Enum.filter(fn {_waiter_id, subscriber} ->
      ExecutionLeaseCodec.scope_identity(subscriber.blocked_scope) == identity and
        waiter_live?(subscriber, now)
    end)
    |> Enum.min_by(
      fn {waiter_id, subscriber} -> {subscriber.inserted_at, waiter_id} end,
      fn -> nil end
    )
  end

  defp waiter_live?(%{owner: owner, deadline_at: deadline}, now) do
    Process.alive?(owner) and (is_nil(deadline) or DateTime.compare(deadline, now) == :gt)
  end

  defp maybe_expire_waiters do
    Enum.each(workspace_ids(), &expire_workspace/1)
  end

  defp expire_workspace(workspace_id) do
    context = SystemContext.workspace(workspace_id, :admission_expiry)

    command = %ExpireAdmission{
      workspace_context: context,
      batch_id:
        "admission-expire:#{System.unique_integer([:positive, :monotonic])}:#{workspace_hash(workspace_id)}",
      limit: 500
    }

    case Persistence.stores().admission.expire(command) do
      {:ok, _release} -> :ok
      {:error, reason} -> log_expiry_failure(reason)
    end
  end

  defp log_expiry_failure(reason) do
    Logger.warning("execution admission expiry failed reason=#{safe_diagnostic(reason)}")
  end

  defp wake_all(state) do
    Enum.each(state.subscribers, fn {waiter_id, subscriber} ->
      if waiter_live?(subscriber, DateTime.utc_now()) do
        send(
          subscriber.owner,
          {:execution_admission_wakeup, waiter_id, subscriber.wake_generation}
        )
      end
    end)
  end

  defp workspace_ids do
    :favn_orchestrator
    |> Application.get_env(:workspace_ids, [])
    |> Enum.filter(&(is_binary(&1) and &1 != ""))
    |> Enum.uniq()
  end

  defp workspace_hash(workspace_id) do
    :crypto.hash(:sha256, workspace_id)
    |> Base.url_encode64(padding: false)
    |> binary_part(0, 16)
  end

  defp call_if_running(message, fallback) do
    case Process.whereis(@name) do
      nil -> fallback
      _pid -> GenServer.call(@name, message, @call_timeout_ms)
    end
  catch
    :exit, _reason -> fallback
  end

  defp required_call(message) do
    case call_if_running(message, {:error, :execution_admission_coordinator_not_running}) do
      :ok -> :ok
      {:error, _reason} = error -> error
    end
  end

  defp cast_if_running(message) do
    case Process.whereis(@name) do
      nil -> :ok
      _pid -> GenServer.cast(@name, message)
    end
  end

  defp safe_diagnostic(reason) do
    reason
    |> Redaction.redact_operational_bounded()
    |> inspect(limit: 20, printable_limit: 2_000)
  end

  defp schedule_poll(delay \\ @poll_interval_ms),
    do: Process.send_after(self(), :poll_persisted_waiters, delay)
end
