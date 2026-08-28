defmodule FavnOrchestrator.MemoryCapacity.Coordinator do
  @moduledoc false

  use GenServer

  alias FavnOrchestrator.MemoryCapacity.Error
  alias FavnOrchestrator.MemoryCapacity.Ledger
  alias FavnOrchestrator.MemoryCapacity.Provider

  @absolute_reserve_bytes 128 * 1_024 * 1_024

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: Keyword.get(opts, :name, __MODULE__))
  end

  @impl true
  def init(opts) do
    provider = Keyword.get(opts, :provider, Provider)
    provider_opts = Keyword.get(opts, :provider_opts, [])
    ledger = Keyword.get(opts, :ledger, Ledger)

    case checkout_ledger(ledger) do
      {:ok, table} ->
        state = %{
          table: table,
          ledger: ledger,
          provider: provider,
          provider_opts: provider_opts,
          monitors: %{},
          closed: nil
        }

        {:ok, reconstruct(state)}

      {:error, reason} ->
        {:ok,
         %{
           table: nil,
           ledger: ledger,
           provider: provider,
           provider_opts: provider_opts,
           monitors: %{},
           closed: reason
         }}
    end
  end

  @impl true
  def handle_call({:acquire, owner, bytes, opts}, _from, state) do
    exclusive? = Keyword.get(opts, :exclusive, false)
    kind = Keyword.get(opts, :kind, :manifest_use)

    with :ok <- ensure_open(state, bytes),
         :ok <- ensure_exclusive(state, exclusive?, bytes),
         {:ok, _snapshot, available} <- capacity(state, bytes, nil),
         true <- bytes <= available do
      id = make_ref()
      monitor = Process.monitor(owner)

      entry = %{
        id: id,
        owner: owner,
        kind: kind,
        exclusive?: exclusive?,
        working_bytes: bytes,
        retained_bytes: 0
      }

      true = :ets.insert(state.table, {id, entry})
      {:reply, {:ok, id}, put_in(state.monitors[monitor], id)}
    else
      false -> {:reply, {:error, unavailable_error(state, bytes)}, state}
      {:error, %Error{} = error} -> {:reply, {:error, error}, state}
    end
  end

  def handle_call({:resize, id, bytes}, _from, state) do
    case lookup(state, id) do
      {:ok, entry} ->
        current = entry.working_bytes + entry.retained_bytes
        required = bytes + entry.retained_bytes

        if required <= current do
          true = :ets.insert(state.table, {id, %{entry | working_bytes: bytes}})
          {:reply, :ok, state}
        else
          with {:ok, _snapshot, available} <- capacity(state, required, id),
               true <- required <= available do
            true = :ets.insert(state.table, {id, %{entry | working_bytes: bytes}})
            {:reply, :ok, state}
          else
            false ->
              {:reply, {:error, unavailable_error(state, required, id)}, state}

            {:error, %Error{} = error} ->
              {:reply, {:error, error}, state}
          end
        end

      :error ->
        {:reply, {:error, unknown_error(bytes)}, state}
    end
  end

  def handle_call({:grow, id, bytes}, _from, state) do
    case lookup(state, id) do
      {:ok, entry} when entry.working_bytes >= bytes ->
        {:reply, :ok, state}

      {:ok, entry} ->
        required = bytes + entry.retained_bytes

        with {:ok, _snapshot, available} <- capacity(state, required, id),
             true <- required <= available do
          true = :ets.insert(state.table, {id, %{entry | working_bytes: bytes}})
          {:reply, :ok, state}
        else
          false -> {:reply, {:error, unavailable_error(state, required, id)}, state}
          {:error, %Error{} = error} -> {:reply, {:error, error}, state}
        end

      :error ->
        {:reply, {:error, unknown_error(bytes)}, state}
    end
  end

  def handle_call({:retain, id, retained_bytes}, _from, state) do
    case lookup(state, id) do
      {:ok, entry} ->
        current = entry.working_bytes + entry.retained_bytes
        required = entry.working_bytes + retained_bytes

        if required <= current do
          true = :ets.insert(state.table, {id, %{entry | retained_bytes: retained_bytes}})
          {:reply, :ok, state}
        else
          with {:ok, _snapshot, available} <- capacity(state, required, id),
               true <- required <= available do
            true = :ets.insert(state.table, {id, %{entry | retained_bytes: retained_bytes}})
            {:reply, :ok, state}
          else
            false -> {:reply, {:error, unavailable_error(state, required, id)}, state}
            {:error, %Error{} = error} -> {:reply, {:error, error}, state}
          end
        end

      :error ->
        {:reply, {:error, unknown_error(retained_bytes)}, state}
    end
  end

  def handle_call({:transfer, id, retained_bytes, working_bytes}, _from, state) do
    case lookup(state, id) do
      {:ok, entry} ->
        current = total_bytes(entry)
        required = retained_bytes + working_bytes
        replacement = %{entry | retained_bytes: retained_bytes, working_bytes: working_bytes}

        if required <= current do
          true = :ets.insert(state.table, {id, replacement})
          {:reply, :ok, state}
        else
          with {:ok, _snapshot, available} <- capacity(state, required, id),
               true <- required <= available do
            true = :ets.insert(state.table, {id, replacement})
            {:reply, :ok, state}
          else
            false -> {:reply, {:error, unavailable_error(state, required, id)}, state}
            {:error, %Error{} = error} -> {:reply, {:error, error}, state}
          end
        end

      :error ->
        {:reply, {:error, unknown_error(retained_bytes + working_bytes)}, state}
    end
  end

  def handle_call({:handoff, id, owner}, _from, state) do
    case lookup(state, id) do
      {:ok, entry} ->
        monitor = Process.monitor(owner)
        old_monitor = monitor_for(state.monitors, id)
        true = :ets.insert(state.table, {id, %{entry | owner: owner}})
        if old_monitor, do: Process.demonitor(old_monitor, [:flush])

        monitors =
          state.monitors
          |> delete_monitor(old_monitor)
          |> Map.put(monitor, id)

        {:reply, :ok, %{state | monitors: monitors}}

      :error ->
        {:reply, {:error, unknown_error(0)}, state}
    end
  end

  def handle_call({:release, id}, _from, state) do
    {:reply, :ok, release_id(state, id)}
  end

  def handle_call(:diagnostics, _from, state) do
    entries = entries(state)
    reserved = Enum.sum(Enum.map(entries, &total_bytes/1))

    result =
      case diagnostic_snapshot(state) do
        {:ok, snap} ->
          safety = safety_reserve(snap.limit_bytes)

          %{
            status: :open,
            source: snap.source,
            limit_bytes: snap.limit_bytes,
            usage_bytes: snap.usage_bytes,
            headroom_bytes: snap.headroom_bytes,
            safety_reserve_bytes: safety,
            reserved_bytes: reserved,
            available_bytes: max(snap.headroom_bytes - safety - reserved, 0),
            active_leases: length(entries),
            by_kind: by_kind(entries)
          }

        {:error, reason} ->
          %{
            status: :closed,
            reason: reason,
            reserved_bytes: reserved,
            active_leases: length(entries),
            by_kind: by_kind(entries)
          }
      end

    {:reply, result, state}
  end

  @impl true
  def handle_info({:"ETS-TRANSFER", table, _from, :ledger_checkout}, state) do
    true = :ets.setopts(table, {:heir, resolve_process(state.ledger), :coordinator_crash})
    {:noreply, %{state | table: table}}
  end

  def handle_info({:DOWN, monitor, :process, _pid, _reason}, state) do
    case Map.pop(state.monitors, monitor) do
      {nil, monitors} ->
        {:noreply, %{state | monitors: monitors}}

      {id, monitors} ->
        if state.table, do: :ets.delete(state.table, id)
        {:noreply, %{state | monitors: monitors}}
    end
  end

  defp reconstruct(%{table: table} = state) do
    Enum.reduce(entries(state), %{state | monitors: %{}, closed: nil}, fn entry, acc ->
      if Process.alive?(entry.owner) do
        monitor = Process.monitor(entry.owner)
        %{acc | monitors: Map.put(acc.monitors, monitor, entry.id)}
      else
        :ets.delete(table, entry.id)
        acc
      end
    end)
  rescue
    ArgumentError -> %{state | table: nil, closed: :lease_ledger_reconstruction_failed}
  end

  defp ensure_open(%{table: nil}, bytes), do: {:error, unknown_error(bytes)}

  defp ensure_open(%{closed: reason}, bytes) when not is_nil(reason),
    do: {:error, unknown_error(bytes)}

  defp ensure_open(_state, _bytes), do: :ok

  defp ensure_exclusive(state, true, bytes) do
    if Enum.any?(entries(state), & &1.exclusive?) do
      {:error, %Error{code: :manifest_capacity_busy, required_bytes: bytes}}
    else
      :ok
    end
  end

  defp ensure_exclusive(_state, false, _bytes), do: :ok

  defp capacity(state, required, excluded_id) do
    case snapshot(state) do
      {:ok, snap} ->
        reserved =
          state
          |> entries()
          |> Enum.reject(&(&1.id == excluded_id))
          |> Enum.map(&total_bytes/1)
          |> Enum.sum()

        available = max(snap.headroom_bytes - safety_reserve(snap.limit_bytes) - reserved, 0)
        {:ok, snap, available}

      {:error, _reason} ->
        {:error, unknown_error(required)}
    end
  end

  defp snapshot(state), do: state.provider.snapshot(state.provider_opts)

  defp diagnostic_snapshot(%{table: nil, closed: reason}), do: {:error, reason}
  defp diagnostic_snapshot(%{closed: reason}) when not is_nil(reason), do: {:error, reason}
  defp diagnostic_snapshot(state), do: snapshot(state)

  defp unavailable_error(state, required, excluded_id \\ nil) do
    case snapshot(state) do
      {:ok, snap} ->
        reserved =
          state
          |> entries()
          |> Enum.reject(&(&1.id == excluded_id))
          |> Enum.map(&total_bytes/1)
          |> Enum.sum()

        available = max(snap.headroom_bytes - safety_reserve(snap.limit_bytes) - reserved, 0)

        %Error{
          code: :manifest_capacity_unavailable,
          required_bytes: required,
          available_bytes: available,
          limit_bytes: snap.limit_bytes,
          usage_bytes: snap.usage_bytes
        }

      {:error, _reason} ->
        unknown_error(required)
    end
  end

  defp unknown_error(required),
    do: %Error{code: :memory_capacity_unknown, required_bytes: required}

  defp safety_reserve(limit), do: max(@absolute_reserve_bytes, div(limit, 5))

  defp entries(%{table: nil}), do: []
  defp entries(state), do: :ets.tab2list(state.table) |> Enum.map(&elem(&1, 1))

  defp lookup(%{table: nil}, _id), do: :error

  defp lookup(state, id) do
    case :ets.lookup(state.table, id) do
      [{^id, entry}] -> {:ok, entry}
      [] -> :error
    end
  end

  defp release_id(state, id) do
    if state.table, do: :ets.delete(state.table, id)
    monitor = monitor_for(state.monitors, id)
    if monitor, do: Process.demonitor(monitor, [:flush])
    %{state | monitors: delete_monitor(state.monitors, monitor)}
  end

  defp monitor_for(monitors, id) do
    Enum.find_value(monitors, fn {monitor, monitored_id} -> if monitored_id == id, do: monitor end)
  end

  defp delete_monitor(monitors, nil), do: monitors
  defp delete_monitor(monitors, monitor), do: Map.delete(monitors, monitor)
  defp total_bytes(entry), do: entry.working_bytes + entry.retained_bytes

  defp by_kind(entries) do
    Enum.reduce(entries, %{}, fn entry, acc ->
      Map.update(acc, entry.kind, total_bytes(entry), &(&1 + total_bytes(entry)))
    end)
  end

  defp resolve_process(pid) when is_pid(pid), do: pid
  defp resolve_process(name), do: Process.whereis(name)

  defp checkout_ledger(ledger) do
    Ledger.checkout(self(), ledger)
  catch
    :exit, reason -> {:error, {:lease_ledger_unavailable, reason}}
  end
end
