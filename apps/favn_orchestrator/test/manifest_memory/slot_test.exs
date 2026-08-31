defmodule FavnOrchestrator.ManifestMemory.SlotTest do
  use ExUnit.Case, async: true

  alias FavnOrchestrator.ManifestMemory
  alias FavnOrchestrator.ManifestMemory.Slot

  setup do
    slot = start_supervised!({Slot, name: nil})
    %{slot: slot}
  end

  test "upload and activation phase combinations do not overlap", %{slot: slot} do
    for {first, second} <- [upload: :upload, upload: :activation, activation: :activation] do
      parent = self()

      holder =
        Task.async(fn ->
          ManifestMemory.with_phase(
            first,
            fn ->
              send(parent, {:held, first})
              receive do: (:release -> :ok)
            end,
            slot: slot,
            capacity_check: fn -> :ok end
          )
        end)

      assert_receive {:held, ^first}

      assert {:error, :manifest_capacity_busy} =
               ManifestMemory.with_phase(second, fn -> flunk("must not overlap") end,
                 slot: slot,
                 capacity_check: fn -> :ok end
               )

      send(holder.pid, :release)
      assert :ok = Task.await(holder)
    end
  end

  test "does not let another process release the owner lease", %{slot: slot} do
    assert {:ok, lease} = Slot.acquire(server: slot)
    task = Task.async(fn -> Slot.release(lease, server: slot) end)
    assert :ok = Task.await(task)
    assert {:error, :manifest_capacity_busy} = Slot.acquire(server: slot)
    assert :ok = Slot.release(lease, server: slot)
  end

  test "does not reopen until every tracked worker has terminated", %{slot: slot} do
    assert {:ok, lease} = Slot.acquire(server: slot)
    worker = spawn(fn -> Process.sleep(:infinity) end)
    assert :ok = Slot.track_worker(lease, worker, server: slot)
    assert :ok = Slot.release(lease, server: slot)
    assert {:error, :manifest_capacity_busy} = Slot.acquire(server: slot)

    Process.exit(worker, :kill)
    assert_eventually(fn -> match?({:ok, _lease}, Slot.acquire(server: slot)) end)
  end

  test "a guard restart terminates its linked workers", %{slot: slot} do
    assert {:ok, lease} = Slot.acquire(server: slot)
    worker = spawn(fn -> Process.sleep(:infinity) end)
    monitor = Process.monitor(worker)
    assert :ok = Slot.track_worker(lease, worker, server: slot)

    Process.exit(slot, :kill)
    assert_receive {:DOWN, ^monitor, :process, ^worker, _reason}
  end

  test "releases the slot when its owner terminates", %{slot: slot} do
    owner =
      spawn(fn ->
        {:ok, _lease} = Slot.acquire(server: slot)
        send(self(), :unused)
      end)

    monitor = Process.monitor(owner)
    assert_receive {:DOWN, ^monitor, :process, ^owner, :normal}
    assert_eventually(fn -> match?({:ok, _lease}, Slot.acquire(server: slot)) end)
  end

  defp assert_eventually(fun, attempts \\ 20)
  defp assert_eventually(_fun, 0), do: flunk("condition did not become true")

  defp assert_eventually(fun, attempts) do
    if fun.() do
      :ok
    else
      Process.sleep(5)
      assert_eventually(fun, attempts - 1)
    end
  end
end
