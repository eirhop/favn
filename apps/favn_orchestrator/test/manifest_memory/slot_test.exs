defmodule FavnOrchestrator.ManifestMemory.SlotTest do
  use ExUnit.Case, async: true

  alias FavnOrchestrator.ManifestMemory.Slot

  setup do
    slot = start_supervised!({Slot, name: nil})
    %{slot: slot}
  end

  test "admits only one manifest phase without waiting", %{slot: slot} do
    assert {:ok, lease} = Slot.acquire(server: slot)
    assert {:error, :manifest_capacity_busy} = Slot.acquire(server: slot)
    assert :ok = Slot.release(lease, server: slot)
    assert {:ok, next_lease} = Slot.acquire(server: slot)
    assert :ok = Slot.release(next_lease, server: slot)
  end

  test "does not let another process release the owner lease", %{slot: slot} do
    assert {:ok, lease} = Slot.acquire(server: slot)
    task = Task.async(fn -> Slot.release(lease, server: slot) end)
    assert :ok = Task.await(task)
    assert {:error, :manifest_capacity_busy} = Slot.acquire(server: slot)
    assert :ok = Slot.release(lease, server: slot)
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
