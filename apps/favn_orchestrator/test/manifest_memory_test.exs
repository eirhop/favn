defmodule FavnOrchestrator.ManifestMemoryTest do
  use ExUnit.Case, async: true

  alias FavnOrchestrator.ManifestMemory
  alias FavnOrchestrator.ManifestMemory.Slot

  test "adapts admission to current finite-cgroup headroom" do
    mib = 1024 * 1024

    assert :ok = ManifestMemory.ensure_headroom(cgroup_options(1024 * mib, 512 * mib))

    assert {:error, :manifest_capacity_unavailable} =
             ManifestMemory.ensure_headroom(cgroup_options(1024 * mib, 512 * mib + 1))
  end

  test "fails closed when capacity cannot be measured" do
    assert {:error, :memory_capacity_unknown} =
             ManifestMemory.ensure_headroom(read_file: fn _path -> {:error, :enoent} end)
  end

  test "always releases the local phase slot" do
    slot = start_supervised!({Slot, name: nil})

    assert {:error, :memory_capacity_unknown} =
             ManifestMemory.with_phase(:upload, fn -> flunk("must not run") end,
               slot: slot,
               capacity_check: fn -> {:error, :memory_capacity_unknown} end
             )

    assert :complete =
             ManifestMemory.with_phase(:upload, fn -> :complete end,
               slot: slot,
               capacity_check: fn -> :ok end
             )
  end

  test "publishes the frozen worker and reserve bounds" do
    mib = 1024 * 1024
    required_headroom = 512 * mib
    package_heap = 96 * mib
    package_result = 64 * mib
    manifest_heap = 256 * mib
    manifest_result = 128 * mib

    assert %{
             required_headroom: ^required_headroom,
             package_heap: ^package_heap,
             package_result: ^package_result,
             package_batch_result: ^package_result,
             manifest_heap: ^manifest_heap,
             manifest_result: ^manifest_result,
             index_result: ^manifest_result,
             worker_timeout: 30_000
           } = ManifestMemory.bounds()
  end

  defp cgroup_options(limit, current) do
    files = %{
      "/proc/cgroup" => "0::/app\n",
      "/proc/mountinfo" =>
        "36 25 0:32 / /sys/fs/cgroup rw,nosuid,nodev - cgroup2 cgroup rw\n",
      "/sys/fs/cgroup/app/memory.max" => Integer.to_string(limit),
      "/sys/fs/cgroup/app/memory.current" => Integer.to_string(current),
      "/sys/fs/cgroup/memory.max" => "max"
    }

    [
      proc_cgroup_path: "/proc/cgroup",
      mountinfo_path: "/proc/mountinfo",
      read_file: fn path -> Map.fetch(files, path) end
    ]
  end
end
