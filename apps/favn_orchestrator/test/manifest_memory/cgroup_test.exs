defmodule FavnOrchestrator.ManifestMemory.CgroupTest do
  use ExUnit.Case, async: true

  alias FavnOrchestrator.ManifestMemory.Cgroup

  test "uses the smallest finite headroom across cgroup v2 ancestors" do
    files =
      proc_files("0::/team/app", v2_mount())
      |> Map.merge(%{
        "/sys/fs/cgroup/team/app/memory.max" => "800\n",
        "/sys/fs/cgroup/team/app/memory.current" => "100\n",
        "/sys/fs/cgroup/team/memory.max" => "1000\n",
        "/sys/fs/cgroup/team/memory.current" => "600\n",
        "/sys/fs/cgroup/memory.max" => "max\n"
      })

    assert {:ok,
            %{
              source: :cgroup_v2,
              limit_bytes: 1_000,
              usage_bytes: 600,
              headroom_bytes: 400
            }} = snapshot(files)
  end

  test "uses the tighter finite hierarchy when v1 and v2 are both visible" do
    files =
      proc_files("0::/v2/app\n5:memory:/v1/app", v2_mount() <> "\n" <> v1_mount())
      |> Map.merge(%{
        "/sys/fs/cgroup/v2/app/memory.max" => "900\n",
        "/sys/fs/cgroup/v2/app/memory.current" => "500\n",
        "/sys/fs/cgroup/v2/memory.max" => "max\n",
        "/sys/fs/cgroup/memory.max" => "max\n",
        "/sys/fs/cgroup/memory/app/memory.limit_in_bytes" => "800\n",
        "/sys/fs/cgroup/memory/app/memory.usage_in_bytes" => "500\n",
        "/sys/fs/cgroup/memory/memory.limit_in_bytes" => "9223372036854771712\n"
      })

    assert {:ok, %{source: :cgroup_v1, headroom_bytes: 300}} = snapshot(files)
  end

  test "fails closed for unlimited, malformed, or unreadable hierarchies" do
    unlimited =
      proc_files("0::/", v2_mount())
      |> Map.put("/sys/fs/cgroup/memory.max", "max\n")

    assert {:error, :memory_capacity_unknown} = snapshot(unlimited)

    assert {:error, :memory_capacity_unknown} =
             snapshot(proc_files("not-a-cgroup", v2_mount()))

    unreadable =
      proc_files("0::/app", v2_mount())
      |> Map.put("/sys/fs/cgroup/app/memory.max", "1024\n")

    assert {:error, :memory_capacity_unknown} = snapshot(unreadable)
  end

  defp snapshot(files) do
    Cgroup.snapshot(
      proc_cgroup_path: "/proc/cgroup",
      mountinfo_path: "/proc/mountinfo",
      read_file: fn path ->
        case Map.fetch(files, path) do
          {:ok, contents} -> {:ok, contents}
          :error -> {:error, :enoent}
        end
      end
    )
  end

  defp proc_files(cgroup, mountinfo) do
    %{
      "/proc/cgroup" => cgroup <> "\n",
      "/proc/mountinfo" => mountinfo <> "\n"
    }
  end

  defp v2_mount,
    do: "36 25 0:32 / /sys/fs/cgroup rw,nosuid,nodev - cgroup2 cgroup rw"

  defp v1_mount,
    do: "37 25 0:33 /v1 /sys/fs/cgroup/memory rw,nosuid,nodev - cgroup cgroup rw,memory"
end
