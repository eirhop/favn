defmodule FavnOrchestrator.ManifestMemory.CgroupTest do
  use ExUnit.Case, async: true

  alias FavnOrchestrator.ManifestMemory.Cgroup
  alias FavnTestSupport.CgroupFiles

  test "uses mounted v1 memory when v2 membership has no visible mount" do
    assert {:ok,
            %{
              source: :cgroup_v1,
              limit_bytes: 1_073_741_824,
              usage_bytes: 314_572_800,
              headroom_bytes: 759_169_024
            }} = Cgroup.snapshot(CgroupFiles.options(CgroupFiles.v1_with_unmounted_v2()))
  end

  test "unmounted v2 does not bypass missing, unreadable, or malformed v1 memory" do
    for file <- ["memory.limit_in_bytes", "memory.usage_in_bytes"],
        value <- [{:error, :enoent}, {:error, :eacces}, "invalid", "-1"] do
      files =
        Map.put(CgroupFiles.v1_with_unmounted_v2(), "/sys/fs/cgroup/memory/" <> file, value)

      assert {:error, :memory_capacity_unknown} = Cgroup.snapshot(CgroupFiles.options(files))
    end
  end

  test "unmounted v2 requires a trustworthy finite v1 limit" do
    files = CgroupFiles.v1_with_unmounted_v2()

    for files <- [
          Map.put(files, "/proc/self/cgroup", "0::/default/example-container\n"),
          Map.put(files, "/sys/fs/cgroup/memory/memory.limit_in_bytes", "9223372036854771712"),
          Map.put(
            files,
            "/proc/self/mountinfo",
            "938 937 0:108 / /sys/fs/cgroup rw - tmpfs tmpfs rw"
          )
        ] do
      assert {:error, :memory_capacity_unknown} = Cgroup.snapshot(CgroupFiles.options(files))
    end
  end

  test "visible v2 errors are not masked by a healthy v1 controller" do
    files = CgroupFiles.v1_with_unmounted_v2()

    for mount <- [v2_mount(), "36 25 0:32 /other /sys/fs/cgroup rw - cgroup2 cgroup rw"],
        limit <- [{:error, :enoent}, {:error, :eacces}, "invalid", "1073741824"] do
      files =
        files
        |> Map.update!("/proc/self/mountinfo", &(&1 <> mount <> "\n"))
        |> Map.put("/sys/fs/cgroup/default/example-container/memory.max", limit)

      assert {:error, :memory_capacity_unknown} = Cgroup.snapshot(CgroupFiles.options(files))
    end
  end

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

    files =
      Map.merge(files, %{
        "/sys/fs/cgroup/v2/memory.max" => "1000\n",
        "/sys/fs/cgroup/v2/memory.current" => "800\n"
      })

    assert {:ok, %{source: :cgroup_v2, headroom_bytes: 200}} = snapshot(files)

    assert {:error, :memory_capacity_unknown} =
             snapshot(Map.delete(files, "/sys/fs/cgroup/memory/app/memory.usage_in_bytes"))
  end

  test "uses the smallest finite headroom across cgroup v1 ancestors" do
    files =
      proc_files("5:memory:/v1/team/app", v1_mount())
      |> Map.merge(%{
        "/sys/fs/cgroup/memory/team/app/memory.limit_in_bytes" => "900\n",
        "/sys/fs/cgroup/memory/team/app/memory.usage_in_bytes" => "100\n",
        "/sys/fs/cgroup/memory/team/memory.limit_in_bytes" => "1000\n",
        "/sys/fs/cgroup/memory/team/memory.usage_in_bytes" => "650\n",
        "/sys/fs/cgroup/memory/memory.limit_in_bytes" => "9223372036854771712\n"
      })

    assert {:ok, %{source: :cgroup_v1, headroom_bytes: 350}} = snapshot(files)
  end

  test "selects the broadest mount so tighter ancestors remain visible" do
    mounts =
      "36 25 0:32 /team /sys/fs/cgroup-narrow rw - cgroup2 cgroup rw\n" <>
        "37 25 0:33 / /sys/fs/cgroup-broad rw - cgroup2 cgroup rw"

    files =
      proc_files("0::/team/app", mounts)
      |> Map.merge(%{
        "/sys/fs/cgroup-narrow/app/memory.max" => "1000\n",
        "/sys/fs/cgroup-narrow/app/memory.current" => "400\n",
        "/sys/fs/cgroup-narrow/memory.max" => "max\n",
        "/sys/fs/cgroup-broad/team/app/memory.max" => "1000\n",
        "/sys/fs/cgroup-broad/team/app/memory.current" => "400\n",
        "/sys/fs/cgroup-broad/team/memory.max" => "max\n",
        "/sys/fs/cgroup-broad/memory.max" => "800\n",
        "/sys/fs/cgroup-broad/memory.current" => "700\n"
      })

    assert {:ok, %{source: :cgroup_v2, headroom_bytes: 100}} = snapshot(files)
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
