defmodule FavnOrchestrator.MemoryCapacity.ProviderTest do
  use ExUnit.Case, async: true

  alias FavnOrchestrator.MemoryCapacity.Provider

  @gib 1_024 * 1_024 * 1_024

  test "uses the smallest v2 ancestor headroom" do
    files =
      v2_files(%{
        "/sys/fs/cgroup/team/job/memory.max" => integer(2 * @gib),
        "/sys/fs/cgroup/team/job/memory.current" => integer(div(@gib, 2)),
        "/sys/fs/cgroup/team/memory.max" => integer(@gib),
        "/sys/fs/cgroup/team/memory.current" => integer(900 * 1_024 * 1_024),
        "/sys/fs/cgroup/memory.max" => "max\n",
        "/sys/fs/cgroup/memory.current" => "0\n"
      })

    assert {:ok, snapshot} = Provider.snapshot(read_file: reader(files))
    assert snapshot.source == :cgroup_v2
    assert snapshot.limit_bytes == @gib
    assert snapshot.usage_bytes == 900 * 1_024 * 1_024
    assert snapshot.headroom_bytes == 124 * 1_024 * 1_024
  end

  test "falls back to a finite v1 memory ancestor and ignores unlimited sentinels" do
    unlimited = 0x7FFF_FFFF_FFFF_0000

    files = %{
      "/proc/self/cgroup" => "5:memory:/team/job\n",
      "/proc/self/mountinfo" =>
        "31 22 0:28 / /sys/fs/cgroup/memory rw - cgroup cgroup rw,memory\n",
      "/sys/fs/cgroup/memory/team/job/memory.limit_in_bytes" => integer(unlimited),
      "/sys/fs/cgroup/memory/team/job/memory.usage_in_bytes" => integer(12),
      "/sys/fs/cgroup/memory/team/memory.limit_in_bytes" => integer(@gib),
      "/sys/fs/cgroup/memory/team/memory.usage_in_bytes" => integer(256 * 1_024 * 1_024),
      "/sys/fs/cgroup/memory/memory.limit_in_bytes" => integer(unlimited)
    }

    assert {:ok, snapshot} = Provider.snapshot(read_file: reader(files))
    assert snapshot.source == :cgroup_v1
    assert snapshot.limit_bytes == @gib
    assert snapshot.usage_bytes == 256 * 1_024 * 1_024
  end

  test "chooses the controlling hierarchy in a hybrid layout" do
    files =
      v2_files(%{
        "/sys/fs/cgroup/team/job/memory.max" => integer(2 * @gib),
        "/sys/fs/cgroup/team/job/memory.current" => integer(256 * 1_024 * 1_024)
      })
      |> Map.put("/proc/self/cgroup", "0::/team/job\n5:memory:/team/job\n")
      |> Map.update!(
        "/proc/self/mountinfo",
        &(&1 <> "31 22 0:28 / /sys/fs/cgroup/memory rw - cgroup cgroup rw,memory\n")
      )
      |> Map.merge(%{
        "/sys/fs/cgroup/memory/team/job/memory.limit_in_bytes" => integer(@gib),
        "/sys/fs/cgroup/memory/team/job/memory.usage_in_bytes" => integer(900 * 1_024 * 1_024),
        "/sys/fs/cgroup/memory/team/memory.limit_in_bytes" => integer(0x7FFF_FFFF_FFFF_0000),
        "/sys/fs/cgroup/memory/memory.limit_in_bytes" => integer(0x7FFF_FFFF_FFFF_0000)
      })

    assert {:ok, snapshot} = Provider.snapshot(read_file: reader(files))
    assert snapshot.source == :cgroup_v1
    assert snapshot.headroom_bytes == 124 * 1_024 * 1_024
  end

  test "uses configured ceiling with process RSS when no finite cgroup exists" do
    files = %{
      "/proc/self/cgroup" => "0::/team/job\n",
      "/proc/self/mountinfo" => "36 25 0:32 / /sys/fs/cgroup rw - cgroup2 cgroup rw\n",
      "/sys/fs/cgroup/team/job/memory.max" => "max\n",
      "/sys/fs/cgroup/team/job/memory.current" => "10\n",
      "/sys/fs/cgroup/team/memory.max" => "max\n",
      "/sys/fs/cgroup/memory.max" => "max\n",
      "/proc/self/status" => "Name:\tbeam.smp\nVmRSS:\t102400 kB\n"
    }

    assert {:ok, snapshot} =
             Provider.snapshot(read_file: reader(files), ceiling_bytes: @gib)

    assert snapshot.source == :configured_process
    assert snapshot.usage_bytes == 100 * 1_024 * 1_024
    assert snapshot.headroom_bytes == 924 * 1_024 * 1_024
  end

  test "an explicit configured-process source does not depend on host cgroup metadata" do
    files = %{
      "/proc/self/cgroup" => "malformed\n",
      "/proc/self/status" => "Name:\tbeam.smp\nVmRSS:\t102400 kB\n"
    }

    assert {:ok, snapshot} =
             Provider.snapshot(
               read_file: reader(files),
               ceiling_bytes: @gib,
               source: :configured_process
             )

    assert snapshot.source == :configured_process
    assert snapshot.limit_bytes == @gib
    assert snapshot.usage_bytes == 100 * 1_024 * 1_024
  end

  test "a configured ceiling only lowers a finite cgroup limit" do
    files =
      v2_files(%{
        "/sys/fs/cgroup/team/job/memory.max" => integer(2 * @gib),
        "/sys/fs/cgroup/team/job/memory.current" => integer(512 * 1_024 * 1_024)
      })

    assert {:ok, lowered} =
             Provider.snapshot(read_file: reader(files), ceiling_bytes: @gib)

    assert lowered.limit_bytes == @gib
    assert lowered.headroom_bytes == 512 * 1_024 * 1_024

    assert {:ok, unchanged} =
             Provider.snapshot(read_file: reader(files), ceiling_bytes: 3 * @gib)

    assert unchanged.limit_bytes == 2 * @gib
  end

  test "rereads usage and a live downward limit change" do
    {:ok, agent} = Agent.start_link(fn -> {2 * @gib, 256 * 1_024 * 1_024} end)

    read = fn
      "/proc/self/cgroup" ->
        {:ok, "0::/team/job\n"}

      "/proc/self/mountinfo" ->
        {:ok, "36 25 0:32 / /sys/fs/cgroup rw - cgroup2 cgroup rw\n"}

      "/sys/fs/cgroup/team/job/memory.max" ->
        {:ok, Agent.get(agent, fn {limit, _usage} -> integer(limit) end)}

      "/sys/fs/cgroup/team/job/memory.current" ->
        {:ok, Agent.get(agent, fn {_limit, usage} -> integer(usage) end)}

      "/sys/fs/cgroup/team/memory.max" ->
        {:ok, "max\n"}

      "/sys/fs/cgroup/memory.max" ->
        {:ok, "max\n"}

      _path ->
        {:error, :enoent}
    end

    assert {:ok, first} = Provider.snapshot(read_file: read)
    Agent.update(agent, fn _ -> {@gib, 900 * 1_024 * 1_024} end)
    assert {:ok, second} = Provider.snapshot(read_file: read)
    assert second.limit_bytes < first.limit_bytes
    assert second.headroom_bytes < first.headroom_bytes
  end

  test "returns unknown when neither a finite cgroup nor configured RSS is readable" do
    assert {:error, :memory_capacity_unknown} =
             Provider.snapshot(read_file: fn _path -> {:error, :enoent} end)
  end

  test "does not use the configured fallback for malformed cgroup metadata" do
    files = %{
      "/proc/self/cgroup" => "malformed\n",
      "/proc/self/mountinfo" => "36 25 0:32 / /sys/fs/cgroup rw - cgroup2 cgroup rw\n",
      "/proc/self/status" => "Name:\tbeam.smp\nVmRSS:\t102400 kB\n"
    }

    assert {:error, :memory_capacity_unknown} =
             Provider.snapshot(read_file: reader(files), ceiling_bytes: @gib)
  end

  test "does not ignore one malformed hierarchy beside a readable hierarchy" do
    files =
      v2_files(%{
        "/proc/self/cgroup" => "0::/team/job\nmalformed\n",
        "/sys/fs/cgroup/team/job/memory.max" => integer(@gib),
        "/sys/fs/cgroup/team/job/memory.current" => integer(128 * 1_024 * 1_024)
      })

    assert {:error, :memory_capacity_unknown} = Provider.snapshot(read_file: reader(files))
  end

  test "does not ignore a structurally complete but invalid hierarchy" do
    files =
      v2_files(%{
        "/proc/self/cgroup" => "0::/team/job\nx::/lower\n",
        "/sys/fs/cgroup/team/job/memory.max" => integer(@gib),
        "/sys/fs/cgroup/team/job/memory.current" => integer(128 * 1_024 * 1_024)
      })

    assert {:error, :memory_capacity_unknown} = Provider.snapshot(read_file: reader(files))
  end

  test "does not accept a memory controller on the unified hierarchy" do
    files =
      v2_files(%{
        "/proc/self/cgroup" => "0:memory:/team/job\n",
        "/sys/fs/cgroup/team/job/memory.max" => integer(@gib),
        "/sys/fs/cgroup/team/job/memory.current" => integer(128 * 1_024 * 1_024)
      })

    assert {:error, :memory_capacity_unknown} = Provider.snapshot(read_file: reader(files))
  end

  test "does not accept a relative cgroup path" do
    files =
      v2_files(%{
        "/proc/self/cgroup" => "0::team/job\n",
        "/sys/fs/cgroup/team/job/memory.max" => integer(@gib),
        "/sys/fs/cgroup/team/job/memory.current" => integer(128 * 1_024 * 1_024)
      })

    assert {:error, :memory_capacity_unknown} = Provider.snapshot(read_file: reader(files))
  end

  test "does not accept structurally complete invalid mount metadata" do
    files =
      v2_files(%{
        "/proc/self/mountinfo" => "x 25 0:32 / /sys/fs/cgroup rw - cgroup2 cgroup rw\n",
        "/sys/fs/cgroup/team/job/memory.max" => integer(@gib),
        "/sys/fs/cgroup/team/job/memory.current" => integer(128 * 1_024 * 1_024)
      })

    assert {:error, :memory_capacity_unknown} = Provider.snapshot(read_file: reader(files))
  end

  test "fails closed when a visible finite ancestor is unreadable" do
    files =
      v2_files(%{
        "/sys/fs/cgroup/team/job/memory.max" => integer(@gib),
        "/sys/fs/cgroup/team/job/memory.current" => integer(128 * 1_024 * 1_024),
        "/sys/fs/cgroup/team/memory.max" => integer(@gib)
      })

    assert {:error, :memory_capacity_unknown} = Provider.snapshot(read_file: reader(files))
  end

  test "fails closed when a visible ancestor limit is malformed" do
    files =
      v2_files(%{
        "/sys/fs/cgroup/team/job/memory.max" => integer(@gib),
        "/sys/fs/cgroup/team/job/memory.current" => integer(128 * 1_024 * 1_024),
        "/sys/fs/cgroup/team/memory.max" => "not-a-limit\n",
        "/sys/fs/cgroup/team/memory.current" => integer(128 * 1_024 * 1_024)
      })

    assert {:error, :memory_capacity_unknown} = Provider.snapshot(read_file: reader(files))
  end

  test "does not hide an unreadable hybrid hierarchy behind a readable one" do
    files =
      v2_files(%{
        "/sys/fs/cgroup/team/job/memory.max" => integer(@gib),
        "/sys/fs/cgroup/team/job/memory.current" => integer(128 * 1_024 * 1_024)
      })
      |> Map.put("/proc/self/cgroup", "0::/team/job\n5:memory:/team/job\n")
      |> Map.update!(
        "/proc/self/mountinfo",
        &(&1 <> "31 22 0:28 / /sys/fs/cgroup/memory rw - cgroup cgroup rw,memory\n")
      )
      |> Map.put(
        "/sys/fs/cgroup/memory/team/job/memory.limit_in_bytes",
        integer(@gib)
      )

    assert {:error, :memory_capacity_unknown} = Provider.snapshot(read_file: reader(files))
  end

  test "fails closed when an advertised hierarchy has no readable mount" do
    files =
      v2_files(%{
        "/proc/self/cgroup" => "0::/team/job\n5:memory:/team/job\n",
        "/sys/fs/cgroup/team/job/memory.max" => integer(@gib),
        "/sys/fs/cgroup/team/job/memory.current" => integer(128 * 1_024 * 1_024)
      })

    assert {:error, :memory_capacity_unknown} = Provider.snapshot(read_file: reader(files))
  end

  defp v2_files(extra) do
    Map.merge(
      %{
        "/proc/self/cgroup" => "0::/team/job\n",
        "/proc/self/mountinfo" => "36 25 0:32 / /sys/fs/cgroup rw - cgroup2 cgroup rw\n",
        "/sys/fs/cgroup/team/memory.max" => "max\n",
        "/sys/fs/cgroup/memory.max" => "max\n"
      },
      extra
    )
  end

  defp reader(files), do: fn path -> Map.fetch(files, path) end
  defp integer(value), do: Integer.to_string(value) <> "\n"
end
