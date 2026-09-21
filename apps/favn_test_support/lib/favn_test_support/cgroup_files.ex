defmodule FavnTestSupport.CgroupFiles do
  @moduledoc "Deterministic Linux cgroup files for memory admission tests."

  @doc "Returns a finite v1 memory controller with unmounted v2 membership."
  @spec v1_with_unmounted_v2() :: %{String.t() => String.t()}
  def v1_with_unmounted_v2 do
    %{
      "/proc/self/cgroup" =>
        "4:memory:/default/example-container\n0::/default/example-container\n",
      "/proc/self/mountinfo" =>
        "938 937 0:108 / /sys/fs/cgroup rw,nosuid,nodev,noexec,relatime - tmpfs tmpfs rw,mode=755\n" <>
          "942 938 0:34 /default/example-container /sys/fs/cgroup/memory ro,nosuid,nodev,noexec,relatime master:12 - cgroup cgroup rw,memory\n",
      "/sys/fs/cgroup/memory/memory.limit_in_bytes" => "1073741824\n",
      "/sys/fs/cgroup/memory/memory.usage_in_bytes" => "314572800\n"
    }
  end

  @doc "Builds snapshot options; missing files return enoent and explicit errors are preserved."
  @spec options(%{String.t() => String.t() | {:error, atom()}}) :: keyword()
  def options(files) do
    [
      read_file: fn path ->
        case Map.fetch(files, path) do
          {:ok, contents} when is_binary(contents) -> {:ok, contents}
          {:ok, {:error, _reason} = error} -> error
          :error -> {:error, :enoent}
        end
      end
    ]
  end
end
