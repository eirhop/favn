defmodule FavnOrchestrator.MemoryCapacity.Provider do
  @moduledoc """
  Reads the effective Linux cgroup memory limit and current usage.

  Both cgroup v2 and the v1 memory controller are supported. Every visible
  ancestor is considered because a parent may impose the controlling limit.
  """

  @v1_unlimited_threshold 0x7FFF_FFFF_FFFF_0000

  @type snapshot :: %{
          source: :cgroup_v2 | :cgroup_v1 | :configured_process,
          limit_bytes: pos_integer(),
          usage_bytes: non_neg_integer(),
          headroom_bytes: non_neg_integer()
        }

  @doc "Returns a finite memory limit, current usage, and effective headroom."
  @spec snapshot(keyword()) :: {:ok, snapshot()} | {:error, term()}
  def snapshot(opts \\ []) when is_list(opts) do
    read = Keyword.get(opts, :read_file, &File.read/1)
    ceiling = Keyword.get(opts, :ceiling_bytes)

    case cgroup_snapshot(read, opts) do
      {:ok, snapshot} -> {:ok, apply_ceiling(snapshot, ceiling)}
      {:error, :finite_cgroup_memory_unavailable} -> configured_snapshot(read, ceiling, opts)
      {:error, _reason} -> {:error, :memory_capacity_unknown}
    end
  end

  @doc false
  @spec parse_mountinfo(binary()) :: [map()]
  def parse_mountinfo(contents) when is_binary(contents) do
    contents
    |> String.split("\n", trim: true)
    |> Enum.flat_map(fn line ->
      with [before, after_separator] <- String.split(line, " - ", parts: 2),
           fields when length(fields) >= 6 <- String.split(before),
           after_fields when length(after_fields) >= 3 <- String.split(after_separator),
           true <- valid_mount_fields?(fields) do
        [
          %{
            root: Enum.at(fields, 3),
            mountpoint: Enum.at(fields, 4),
            filesystem: Enum.at(after_fields, 0),
            super_options: Enum.at(after_fields, 2) |> String.split(",")
          }
        ]
      else
        _invalid -> []
      end
    end)
  end

  @doc false
  @spec parse_cgroups(binary()) :: [map()]
  def parse_cgroups(contents) when is_binary(contents) do
    contents
    |> String.split("\n", trim: true)
    |> Enum.flat_map(fn line ->
      case String.split(line, ":", parts: 3) do
        [hierarchy, controllers, path] ->
          parsed_controllers = String.split(controllers, ",", trim: true)

          if valid_cgroup_fields?(hierarchy, parsed_controllers, path) do
            [
              %{
                hierarchy: hierarchy,
                controllers: parsed_controllers,
                path: path
              }
            ]
          else
            []
          end

        _invalid ->
          []
      end
    end)
  end

  defp cgroup_snapshot(read, opts) do
    cgroup_path = Keyword.get(opts, :proc_cgroup_path, "/proc/self/cgroup")
    mountinfo_path = Keyword.get(opts, :mountinfo_path, "/proc/self/mountinfo")

    with {:ok, cgroup_contents} <- read.(cgroup_path),
         {:ok, mount_contents} <- read.(mountinfo_path) do
      cgroups = parse_cgroups(cgroup_contents)
      mounts = parse_mountinfo(mount_contents)

      with :ok <- validate_parsed_cgroups(cgroup_contents, cgroups),
           :ok <- validate_parsed_proc_file(mount_contents, mounts, :mountinfo) do
        results = [v2_snapshot(cgroups, mounts, read), v1_snapshot(cgroups, mounts, read)]

        with [] <- for({:error, reason} <- results, do: reason) do
          snapshots = for {:ok, snapshot} <- results, do: snapshot

          case snapshots do
            [] -> {:error, :finite_cgroup_memory_unavailable}
            values -> {:ok, Enum.min_by(values, & &1.headroom_bytes)}
          end
        else
          [reason | _rest] -> {:error, reason}
        end
      end
    end
  end

  defp validate_parsed_cgroups(contents, parsed) do
    lines = String.split(contents, "\n", trim: true)

    if length(lines) == length(parsed),
      do: :ok,
      else: {:error, {:invalid_proc_file, :cgroup}}
  end

  defp validate_parsed_proc_file(contents, parsed, kind) do
    if length(String.split(contents, "\n", trim: true)) == length(parsed),
      do: :ok,
      else: {:error, {:invalid_proc_file, kind}}
  end

  defp valid_cgroup_fields?(hierarchy, controllers, path) do
    case Integer.parse(hierarchy) do
      {0, ""} -> controllers == [] and absolute_proc_path?(path)
      {value, ""} when value > 0 -> controllers != [] and absolute_proc_path?(path)
      _invalid -> false
    end
  end

  defp valid_mount_fields?(fields) do
    valid_positive_integer?(Enum.at(fields, 0)) and
      valid_positive_integer?(Enum.at(fields, 1)) and
      valid_device_number?(Enum.at(fields, 2)) and
      absolute_proc_path?(Enum.at(fields, 3)) and
      absolute_proc_path?(Enum.at(fields, 4))
  end

  defp valid_positive_integer?(value) do
    case Integer.parse(value) do
      {integer, ""} when integer > 0 -> true
      _invalid -> false
    end
  end

  defp valid_device_number?(value) do
    case String.split(value, ":", parts: 2) do
      [major, minor] ->
        valid_non_negative_integer?(major) and valid_non_negative_integer?(minor)

      _invalid ->
        false
    end
  end

  defp valid_non_negative_integer?(value) do
    case Integer.parse(value) do
      {integer, ""} when integer >= 0 -> true
      _invalid -> false
    end
  end

  defp absolute_proc_path?(path), do: is_binary(path) and String.starts_with?(path, "/")

  defp v2_snapshot(cgroups, mounts, read) do
    case Enum.find(cgroups, &(&1.hierarchy == "0" and &1.controllers == [])) do
      %{path: path} ->
        case Enum.find(mounts, &(&1.filesystem == "cgroup2")) do
          %{filesystem: "cgroup2"} = mount ->
            hierarchy_snapshot(:cgroup_v2, mount, path, "memory.max", "memory.current", read)

          nil ->
            {:error, :cgroup_v2_mount_unavailable}
        end

      nil ->
        :absent
    end
  end

  defp v1_snapshot(cgroups, mounts, read) do
    case Enum.find(cgroups, &("memory" in &1.controllers)) do
      %{path: path} ->
        case Enum.find(
               mounts,
               &(&1.filesystem == "cgroup" and "memory" in &1.super_options)
             ) do
          %{filesystem: "cgroup"} = mount ->
            hierarchy_snapshot(
              :cgroup_v1,
              mount,
              path,
              "memory.limit_in_bytes",
              "memory.usage_in_bytes",
              read
            )

          nil ->
            {:error, :cgroup_v1_memory_mount_unavailable}
        end

      nil ->
        :absent
    end
  end

  defp hierarchy_snapshot(source, mount, cgroup_path, limit_file, usage_file, read) do
    with {:ok, directory} <- mounted_path(mount, cgroup_path) do
      with {:ok, candidates} <-
             read_hierarchy(
               ancestors(directory, mount.mountpoint),
               source,
               limit_file,
               usage_file,
               read
             ) do
        case candidates do
          [] ->
            :unlimited

          values ->
            controlling = Enum.min_by(values, & &1.headroom)

            {:ok,
             %{
               source: source,
               limit_bytes: Enum.min(Enum.map(values, & &1.limit)),
               usage_bytes: controlling.usage,
               headroom_bytes: controlling.headroom
             }}
        end
      end
    end
  end

  defp read_hierarchy(paths, source, limit_file, usage_file, read) do
    Enum.reduce_while(paths, {:ok, []}, fn path, {:ok, candidates} ->
      limit_path = Path.join(path, limit_file)
      usage_path = Path.join(path, usage_file)

      case read.(limit_path) do
        {:ok, limit_raw} ->
          case parse_limit(limit_raw, source) do
            {:ok, limit} ->
              with {:ok, usage_raw} <- read.(usage_path),
                   {:ok, usage} <- parse_non_negative(usage_raw) do
                candidate = %{limit: limit, usage: usage, headroom: max(limit - usage, 0)}
                {:cont, {:ok, [candidate | candidates]}}
              else
                _invalid -> {:halt, {:error, {:invalid_cgroup_memory_usage, usage_path}}}
              end

            {:error, :unlimited} ->
              {:cont, {:ok, candidates}}

            {:error, _reason} ->
              {:halt, {:error, {:invalid_cgroup_memory_limit, limit_path}}}
          end

        {:error, _reason} ->
          {:halt, {:error, {:unreadable_cgroup_memory_limit, limit_path}}}
      end
    end)
  end

  defp configured_snapshot(_read, nil, _opts), do: {:error, :memory_capacity_unknown}

  defp configured_snapshot(read, ceiling, opts) when is_integer(ceiling) and ceiling > 0 do
    status_path = Keyword.get(opts, :proc_status_path, "/proc/self/status")

    with {:ok, contents} <- read.(status_path),
         {:ok, rss} <- parse_rss(contents) do
      {:ok,
       %{
         source: :configured_process,
         limit_bytes: ceiling,
         usage_bytes: rss,
         headroom_bytes: max(ceiling - rss, 0)
       }}
    else
      _unavailable -> {:error, :memory_capacity_unknown}
    end
  end

  defp configured_snapshot(_read, _invalid, _opts), do: {:error, :memory_capacity_unknown}

  defp apply_ceiling(snapshot, nil), do: snapshot

  defp apply_ceiling(snapshot, ceiling) when is_integer(ceiling) and ceiling > 0 do
    configured_headroom = max(ceiling - snapshot.usage_bytes, 0)

    if ceiling < snapshot.limit_bytes or configured_headroom < snapshot.headroom_bytes do
      %{
        snapshot
        | limit_bytes: min(snapshot.limit_bytes, ceiling),
          headroom_bytes: min(snapshot.headroom_bytes, configured_headroom)
      }
    else
      snapshot
    end
  end

  defp apply_ceiling(snapshot, _invalid), do: snapshot

  defp mounted_path(mount, cgroup_path) do
    root = Path.expand(mount.root)
    path = Path.expand(cgroup_path)

    if within?(path, root) do
      relative = if path == root, do: "", else: Path.relative_to(path, root)
      {:ok, Path.join(mount.mountpoint, relative)}
    else
      {:error, :cgroup_outside_mount_root}
    end
  end

  defp ancestors(directory, boundary) do
    Stream.unfold(Path.expand(directory), fn
      nil ->
        nil

      current ->
        cond do
          current == Path.dirname(current) -> nil
          not within?(current, boundary) -> nil
          current == Path.expand(boundary) -> {current, nil}
          true -> {current, Path.dirname(current)}
        end
    end)
    |> Enum.to_list()
  end

  defp within?(path, boundary) do
    expanded = Path.expand(path)
    root = Path.expand(boundary)
    expanded == root or root == "/" or String.starts_with?(expanded, root <> "/")
  end

  defp parse_limit(raw, source) do
    case String.trim(raw) do
      "max" ->
        {:error, :unlimited}

      value ->
        case Integer.parse(value) do
          {integer, ""} when integer > 0 ->
            if source == :cgroup_v1 and integer >= @v1_unlimited_threshold,
              do: {:error, :unlimited},
              else: {:ok, integer}

          _invalid ->
            {:error, :invalid_limit}
        end
    end
  end

  defp parse_non_negative(raw) do
    case Integer.parse(String.trim(raw)) do
      {integer, ""} when integer >= 0 -> {:ok, integer}
      _invalid -> {:error, :invalid_usage}
    end
  end

  defp parse_rss(contents) do
    case Regex.run(~r/^VmRSS:\s+(\d+)\s+kB$/m, contents, capture: :all_but_first) do
      [kilobytes] ->
        case Integer.parse(kilobytes) do
          {value, ""} -> {:ok, value * 1_024}
          _invalid -> {:error, :invalid_rss}
        end

      _missing ->
        {:error, :rss_unavailable}
    end
  end
end
