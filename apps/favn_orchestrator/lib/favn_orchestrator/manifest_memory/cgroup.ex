defmodule FavnOrchestrator.ManifestMemory.Cgroup do
  @moduledoc """
  Reads effective memory headroom from finite Linux cgroup v2 or v1 hierarchies.

  Every visible ancestor is checked because a parent may impose the controlling
  limit. Missing, unlimited, unreadable, or malformed hierarchies fail closed.
  """

  @v1_unlimited_threshold 0x7FFF_FFFF_FFFF_0000

  @type snapshot :: %{
          source: :cgroup_v2 | :cgroup_v1,
          limit_bytes: pos_integer(),
          usage_bytes: non_neg_integer(),
          headroom_bytes: non_neg_integer()
        }

  @doc "Returns the controlling finite cgroup memory snapshot."
  @spec snapshot(keyword()) :: {:ok, snapshot()} | {:error, :memory_capacity_unknown}
  # sobelow_skip ["Traversal.FileModule"]
  def snapshot(opts \\ []) when is_list(opts) do
    read = Keyword.get(opts, :read_file, &File.read/1)
    cgroup_path = Keyword.get(opts, :proc_cgroup_path, "/proc/self/cgroup")
    mountinfo_path = Keyword.get(opts, :mountinfo_path, "/proc/self/mountinfo")

    with {:ok, cgroup_contents} <- read.(cgroup_path),
         {:ok, mount_contents} <- read.(mountinfo_path),
         {:ok, cgroups} <- parse_all(cgroup_contents, &parse_cgroup/1),
         {:ok, mounts} <- parse_all(mount_contents, &parse_mount/1),
         {:ok, snapshot} <- hierarchy_snapshot(cgroups, mounts, read) do
      {:ok, snapshot}
    else
      _unavailable -> {:error, :memory_capacity_unknown}
    end
  rescue
    _invalid -> {:error, :memory_capacity_unknown}
  catch
    _kind, _reason -> {:error, :memory_capacity_unknown}
  end

  defp parse_all(contents, parser) do
    lines = String.split(contents, "\n", trim: true)
    parsed = Enum.map(lines, parser)

    if lines != [] and Enum.all?(parsed),
      do: {:ok, parsed},
      else: {:error, :invalid_proc_file}
  end

  defp parse_cgroup(line) do
    with [hierarchy, controllers, path] <- String.split(line, ":", parts: 3),
         {hierarchy_number, ""} <- Integer.parse(hierarchy),
         true <- hierarchy_number >= 0 and absolute_path?(path),
         controller_list <- String.split(controllers, ",", trim: true),
         true <- valid_controllers?(hierarchy_number, controller_list) do
      %{hierarchy: hierarchy_number, controllers: controller_list, path: path}
    else
      _invalid -> nil
    end
  end

  defp valid_controllers?(0, controllers), do: controllers == []
  defp valid_controllers?(hierarchy, controllers), do: hierarchy > 0 and controllers != []

  defp parse_mount(line) do
    with [before, after_separator] <- String.split(line, " - ", parts: 2),
         fields when length(fields) >= 6 <- String.split(before),
         after_fields when length(after_fields) >= 3 <- String.split(after_separator),
         true <- valid_mount_fields?(fields) do
      %{
        root: Enum.at(fields, 3),
        mountpoint: Enum.at(fields, 4),
        filesystem: Enum.at(after_fields, 0),
        super_options: Enum.at(after_fields, 2) |> String.split(",")
      }
    else
      _invalid -> nil
    end
  end

  defp valid_mount_fields?(fields) do
    positive_integer?(Enum.at(fields, 0)) and positive_integer?(Enum.at(fields, 1)) and
      device_number?(Enum.at(fields, 2)) and absolute_path?(Enum.at(fields, 3)) and
      absolute_path?(Enum.at(fields, 4))
  end

  defp positive_integer?(value), do: integer?(value, 1)
  defp non_negative_integer?(value), do: integer?(value, 0)

  defp integer?(value, minimum) do
    case Integer.parse(value) do
      {integer, ""} when integer >= minimum -> true
      _invalid -> false
    end
  end

  defp device_number?(value) do
    case String.split(value, ":", parts: 2) do
      [major, minor] -> non_negative_integer?(major) and non_negative_integer?(minor)
      _invalid -> false
    end
  end

  defp absolute_path?(path), do: is_binary(path) and String.starts_with?(path, "/")

  defp hierarchy_snapshot(cgroups, mounts, read) do
    results = [v2_snapshot(cgroups, mounts, read), v1_snapshot(cgroups, mounts, read)]

    case Enum.find(results, &match?({:error, _reason}, &1)) do
      {:error, reason} ->
        {:error, reason}

      nil ->
        case for({:ok, snapshot} <- results, do: snapshot) do
          [] -> {:error, :finite_cgroup_memory_unavailable}
          snapshots -> {:ok, Enum.min_by(snapshots, & &1.headroom_bytes)}
        end
    end
  end

  defp v2_snapshot(cgroups, mounts, read) do
    case Enum.find(cgroups, &(&1.hierarchy == 0 and &1.controllers == [])) do
      %{path: path} ->
        case matching_mount(mounts, path, &(&1.filesystem == "cgroup2")) do
          nil -> {:error, :cgroup_v2_mount_unavailable}
          mount -> read_hierarchy(:cgroup_v2, mount, path, "memory.max", "memory.current", read)
        end

      nil ->
        :absent
    end
  end

  defp v1_snapshot(cgroups, mounts, read) do
    case Enum.find(cgroups, &("memory" in &1.controllers)) do
      %{path: path} ->
        case matching_mount(
               mounts,
               path,
               &(&1.filesystem == "cgroup" and "memory" in &1.super_options)
             ) do
          nil ->
            {:error, :cgroup_v1_mount_unavailable}

          mount ->
            read_hierarchy(
              :cgroup_v1,
              mount,
              path,
              "memory.limit_in_bytes",
              "memory.usage_in_bytes",
              read
            )
        end

      nil ->
        :absent
    end
  end

  defp matching_mount(mounts, cgroup_path, predicate) do
    mounts
    |> Enum.filter(predicate)
    |> Enum.filter(&match?({:ok, _directory}, mounted_path(&1, cgroup_path)))
    |> Enum.min_by(&byte_size(Path.expand(&1.root)), fn -> nil end)
  end

  defp read_hierarchy(source, mount, cgroup_path, limit_file, usage_file, read) do
    with {:ok, directory} <- mounted_path(mount, cgroup_path),
         {:ok, candidates} <-
           read_ancestors(
             ancestors(directory, mount.mountpoint),
             source,
             limit_file,
             usage_file,
             read
           ) do
      case candidates do
        [] ->
          :unlimited

        candidates ->
          controlling = Enum.min_by(candidates, & &1.headroom)

          {:ok,
           %{
             source: source,
             limit_bytes: controlling.limit,
             usage_bytes: controlling.usage,
             headroom_bytes: controlling.headroom
           }}
      end
    end
  end

  defp read_ancestors(paths, source, limit_file, usage_file, read) do
    Enum.reduce_while(paths, {:ok, []}, fn path, {:ok, candidates} ->
      limit_path = Path.join(path, limit_file)
      usage_path = Path.join(path, usage_file)

      with {:ok, limit_raw} <- read.(limit_path),
           {:ok, limit} <- parse_limit(limit_raw, source),
           {:ok, usage_raw} <- read.(usage_path),
           {:ok, usage} <- parse_usage(usage_raw) do
        candidate = %{limit: limit, usage: usage, headroom: max(limit - usage, 0)}
        {:cont, {:ok, [candidate | candidates]}}
      else
        {:error, :unlimited} -> {:cont, {:ok, candidates}}
        _invalid -> {:halt, {:error, :invalid_cgroup_memory_value}}
      end
    end)
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

  defp parse_usage(raw) do
    case Integer.parse(String.trim(raw)) do
      {integer, ""} when integer >= 0 -> {:ok, integer}
      _invalid -> {:error, :invalid_usage}
    end
  end

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
    Stream.unfold(Path.expand(directory), fn current ->
      cond do
        is_nil(current) -> nil
        not within?(current, boundary) -> nil
        current == Path.expand(boundary) -> {current, nil}
        current == Path.dirname(current) -> nil
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
end
