defmodule Favn.Dev.RuntimeSource do
  @moduledoc false

  alias Favn.Dev.Paths
  alias Favn.Dev.RuntimeTreePolicy

  @type source_kind :: :root_override | :cwd | :dependency_checkout

  @type t :: %{
          kind: source_kind(),
          root: Path.t()
        }

  @spec resolve(keyword()) :: {:ok, t()} | {:error, term()}
  def resolve(opts) when is_list(opts) do
    if Keyword.has_key?(opts, :root_dir) do
      root = Paths.root_dir(opts) |> Path.expand()

      if valid_runtime_root?(root) do
        {:ok, %{kind: :root_override, root: root}}
      else
        {:error, {:invalid_runtime_source_root, root}}
      end
    else
      with {:error, :not_found} <- source_tree_root(),
           {:error, :not_found} <- dependency_root(),
           {:error, :not_found} <- cwd_root() do
        {:error, :runtime_source_not_found}
      else
        {:ok, _source} = ok -> ok
      end
    end
  end

  @spec resolve_runtime_root(Path.t()) :: {:ok, Path.t()} | {:error, :not_found}
  def resolve_runtime_root(path) when is_binary(path) do
    path
    |> Path.expand()
    |> candidate_roots()
    |> Enum.find(&valid_runtime_root?/1)
    |> case do
      nil -> {:error, :not_found}
      root -> {:ok, root}
    end
  end

  @spec fingerprint(t()) :: {:ok, map()} | {:error, term()}
  def fingerprint(%{root: root}) when is_binary(root) do
    case valid_runtime_root?(root) do
      true ->
        with {:ok, source_tree} <- source_tree_fingerprint(root) do
          {:ok,
           %{
             "runtime_mix_lock_sha256" => file_sha256(Path.join(root, "mix.lock")),
             "runtime_mix_exs_sha256" => file_sha256(Path.join(root, "mix.exs")),
             "runner_mix_exs_sha256" => file_sha256(Path.join(root, "apps/favn_runner/mix.exs")),
             "orchestrator_mix_exs_sha256" =>
               file_sha256(Path.join(root, "apps/favn_orchestrator/mix.exs")),
             "view_mix_exs_sha256" => file_sha256(Path.join(root, "apps/favn_view/mix.exs")),
             "runtime_source_tree" => source_tree
           }}
        end

      false ->
        {:error, {:invalid_runtime_source_root, root}}
    end
  end

  @spec valid_runtime_root?(Path.t()) :: boolean()
  def valid_runtime_root?(root) when is_binary(root) do
    required = [
      "apps/favn_runner/mix.exs",
      "apps/favn_orchestrator/mix.exs",
      "apps/favn_view/mix.exs"
    ]

    Enum.all?(required, &File.exists?(Path.join(root, &1)))
  end

  defp dependency_root do
    case Mix.Project.deps_paths()[:favn] do
      nil ->
        {:error, :not_found}

      root ->
        with {:ok, runtime_root} <- resolve_runtime_root(root) do
          {:ok, %{kind: :dependency_checkout, root: runtime_root}}
        end
    end
  rescue
    _ -> {:error, :not_found}
  end

  defp source_tree_root do
    root = Path.expand("../../../../../", __DIR__)

    with {:ok, runtime_root} <- resolve_runtime_root(root) do
      {:ok, %{kind: :dependency_checkout, root: runtime_root}}
    end
  end

  defp cwd_root do
    root = Path.expand(File.cwd!())

    with {:ok, runtime_root} <- resolve_runtime_root(root) do
      {:ok, %{kind: :cwd, root: runtime_root}}
    end
  end

  defp candidate_roots(path) when is_binary(path) do
    path
    |> do_candidate_roots([])
    |> Enum.reverse()
  end

  defp do_candidate_roots(path, acc) do
    parent = Path.dirname(path)
    next_acc = [path | acc]

    if parent == path do
      next_acc
    else
      do_candidate_roots(parent, next_acc)
    end
  end

  defp source_tree_fingerprint(root) do
    case git_tree_fingerprint(root) do
      {:ok, fingerprint} -> {:ok, fingerprint}
      :fallback -> content_tree_fingerprint(root)
    end
  end

  defp content_tree_fingerprint(root) do
    with {:ok, records} <- fingerprint_records(root) do
      sorted_records = Enum.sort_by(records, &record_sort_key/1)

      {:ok,
       %{
         "strategy" => "content",
         "sha256" => hash_records(sorted_records),
         "file_count" =>
           Enum.count(sorted_records, &match?({:file, _relative, _size, _sha256}, &1)),
         "directory_count" => Enum.count(sorted_records, &match?({:directory, _relative}, &1)),
         "byte_count" => byte_count(sorted_records),
         "entries" => RuntimeTreePolicy.entries(),
         "ignored_entries" => RuntimeTreePolicy.ignored_entries() |> Enum.sort(),
         "ignored_relative_entries" => RuntimeTreePolicy.ignored_relative_entries()
       }}
    end
  end

  defp git_tree_fingerprint(root) do
    with git when is_binary(git) <- System.find_executable("git"),
         {git_root, 0} <-
           System.cmd(git, ["-C", root, "rev-parse", "--show-toplevel"], stderr_to_stdout: true),
         git_root <- String.trim(git_root),
         {:ok, relative_root} <- relative_git_root(root, git_root),
         pathspecs <- git_pathspecs(relative_root),
         {status, 0} <-
           System.cmd(
             git,
             [
               "-C",
               git_root,
               "status",
               "--porcelain=v1",
               "-z",
               "--untracked-files=all",
               "--ignored=matching",
               "--no-renames",
               "--"
               | pathspecs
             ],
             stderr_to_stdout: true
           ),
         {tree, 0} <-
           System.cmd(
             git,
             [
               "-C",
               git_root,
               "ls-tree",
               "-r",
               "-z",
               "HEAD",
               "--" | git_included_pathspecs(relative_root)
             ],
             stderr_to_stdout: true
           ),
         {:ok, records} <- git_tree_records(tree, relative_root),
         {:ok, changed_paths} <- git_status_paths(status, relative_root) do
      git_worktree_fingerprint(root, records, changed_paths)
    else
      _reason -> :fallback
    end
  end

  defp git_worktree_fingerprint(_root, records, []) do
    {:ok, git_fingerprint("git_tree", records, 0)}
  end

  defp git_worktree_fingerprint(root, records, changed_paths) do
    with {:ok, records} <- overlay_worktree_records(root, records, changed_paths) do
      {:ok, git_fingerprint("git_worktree", records, length(changed_paths))}
    end
  end

  defp git_fingerprint(strategy, records, changed_path_count) do
    %{
      "strategy" => strategy,
      "sha256" => hash_git_records(records),
      "file_count" => length(records),
      "changed_path_count" => changed_path_count,
      "entries" => RuntimeTreePolicy.entries(),
      "ignored_entries" => RuntimeTreePolicy.ignored_entries() |> Enum.sort(),
      "ignored_relative_entries" => RuntimeTreePolicy.ignored_relative_entries()
    }
  end

  defp relative_git_root(root, git_root) do
    relative = root |> Path.expand() |> Path.relative_to(Path.expand(git_root))

    if relative == ".." or String.starts_with?(relative, "../") do
      :error
    else
      {:ok, if(relative == ".", do: "", else: relative)}
    end
  end

  defp git_pathspecs(relative_root) do
    excluded =
      Enum.map(RuntimeTreePolicy.ignored_relative_entries(), fn segments ->
        ":(exclude)" <> join_relative_root(relative_root, Path.join(segments))
      end)

    git_included_pathspecs(relative_root) ++ excluded
  end

  defp git_included_pathspecs(relative_root) do
    Enum.map(RuntimeTreePolicy.entries(), &join_relative_root(relative_root, &1))
  end

  defp join_relative_root("", relative), do: relative
  defp join_relative_root(root, relative), do: Path.join(root, relative)

  defp git_tree_records(tree, relative_root) do
    prefix = if relative_root == "", do: "", else: relative_root <> "/"

    tree
    |> String.split(<<0>>, trim: true)
    |> Enum.reduce_while({:ok, []}, fn record, {:ok, records} ->
      with [identity, path] <- String.split(record, "\t", parts: 2),
           false <- String.starts_with?(identity, "120000 "),
           {:ok, relative} <- relative_git_path(path, prefix) do
        if RuntimeTreePolicy.included_relative_path?(relative) do
          {:cont, {:ok, [{relative, identity} | records]}}
        else
          {:cont, {:ok, records}}
        end
      else
        _reason -> {:halt, :error}
      end
    end)
    |> case do
      {:ok, records} -> {:ok, Enum.sort(records)}
      :error -> :error
    end
  end

  defp git_status_paths("", _relative_root), do: {:ok, []}

  defp git_status_paths(status, relative_root) do
    prefix = if relative_root == "", do: "", else: relative_root <> "/"

    status
    |> String.split(<<0>>, trim: true)
    |> Enum.reduce_while({:ok, []}, fn
      <<_index_status, _worktree_status, " ", path::binary>>, {:ok, paths} ->
        case relative_git_path(path, prefix) do
          {:ok, relative} ->
            if RuntimeTreePolicy.included_relative_path?(relative) do
              {:cont, {:ok, [relative | paths]}}
            else
              {:cont, {:ok, paths}}
            end

          :error ->
            {:halt, :error}
        end

      _record, _acc ->
        {:halt, :error}
    end)
    |> case do
      {:ok, paths} -> {:ok, paths |> Enum.uniq() |> Enum.sort()}
      :error -> :error
    end
  end

  defp overlay_worktree_records(root, records, changed_paths) do
    changed_paths
    |> Enum.reduce_while({:ok, Map.new(records)}, fn relative, {:ok, record_map} ->
      path = Path.join(root, relative)

      case overlay_worktree_path(record_map, path, relative) do
        {:ok, record_map} -> {:cont, {:ok, record_map}}
        :deleted -> {:cont, {:ok, Map.delete(record_map, relative)}}
        :fallback -> {:halt, :fallback}
      end
    end)
    |> case do
      {:ok, record_map} -> {:ok, record_map |> Map.to_list() |> Enum.sort()}
      :fallback -> :fallback
    end
  end

  defp overlay_worktree_path(record_map, path, relative) do
    case File.lstat(path) do
      {:ok, %{type: :regular, size: size}} ->
        case file_sha256_result(path) do
          {:ok, sha256} -> {:ok, Map.put(record_map, relative, worktree_identity(size, sha256))}
          {:error, _reason} -> :fallback
        end

      {:ok, %{type: :directory}} ->
        overlay_worktree_directory(record_map, path, relative)

      {:ok, %{type: _other}} ->
        :fallback

      {:error, :enoent} ->
        :deleted

      {:error, _reason} ->
        :fallback
    end
  end

  defp overlay_worktree_directory(record_map, path, relative) do
    with {:ok, records} <- collect_records(path, relative) do
      record_map = remove_record_subtree(record_map, relative)

      {:ok,
       Enum.reduce(records, record_map, fn
         {:file, file_relative, size, sha256}, acc ->
           Map.put(acc, file_relative, worktree_identity(size, sha256))

         {:directory, _directory_relative}, acc ->
           acc
       end)}
    else
      {:error, _reason} -> :fallback
    end
  end

  defp remove_record_subtree(record_map, relative) do
    prefix = String.trim_trailing(relative, "/") <> "/"

    Map.reject(record_map, fn {path, _identity} ->
      path == relative or String.starts_with?(path, prefix)
    end)
  end

  defp worktree_identity(size, sha256), do: "worktree #{size} #{sha256}"

  defp hash_git_records(records) do
    records
    |> Enum.reduce(:crypto.hash_init(:sha256), fn {relative, identity}, state ->
      :crypto.hash_update(state, [relative, <<0>>, identity, <<0>>])
    end)
    |> :crypto.hash_final()
    |> Base.encode16(case: :lower)
  end

  defp relative_git_path(path, ""), do: {:ok, path}

  defp relative_git_path(path, prefix) do
    if String.starts_with?(path, prefix) do
      {:ok, String.replace_prefix(path, prefix, "")}
    else
      :error
    end
  end

  defp fingerprint_records(root) do
    case RuntimeTreePolicy.reduce_required_entries(root, [], fn path, relative, acc ->
           case collect_records(path, relative) do
             {:ok, records} -> {:ok, [records | acc]}
             {:error, reason} -> {:error, reason}
           end
         end) do
      {:ok, records} -> {:ok, List.flatten(records)}
      {:error, reason} -> {:error, reason}
    end
  end

  defp collect_records(path, relative) do
    case File.lstat(path) do
      {:ok, %{type: :directory}} ->
        collect_directory_records(path, relative)

      {:ok, %{type: :regular, size: size}} ->
        case file_sha256_result(path) do
          {:ok, sha256} -> {:ok, [{:file, normalize_relative(relative), size, sha256}]}
          {:error, reason} -> {:error, reason}
        end

      {:ok, %{type: other}} ->
        {:error, {:unsupported_runtime_entry, path, other}}

      {:error, reason} ->
        {:error, {:stat_failed, path, reason}}
    end
  end

  defp collect_directory_records(path, relative) do
    RuntimeTreePolicy.reduce_child_entries(
      path,
      [[{:directory, normalize_relative(relative)}]],
      &{:list_failed, path, &1},
      fn child_path, entry, acc ->
        child_relative = Path.join(relative, entry)

        case collect_records(child_path, child_relative) do
          {:ok, records} -> {:ok, [records | acc]}
          {:error, reason} -> {:error, reason}
        end
      end
    )
    |> case do
      {:ok, records} -> {:ok, List.flatten(records)}
      {:error, reason} -> {:error, reason}
    end
  end

  defp record_sort_key({:directory, relative}), do: relative
  defp record_sort_key({:file, relative, _size, _sha256}), do: relative

  defp byte_count(records) do
    Enum.reduce(records, 0, fn
      {:file, _relative, size, _sha256}, total -> total + size
      {:directory, _relative}, total -> total
    end)
  end

  defp hash_records(records) do
    records
    |> Enum.reduce(:crypto.hash_init(:sha256), fn record, state ->
      :crypto.hash_update(state, record_fingerprint_data(record))
    end)
    |> :crypto.hash_final()
    |> Base.encode16(case: :lower)
  end

  defp record_fingerprint_data({:directory, relative}) do
    ["directory", <<0>>, relative, <<0>>]
  end

  defp record_fingerprint_data({:file, relative, size, sha256}) do
    ["file", <<0>>, relative, <<0>>, Integer.to_string(size), <<0>>, sha256, <<0>>]
  end

  defp normalize_relative(path) do
    path
    |> Path.split()
    |> Path.join()
  end

  defp file_sha256(path) do
    case file_sha256_result(path) do
      {:ok, sha256} -> sha256
      {:error, {:read_failed, ^path, :enoent}} -> nil
      {:error, _reason} -> nil
    end
  end

  defp file_sha256_result(path) do
    case File.read(path) do
      {:ok, bytes} -> {:ok, :crypto.hash(:sha256, bytes) |> Base.encode16(case: :lower)}
      {:error, reason} -> {:error, {:read_failed, path, reason}}
    end
  end
end
