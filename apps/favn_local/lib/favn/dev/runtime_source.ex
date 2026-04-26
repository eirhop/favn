defmodule Favn.Dev.RuntimeSource do
  @moduledoc false

  alias Favn.Dev.Paths

  @type source_kind :: :root_override | :cwd | :dependency_checkout

  @type t :: %{
          kind: source_kind(),
          root: Path.t()
        }

  @fingerprinted_entries ["mix.exs", "mix.lock", "config", "apps", "web/favn_web"]
  @optional_fingerprinted_entries MapSet.new(["mix.exs", "mix.lock", "config"])

  @ignored_fingerprint_entries MapSet.new([
                                 ".elixir_ls",
                                 ".favn",
                                 ".git",
                                 ".svelte-kit",
                                 "_build",
                                 "cover",
                                 "deps",
                                 "dist",
                                 "node_modules",
                                 "test-results"
                               ])

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
             "web_package_json_sha256" =>
               file_sha256(Path.join(root, "web/favn_web/package.json")),
             "web_package_lock_sha256" =>
               file_sha256(Path.join(root, "web/favn_web/package-lock.json")),
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
      "web/favn_web/package.json"
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
    with {:ok, records} <- fingerprint_records(root) do
      sorted_records = Enum.sort_by(records, &record_sort_key/1)

      {:ok,
       %{
         "sha256" => hash_records(sorted_records),
         "file_count" =>
           Enum.count(sorted_records, &match?({:file, _relative, _size, _sha256}, &1)),
         "directory_count" => Enum.count(sorted_records, &match?({:directory, _relative}, &1)),
         "byte_count" => byte_count(sorted_records),
         "entries" => @fingerprinted_entries,
         "ignored_entries" => @ignored_fingerprint_entries |> MapSet.to_list() |> Enum.sort()
       }}
    end
  end

  defp fingerprint_records(root) do
    Enum.reduce_while(@fingerprinted_entries, {:ok, []}, fn relative, {:ok, acc} ->
      path = Path.join(root, relative)

      cond do
        not File.exists?(path) and MapSet.member?(@optional_fingerprinted_entries, relative) ->
          {:cont, {:ok, acc}}

        not File.exists?(path) ->
          {:halt, {:error, {:missing_runtime_entry, relative, path}}}

        true ->
          case collect_records(path, relative) do
            {:ok, records} -> {:cont, {:ok, records ++ acc}}
            {:error, reason} -> {:halt, {:error, reason}}
          end
      end
    end)
  end

  defp collect_records(path, relative) do
    case File.stat(path) do
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
    case File.ls(path) do
      {:ok, entries} ->
        Enum.reduce_while(entries, {:ok, [{:directory, normalize_relative(relative)}]}, fn entry,
                                                                                           {:ok,
                                                                                            acc} ->
          if MapSet.member?(@ignored_fingerprint_entries, entry) do
            {:cont, {:ok, acc}}
          else
            child_path = Path.join(path, entry)
            child_relative = Path.join(relative, entry)

            case collect_records(child_path, child_relative) do
              {:ok, records} -> {:cont, {:ok, records ++ acc}}
              {:error, reason} -> {:halt, {:error, reason}}
            end
          end
        end)

      {:error, reason} ->
        {:error, {:list_failed, path, reason}}
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
