defmodule Favn.Dev.Build.SourceInputSet do
  @moduledoc false

  defmodule Entry do
    @moduledoc false

    @enforce_keys [:path, :sha256, :size, :executable]
    defstruct @enforce_keys

    @type t :: %__MODULE__{
            path: Path.t(),
            sha256: String.t(),
            size: non_neg_integer(),
            executable: boolean()
          }
  end

  @application_files ~w(
    mix.lock
    Makefile
    Makefile.win
    CMakeLists.txt
    rebar.config
    rebar.lock
    checksum.exs
    Cargo.toml
    Cargo.lock
  )
  @application_trees ~w(lib priv c_src native include src 3rd_party bin)

  @enforce_keys [:root, :declared_roots, :entries, :selection]
  defstruct @enforce_keys

  @type t :: %__MODULE__{
          root: Path.t(),
          declared_roots: [Path.t()],
          entries: [Entry.t()],
          selection: :git | :filesystem
        }

  @doc false
  @spec application(Path.t(), keyword()) :: {:ok, t()} | {:error, term()}
  def application(root, opts \\ []) when is_binary(root) and is_list(opts) do
    specs =
      [{"mix.exs", :file}] ++
        Enum.map(@application_files, &{&1, :file}) ++
        Enum.map(@application_trees, &{&1, :tree}) ++
        if(Keyword.get(opts, :runtime_config, false),
          do: [{"config/runtime.exs", :file}],
          else: []
        )

    collect(root, specs, ["mix.exs"])
  end

  @doc false
  @spec maintainer_checkout(Path.t(), [Path.t()]) :: {:ok, t()} | {:error, term()}
  def maintainer_checkout(root, application_dirs)
      when is_binary(root) and is_list(application_dirs) do
    with {:ok, application_dirs} <- application_directories(application_dirs) do
      root_specs = [
        {"mix.exs", :file},
        {"mix.lock", :file},
        {"config/config.exs", :file},
        {"config/prod.exs", :file},
        {"config/runtime.exs", :file},
        {"scripts/control_plane_build_id.exs", :file},
        {"rel/control_plane", :tree}
      ]

      application_specs =
        Enum.flat_map(application_dirs, fn directory ->
          [{Path.join(directory, "mix.exs"), :file}] ++
            Enum.map(@application_files, &{Path.join(directory, &1), :file}) ++
            Enum.map(@application_trees, &{Path.join(directory, &1), :tree}) ++
            if(Path.basename(directory) == "favn_view",
              do: [{Path.join(directory, "assets"), :tree}],
              else: []
            )
        end)

      required =
        ["mix.exs", "scripts/control_plane_build_id.exs"] ++
          Enum.map(application_dirs, &Path.join(&1, "mix.exs"))

      collect(root, root_specs ++ application_specs, required)
    end
  end

  @doc false
  @spec fingerprint(t()) :: String.t()
  def fingerprint(%__MODULE__{} = input_set) do
    input_set.entries
    |> Enum.map(&{&1.path, &1.executable, &1.size, &1.sha256})
    |> :erlang.term_to_binary([:deterministic])
    |> sha256()
  end

  @doc false
  @spec summary(t()) :: map()
  def summary(%__MODULE__{} = input_set) do
    %{
      "selection" => Atom.to_string(input_set.selection),
      "declared_roots" => input_set.declared_roots,
      "file_count" => length(input_set.entries),
      "total_bytes" => Enum.sum(Enum.map(input_set.entries, & &1.size))
    }
  end

  @doc false
  @spec copy(t(), Path.t()) :: :ok | {:error, term()}
  def copy(%__MODULE__{} = input_set, destination) when is_binary(destination) do
    with :ok <- File.mkdir_p(destination) do
      Enum.reduce_while(input_set.entries, :ok, fn entry, :ok ->
        case copy_entry(input_set.root, destination, entry) do
          :ok -> {:cont, :ok}
          {:error, _reason} = error -> {:halt, error}
        end
      end)
    end
  end

  @doc false
  @spec git_dirty?(t(), (String.t(), [String.t()], keyword() -> {String.t(), integer()})) ::
          {:ok, boolean()} | {:error, term()}
  def git_dirty?(input_set, runner \\ &System.cmd/3)

  def git_dirty?(%__MODULE__{} = input_set, runner) when is_function(runner, 3) do
    args = [
      "-C",
      input_set.root,
      "status",
      "--porcelain",
      "--untracked-files=all",
      "--"
      | input_set.declared_roots
    ]

    case runner.("git", args, stderr_to_stdout: true) do
      {status, 0} -> {:ok, String.trim(status) != ""}
      _invalid -> {:error, {:source_input_git_unavailable, input_set.root}}
    end
  rescue
    error -> {:error, {:source_input_git_unavailable, Exception.message(error)}}
  end

  defp collect(root, specs, required) do
    root = Path.expand(root)
    declared_roots = specs |> Enum.map(&elem(&1, 0)) |> Enum.uniq() |> Enum.sort()

    with :ok <- validate_root(root),
         {:ok, selection, paths} <- selected_paths(root, specs),
         :ok <- validate_required(paths, required),
         {:ok, entries} <- read_entries(root, paths) do
      {:ok,
       %__MODULE__{
         root: root,
         declared_roots: declared_roots,
         entries: entries,
         selection: selection
       }}
    end
  end

  defp validate_root(root) do
    case File.lstat(root) do
      {:ok, %{type: :directory}} -> :ok
      {:ok, %{type: :symlink}} -> {:error, {:source_input_root_symlink, Path.basename(root)}}
      _invalid -> {:error, {:source_input_root_missing, Path.basename(root)}}
    end
  end

  defp selected_paths(root, specs) do
    if git_scope?(root) do
      with {:ok, paths} <- git_paths(root, specs), do: {:ok, :git, paths}
    else
      with {:ok, paths} <- filesystem_paths(root, specs), do: {:ok, :filesystem, paths}
    end
  end

  defp git_scope?(root) do
    with {top_level, 0} <-
           System.cmd("git", ["-C", root, "rev-parse", "--show-toplevel"], stderr_to_stdout: true),
         true <- String.trim(top_level) != "",
         {_output, 1} <-
           System.cmd("git", ["-C", root, "check-ignore", "-q", "--", "."],
             stderr_to_stdout: true
           ) do
      true
    else
      _not_git_scope -> false
    end
  rescue
    _error -> false
  end

  defp git_paths(root, specs) do
    declared = specs |> Enum.map(&elem(&1, 0)) |> Enum.uniq()

    case System.cmd(
           "git",
           [
             "-C",
             root,
             "ls-files",
             "--cached",
             "--others",
             "--exclude-standard",
             "-z",
             "--"
             | declared
           ],
           stderr_to_stdout: true
         ) do
      {encoded, 0} ->
        paths =
          encoded
          |> :binary.split(<<0>>, [:global])
          |> Enum.reject(&(&1 == ""))
          |> Enum.uniq()
          |> Enum.sort()

        if Enum.all?(paths, &safe_relative_path?/1),
          do: {:ok, paths},
          else: {:error, :unsafe_source_input_path}

      {_output, _status} ->
        {:error, {:source_input_git_failed, Path.basename(root)}}
    end
  rescue
    error -> {:error, {:source_input_git_failed, Exception.message(error)}}
  end

  defp filesystem_paths(root, specs) do
    specs
    |> Enum.reduce_while({:ok, []}, fn spec, {:ok, acc} ->
      case filesystem_spec_paths(root, spec) do
        {:ok, paths} -> {:cont, {:ok, paths ++ acc}}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, paths} -> {:ok, paths |> Enum.uniq() |> Enum.sort()}
      {:error, _reason} = error -> error
    end
  end

  defp filesystem_spec_paths(root, {relative, kind}) do
    case File.lstat(Path.join(root, relative)) do
      {:ok, %{type: :regular}} when kind == :file ->
        {:ok, [relative]}

      {:ok, %{type: :directory}} when kind == :tree ->
        walk_tree(root, relative)

      {:ok, %{type: :symlink}} ->
        {:error, {:symlink_not_supported, relative}}

      {:ok, _other} ->
        {:error, {:source_input_not_regular, relative}}

      {:error, :enoent} ->
        {:ok, []}

      {:error, reason} ->
        {:error, {:source_input_read_failed, relative, reason}}
    end
  end

  defp walk_tree(root, relative) do
    with {:ok, names} <- File.ls(Path.join(root, relative)) do
      names
      |> Enum.sort()
      |> Enum.reduce_while({:ok, []}, fn name, {:ok, acc} ->
        child = Path.join(relative, name)

        case File.lstat(Path.join(root, child)) do
          {:ok, %{type: :regular}} ->
            {:cont, {:ok, [child | acc]}}

          {:ok, %{type: :directory}} ->
            case walk_tree(root, child) do
              {:ok, paths} -> {:cont, {:ok, paths ++ acc}}
              {:error, _reason} = error -> {:halt, error}
            end

          {:ok, %{type: :symlink}} ->
            {:halt, {:error, {:symlink_not_supported, child}}}

          {:ok, _other} ->
            {:halt, {:error, {:source_input_not_regular, child}}}

          {:error, reason} ->
            {:halt, {:error, {:source_input_read_failed, child, reason}}}
        end
      end)
    else
      {:error, reason} -> {:error, {:source_input_read_failed, relative, reason}}
    end
  end

  defp validate_required(paths, required) do
    selected = MapSet.new(paths)

    case Enum.find(required, &(not MapSet.member?(selected, &1))) do
      nil -> :ok
      missing -> {:error, {:required_source_input_not_selected, missing}}
    end
  end

  defp read_entries(root, paths) do
    paths
    |> Enum.reduce_while({:ok, []}, fn relative, {:ok, acc} ->
      case read_entry(root, relative) do
        {:ok, entry, _bytes} -> {:cont, {:ok, [entry | acc]}}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, entries} -> {:ok, Enum.sort_by(entries, & &1.path)}
      {:error, _reason} = error -> error
    end
  end

  defp read_entry(root, relative) do
    path = Path.join(root, relative)

    with :ok <- reject_symlink_components(root, relative),
         {:ok, %{type: :regular, mode: mode}} <- File.lstat(path),
         {:ok, bytes} <- File.read(path) do
      if sensitive_source_file?(relative, bytes) do
        {:error, {:sensitive_source_file, relative}}
      else
        {:ok,
         %Entry{
           path: relative,
           sha256: sha256(bytes),
           size: byte_size(bytes),
           executable: Bitwise.band(mode, 0o111) != 0
         }, bytes}
      end
    else
      {:ok, %{type: :symlink}} -> {:error, {:symlink_not_supported, relative}}
      {:ok, _other} -> {:error, {:source_input_not_regular, relative}}
      {:error, {:symlink_not_supported, _path}} = error -> error
      {:error, reason} -> {:error, {:source_input_read_failed, relative, reason}}
    end
  end

  defp copy_entry(root, destination, %Entry{} = expected) do
    with {:ok, actual, bytes} <- read_entry(root, expected.path),
         true <- actual == expected,
         target <- Path.join(destination, expected.path),
         :ok <- File.mkdir_p(Path.dirname(target)),
         :ok <- File.write(target, bytes),
         :ok <- File.chmod(target, if(expected.executable, do: 0o755, else: 0o644)) do
      :ok
    else
      false -> {:error, {:source_input_changed, expected.path}}
      {:error, _reason} = error -> error
    end
  end

  defp reject_symlink_components(root, relative) do
    relative
    |> Path.split()
    |> Enum.reduce_while({:ok, root}, fn component, {:ok, parent} ->
      path = Path.join(parent, component)

      case File.lstat(path) do
        {:ok, %{type: :symlink}} ->
          {:halt, {:error, {:symlink_not_supported, Path.relative_to(path, root)}}}

        {:ok, _other} ->
          {:cont, {:ok, path}}

        {:error, reason} ->
          {:halt, {:error, reason}}
      end
    end)
    |> case do
      {:ok, _path} -> :ok
      {:error, _reason} = error -> error
    end
  end

  defp application_directories(paths) do
    directories = Enum.map(paths, &normalize_relative/1)

    if Enum.all?(directories, &safe_application_directory?/1),
      do: {:ok, directories |> Enum.uniq() |> Enum.sort()},
      else: {:error, :invalid_maintainer_application_directories}
  end

  defp safe_application_directory?(relative) do
    safe_relative_path?(relative) and
      case Path.split(relative) do
        ["apps", app] -> app != ""
        _other -> false
      end
  end

  defp normalize_relative(path) do
    path
    |> String.replace("\\", "/")
    |> String.trim()
    |> String.trim_trailing("/")
  end

  defp safe_relative_path?(path) do
    path != "" and Path.type(path) == :relative and
      not Enum.any?(Path.split(path), &(&1 in ["", ".", ".."]))
  end

  defp sensitive_source_file?(relative, bytes) do
    components = relative |> Path.split() |> Enum.map(&String.downcase/1)
    basename = List.last(components)

    basename in [
      ".env",
      ".npmrc",
      ".netrc",
      "credentials",
      "credentials.json",
      "service-account.json",
      "service_account.json",
      "id_rsa",
      "id_ed25519"
    ] or
      (String.starts_with?(basename, ".env.") and
         basename not in [".env.example", ".env.sample", ".env.template"]) or
      Path.extname(basename) in [".key", ".p12", ".pfx"] or
      Enum.any?(components, &(&1 in [".aws", ".azure", ".kube"])) or
      private_key?(bytes)
  end

  defp private_key?(bytes) do
    Enum.any?(
      ["PRIVATE", "RSA PRIVATE", "EC PRIVATE", "OPENSSH PRIVATE"],
      &(:binary.match(bytes, "-----BEGIN #{&1} KEY-----") != :nomatch)
    )
  end

  defp sha256(bytes), do: :crypto.hash(:sha256, bytes) |> Base.encode16(case: :lower)
end
