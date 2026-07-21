defmodule Favn.Dev.Build.ControlPlaneInputs do
  @moduledoc false

  alias Favn.ControlPlaneBuild
  alias Favn.Manifest.ContractVersions

  @builder_image "hexpm/elixir:1.20.2-erlang-28.3.3-debian-bookworm-20260713-slim@sha256:874b36d3e432c42a4f78e12fbe251c5e6c3b1342c8f1072e25dc418b823c31ba"
  @runtime_image "debian:bookworm-slim@sha256:63a496b5d3b99214b39f5ed70eb71a61e590a77979c79cbee4faf991f8c0783e"
  @target "linux/amd64"
  @target_os "debian/bookworm-slim"
  @elixir_version "1.20.2"
  @otp_version "28.3.3"
  @ignored_directory_components ~w(.git .favn _build deps test docs doc node_modules)
  @assembly_inputs [
    "apps/favn_local/lib/favn/dev/build/artifact.ex",
    "apps/favn_local/lib/favn/dev/build/control_plane.ex",
    "apps/favn_local/lib/favn/dev/build/control_plane_inputs.ex"
  ]
  @project_modules [
    FavnCore.MixProject,
    FavnOrchestrator.MixProject,
    FavnStoragePostgres.MixProject,
    FavnView.MixProject
  ]
  @project_files [
    {FavnCore.MixProject, "apps/favn_core/mix.exs"},
    {FavnOrchestrator.MixProject, "apps/favn_orchestrator/mix.exs"},
    {FavnStoragePostgres.MixProject, "apps/favn_storage_postgres/mix.exs"},
    {FavnView.MixProject, "apps/favn_view/mix.exs"}
  ]

  @type collected :: %{
          required(:descriptor) => ControlPlaneBuild.descriptor(),
          required(:source_paths) => [String.t()],
          required(:dependency_lock_apps) => [String.t()],
          required(:dependency_lock) => %{required(String.t()) => term()},
          required(:release_version) => String.t()
        }

  @spec builder_image() :: String.t()
  def builder_image, do: @builder_image

  @spec runtime_image() :: String.t()
  def runtime_image, do: @runtime_image

  @spec target() :: String.t()
  def target, do: @target

  @spec collect(Path.t()) :: {:ok, collected()} | {:error, term()}
  def collect(root_dir) when is_binary(root_dir) do
    root_dir = Path.expand(root_dir)

    with :ok <- validate_repository_root(root_dir),
         {:ok, applications, dependency_roots} <- release_contract(root_dir),
         :ok <- validate_project_dependency_roots(root_dir, applications, dependency_roots),
         {:ok, source_paths} <- source_paths(root_dir, applications),
         {:ok, source_records} <-
           source_records(root_dir, Enum.sort(source_paths ++ @assembly_inputs)),
         {:ok, lock} <- read_lock(root_dir),
         {:ok, lock_records, lock_apps} <- lock_records(lock, dependency_roots),
         {:ok, dependency_lock} <- selected_lock(lock, lock_apps),
         {:ok, control_plane_version} <- control_plane_version(root_dir),
         {:ok, release_version} <- release_version(root_dir),
         {:ok, descriptor} <-
           ControlPlaneBuild.new(
             source_records ++ lock_records,
             identity(applications, lock_apps, control_plane_version)
           ) do
      {:ok,
       %{
         descriptor: descriptor,
         source_paths: source_paths,
         dependency_lock_apps: lock_apps,
         dependency_lock: dependency_lock,
         release_version: release_version
       }}
    end
  end

  @doc false
  @spec lock_records(map(), [atom() | String.t()]) ::
          {:ok, [ControlPlaneBuild.input_record()], [String.t()]} | {:error, term()}
  def lock_records(lock, roots) when is_map(lock) and is_list(roots) do
    normalized_lock = Map.new(lock, fn {name, entry} -> {to_string(name), entry} end)
    queue = roots |> Enum.map(&to_string/1) |> Enum.uniq() |> Enum.sort()

    with {:ok, selected} <- resolve_lock_closure(normalized_lock, queue, MapSet.new()) do
      apps = selected |> MapSet.to_list() |> Enum.sort()

      records =
        Enum.map(apps, fn app ->
          bytes = normalized_lock_entry(app, Map.fetch!(normalized_lock, app))
          %{path: "mix.lock/#{app}", sha256: sha256(bytes), size: byte_size(bytes)}
        end)

      {:ok, records, apps}
    end
  end

  defp validate_repository_root(root_dir) do
    required = [
      "mix.lock",
      "config/config.exs",
      "rel/control_plane/release.exs",
      "rel/control_plane/context.mix.exs",
      "rel/control_plane/Dockerfile"
    ]

    case Enum.find(required, &(not File.regular?(Path.join(root_dir, &1)))) do
      nil -> :ok
      path -> {:error, {:not_favn_repository_root, path}}
    end
  end

  defp release_contract(root_dir) do
    release_file = Path.join(root_dir, "rel/control_plane/release.exs")

    with {:module, FavnControlPlane.Release} <-
           Code.ensure_compiled(FavnControlPlane.Release) do
      {:ok, FavnControlPlane.Release.applications(), FavnControlPlane.Release.dependency_roots()}
    else
      _unavailable ->
        case Code.require_file(release_file) do
          [{FavnControlPlane.Release, _binary}] ->
            {:ok, FavnControlPlane.Release.applications(),
             FavnControlPlane.Release.dependency_roots()}

          _invalid ->
            {:error, :control_plane_release_contract_unavailable}
        end
    end
  end

  defp validate_project_dependency_roots(root_dir, applications, expected) do
    with :ok <- ensure_project_modules(root_dir) do
      dependencies =
        @project_modules
        |> Enum.flat_map(fn project -> project.project() |> Keyword.fetch!(:deps) end)

      validate_dependency_roots(dependencies, applications, expected)
    end
  end

  @doc false
  @spec validate_dependency_roots([tuple()], [atom()], [atom()]) :: :ok | {:error, term()}
  def validate_dependency_roots(dependencies, applications, expected)
      when is_list(dependencies) and is_list(applications) and is_list(expected) do
    actual =
      dependencies
      |> Enum.filter(&production_dependency?/1)
      |> Enum.map(&dependency_app/1)
      |> Enum.reject(&(&1 in applications))
      |> Enum.uniq()
      |> Enum.sort()

    if actual == Enum.sort(expected) do
      :ok
    else
      {:error,
       {:control_plane_dependency_roots_mismatch,
        %{expected: Enum.sort(expected), actual: actual}}}
    end
  end

  defp ensure_project_modules(root_dir) do
    Mix.start()

    Enum.reduce_while(@project_files, :ok, fn {module, relative}, :ok ->
      if Code.ensure_loaded?(module) do
        {:cont, :ok}
      else
        path = Path.join(root_dir, relative)

        try do
          Code.require_file(path)

          if Code.ensure_loaded?(module) do
            {:cont, :ok}
          else
            {:halt, {:error, {:control_plane_project_module_unavailable, relative}}}
          end
        rescue
          _exception ->
            {:halt, {:error, {:control_plane_project_module_unavailable, relative}}}
        end
      end
    end)
  end

  defp production_dependency?(dependency) do
    case dependency_options(dependency) |> Keyword.get(:only) do
      nil -> true
      environment when is_atom(environment) -> environment == :prod
      environments when is_list(environments) -> :prod in environments
    end
  end

  defp dependency_options({_app, options}) when is_list(options), do: options
  defp dependency_options({_app, _requirement}), do: []
  defp dependency_options({_app, _requirement, options}) when is_list(options), do: options
  defp dependency_app({app, _value}), do: app
  defp dependency_app({app, _requirement, _options}), do: app

  defp source_paths(root_dir, applications) do
    roots =
      Enum.flat_map(applications, fn application ->
        app_dir = "apps/#{application}"

        ["#{app_dir}/lib", "#{app_dir}/priv"] ++
          if(application == :favn_view, do: ["#{app_dir}/assets"], else: [])
      end) ++ ["rel/control_plane"]

    with {:ok, discovered} <- collect_regular_files(root_dir, roots) do
      explicit =
        Enum.map(applications, &"apps/#{&1}/mix.exs") ++
          ["config/config.exs", "config/prod.exs", "config/runtime.exs"]

      paths =
        (explicit ++ discovered)
        |> Enum.filter(&context_input_path?/1)
        |> Enum.uniq()
        |> Enum.sort()

      case Enum.find(paths, &(not safe_relative_path?(&1))) do
        nil -> {:ok, paths}
        unsafe -> {:error, {:unsafe_control_plane_input_path, unsafe}}
      end
    end
  end

  defp collect_regular_files(root_dir, roots) do
    Enum.reduce_while(roots, {:ok, []}, fn relative, {:ok, acc} ->
      case regular_files(root_dir, relative) do
        {:ok, paths} -> {:cont, {:ok, paths ++ acc}}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
  end

  @doc false
  @spec regular_files(Path.t(), Path.t()) :: {:ok, [Path.t()]} | {:error, term()}
  def regular_files(root_dir, relative) when is_binary(root_dir) and is_binary(relative) do
    if safe_relative_path?(relative) do
      walk_regular_files(Path.expand(root_dir), relative)
    else
      {:error, {:unsafe_control_plane_input_path, relative}}
    end
  end

  defp walk_regular_files(root_dir, relative) do
    path = Path.join(root_dir, relative)

    if ignored_directory_path?(relative) do
      {:ok, []}
    else
      case File.lstat(path) do
        {:ok, %{type: :regular}} ->
          {:ok, [relative]}

        {:ok, %{type: :directory}} ->
          with {:ok, entries} <- File.ls(path) do
            entries
            |> Enum.sort()
            |> Enum.reduce_while({:ok, []}, fn entry, {:ok, acc} ->
              child = Path.join(relative, entry)

              case walk_regular_files(root_dir, child) do
                {:ok, paths} -> {:cont, {:ok, paths ++ acc}}
                {:error, _reason} = error -> {:halt, error}
              end
            end)
          else
            {:error, reason} ->
              {:error, {:control_plane_input_read_failed, relative, reason}}
          end

        {:ok, %{type: :symlink}} ->
          {:error, {:control_plane_input_symlink, relative}}

        {:ok, _other} ->
          {:error, {:control_plane_input_not_regular, relative}}

        {:error, :enoent} ->
          {:ok, []}

        {:error, reason} ->
          {:error, {:control_plane_input_read_failed, relative, reason}}
      end
    end
  end

  defp ignored_context_input?("apps/favn_view/priv/static/assets/" <> _rest), do: true

  defp ignored_context_input?("apps/favn_view/priv/static/" <> relative) do
    basename = Path.basename(relative)

    basename == "cache_manifest.json" or
      Path.extname(basename) in [".gz", ".br"] or
      Regex.match?(~r/-[0-9a-fA-F]{32}/, basename)
  end

  defp ignored_context_input?(path) do
    basename = Path.basename(path)

    String.ends_with?(path, ".md") or String.starts_with?(basename, ".env") or
      basename == "erl_crash.dump" or
      Enum.any?(Path.split(path), &(&1 in @ignored_directory_components))
  end

  @doc false
  @spec context_input_path?(Path.t()) :: boolean()
  def context_input_path?(path) when is_binary(path), do: not ignored_context_input?(path)

  defp ignored_directory_path?(path) do
    Enum.any?(Path.split(path), &(&1 in @ignored_directory_components))
  end

  defp source_records(root_dir, paths) do
    paths
    |> Enum.reduce_while({:ok, []}, fn relative, {:ok, acc} ->
      path = Path.join(root_dir, relative)

      with :ok <- reject_symlink_components(root_dir, relative) do
        case File.lstat(path) do
          {:ok, %{type: :regular}} ->
            case File.read(path) do
              {:ok, bytes} ->
                record = %{path: relative, sha256: sha256(bytes), size: byte_size(bytes)}
                {:cont, {:ok, [record | acc]}}

              {:error, reason} ->
                {:halt, {:error, {:control_plane_input_read_failed, relative, reason}}}
            end

          {:ok, %{type: :symlink}} ->
            {:halt, {:error, {:control_plane_input_symlink, relative}}}

          {:ok, _other} ->
            {:halt, {:error, {:control_plane_input_not_regular, relative}}}

          {:error, reason} ->
            {:halt, {:error, {:control_plane_input_read_failed, relative, reason}}}
        end
      else
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
    |> case do
      {:ok, records} -> {:ok, Enum.reverse(records)}
      {:error, _reason} = error -> error
    end
  end

  defp reject_symlink_components(root_dir, relative) do
    relative
    |> Path.split()
    |> Enum.reduce_while({:ok, Path.expand(root_dir)}, fn component, {:ok, parent} ->
      path = Path.join(parent, component)

      case File.lstat(path) do
        {:ok, %{type: :symlink}} ->
          traversed = Path.relative_to(path, root_dir)
          {:halt, {:error, {:control_plane_input_symlink, traversed}}}

        {:ok, _other} ->
          {:cont, {:ok, path}}

        {:error, reason} ->
          {:halt, {:error, {:control_plane_input_read_failed, relative, reason}}}
      end
    end)
    |> case do
      {:ok, _path} -> :ok
      {:error, _reason} = error -> error
    end
  end

  defp read_lock(root_dir) do
    path = Path.join(root_dir, "mix.lock")

    Mix.start()

    case Mix.Dep.Lock.read(path) do
      lock when is_map(lock) -> {:ok, lock}
      _invalid -> {:error, :invalid_mix_lock}
    end
  rescue
    _exception -> {:error, :invalid_mix_lock}
  end

  defp selected_lock(lock, lock_apps) do
    normalized = Map.new(lock, fn {app, entry} -> {to_string(app), entry} end)

    if Enum.all?(lock_apps, &Map.has_key?(normalized, &1)) do
      {:ok, Map.take(normalized, lock_apps)}
    else
      {:error, :control_plane_dependency_lock_incomplete}
    end
  end

  defp resolve_lock_closure(_lock, [], selected), do: {:ok, selected}

  defp resolve_lock_closure(lock, [app | rest], selected) do
    if MapSet.member?(selected, app) do
      resolve_lock_closure(lock, rest, selected)
    else
      case Map.fetch(lock, app) do
        {:ok, entry} ->
          with {:ok, dependencies} <- required_lock_dependencies(entry) do
            resolve_lock_closure(
              lock,
              Enum.sort(dependencies) ++ rest,
              MapSet.put(selected, app)
            )
          end

        :error ->
          {:error, {:unresolved_control_plane_dependency, app}}
      end
    end
  end

  defp required_lock_dependencies(
         {:hex, _package, _version, _checksum, _managers, deps, _repo, _outer}
       )
       when is_list(deps) do
    deps
    |> Enum.reduce_while({:ok, []}, fn
      {app, _requirement, options}, {:ok, acc} when is_list(options) ->
        if Keyword.get(options, :optional, false) do
          {:cont, {:ok, acc}}
        else
          {:cont, {:ok, [to_string(app) | acc]}}
        end

      _invalid, _acc ->
        {:halt, {:error, :invalid_control_plane_lock_dependency}}
    end)
  end

  defp required_lock_dependencies({:git, _repository, _revision, options})
       when is_list(options),
       do: {:ok, []}

  defp required_lock_dependencies(_entry),
    do: {:error, :unsupported_control_plane_lock_entry}

  defp normalized_lock_entry(app, entry) do
    app <>
      "\n" <>
      inspect(entry,
        pretty: false,
        limit: :infinity,
        printable_limit: :infinity,
        width: :infinity,
        charlists: :as_lists
      ) <>
      "\n"
  end

  defp control_plane_version(root_dir) do
    version_from_mix(
      root_dir,
      "apps/favn_orchestrator/mix.exs",
      :control_plane_version_unavailable
    )
  end

  @doc false
  @spec release_version(Path.t()) :: {:ok, String.t()} | {:error, :release_version_unavailable}
  def release_version(root_dir) when is_binary(root_dir) do
    version_from_mix(root_dir, "apps/favn/mix.exs", :release_version_unavailable)
  end

  defp version_from_mix(root_dir, relative, error) do
    path = Path.join(root_dir, relative)

    with {:ok, source} <- File.read(path),
         [version] <- Regex.run(~r/version:\s*"([^"]+)"/, source, capture: :all_but_first) do
      {:ok, version}
    else
      _invalid -> {:error, error}
    end
  end

  defp identity(applications, lock_apps, control_plane_version) do
    %{
      "applications" => Enum.map(applications, &Atom.to_string/1),
      "builder_image" => @builder_image,
      "dependency_lock_apps" => lock_apps,
      "elixir_version" => @elixir_version,
      "control_plane_version" => control_plane_version,
      "manifest_schema_version" => ContractVersions.manifest_schema_version(),
      "otp_version" => @otp_version,
      "runner_contract_version" => ContractVersions.runner_contract_version(),
      "runtime_image" => @runtime_image,
      "target" => @target,
      "target_os" => @target_os
    }
  end

  defp safe_relative_path?(path) do
    is_binary(path) and path != "" and Path.type(path) == :relative and
      not String.contains?(path, "\\") and
      not Enum.any?(Path.split(path), &(&1 in ["", ".", ".."]))
  end

  defp sha256(bytes), do: :crypto.hash(:sha256, bytes) |> Base.encode16(case: :lower)
end
