defmodule Favn.Dev.Build.RunnerInputs do
  @moduledoc false

  alias Favn.Manifest.Build
  alias Favn.RunnerRelease
  alias Favn.RunnerRelease.{ApplicationFingerprint, ModuleClosure, PluginFingerprint}
  alias Favn.RunnerRelease.RuntimeRoots
  alias Favn.Dev.Build.RunnerConfig

  @required_runner_applications [:favn_core, :favn_runner, :favn_sql_runtime]
  @build_only_favn_applications [:favn, :favn_authoring, :favn_local]

  @type inventory_entry :: %{app: atom(), beam: binary(), path: Path.t()}
  @type plugin_entry :: %{
          module: module(),
          opts: keyword(),
          applications: [atom()],
          child_modules: [module()],
          capabilities: [String.t()]
        }

  @type t :: %{
          descriptor: RunnerRelease.t(),
          current_application: String.t(),
          closure: ModuleClosure.t(),
          inventory: %{String.t() => inventory_entry()},
          applications: [ApplicationFingerprint.t()],
          application_sources: %{String.t() => Path.t()},
          dependency_lock: %{String.t() => term()},
          build_only_applications: [String.t()],
          packaged_config: RunnerConfig.t(),
          plugins: [PluginFingerprint.t()],
          plugin_entries: [plugin_entry()]
        }

  @spec collect(Build.t(), keyword()) :: {:ok, t()} | {:error, term()}
  def collect(%Build{} = build, opts \\ []) when is_list(opts) do
    current_app = Keyword.get(opts, :current_app, Mix.Project.config()[:app])

    with {:ok, authoring_roots} <- FavnAuthoring.runtime_roots(build),
         {:ok, packaged_config} <- RunnerConfig.collect(opts),
         {:ok, runner_build} <- runner_build_config(opts),
         {:ok, favn_source_revision} <- pinned_favn_source_revision(opts),
         {:ok, plugin_entries} <- plugin_entries(opts),
         {:ok, roots} <-
           extend_roots(authoring_roots, plugin_entries, runner_build, packaged_config, opts),
         {:ok, inventory} <- module_inventory(roots, opts),
         available <- Map.new(inventory, fn {name, entry} -> {name, entry.beam} end),
         {:ok, closure} <- ModuleClosure.build(roots, available),
         {:ok, applications, sources} <- application_fingerprints(closure, inventory, opts),
         {:ok, packaged_config} <- RunnerConfig.finalize(packaged_config, applications, opts),
         {:ok, applications} <-
           bind_runner_config(
             applications,
             current_app,
             RunnerConfig.fingerprint(packaged_config)
           ),
         {:ok, plugins} <- plugin_fingerprints(plugin_entries, closure, inventory),
         {:ok, descriptor} <-
           RunnerRelease.new(%{
             schema_version: RunnerRelease.current_schema_version(),
             favn_version: RunnerRelease.current_favn_version(),
             runner_contract_version:
               Favn.Manifest.Compatibility.current_runner_contract_version(),
             elixir_version: System.version(),
             otp_release: to_string(:erlang.system_info(:otp_release)),
             target: RunnerRelease.current_target(),
             runtime_modules: closure.modules,
             runtime_applications: applications,
             plugins: plugins,
             build_profile: "prod",
             build_metadata: build_metadata(opts, favn_source_revision)
           }) do
      build_sources = build_dependency_sources(opts)
      release_sources = Map.merge(build_sources, sources)

      build_only_applications =
        build_sources
        |> Map.keys()
        |> Enum.reject(&Map.has_key?(sources, &1))
        |> Enum.sort()

      with {:ok, dependency_lock} <- release_dependency_lock(release_sources, opts) do
        {:ok,
         %{
           descriptor: descriptor,
           current_application: Atom.to_string(current_app),
           closure: closure,
           inventory: inventory,
           applications: applications,
           application_sources: release_sources,
           dependency_lock: dependency_lock,
           build_only_applications: build_only_applications,
           packaged_config: packaged_config,
           plugins: plugins,
           plugin_entries: plugin_entries
         }}
      end
    end
  end

  @doc false
  @spec select_dependency_lock(map(), [String.t()], %{String.t() => [String.t()]}) ::
          {:ok, %{String.t() => term()}} | {:error, term()}
  def select_dependency_lock(lock, vendored_apps, dependency_graph)
      when is_map(lock) and is_list(vendored_apps) and is_map(dependency_graph) do
    normalized_lock = Map.new(lock, fn {app, entry} -> {to_string(app), entry} end)
    vendored = MapSet.new(vendored_apps)
    selected = expand_dependency_graph(vendored_apps, dependency_graph, MapSet.new())

    unresolved =
      selected
      |> MapSet.difference(vendored)
      |> Enum.reject(&Map.has_key?(normalized_lock, &1))
      |> Enum.sort()

    case unresolved do
      [] ->
        lock_apps =
          selected
          |> MapSet.difference(vendored)
          |> MapSet.to_list()

        {:ok, Map.take(normalized_lock, lock_apps)}

      _missing ->
        {:error, {:runner_dependency_lock_incomplete, unresolved}}
    end
  end

  @spec compare(RunnerRelease.t(), RunnerRelease.t()) ::
          :ok | {:error, {:runner_rebuild_required, [atom()]}}
  def compare(%RunnerRelease{} = expected, %RunnerRelease{} = actual) do
    categories =
      []
      |> changed(:runtime_code, expected.runtime_modules != actual.runtime_modules)
      |> changed(
        :runtime_dependencies,
        expected.runtime_applications != actual.runtime_applications
      )
      |> changed(:plugins, expected.plugins != actual.plugins)
      |> changed(:runtime_toolchain, runtime_toolchain(expected) != runtime_toolchain(actual))
      |> Enum.reverse()

    if categories == [], do: :ok, else: {:error, {:runner_rebuild_required, categories}}
  end

  defp changed(categories, _category, false), do: categories
  defp changed(categories, category, true), do: [category | categories]

  defp runtime_toolchain(descriptor) do
    Map.take(descriptor, [
      :schema_version,
      :favn_version,
      :runner_contract_version,
      :elixir_version,
      :otp_release,
      :target,
      :build_profile
    ])
  end

  defp extend_roots(authoring_roots, plugin_entries, runner_build, packaged_config, opts) do
    current_app = Keyword.get(opts, :current_app, Mix.Project.config()[:app])
    current_app_module = current_application_module(current_app)

    RuntimeRoots.new(%{
      asset_modules: authoring_roots.asset_modules,
      runtime_input_resolver_modules: authoring_roots.runtime_input_resolver_modules,
      plugin_modules: Enum.map(plugin_entries, & &1.module),
      supervised_child_modules: Enum.flat_map(plugin_entries, & &1.child_modules),
      extra_modules:
        configured_dynamic_modules(packaged_config) ++
          current_app_module ++
          runner_build.extra_modules ++ Keyword.get(opts, :extra_modules, []),
      extra_applications:
        authoring_roots.extra_applications ++
          Enum.flat_map(plugin_entries, & &1.applications) ++
          runner_build.extra_applications ++
          Keyword.get(opts, :extra_applications, [])
    })
  end

  defp runner_build_config(opts) do
    value = Keyword.get(opts, :runner_build, Application.get_env(:favn, :runner_build, []))

    with true <- Keyword.keyword?(value),
         [] <- Keyword.keys(value) -- [:extra_modules, :extra_applications],
         {:ok, extra_modules} <- atom_list(Keyword.get(value, :extra_modules, [])),
         {:ok, extra_applications} <- atom_list(Keyword.get(value, :extra_applications, [])) do
      {:ok, %{extra_modules: extra_modules, extra_applications: extra_applications}}
    else
      _invalid -> {:error, :invalid_runner_build_config}
    end
  end

  defp atom_list(value) when is_list(value) do
    if Enum.all?(value, &is_atom/1), do: {:ok, value}, else: {:error, :invalid_atom_list}
  end

  defp atom_list(_value), do: {:error, :invalid_atom_list}

  defp current_application_module(nil), do: []

  defp current_application_module(app) when is_atom(app) do
    case packaged_application_properties(app) do
      {:ok, properties} ->
        case Keyword.get(properties, :mod) do
          {module, _argument} when is_atom(module) -> [module]
          _none -> []
        end

      :error ->
        []
    end
  end

  defp configured_dynamic_modules(packaged_config) do
    packaged_config
    |> Enum.flat_map(fn {_key, value} -> modules_in_term(value) end)
    |> Enum.uniq()
    |> Enum.sort()
  end

  defp modules_in_term(value) when is_atom(value) do
    if String.starts_with?(Atom.to_string(value), "Elixir."), do: [value], else: []
  end

  defp modules_in_term(value) when is_list(value), do: Enum.flat_map(value, &modules_in_term/1)

  defp modules_in_term(value) when is_tuple(value) do
    value |> Tuple.to_list() |> Enum.flat_map(&modules_in_term/1)
  end

  defp modules_in_term(value) when is_map(value) do
    value
    |> Map.to_list()
    |> Enum.flat_map(fn {key, child} -> modules_in_term(key) ++ modules_in_term(child) end)
  end

  defp modules_in_term(_value), do: []

  defp plugin_entries(opts) do
    configured =
      Keyword.get(opts, :runner_plugins, Application.get_env(:favn, :runner_plugins, []))

    if is_list(configured) do
      configured
      |> Enum.reduce_while({:ok, []}, fn entry, {:ok, acc} ->
        case inspect_plugin(entry) do
          {:ok, inspected} -> {:cont, {:ok, [inspected | acc]}}
          {:error, reason} -> {:halt, {:error, reason}}
        end
      end)
      |> reverse_ok()
    else
      {:error, :invalid_runner_plugins}
    end
  end

  defp inspect_plugin(module) when is_atom(module), do: inspect_plugin({module, []})

  defp inspect_plugin({module, opts}) when is_atom(module) and is_list(opts) do
    with {:module, ^module} <- Code.ensure_loaded(module),
         true <- Keyword.keyword?(opts),
         true <- function_exported?(module, :child_specs, 1),
         {:ok, applications} <- plugin_applications(module, opts),
         {:ok, child_modules} <- plugin_child_modules(module, opts) do
      {:ok,
       %{
         module: module,
         opts: opts,
         applications: applications,
         child_modules: child_modules,
         capabilities: plugin_capabilities(module)
       }}
    else
      {:error, _reason} = error -> error
      _invalid -> {:error, {:invalid_runner_plugin, module}}
    end
  end

  defp inspect_plugin(_entry), do: {:error, :invalid_runner_plugins}

  defp plugin_applications(module, opts) do
    if function_exported?(module, :applications, 1) do
      case safe_plugin_call(module, :applications, opts) do
        {:ok, applications} when is_list(applications) ->
          if Enum.all?(applications, &is_atom/1),
            do: {:ok, applications |> Enum.uniq() |> Enum.sort()},
            else: {:error, {:invalid_plugin_applications, module}}

        _invalid ->
          {:error, {:invalid_plugin_applications, module}}
      end
    else
      {:ok, []}
    end
  end

  defp plugin_child_modules(module, opts) do
    case safe_plugin_call(module, :child_specs, opts) do
      {:ok, specs} when is_list(specs) ->
        specs
        |> Enum.reduce_while({:ok, []}, fn spec, {:ok, acc} ->
          case child_module(spec) do
            {:ok, child} -> {:cont, {:ok, [child | acc]}}
            :error -> {:halt, {:error, {:invalid_plugin_child_spec, module}}}
          end
        end)
        |> reverse_ok()

      _invalid ->
        {:error, {:invalid_plugin_result, module}}
    end
  end

  defp safe_plugin_call(module, callback, opts) do
    task =
      Task.async(fn ->
        try do
          apply(module, callback, [opts])
        rescue
          _error -> {:error, :plugin_callback_failed}
        catch
          _kind, _reason -> {:error, :plugin_callback_failed}
        end
      end)

    case Task.yield(task, 5_000) || Task.shutdown(task, :brutal_kill) do
      {:ok, result} -> result
      _timeout -> {:error, :plugin_callback_failed}
    end
  end

  defp child_module(module) when is_atom(module), do: {:ok, module}
  defp child_module({module, _arg}) when is_atom(module), do: {:ok, module}
  defp child_module(%{start: {module, _fun, _args}}) when is_atom(module), do: {:ok, module}
  defp child_module(_spec), do: :error

  defp plugin_capabilities(_module), do: []

  defp module_inventory(roots, opts) do
    case Keyword.fetch(opts, :module_inventory) do
      {:ok, inventory} when is_map(inventory) -> {:ok, inventory}
      {:ok, _invalid} -> {:error, :invalid_module_inventory}
      :error -> read_module_inventory(roots)
    end
  end

  defp read_module_inventory(roots) do
    entries =
      Mix.Project.build_path()
      |> Path.join("lib/*/ebin/*.beam")
      |> Path.wildcard()
      |> Enum.map(fn path ->
        %{
          app: path |> Path.dirname() |> Path.dirname() |> Path.basename() |> String.to_atom(),
          module: Path.basename(path, ".beam"),
          path: path
        }
      end)

    root_names = MapSet.new(RuntimeRoots.module_roots(roots))

    selected_apps =
      entries
      |> Enum.filter(&MapSet.member?(root_names, &1.module))
      |> Enum.map(& &1.app)
      |> Kernel.++(@required_runner_applications)
      |> Kernel.++(Enum.map(roots.extra_applications, &String.to_existing_atom/1))
      |> MapSet.new()

    entries
    |> Enum.filter(&MapSet.member?(selected_apps, &1.app))
    |> Enum.reduce_while({:ok, %{}}, fn entry, {:ok, acc} ->
      with {:ok, beam} <- File.read(entry.path),
           false <- Map.has_key?(acc, entry.module) do
        {:cont,
         {:ok, Map.put(acc, entry.module, %{app: entry.app, beam: beam, path: entry.path})}}
      else
        true ->
          {:halt, {:error, {:duplicate_available_module, entry.module}}}

        {:error, reason} ->
          {:halt, {:error, {:invalid_compiled_module, Path.basename(entry.path), reason}}}
      end
    end)
  end

  defp application_fingerprints(closure, inventory, opts) do
    current_app = Keyword.get(opts, :current_app, Mix.Project.config()[:app])

    dependency_sources =
      opts
      |> dependency_sources()
      |> put_current_application_source(current_app, opts)

    apps =
      closure.modules
      |> Enum.map(&Map.fetch!(inventory, &1.module).app)
      |> Kernel.++(@required_runner_applications)
      |> Kernel.++(List.wrap(current_app))
      |> Kernel.++(Enum.map(closure.extra_applications, &String.to_existing_atom/1))
      |> Kernel.++(imported_applications(closure, inventory))
      |> expand_application_dependencies(dependency_sources)
      |> Enum.uniq()
      |> Enum.sort()

    apps
    |> Enum.reduce_while({:ok, [], %{}}, fn app, {:ok, fingerprints, sources} ->
      with {:ok, version} <- application_version(app),
           {:ok, source} <- application_source(app, dependency_sources),
           {:ok, lock_fingerprint} <-
             application_lock_fingerprint(
               app,
               source,
               Keyword.put(opts, :current_app, current_app)
             ),
           {:ok, fingerprint} <-
             ApplicationFingerprint.new(%{
               application: app,
               version: version,
               lock_fingerprint: lock_fingerprint
             }) do
        {:cont,
         {:ok, [fingerprint | fingerprints], Map.put(sources, Atom.to_string(app), source)}}
      else
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
    |> case do
      {:ok, fingerprints, sources} ->
        {:ok, Enum.sort_by(fingerprints, & &1.application), sources}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp imported_applications(closure, inventory) do
    owners = compiled_module_owners()
    selected = MapSet.new(closure.modules, & &1.module)

    closure.modules
    |> Enum.flat_map(fn fingerprint ->
      entry = Map.fetch!(inventory, fingerprint.module)

      case Favn.RunnerRelease.BeamDigest.metadata(entry.beam) do
        {:ok, metadata} ->
          metadata.imports
          |> Enum.reject(&MapSet.member?(selected, &1))
          |> Enum.flat_map(fn imported ->
            case Map.get(owners, imported) do
              app when is_atom(app) -> [app]
              _missing -> []
            end
          end)

        {:error, _reason} ->
          []
      end
    end)
  end

  defp compiled_module_owners do
    Mix.Project.build_path()
    |> Path.join("lib/*/ebin/*.beam")
    |> Path.wildcard()
    |> Map.new(fn path ->
      module = Path.basename(path, ".beam")
      app = path |> Path.dirname() |> Path.dirname() |> Path.basename() |> String.to_atom()
      {module, app}
    end)
  end

  defp expand_application_dependencies(apps, sources) do
    available =
      sources
      |> Map.keys()
      |> Kernel.++(@required_runner_applications)
      |> MapSet.new()

    roots = MapSet.new(apps)
    expand_application_dependencies(Enum.uniq(apps), available, roots, MapSet.new())
  end

  defp expand_application_dependencies([], _available, _roots, selected),
    do: MapSet.to_list(selected)

  defp expand_application_dependencies([app | rest], available, roots, selected) do
    if MapSet.member?(selected, app) or not MapSet.member?(available, app) do
      expand_application_dependencies(rest, available, roots, selected)
    else
      dependencies =
        app
        |> packaged_application_dependencies()
        |> Enum.filter(&MapSet.member?(available, &1))
        |> Enum.reject(&(&1 in @build_only_favn_applications and not MapSet.member?(roots, &1)))

      expand_application_dependencies(
        rest ++ dependencies,
        available,
        roots,
        MapSet.put(selected, app)
      )
    end
  end

  defp packaged_application_dependencies(app) do
    case packaged_application_properties(app) do
      {:ok, properties} ->
        properties
        |> Keyword.get(:applications, [])
        |> Enum.filter(&is_atom/1)

      :error ->
        []
    end
  end

  defp packaged_application_properties(app) do
    app_file =
      Path.join([Mix.Project.build_path(), "lib", Atom.to_string(app), "ebin", "#{app}.app"])

    case :file.consult(String.to_charlist(app_file)) do
      {:ok, [{:application, ^app, properties}]} -> {:ok, properties}
      _invalid -> :error
    end
  end

  defp dependency_sources(opts) do
    case Keyword.fetch(opts, :dependency_sources) do
      {:ok, sources} when is_map(sources) ->
        sources

      _missing ->
        Mix.Dep.load_and_cache()
        |> Enum.reduce(%{}, fn dep, acc ->
          case Keyword.get(dep.opts, :dest) do
            path when is_binary(path) -> Map.put(acc, dep.app, path)
            _missing -> acc
          end
        end)
    end
  end

  defp build_dependency_sources(opts) do
    case Keyword.fetch(opts, :build_dependency_sources) do
      {:ok, sources} when is_map(sources) ->
        sources

      _missing ->
        Mix.Dep.load_and_cache()
        |> Enum.filter(
          &(Keyword.get(&1.opts, :runtime, true) == false or
              &1.app in @build_only_favn_applications)
        )
        |> Enum.filter(&production_dependency?/1)
        |> Enum.reduce(%{}, fn dep, acc ->
          case Keyword.get(dep.opts, :dest) do
            path when is_binary(path) -> Map.put(acc, Atom.to_string(dep.app), path)
            _missing -> acc
          end
        end)
    end
  end

  defp release_dependency_lock(release_sources, opts) do
    lock = Keyword.get_lazy(opts, :lock, &Mix.Dep.Lock.read/0)

    dependency_graph =
      Mix.Dep.load_and_cache()
      |> Map.new(fn dependency ->
        children =
          dependency.deps
          |> Enum.filter(&production_dependency?/1)
          |> Enum.reject(&Keyword.get(&1.opts, :optional, false))
          |> Enum.map(&Atom.to_string(&1.app))
          |> Enum.uniq()
          |> Enum.sort()

        {Atom.to_string(dependency.app), children}
      end)

    select_dependency_lock(lock, Map.keys(release_sources), dependency_graph)
  end

  defp expand_dependency_graph([], _graph, selected), do: selected

  defp expand_dependency_graph([app | rest], graph, selected) do
    if MapSet.member?(selected, app) do
      expand_dependency_graph(rest, graph, selected)
    else
      children = Map.get(graph, app, [])
      expand_dependency_graph(rest ++ children, graph, MapSet.put(selected, app))
    end
  end

  defp production_dependency?(dep) do
    case Keyword.get(dep.opts, :only) do
      nil -> true
      :prod -> true
      environments when is_list(environments) -> :prod in environments
      _other -> false
    end
  end

  defp application_source(app, sources) do
    case Map.get(sources, app) do
      path when is_binary(path) -> {:ok, path}
      _missing -> {:error, {:runner_application_source_missing, Atom.to_string(app)}}
    end
  end

  defp application_version(app) do
    case packaged_application_properties(app) do
      {:ok, properties} ->
        case Keyword.get(properties, :vsn) do
          nil -> {:error, {:runner_application_version_missing, Atom.to_string(app)}}
          version -> {:ok, to_string(version)}
        end

      :error ->
        {:error, {:runner_application_version_missing, Atom.to_string(app)}}
    end
  end

  defp application_lock_fingerprint(app, source, opts) do
    lock = Keyword.get_lazy(opts, :lock, &Mix.Dep.Lock.read/0)
    current_app = Keyword.get(opts, :current_app)

    bytes =
      cond do
        app == current_app ->
          :erlang.term_to_binary(
            {
              app,
              :customer_runtime_inputs,
              lock |> Enum.sort_by(&elem(&1, 0)),
              customer_runtime_resource_identity(source)
            },
            [:deterministic]
          )

        Map.has_key?(lock, app) ->
          :erlang.term_to_binary(
            {app, Map.fetch!(lock, app), source_tree_identity(source)},
            [:deterministic]
          )

        true ->
          source_tree_identity(source)
      end

    {:ok, sha256(bytes)}
  rescue
    _error -> {:error, {:runner_application_fingerprint_failed, Atom.to_string(app)}}
  end

  defp bind_runner_config(applications, current_app, fingerprint) do
    applications
    |> Enum.reduce_while({:ok, []}, fn application, {:ok, acc} ->
      if application.application == Atom.to_string(current_app) do
        lock_fingerprint =
          sha256(
            :erlang.term_to_binary(
              {application.lock_fingerprint, :runner_config, fingerprint},
              [:deterministic]
            )
          )

        case ApplicationFingerprint.new(%{application | lock_fingerprint: lock_fingerprint}) do
          {:ok, updated} -> {:cont, {:ok, [updated | acc]}}
          {:error, reason} -> {:halt, {:error, reason}}
        end
      else
        {:cont, {:ok, [application | acc]}}
      end
    end)
    |> case do
      {:ok, updated} -> {:ok, Enum.reverse(updated)}
      {:error, reason} -> {:error, reason}
    end
  end

  defp source_tree_identity(source) do
    source
    |> Path.join("**/*")
    |> Path.wildcard(match_dot: true)
    |> Enum.filter(&File.regular?/1)
    |> Enum.reject(&ignored_source_file?(source, &1))
    |> Enum.sort()
    |> Enum.map(fn path ->
      relative = Path.relative_to(path, source)
      stat = File.stat!(path)

      [
        <<byte_size(relative)::32>>,
        relative,
        <<executable_bit(stat.mode)::8, stat.size::64>>,
        File.read!(path)
      ]
    end)
    |> IO.iodata_to_binary()
  end

  defp customer_runtime_resource_identity(source) do
    selected = [
      "mix.exs",
      "config/runtime.exs",
      "priv",
      "c_src",
      "native",
      "include",
      "src"
    ]

    selected
    |> Enum.flat_map(fn entry ->
      path = Path.join(source, entry)

      cond do
        File.regular?(path) -> [path]
        File.dir?(path) -> Path.wildcard(Path.join(path, "**/*"), match_dot: true)
        true -> []
      end
    end)
    |> Enum.filter(&File.regular?/1)
    |> Enum.reject(&ignored_source_file?(source, &1))
    |> Enum.uniq()
    |> Enum.sort()
    |> Enum.map(fn path ->
      relative = Path.relative_to(path, source)
      bytes = File.read!(path)
      mode = File.stat!(path).mode

      [
        <<byte_size(relative)::32>>,
        relative,
        <<executable_bit(mode)::8, byte_size(bytes)::64>>,
        bytes
      ]
    end)
    |> IO.iodata_to_binary()
    |> sha256()
  end

  defp put_current_application_source(sources, nil, _opts), do: sources

  defp put_current_application_source(sources, current_app, opts) do
    source =
      Keyword.get_lazy(opts, :current_app_source, fn ->
        Mix.Project.project_file() |> Path.expand() |> Path.dirname()
      end)

    Map.put(sources, current_app, source)
  end

  defp ignored_source_file?(source, path) do
    case path |> Path.relative_to(source) |> Path.split() do
      [top | _rest] -> top in [".git", ".favn", "_build", "deps", "test"]
      [] -> false
    end
  end

  defp executable_bit(mode) do
    if Bitwise.band(mode, 0o111) == 0, do: 0, else: 1
  end

  defp plugin_fingerprints(entries, closure, inventory) do
    module_names = MapSet.new(closure.modules, & &1.module)

    entries
    |> Enum.reduce_while({:ok, []}, fn entry, {:ok, acc} ->
      modules =
        [entry.module | entry.child_modules]
        |> Enum.map(&Atom.to_string/1)
        |> Enum.filter(&MapSet.member?(module_names, &1))
        |> Enum.uniq()
        |> Enum.sort()

      app = Map.fetch!(inventory, Atom.to_string(entry.module)).app

      with {:ok, version} <- application_version(app),
           {:ok, fingerprint} <-
             PluginFingerprint.new(%{
               plugin: entry.module,
               version: version,
               modules: modules,
               capabilities: entry.capabilities
             }) do
        {:cont, {:ok, [fingerprint | acc]}}
      else
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
    |> reverse_ok()
  end

  defp pinned_favn_source_revision(opts) do
    sources = dependency_sources(opts)

    case Map.get(sources, :favn_core) do
      source when is_binary(source) -> verify_favn_checkout(source, opts)
      _missing -> {:error, :favn_source_revision_unidentifiable}
    end
  end

  @doc false
  @spec verify_favn_checkout(Path.t(), keyword()) :: {:ok, String.t()} | {:error, atom()}
  def verify_favn_checkout(source, opts \\ []) when is_binary(source) and is_list(opts) do
    with {root, 0} <- git(source, ["rev-parse", "--show-toplevel"]),
         root = String.trim(root),
         {revision, 0} <- git(root, ["rev-parse", "--verify", "HEAD"]),
         revision = String.trim(revision),
         true <- revision =~ ~r/\A[0-9a-f]{40}\z/,
         :ok <- verify_pinned_checkout(root, opts) do
      {:ok, revision}
    else
      {:error, _reason} = error -> error
      _invalid -> {:error, :favn_source_revision_unidentifiable}
    end
  end

  defp verify_pinned_checkout(root, opts) do
    if Mix.env() == :test and Keyword.get(opts, :allow_unpinned_favn, false) do
      :ok
    else
      with {_branch, 1} <- git(root, ["symbolic-ref", "-q", "HEAD"]),
           {status, 0} <- git(root, ["status", "--porcelain", "--untracked-files=all"]),
           true <- String.trim(status) == "" do
        :ok
      else
        _floating_or_dirty -> {:error, :favn_checkout_not_pinned}
      end
    end
  end

  defp git(directory, args) do
    System.cmd("git", ["-C", directory | args], stderr_to_stdout: true)
  rescue
    _error -> {"", 1}
  end

  defp build_metadata(opts, favn_source_revision) do
    %{}
    |> maybe_put("source_revision", Keyword.get(opts, :source_revision, git_revision()))
    |> Map.put("favn_source_revision", favn_source_revision)
  end

  defp git_revision do
    case System.cmd("git", ["rev-parse", "--verify", "HEAD"], stderr_to_stdout: true) do
      {revision, 0} -> String.trim(revision)
      _error -> nil
    end
  end

  defp maybe_put(map, _key, nil), do: map
  defp maybe_put(map, key, value), do: Map.put(map, key, value)

  defp sha256(value), do: :crypto.hash(:sha256, value) |> Base.encode16(case: :lower)

  defp reverse_ok({:ok, values}), do: {:ok, Enum.reverse(values)}
  defp reverse_ok({:error, reason}), do: {:error, reason}
end
