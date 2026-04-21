defmodule Favn.Dev.Build.Runner do
  @moduledoc """
  Project-local runner build target.

  This target currently builds the manifest and compiled user modules from the
  current Mix project root (`File.cwd!/0`). A different `:root_dir` is not
  accepted for project selection.
  """

  alias Favn.Dev.Install
  alias Favn.Dev.Paths
  alias Favn.Dev.State

  @schema_version 1
  @target "runner"

  @type root_opt :: [root_dir: Path.t()]

  @spec run(root_opt()) :: {:ok, map()} | {:error, term()}
  def run(opts \\ []) when is_list(opts) do
    with :ok <- ensure_project_root(opts),
         :ok <- Install.ensure_ready(opts),
         :ok <- State.ensure_layout(opts),
         :ok <- force_compile(opts),
         {:ok, build} <- FavnAuthoring.build_manifest(),
         {:ok, version} <- FavnAuthoring.pin_manifest_version(build.manifest),
         {:ok, serialized_manifest} <- FavnAuthoring.serialize_manifest(version.manifest),
         {build_id, root_dir} <- {build_id(), Paths.root_dir(opts)},
         build_dir <- Paths.build_runner_dir(root_dir, build_id),
         dist_dir <- Paths.dist_runner_dir(root_dir, build_id),
         :ok <- File.mkdir_p(build_dir),
         :ok <- File.mkdir_p(dist_dir),
         modules <- user_modules(version.manifest),
         copied_modules <- copy_module_beams(modules, Path.join(dist_dir, "ebin")),
         plugins <- selected_plugins(root_dir),
         :ok <- write_manifest_cache(version, serialized_manifest, opts),
         build_json <- build_json(build_id, version, modules, plugins, copied_modules, opts),
         metadata_json <- metadata_json(build_id, version, modules, plugins, copied_modules, opts),
         :ok <- write_json(Path.join(build_dir, "build.json"), build_json),
         :ok <- write_json(Path.join(dist_dir, "metadata.json"), metadata_json),
         :ok <- File.write(Path.join(dist_dir, "manifest.json"), serialized_manifest <> "\n") do
      {:ok, %{build_id: build_id, build_dir: build_dir, dist_dir: dist_dir}}
    end
  end

  defp ensure_project_root(opts) do
    requested_root = opts |> Paths.root_dir() |> Path.expand()
    current_root = File.cwd!() |> Path.expand()

    if requested_root == current_root or Keyword.get(opts, :skip_project_root_check, false) do
      :ok
    else
      {:error, {:unsupported_root_dir, requested_root, current_root}}
    end
  end

  defp force_compile(opts) do
    if Keyword.get(opts, :skip_compile, false) do
      :ok
    else
      :ok = Mix.Task.reenable("compile")
      _ = Mix.Task.run("compile", ["--force"])
      :ok
    end
  end

  defp user_modules(manifest) do
    asset_modules = manifest_modules(manifest, :assets)
    pipeline_modules = manifest_modules(manifest, :pipelines)
    schedule_modules = manifest_modules(manifest, :schedules)

    (asset_modules ++ pipeline_modules ++ schedule_modules)
    |> Enum.uniq()
    |> Enum.sort_by(&Atom.to_string/1)
  end

  defp manifest_modules(manifest, key) do
    manifest
    |> map_get(key)
    |> List.wrap()
    |> Enum.map(&map_get(&1, :module))
    |> Enum.filter(&is_atom/1)
  end

  defp map_get(map, key) when is_map(map) and is_atom(key) do
    Map.get(map, key) || Map.get(map, Atom.to_string(key))
  end

  defp map_get(_other, _key), do: nil

  defp copy_module_beams(modules, target_dir) do
    :ok = File.mkdir_p(target_dir)

    Enum.reduce(modules, [], fn module, copied ->
      with {:module, _loaded} <- Code.ensure_loaded(module),
           beam when is_list(beam) <- :code.which(module),
           beam_path <- List.to_string(beam),
           true <- String.ends_with?(beam_path, ".beam"),
           true <- File.exists?(beam_path),
           destination <- Path.join(target_dir, Path.basename(beam_path)),
           :ok <- File.cp(beam_path, destination) do
        [Atom.to_string(module) | copied]
      else
        _ -> copied
      end
    end)
    |> Enum.reverse()
  end

  defp selected_plugins(root_dir) do
    configured =
      Application.get_env(:favn, :runner_plugins, [])
      |> List.wrap()
      |> Enum.map(&to_string/1)

    known =
      ["favn_duckdb"]
      |> Enum.filter(fn app_name ->
        File.dir?(Path.join(root_dir, "apps/#{app_name}"))
      end)

    (configured ++ known)
    |> Enum.uniq()
    |> Enum.sort()
  end

  defp write_manifest_cache(version, serialized_manifest, opts) do
    root_dir = Paths.root_dir(opts)
    path = Path.join(Paths.manifest_cache_dir(root_dir), "#{version.manifest_version_id}.json")
    File.mkdir_p!(Path.dirname(path))
    File.write(path, serialized_manifest <> "\n")
  end

  defp build_json(build_id, version, modules, plugins, copied_modules, opts) do
    base(build_id, version, opts)
    |> Map.put("phase", "build")
    |> Map.put("target", @target)
    |> Map.put("project_root", Paths.root_dir(opts))
    |> Map.put("user_modules", Enum.map(modules, &Atom.to_string/1))
    |> Map.put("copied_module_beams", copied_modules)
    |> Map.put("plugins", plugins)
  end

  defp metadata_json(build_id, version, modules, plugins, copied_modules, opts) do
    base(build_id, version, opts)
    |> Map.put("phase", "dist")
    |> Map.put("target", @target)
    |> Map.put("compatibility", %{
      "manifest_schema_version" => version.schema_version,
      "runner_contract_version" => version.runner_contract_version,
      "serialization_format" => version.serialization_format
    })
    |> Map.put("manifest", %{
      "manifest_version_id" => version.manifest_version_id,
      "content_hash" => version.content_hash
    })
    |> Map.put("plugins", plugins)
    |> Map.put("user_modules", Enum.map(modules, &Atom.to_string/1))
    |> Map.put("copied_module_beams", copied_modules)
    |> Map.put("required_env", ["FAVN_ORCHESTRATOR_BASE_URL", "FAVN_ORCHESTRATOR_SERVICE_TOKEN"])
  end

  defp base(build_id, version, opts) do
    now = DateTime.utc_now() |> DateTime.to_iso8601()

    %{
      "schema_version" => @schema_version,
      "build_id" => build_id,
      "built_at" => now,
      "favn_version" => to_string(Application.spec(:favn, :vsn) || "unknown"),
      "install_fingerprint" => read_install_fingerprint(opts),
      "elixir_version" => System.version(),
      "otp_release" => :erlang.system_info(:otp_release) |> List.to_string(),
      "manifest_version_id" => version.manifest_version_id
    }
  end

  defp read_install_fingerprint(opts) do
    case State.read_install(opts) do
      {:ok, %{"fingerprint" => fingerprint}} when is_map(fingerprint) -> fingerprint
      _ -> %{}
    end
  end

  defp write_json(path, map) when is_map(map) do
    encoded = JSON.encode_to_iodata!(map)
    File.write(path, [encoded, "\n"])
  end

  defp build_id do
    stamp = DateTime.utc_now() |> Calendar.strftime("%Y%m%d%H%M%S")
    unique = System.unique_integer([:positive])
    "rb_#{stamp}_#{unique}"
  end
end
