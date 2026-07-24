defmodule FavnAuthoring do
  @moduledoc """
  Authoring implementation facade for compilation and manifest generation.

  This facade intentionally avoids runtime and orchestrator concerns.

  ## Read This Module When

  - you need authoring ownership rather than the public facade
  - you need to trace manifest generation, pipeline resolution, or planning
  - you are debugging delegation from `Favn`

  ## Related Modules

  - `Favn`: public facade
  - `Favn.ModuleDiscovery`: app-scoped `:all` discovery for assets, pipelines,
    schedules, and connections
  - `FavnLocal`: Docker-free source-development lifecycle
  - `Favn.Manifest.Generator`, `Favn.Pipeline.Resolver`, `Favn.Assets.Planner`:
    deeper internals
  """

  alias Favn.Asset
  alias Favn.Assets.Compiler
  alias Favn.Assets.Planner
  alias Favn.Manifest
  alias Favn.Manifest.Build
  alias Favn.Manifest.Compatibility
  alias Favn.Manifest.Environment
  alias Favn.Manifest.Generator
  alias Favn.Manifest.Publication
  alias Favn.Manifest.Serializer
  alias Favn.Manifest.Version
  alias Favn.ModuleDiscovery
  alias Favn.Pipeline
  alias Favn.Pipeline.Resolver
  alias Favn.Triggers.Schedules, as: PipelineSchedules

  @type asset_ref :: Favn.Ref.t()
  @type asset :: Asset.t()
  @type asset_error :: :not_asset_module | :asset_not_found
  @type dependencies_mode :: :all | :none
  @type manifest_opts :: [
          asset_modules: [module()] | :all,
          pipeline_modules: [module()] | :all,
          schedule_modules: [module()] | :all,
          connection_modules: [module()] | :all,
          runner_release_id: String.t()
        ]

  @doc """
  Returns true when a module compiles as an asset module.
  """
  @spec asset_module?(module()) :: boolean()
  def asset_module?(module) when is_atom(module) do
    match?({:ok, _assets}, Compiler.compile_module_assets(module))
  end

  def asset_module?(_other), do: false

  @doc """
  Compiles all configured asset modules.
  """
  @spec list_assets() :: {:ok, [asset()]} | {:error, term()}
  def list_assets do
    with {:ok, modules} <- default_asset_modules() do
      list_assets(modules)
    end
  end

  @doc """
  Compiles and returns assets for one module or a list of modules.
  """
  @spec list_assets(module() | [module()]) :: {:ok, [asset()]} | {:error, term()}
  def list_assets(module) when is_atom(module) do
    with {:ok, modules} <- default_asset_modules(),
         true <- module in modules do
      list_assets_for_module_from_catalog(module, modules)
    else
      _ ->
        Compiler.compile_module_assets(module)
    end
  end

  def list_assets(modules) when is_list(modules) do
    case Generator.build_catalog(asset_modules: modules) do
      {:ok, catalog} ->
        {:ok, catalog.assets}

      {:error, {:asset_compile_failed, module, reason}} ->
        {:error, {module, reason}}

      {:error, _reason} = error ->
        error
    end
  end

  def list_assets(_invalid), do: {:error, :invalid_asset_modules}

  defp list_assets_for_module_from_catalog(module, modules)
       when is_atom(module) and is_list(modules) do
    with {:ok, assets} <- list_assets(modules) do
      case Enum.filter(assets, &(&1.module == module)) do
        [] -> {:error, :not_asset_module}
        module_assets -> {:ok, module_assets}
      end
    end
  end

  @doc """
  Fetches one compiled asset by module shorthand or canonical ref.
  """
  @spec get_asset(module() | asset_ref()) :: {:ok, asset()} | {:error, asset_error() | term()}
  def get_asset(module) when is_atom(module) do
    get_asset({module, :asset})
  end

  def get_asset({module, name} = ref) when is_atom(module) and is_atom(name) do
    with {:ok, assets} <- list_assets(module),
         %Asset{} = asset <- Enum.find(assets, &(&1.ref == ref)) do
      {:ok, asset}
    else
      {:error, :not_asset_module} ->
        {:error, :not_asset_module}

      {:error, {^module, _reason}} ->
        {:error, :not_asset_module}

      {:error, {reason_tag, ^module}} when is_atom(reason_tag) ->
        {:error, :not_asset_module}

      {:error, {other_module, reason}} when is_atom(other_module) ->
        {:error, {other_module, reason}}

      {:error, _reason} ->
        {:error, :not_asset_module}

      nil ->
        {:error, :asset_not_found}
    end
  end

  def get_asset(_invalid), do: {:error, :asset_not_found}

  @doc """
  Fetches a compiled pipeline definition.
  """
  @spec get_pipeline(module()) :: {:ok, Favn.Pipeline.Definition.t()} | {:error, term()}
  def get_pipeline(module) when is_atom(module), do: Pipeline.fetch(module)
  def get_pipeline(_invalid), do: {:error, :not_pipeline_module}

  @doc """
  Generates a manifest from explicit modules or app config and an
  operator-owned runner release ID.
  """
  @spec generate_manifest(manifest_opts()) :: {:ok, Manifest.t()} | {:error, term()}
  def generate_manifest(opts \\ []) when is_list(opts) do
    with {:ok, opts} <- with_default_manifest_modules(opts),
         {:ok, opts} <- with_manifest_environment(opts) do
      Generator.generate(opts)
    end
  end

  @doc """
  Generates a manifest build output with build-only metadata separated from
  canonical runtime payload data. The required runner release ID is supplied
  explicitly by the user or their CI system.
  """
  @spec build_manifest(manifest_opts()) :: {:ok, Build.t()} | {:error, term()}
  def build_manifest(opts \\ []) when is_list(opts) do
    with {:ok, opts} <- with_default_manifest_modules(opts),
         {:ok, opts} <- with_manifest_environment(opts) do
      Generator.build(opts)
    end
  end

  @doc """
  Serializes a canonical manifest payload to stable JSON bytes.
  """
  @spec serialize_manifest(map() | struct()) :: {:ok, binary()} | {:error, term()}
  def serialize_manifest(manifest), do: Serializer.encode_manifest(manifest)

  @doc """
  Computes the content hash for a canonical manifest payload.
  """
  @spec hash_manifest(map() | struct()) :: {:ok, String.t()} | {:error, term()}
  def hash_manifest(manifest) do
    with {:ok, version} <- Version.new(manifest) do
      {:ok, version.content_hash}
    end
  end

  @doc """
  Validates manifest schema and runner contract compatibility.
  """
  @spec validate_manifest_compatibility(map() | struct()) :: :ok | {:error, term()}
  def validate_manifest_compatibility(manifest), do: Compatibility.validate_manifest(manifest)

  @doc """
  Pins a manifest into an immutable manifest-version envelope.
  """
  @spec pin_manifest_version(map() | struct(), keyword()) :: {:ok, Version.t()} | {:error, term()}
  def pin_manifest_version(manifest, opts \\ []) when is_list(opts) do
    Version.new(manifest, opts)
  end

  @doc """
  Pins a manifest build into its compact index version and exact package set.
  """
  @spec prepare_manifest_publication(Build.t(), keyword()) ::
          {:ok, Publication.t()} | {:error, term()}
  def prepare_manifest_publication(%Build{} = build, opts \\ []) when is_list(opts) do
    Publication.new(build, opts)
  end

  defp with_default_manifest_modules(opts) when is_list(opts) do
    with {:ok, opts} <-
           put_default_modules(opts, :asset_modules, :assets, &default_asset_modules/0),
         {:ok, opts} <-
           put_default_modules(opts, :pipeline_modules, :pipelines, &default_pipeline_modules/0),
         {:ok, opts} <-
           put_default_modules(opts, :schedule_modules, :schedules, &default_schedule_modules/0),
         {:ok, opts} <-
           put_default_modules(
             opts,
             :connection_modules,
             :connections,
             &default_connection_modules/0
           ) do
      {:ok, opts}
    end
  end

  defp put_default_modules(opts, key, discovery_key, default_fun) do
    case Keyword.fetch(opts, key) do
      {:ok, :all} ->
        with {:ok, modules} <- discover_modules(discovery_key) do
          {:ok, Keyword.put(opts, key, modules)}
        end

      {:ok, _modules} ->
        {:ok, opts}

      :error ->
        with {:ok, modules} <- default_fun.() do
          {:ok, Keyword.put(opts, key, modules)}
        end
    end
  end

  defp default_asset_modules do
    default_modules(:asset_modules, :assets)
  end

  defp default_pipeline_modules do
    default_modules(:pipeline_modules, :pipelines)
  end

  defp default_schedule_modules do
    default_modules(:schedule_modules, :schedules)
  end

  defp default_connection_modules do
    default_modules(:connection_modules, :connections)
  end

  defp with_manifest_environment(opts) do
    case Environment.new(
           default_timezone: Application.get_env(:favn, :default_timezone),
           coverage_scope: Application.get_env(:favn, :coverage_scope)
         ) do
      {:ok, environment} -> {:ok, Keyword.put(opts, :environment, environment)}
      {:error, _reason} = error -> error
    end
  end

  defp default_modules(config_key, discovery_key) do
    discovery = Application.get_env(:favn, :discovery, [])

    case Application.get_env(:favn, config_key, :unset) do
      :unset ->
        if discovery_enabled?(discovery, discovery_key) do
          discover_modules(discovery_key)
        else
          {:ok, []}
        end

      :all ->
        discover_modules(discovery_key)

      modules ->
        {:ok, modules}
    end
  end

  defp discover_modules(discovery_key) do
    discovery = Application.get_env(:favn, :discovery, [])
    ModuleDiscovery.discover(discovery_key, discovery)
  end

  defp discovery_enabled?(discovery, discovery_key) when is_list(discovery) do
    Keyword.get(discovery, discovery_key) == :all
  end

  defp discovery_enabled?(_discovery, _discovery_key), do: false

  @doc false
  @spec plan_asset_run(asset_ref() | [asset_ref()], keyword()) ::
          {:ok, Favn.Plan.t()} | {:error, term()}
  def plan_asset_run(target_refs, opts \\ []) do
    with {:ok, default_asset_modules} <- default_asset_modules(),
         {:ok, connection_modules} <- default_connection_modules(),
         {:ok, environment} <-
           Environment.new(
             default_timezone: Application.get_env(:favn, :default_timezone),
             coverage_scope: Application.get_env(:favn, :coverage_scope)
           ),
         asset_modules <- Keyword.get(opts, :asset_modules, default_asset_modules),
         {:ok, planning_index} <-
           Generator.planning_index(
             asset_modules: asset_modules,
             connection_modules: connection_modules,
             environment: environment
           ) do
      opts =
        opts
        |> Keyword.delete(:asset_modules)
        |> Keyword.delete(:runner_release_id)
        |> Keyword.put(:planning_index, planning_index)

      Planner.plan(target_refs, opts)
    end
  end

  @doc false
  @spec resolve_pipeline(module(), keyword()) ::
          {:ok, Favn.Pipeline.Resolution.t()} | {:error, term()}
  def resolve_pipeline(pipeline_module, opts \\ [])

  def resolve_pipeline(pipeline_module, opts)
      when is_atom(pipeline_module) and is_list(opts) do
    with {:ok, pipeline} <- Pipeline.fetch(pipeline_module),
         {:ok, resolve_opts} <- resolve_pipeline_opts(opts) do
      Resolver.resolve(pipeline, resolve_opts)
    end
  end

  def resolve_pipeline(_pipeline_module, _opts), do: {:error, :invalid_pipeline}

  defp resolve_pipeline_opts(opts) when is_list(opts) do
    with {:ok, environment} <-
           Environment.new(default_timezone: Application.get_env(:favn, :default_timezone)) do
      opts =
        opts
        |> Keyword.put_new(:schedule_lookup, &PipelineSchedules.fetch/2)
        |> Keyword.put_new(:default_timezone, environment.default_timezone)
        |> Keyword.put_new(:default_timezone_source, environment.default_timezone_source)

      case Keyword.fetch(opts, :assets) do
        {:ok, _assets} ->
          {:ok, opts}

        :error ->
          with {:ok, assets} <- list_assets() do
            {:ok, Keyword.put(opts, :assets, assets)}
          end
      end
    end
  end
end
