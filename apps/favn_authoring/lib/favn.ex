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
  - `Favn.Dev`: local tooling owner
  - `Favn.Manifest.Generator`, `Favn.Pipeline.Resolver`, `Favn.Assets.Planner`:
    deeper internals
  """

  alias Favn.Asset
  alias Favn.Assets.Compiler
  alias Favn.Assets.Planner
  alias Favn.Manifest
  alias Favn.Manifest.Build
  alias Favn.Manifest.Compatibility
  alias Favn.Manifest.Generator
  alias Favn.Manifest.Identity
  alias Favn.Manifest.Serializer
  alias Favn.Manifest.Version
  alias Favn.Pipeline
  alias Favn.Pipeline.Resolver
  alias Favn.Triggers.Schedules, as: PipelineSchedules

  @type asset_ref :: Favn.Ref.t()
  @type asset :: Asset.t()
  @type asset_error :: :not_asset_module | :asset_not_found
  @type dependencies_mode :: :all | :none
  @type manifest_opts :: [
          asset_modules: [module()],
          pipeline_modules: [module()],
          schedule_modules: [module()]
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
    modules = Application.get_env(:favn, :asset_modules, [])
    list_assets(modules)
  end

  @doc """
  Compiles and returns assets for one module or a list of modules.
  """
  @spec list_assets(module() | [module()]) :: {:ok, [asset()]} | {:error, term()}
  def list_assets(module) when is_atom(module) do
    case Application.get_env(:favn, :asset_modules, :unset) do
      modules when is_list(modules) ->
        if module in modules do
          list_assets_for_module_from_catalog(module, modules)
        else
          compile_module_assets(module)
        end

      _other ->
        compile_module_assets(module)
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

  defp compile_module_assets(module) when is_atom(module) do
    case Compiler.compile_module_assets(module) do
      {:ok, assets} -> {:ok, assets}
      {:error, reason} -> {:error, reason}
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
  Generates a manifest from explicit modules or app config.
  """
  @spec generate_manifest(manifest_opts()) :: {:ok, Manifest.t()} | {:error, term()}
  def generate_manifest(opts \\ []) when is_list(opts),
    do: opts |> with_default_manifest_modules() |> Generator.generate()

  @doc """
  Generates a manifest build output with build-only metadata separated from
  canonical runtime payload data.
  """
  @spec build_manifest(manifest_opts()) :: {:ok, Build.t()} | {:error, term()}
  def build_manifest(opts \\ []) when is_list(opts) do
    opts
    |> with_default_manifest_modules()
    |> Generator.build()
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
  def hash_manifest(manifest), do: Identity.hash_manifest(manifest)

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

  defp with_default_manifest_modules(opts) when is_list(opts) do
    opts
    |> Keyword.put_new(:asset_modules, Application.get_env(:favn, :asset_modules, []))
    |> Keyword.put_new(:pipeline_modules, Application.get_env(:favn, :pipeline_modules, []))
    |> Keyword.put_new(:schedule_modules, Application.get_env(:favn, :schedule_modules, []))
  end

  @doc false
  @spec plan_asset_run(asset_ref() | [asset_ref()], keyword()) ::
          {:ok, Favn.Plan.t()} | {:error, term()}
  def plan_asset_run(target_refs, opts \\ []) do
    opts = Keyword.put_new(opts, :asset_modules, Application.get_env(:favn, :asset_modules, []))
    Planner.plan(target_refs, opts)
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
    opts = Keyword.put_new(opts, :schedule_lookup, &PipelineSchedules.fetch/2)

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
