defmodule Favn.Manifest.Generator do
  @moduledoc """
  Generates canonical `%Favn.Manifest{}` values from authored module inputs.

  This module owns the compilation path from authored assets, pipelines, and
  schedules into the explicit manifest consumed by planning and runtime layers.

  ## Read This Module When

  Read `Favn.Manifest.Generator` when you need the exact internal path from:

  - asset modules to compiled `%Favn.Asset{}` values
  - pipeline modules to `%Favn.Pipeline.Definition{}` values
  - schedule modules to manifest schedule entries
  - catalog inputs to the final `%Favn.Manifest{}`

  Prefer `Favn.generate_manifest/1` or `Favn.build_manifest/1` for normal usage.
  This module is the next stop when the public facade docs are not detailed
  enough.
  """

  alias Favn.Assets.DependencyInference
  alias Favn.Manifest
  alias Favn.Manifest.Asset, as: ManifestAsset
  alias Favn.Manifest.Build
  alias Favn.Manifest.Catalog
  alias Favn.Manifest.Compatibility
  alias Favn.Manifest.Graph
  alias Favn.Manifest.Pipeline, as: ManifestPipeline
  alias Favn.Manifest.Schedule, as: ManifestSchedule

  @type opts :: [
          asset_modules: [module()],
          pipeline_modules: [module()],
          schedule_modules: [module()]
        ]

  @doc """
  Generates a canonical manifest from authored module inputs.

  This is the internal owner behind `Favn.generate_manifest/1`.
  """
  @spec generate(opts()) :: {:ok, Manifest.t()} | {:error, term()}
  def generate(opts \\ []) when is_list(opts) do
    with {:ok, catalog} <- build_catalog(opts) do
      manifest_from_catalog(catalog)
    end
  end

  @doc """
  Builds a manifest plus build-only diagnostics metadata.

  This is the internal owner behind `Favn.build_manifest/1`.
  """
  @spec build(opts()) :: {:ok, Build.t()} | {:error, term()}
  def build(opts \\ []) when is_list(opts) do
    with {:ok, catalog} <- build_catalog(opts),
         {:ok, manifest} <- manifest_from_catalog(catalog) do
      {:ok, Build.new(manifest, diagnostics: catalog.diagnostics)}
    end
  end

  @doc """
  Compiles authored modules into an intermediate `%Favn.Manifest.Catalog{}`.

  Read this function when you need the pre-manifest compilation boundary, such
  as asset diagnostics or the split between catalog assembly and final manifest
  graph construction.
  """
  @spec build_catalog(opts()) :: {:ok, Catalog.t()} | {:error, term()}
  def build_catalog(opts) when is_list(opts) do
    with :ok <- validate_opts(opts),
         {:ok, assets, diagnostics} <- compile_assets(resolve_modules(opts, :asset_modules)),
         {:ok, pipelines} <- compile_pipelines(resolve_modules(opts, :pipeline_modules)),
         {:ok, schedules} <- compile_schedules(resolve_modules(opts, :schedule_modules)) do
      {:ok,
       %Catalog{
         assets: assets,
         assets_by_ref: Map.new(assets, &{Map.get(&1, :ref), &1}),
         pipelines: pipelines,
         schedules: schedules,
         diagnostics: diagnostics
       }}
    end
  end

  defp manifest_from_catalog(%Catalog{} = catalog) do
    assets = manifest_assets_from_catalog(catalog)
    pipelines = manifest_pipelines_from_catalog(catalog)
    schedules = manifest_schedules_from_catalog(catalog)

    with {:ok, graph} <- Graph.build(assets) do
      {:ok,
       %Manifest{
         schema_version: Compatibility.current_schema_version(),
         runner_contract_version: Compatibility.current_runner_contract_version(),
         assets: assets,
         pipelines: pipelines,
         schedules: schedules,
         graph: graph,
         metadata: %{}
       }}
    end
  end

  defp manifest_assets_from_catalog(%Catalog{} = catalog) do
    catalog.assets
    |> Enum.map(&ManifestAsset.from_asset/1)
    |> Enum.sort(&compare_assets/2)
  end

  defp manifest_pipelines_from_catalog(%Catalog{} = catalog) do
    catalog.pipelines
    |> Enum.map(&ManifestPipeline.from_definition/1)
    |> Enum.sort(&compare_pipelines/2)
  end

  defp manifest_schedules_from_catalog(%Catalog{} = catalog) do
    catalog.schedules
    |> Enum.map(fn {module, name, schedule} ->
      ManifestSchedule.from_schedule(module, name, schedule)
    end)
    |> Enum.sort(&compare_schedules/2)
  end

  defp resolve_modules(opts, key) do
    Keyword.get(opts, key, [])
  end

  defp validate_opts(opts) do
    allowed = [:asset_modules, :pipeline_modules, :schedule_modules]

    case Enum.find(opts, fn {key, _value} -> key not in allowed end) do
      nil -> :ok
      {key, _value} -> {:error, {:unknown_opt, key}}
    end
  end

  defp compile_assets(modules) when is_list(modules) do
    modules
    |> Enum.reduce_while({:ok, []}, fn module, {:ok, acc} ->
      case compile_module_assets(module) do
        {:ok, assets} -> {:cont, {:ok, assets ++ acc}}
        {:error, reason} -> {:halt, {:error, {:asset_compile_failed, module, reason}}}
      end
    end)
    |> case do
      {:ok, assets} ->
        assets = assets |> Enum.uniq_by(&Map.get(&1, :ref)) |> Enum.sort(&compare_assets/2)

        with {:ok, inferred_catalog} <- DependencyInference.infer_assets(assets) do
          {:ok, inferred_catalog.assets, inferred_catalog.diagnostics}
        end

      {:error, _} = error ->
        error
    end
  end

  defp compile_assets(_invalid), do: {:error, :invalid_asset_modules}

  defp compile_pipelines(modules) when is_list(modules) do
    modules
    |> Enum.reduce_while({:ok, []}, fn module, {:ok, acc} ->
      case fetch_pipeline_definition(module) do
        {:ok, definition} -> {:cont, {:ok, [definition | acc]}}
        {:error, reason} -> {:halt, {:error, {:pipeline_compile_failed, module, reason}}}
      end
    end)
    |> case do
      {:ok, definitions} -> {:ok, Enum.reverse(definitions)}
      {:error, _} = error -> error
    end
  end

  defp compile_pipelines(_invalid), do: {:error, :invalid_pipeline_modules}

  defp compile_schedules(modules) when is_list(modules) do
    modules
    |> Enum.reduce_while({:ok, []}, fn module, {:ok, acc} ->
      case compile_module_schedules(module) do
        {:ok, schedules} -> {:cont, {:ok, schedules ++ acc}}
        {:error, reason} -> {:halt, {:error, {:schedule_compile_failed, module, reason}}}
      end
    end)
    |> case do
      {:ok, schedules} -> {:ok, Enum.reverse(schedules)}
      {:error, _} = error -> error
    end
  end

  defp compile_schedules(_invalid), do: {:error, :invalid_schedule_modules}

  defp compile_module_schedules(module) do
    with {:module, ^module} <- Code.ensure_loaded(module),
         true <- function_exported?(module, :__favn_schedules__, 0) do
      schedules =
        module
        |> then(& &1.__favn_schedules__())
        |> Map.to_list()
        |> Enum.sort_by(&elem(&1, 0))
        |> Enum.map(fn {name, schedule} -> {module, name, schedule} end)

      {:ok, schedules}
    else
      _ -> {:error, :not_schedule_module}
    end
  end

  defp compare_assets(left, right), do: compare_refs(left.ref, right.ref)

  defp compare_pipelines(left, right) do
    compare_refs({left.module, left.name}, {right.module, right.name})
  end

  defp compare_schedules(left, right) do
    compare_refs({left.module, left.name}, {right.module, right.name})
  end

  defp compare_refs({left_module, left_name}, {right_module, right_name}) do
    left = {Atom.to_string(left_module), Atom.to_string(left_name)}
    right = {Atom.to_string(right_module), Atom.to_string(right_name)}
    left <= right
  end

  defp compile_module_assets(module) when is_atom(module) do
    compiler_module = Module.concat([Favn, Assets, Compiler])

    if function_exported?(compiler_module, :compile_module_assets, 1) do
      compiler_module.compile_module_assets(module)
    else
      with {:module, ^compiler_module} <- Code.ensure_loaded(compiler_module),
           true <- function_exported?(compiler_module, :compile_module_assets, 1) do
        compiler_module.compile_module_assets(module)
      else
        _ -> {:error, :compiler_not_available}
      end
    end
  end

  defp fetch_pipeline_definition(module) when is_atom(module) do
    with {:module, ^module} <- Code.ensure_loaded(module),
         true <- function_exported?(module, :__favn_pipeline__, 0),
         definition <- module.__favn_pipeline__(),
         true <- is_struct(definition, Favn.Pipeline.Definition) do
      {:ok, definition}
    else
      _ -> {:error, :not_pipeline_module}
    end
  rescue
    _ -> {:error, :pipeline_not_defined}
  end
end
