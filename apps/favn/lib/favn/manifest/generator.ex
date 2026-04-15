defmodule Favn.Manifest.Generator do
  @moduledoc """
  Generates `%Favn.Manifest{}` from explicit module inputs.
  """

  alias Favn.Assets.Compiler
  alias Favn.Manifest
  alias Favn.Manifest.Asset, as: ManifestAsset
  alias Favn.Manifest.Catalog
  alias Favn.Manifest.Pipeline, as: ManifestPipeline
  alias Favn.Manifest.Schedule, as: ManifestSchedule
  alias Favn.Pipeline

  @type opts :: [
          asset_modules: [module()],
          pipeline_modules: [module()],
          schedule_modules: [module()]
        ]

  @spec generate(opts()) :: {:ok, Manifest.t()} | {:error, term()}
  def generate(opts \\ []) when is_list(opts) do
    with :ok <- validate_opts(opts),
         {:ok, catalog} <- build_catalog(opts) do
      {:ok,
       %Manifest{
         version: 1,
         generated_at: DateTime.utc_now(),
         assets: Enum.map(catalog.assets, &ManifestAsset.from_asset/1),
         pipelines: catalog.pipelines,
         schedules: catalog.schedules,
         diagnostics: catalog.diagnostics
       }}
    end
  end

  @spec build_catalog(opts()) :: {:ok, Catalog.t()} | {:error, term()}
  def build_catalog(opts) when is_list(opts) do
    asset_modules =
      Keyword.get(opts, :asset_modules, Application.get_env(:favn, :asset_modules, []))

    pipeline_modules =
      Keyword.get(opts, :pipeline_modules, Application.get_env(:favn, :pipeline_modules, []))

    schedule_modules =
      Keyword.get(opts, :schedule_modules, Application.get_env(:favn, :schedule_modules, []))

    with {:ok, assets} <- compile_assets(asset_modules),
         {:ok, pipelines} <- compile_pipelines(pipeline_modules),
         {:ok, schedules} <- compile_schedules(schedule_modules) do
      assets_by_ref = Map.new(assets, &{&1.ref, &1})

      {:ok,
       %Catalog{
         assets: assets,
         assets_by_ref: assets_by_ref,
         pipelines: pipelines,
         schedules: schedules,
         diagnostics: []
       }}
    end
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
      case Compiler.compile_module_assets(module) do
        {:ok, assets} -> {:cont, {:ok, assets ++ acc}}
        {:error, reason} -> {:halt, {:error, {:asset_compile_failed, module, reason}}}
      end
    end)
    |> case do
      {:ok, assets} ->
        assets = assets |> Enum.uniq_by(& &1.ref) |> Enum.sort_by(& &1.ref)
        {:ok, assets}

      {:error, _} = error ->
        error
    end
  end

  defp compile_assets(_invalid), do: {:error, :invalid_asset_modules}

  defp compile_pipelines(modules) when is_list(modules) do
    modules
    |> Enum.reduce_while({:ok, []}, fn module, {:ok, acc} ->
      case Pipeline.fetch(module) do
        {:ok, definition} -> {:cont, {:ok, [ManifestPipeline.from_definition(definition) | acc]}}
        {:error, reason} -> {:halt, {:error, {:pipeline_compile_failed, module, reason}}}
      end
    end)
    |> case do
      {:ok, pipelines} -> {:ok, Enum.reverse(pipelines)}
      {:error, _} = error -> error
    end
  end

  defp compile_pipelines(_invalid), do: {:error, :invalid_pipeline_modules}

  defp compile_schedules(modules) when is_list(modules) do
    modules
    |> Enum.reduce_while({:ok, []}, fn module, {:ok, acc} ->
      if function_exported?(module, :__favn_schedules__, 0) do
        entries = module.__favn_schedules__() |> Map.to_list() |> Enum.sort_by(&elem(&1, 0))

        schedules =
          Enum.map(entries, fn {name, schedule} ->
            %ManifestSchedule{module: module, name: name, schedule: schedule}
          end)

        {:cont, {:ok, schedules ++ acc}}
      else
        {:halt, {:error, {:schedule_compile_failed, module, :not_schedule_module}}}
      end
    end)
    |> case do
      {:ok, schedules} -> {:ok, Enum.reverse(schedules)}
      {:error, _} = error -> error
    end
  end

  defp compile_schedules(_invalid), do: {:error, :invalid_schedule_modules}
end
