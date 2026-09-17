defmodule FavnAuthoring.Semantic.Builder do
  @moduledoc """
  Builds semantic artifacts from ordinary asset discovery without runner releases.

  This authoring boundary enriches assets with output contracts and passes only
  captured declarations and values to Core. It does not start applications,
  connect to customer data, build execution releases, or activate manifests.
  Callers own isolated compilation before invoking this module.
  """

  alias Favn.Semantic.{Artifact, Compiler}

  @doc "Builds and writes one immutable artifact from configured or explicit asset modules."
  @spec build(keyword()) :: {:ok, map()} | {:error, term()}
  def build(opts) do
    with {:ok, assets} <- assets(opts),
         {:ok, artifact} <-
           compile(assets, Keyword.get(opts, :validator, FavnDuckdbADBC.SemanticCompiler)),
         {:ok, result} <- Artifact.write(artifact, Keyword.fetch!(opts, :output)) do
      {:ok, result}
    end
  end

  @doc "Compiles captured assets without writing, using a `Favn.Semantic.Validator` adapter or three-argument function."
  @spec compile([map()], module() | function()) :: {:ok, Artifact.t()} | {:error, term()}
  def compile(assets, validator) do
    models =
      assets
      |> Enum.map(& &1.module)
      |> Enum.uniq()
      |> Enum.sort()
      |> Enum.flat_map(fn module ->
        if function_exported?(module, :__favn_semantic__, 0) do
          List.wrap(module.__favn_semantic__())
        else
          []
        end
      end)

    enriched =
      Enum.map(assets, fn asset ->
        contract =
          if function_exported?(asset.module, :__favn_sql_asset_definition__, 0),
            do: asset.module.__favn_sql_asset_definition__().contract,
            else: nil

        Map.put(asset, :contract, contract)
      end)

    Compiler.compile(models, enriched, validator)
  end

  defp assets(opts) do
    case Keyword.fetch(opts, :asset_modules) do
      {:ok, modules} -> FavnAuthoring.list_assets(modules)
      :error -> FavnAuthoring.list_assets()
    end
  end
end
