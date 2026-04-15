defmodule Favn.SQLAsset.Input do
  @moduledoc false

  alias Favn.SQLAsset.Compiler, as: SQLAssetCompiler
  alias Favn.SQLAsset.Error, as: SQLAssetError

  @type input :: module() | Favn.asset_ref() | Favn.asset()

  @spec normalize(input()) :: {:ok, Favn.asset()} | {:error, SQLAssetError.t()}
  def normalize(%Favn.Asset{type: :sql} = asset), do: {:ok, asset}

  def normalize(%Favn.Asset{} = asset) do
    {:error,
     %SQLAssetError{
       type: :not_sql_asset,
       phase: :render,
       asset_ref: asset.ref,
       message: "asset #{inspect(asset.ref)} is not a SQL asset"
     }}
  end

  def normalize({module, name} = ref) when is_atom(module) and is_atom(name) do
    case name do
      :asset ->
        resolve_sql_asset_module(module, ref)

      _other ->
        {:error,
         %SQLAssetError{
           type: :invalid_asset_input,
           phase: :render,
           asset_ref: ref,
           message: "invalid SQL asset ref #{inspect(ref)}; expected {module, :asset}",
           details: %{reason: :invalid_sql_asset_ref_name}
         }}
    end
  end

  def normalize(module) when is_atom(module) do
    resolve_sql_asset_module(module, {module, :asset})
  end

  def normalize(other) do
    {:error,
     %SQLAssetError{
       type: :invalid_asset_input,
       phase: :render,
       message: "invalid SQL asset input; expected module, {module, :asset}, or %Favn.Asset{}",
       details: %{input: other}
     }}
  end

  defp resolve_sql_asset_module(module, asset_ref) do
    case SQLAssetCompiler.fetch_definition(module) do
      {:ok, %Favn.SQLAsset.Definition{asset: %Favn.Asset{} = asset}} ->
        {:ok, asset}

      {:error, _reason} ->
        case Favn.get_asset(module) do
          {:ok, %Favn.Asset{} = asset} ->
            normalize(asset)

          {:error, reason} ->
            {:error,
             %SQLAssetError{
               type: :invalid_asset_input,
               phase: :render,
               asset_ref: asset_ref,
               message: "could not resolve SQL asset #{inspect(module)}",
               details: %{reason: reason}
             }}
        end
    end
  end
end
