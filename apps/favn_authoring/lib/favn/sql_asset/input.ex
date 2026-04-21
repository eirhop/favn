defmodule Favn.SQLAsset.Input do
  @moduledoc false

  alias Favn.SQLAsset.Compiler, as: SQLAssetCompiler

  @type input :: module() | Favn.asset_ref() | Favn.asset()

  @spec normalize(input()) :: {:ok, Favn.asset()} | {:error, map() | struct()}
  def normalize(%Favn.Asset{type: :sql} = asset), do: {:ok, asset}

  def normalize(%Favn.Asset{} = asset) do
    {:error, error(:not_sql_asset, "asset #{inspect(asset.ref)} is not a SQL asset", asset.ref)}
  end

  def normalize({module, name} = ref) when is_atom(module) and is_atom(name) do
    case name do
      :asset ->
        resolve_sql_asset_module(module, ref)

      _other ->
        {:error,
         error(
           :invalid_asset_input,
           "invalid SQL asset ref #{inspect(ref)}; expected {module, :asset}",
           ref,
           %{reason: :invalid_sql_asset_ref_name}
         )}
    end
  end

  def normalize(module) when is_atom(module) do
    resolve_sql_asset_module(module, {module, :asset})
  end

  def normalize(other) do
    {:error,
     error(
       :invalid_asset_input,
       "invalid SQL asset input; expected module, {module, :asset}, or %Favn.Asset{}",
       nil,
       %{input: other}
     )}
  end

  defp resolve_sql_asset_module(module, asset_ref) do
    case SQLAssetCompiler.fetch_definition(module) do
      {:ok, %Favn.SQLAsset.Definition{asset: %Favn.Asset{} = asset}} ->
        {:ok, asset}

      {:error, _reason} ->
        case FavnAuthoring.get_asset(module) do
          {:ok, %Favn.Asset{} = asset} ->
            normalize(asset)

          {:error, reason} ->
            {:error,
             error(
               :invalid_asset_input,
               "could not resolve SQL asset #{inspect(module)}",
               asset_ref,
               %{reason: reason}
             )}
        end
    end
  end

  defp error(type, message, asset_ref, details \\ %{}) do
    payload = %{
      type: type,
      phase: :render,
      asset_ref: asset_ref,
      message: message,
      details: details
    }

    if Code.ensure_loaded?(Favn.SQLAsset.Error) do
      struct(Favn.SQLAsset.Error, payload)
    else
      payload
    end
  end
end
