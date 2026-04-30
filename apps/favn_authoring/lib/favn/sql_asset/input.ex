defmodule Favn.SQLAsset.Input do
  @moduledoc false

  alias Favn.Asset
  alias Favn.SQLAsset.Compiler, as: SQLAssetCompiler
  alias Favn.SQLAsset.Definition, as: SQLAssetDefinition

  @type input :: module() | Favn.asset_ref() | Favn.asset()

  @spec normalize(input()) :: {:ok, Favn.asset()} | {:error, map() | struct()}
  def normalize(%Asset{type: :sql} = asset), do: {:ok, asset}

  def normalize(%Asset{} = asset) do
    {:error, error(:not_sql_asset, "asset #{inspect(asset.ref)} is not a SQL asset", asset.ref)}
  end

  def normalize({module, name} = ref) when is_atom(module) and is_atom(name) do
    case name do
      :asset ->
        resolve_sql_asset_module(module)

      _other ->
        {:error, invalid_ref_name_error(ref)}
    end
  end

  def normalize(module) when is_atom(module) do
    resolve_sql_asset_module(module)
  end

  def normalize(other) do
    {:error, invalid_input_error(other)}
  end

  defp resolve_sql_asset_module(module) do
    asset_ref = {module, :asset}

    case SQLAssetCompiler.fetch_definition(module) do
      {:ok, %SQLAssetDefinition{asset: %Asset{} = asset}} ->
        {:ok, asset}

      {:error, _reason} ->
        case FavnAuthoring.get_asset(module) do
          {:ok, %Asset{} = asset} ->
            normalize(asset)

          {:error, reason} ->
            {:error, unresolved_asset_error(module, asset_ref, reason)}
        end
    end
  end

  defp invalid_ref_name_error(ref) do
    error(
      :invalid_asset_input,
      "invalid SQL asset ref #{inspect(ref)}; expected {module, :asset}",
      ref,
      %{reason: :invalid_sql_asset_ref_name}
    )
  end

  defp invalid_input_error(input) do
    error(
      :invalid_asset_input,
      "invalid SQL asset input; expected module, {module, :asset}, or %Favn.Asset{}",
      nil,
      %{input: input}
    )
  end

  defp unresolved_asset_error(module, asset_ref, reason) do
    error(
      :invalid_asset_input,
      "could not resolve SQL asset #{inspect(module)}",
      asset_ref,
      %{reason: reason}
    )
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
