defmodule Favn.SQLAsset.Compiler do
  @moduledoc """
  Compiler bridge for `Favn.SQLAsset` modules.
  """

  @behaviour Favn.Assets.Compiler

  alias Favn.SQLAsset.Definition

  @impl true
  def compile_assets(module) when is_atom(module) do
    case fetch_definition(module) do
      {:ok, %Definition{asset: asset}} ->
        {:ok, [asset]}

      {:error, {:invalid_sql_asset_definition, message}} ->
        {:error, {:invalid_compiled_assets, message}}

      {:error, _reason} ->
        {:error, :invalid_compiled_assets}
    end
  end

  @spec fetch_definition(module()) :: {:ok, Definition.t()} | {:error, term()}
  def fetch_definition(module) when is_atom(module) do
    with {:module, ^module} <- Code.ensure_loaded(module),
         true <- function_exported?(module, :__favn_sql_asset_definition__, 0) do
      case module.__favn_sql_asset_definition__() do
        %Definition{} = definition -> {:ok, definition}
        _other -> {:error, :invalid_sql_asset_definition}
      end
    else
      _ -> {:error, :invalid_sql_asset_definition}
    end
  rescue
    error in CompileError -> {:error, {:invalid_sql_asset_definition, Exception.message(error)}}
    error in ArgumentError -> {:error, {:invalid_sql_asset_definition, Exception.message(error)}}
    _ -> {:error, :invalid_sql_asset_definition}
  end
end
