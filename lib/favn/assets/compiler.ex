defmodule Favn.Assets.Compiler do
  @moduledoc """
  Asset compilation seam for loading canonical `%Favn.Asset{}` values.

  Today, Elixir assets expose `__favn_assets__/0`. Future asset frontends
  (for example SQL authoring) can expose `__favn_asset_compiler__/0` that
  returns a compiler module implementing `c:compile_assets/1`.
  """

  alias Favn.Asset

  @callback compile_assets(module()) :: {:ok, [Asset.t()]} | {:error, term()}

  @spec compile_module_assets(module()) :: {:ok, [Asset.t()]} | {:error, term()}
  def compile_module_assets(module) when is_atom(module) do
    cond do
      function_exported?(module, :__favn_assets__, 0) ->
        normalize_assets(module.__favn_assets__())

      function_exported?(module, :__favn_asset_compiler__, 0) ->
        with compiler when is_atom(compiler) <- module.__favn_asset_compiler__(),
             true <- function_exported?(compiler, :compile_assets, 1),
             {:ok, assets} <- compiler.compile_assets(module) do
          normalize_assets(assets)
        else
          false -> {:error, {:invalid_asset_compiler, module}}
          _ -> {:error, {:invalid_asset_compiler, module}}
        end

      true ->
        {:error, :not_asset_module}
    end
  rescue
    _ -> {:error, {:invalid_asset_module, module}}
  end

  defp normalize_assets(assets) when is_list(assets) do
    if Enum.all?(assets, &match?(%Asset{}, &1)) do
      try do
        {:ok, Enum.map(assets, &Asset.validate!/1)}
      rescue
        error in ArgumentError -> {:error, {:invalid_compiled_assets, error.message}}
      end
    else
      {:error, :invalid_compiled_assets}
    end
  end

  defp normalize_assets(_), do: {:error, :invalid_compiled_assets}
end
