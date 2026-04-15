defmodule Favn.Assets.Compiler do
  @moduledoc """
  Asset compilation seam for loading canonical `%Favn.Asset{}` values.

  Today, Elixir assets expose `__favn_assets__/0`. Future asset frontends
  (for example SQL authoring) can expose `__favn_asset_compiler__/0` that
  returns a compiler module implementing `c:compile_assets/1`.
  """

  alias Favn.Asset
  alias Favn.Asset.RelationResolver
  alias Favn.Namespace

  @callback compile_assets(module()) :: {:ok, [Asset.t()]} | {:error, term()}

  @spec compile_module_assets(module()) :: {:ok, [Asset.t()]} | {:error, term()}
  def compile_module_assets(module) when is_atom(module) do
    cond do
      function_exported?(module, :__favn_assets__, 0) ->
        module.__favn_assets__()
        |> normalize_module_assets(module)

      function_exported?(module, :__favn_asset_compiler__, 0) ->
        with compiler when is_atom(compiler) <- module.__favn_asset_compiler__(),
             {:module, _loaded} <- Code.ensure_loaded(compiler),
             true <- function_exported?(compiler, :compile_assets, 1) do
          case compiler.compile_assets(module) do
            {:ok, assets} -> normalize_compiled_assets(assets, module)
            {:error, reason} -> {:error, reason}
            _ -> {:error, {:invalid_asset_compiler, module}}
          end
        else
          {:error, _reason} -> {:error, {:invalid_asset_compiler, module}}
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

  defp normalize_compiled_assets(assets, module) do
    normalize_assets(assets)
    |> case do
      {:ok, assets} ->
        try do
          validate_single_asset_depends_shorthand!(module)
          {:ok, assets}
        rescue
          error in ArgumentError -> {:error, {:invalid_compiled_assets, error.message}}
        end

      {:error, _reason} = error ->
        error
    end
  end

  defp normalize_module_assets(assets, module) when is_list(assets) do
    normalize_assets(assets)
    |> case do
      {:ok, assets} ->
        try do
          validate_single_asset_depends_shorthand!(module)
          resolve_relations(assets, module)
        rescue
          error in ArgumentError -> {:error, {:invalid_compiled_assets, error.message}}
        end

      {:error, _reason} = error ->
        error
    end
  end

  defp validate_single_asset_depends_shorthand!(module) do
    if function_exported?(module, :__favn_single_asset__, 0) do
      case fetch_raw_assets(module) do
        {:ok, raw_assets} ->
          raw_assets
          |> Enum.flat_map(&Map.get(&1, :depends, []))
          |> Enum.each(fn
            dependency_module when is_atom(dependency_module) ->
              ensure_single_asset_dependency_module!(module, dependency_module)

            _other ->
              :ok
          end)

        :error ->
          :ok
      end
    end

    :ok
  end

  defp fetch_raw_assets(module) do
    if function_exported?(module, :__favn_assets_raw__, 0) do
      {:ok, module.__favn_assets_raw__()}
    else
      :error
    end
  end

  defp ensure_single_asset_dependency_module!(module, dependency_module) do
    case Code.ensure_loaded(dependency_module) do
      {:module, _loaded} ->
        if function_exported?(dependency_module, :__favn_single_asset__, 0) do
          :ok
        else
          raise ArgumentError,
                "invalid @depends entry #{inspect(dependency_module)} in #{inspect(module)}; module shorthand requires a single-asset module, use {Module, :asset_name} for multi-asset modules"
        end

      _ ->
        raise ArgumentError,
              "invalid @depends entry #{inspect(dependency_module)} in #{inspect(module)}; module shorthand requires a loadable single-asset module"
    end
  end

  defp resolve_relations(assets, module) do
    defaults = Namespace.resolve_relation(module)

    try do
      assets = Enum.map(assets, &resolve_relation(&1, defaults))
      :ok = RelationResolver.ensure_unique_relation_owners!(assets)
      {:ok, assets}
    rescue
      error in ArgumentError -> {:error, {:invalid_compiled_assets, error.message}}
    end
  end

  defp resolve_relation(%Asset{} = asset, defaults) do
    inferred_name = RelationResolver.inferred_relation_name_for_asset(asset)

    relation =
      case fetch_raw_relation(asset.module, asset.name) do
        nil ->
          asset.relation

        authored_value ->
          RelationResolver.resolve_explicit_relation!(authored_value, defaults, inferred_name)
      end

    %{asset | relation: relation}
  end

  defp fetch_raw_relation(module, name) do
    with true <- function_exported?(module, :__favn_assets_raw__, 0),
         entries when is_list(entries) <- module.__favn_assets_raw__(),
         %{relation: relation} <- Enum.find(entries, &(&1.name == name)) do
      case relation do
        [] -> nil
        [value] -> value
      end
    else
      _ -> nil
    end
  end
end
