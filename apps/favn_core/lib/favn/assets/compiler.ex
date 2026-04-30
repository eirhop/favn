defmodule Favn.Assets.Compiler do
  @moduledoc """
  Asset compilation seam for loading canonical `%Favn.Asset{}` values.

  Today, Elixir assets expose `__favn_assets__/0`. Future asset frontends
  (for example SQL authoring) can expose `__favn_asset_compiler__/0` that
  returns a compiler module implementing `c:compile_assets/1`.
  """

  alias Favn.Asset.RelationResolver

  @callback compile_assets(module()) :: {:ok, [map()]} | {:error, term()}

  @spec compile_module_assets(module()) :: {:ok, [map()]} | {:error, term()}
  def compile_module_assets(module) when is_atom(module) do
    case Code.ensure_loaded(module) do
      {:module, ^module} ->
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

      {:error, _reason} ->
        {:error, :not_asset_module}
    end
  rescue
    _ -> {:error, {:invalid_asset_module, module}}
  end

  defp normalize_assets(assets) when is_list(assets) do
    if Enum.all?(assets, &is_map/1) do
      try do
        assets
        |> Enum.reduce_while({:ok, []}, fn asset, {:ok, acc} ->
          case validate_asset(asset) do
            {:ok, validated} -> {:cont, {:ok, [validated | acc]}}
            {:error, _reason} = error -> {:halt, error}
          end
        end)
        |> case do
          {:ok, validated_assets} -> {:ok, Enum.reverse(validated_assets)}
          {:error, _reason} = error -> error
        end
      rescue
        error in ArgumentError -> {:error, {:invalid_compiled_assets, error.message}}
      end
    else
      {:error, :invalid_compiled_assets}
    end
  end

  defp normalize_assets(_), do: {:error, :invalid_compiled_assets}

  defp normalize_compiled_assets(assets, module), do: normalize_and_resolve_assets(assets, module)

  defp normalize_module_assets(assets, module) when is_list(assets),
    do: normalize_and_resolve_assets(assets, module)

  defp normalize_and_resolve_assets(assets, module) do
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
    relation_defaults = namespace_defaults(module)

    try do
      assets = Enum.map(assets, &resolve_relation(&1, relation_defaults))
      :ok = RelationResolver.ensure_unique_relation_owners!(assets)
      {:ok, assets}
    rescue
      error in ArgumentError -> {:error, {:invalid_compiled_assets, error.message}}
    end
  end

  defp resolve_relation(asset, relation_defaults) when is_map(asset) do
    inferred_name = RelationResolver.inferred_relation_name_for_asset(asset)

    asset_module = Map.get(asset, :module)
    asset_name = Map.get(asset, :name)

    relation =
      case fetch_authored_relation(asset_module, asset_name) do
        nil ->
          resolve_existing_relation(Map.get(asset, :relation), relation_defaults, inferred_name)

        authored_value ->
          RelationResolver.resolve_explicit_relation!(
            authored_value,
            relation_defaults,
            inferred_name
          )
      end

    asset
    |> Map.put(:relation, relation)
    |> resolve_relation_inputs(relation_defaults)
  end

  defp fetch_authored_relation(module, name) do
    with true <- function_exported?(module, :__favn_assets_raw__, 0),
         entries when is_list(entries) <- module.__favn_assets_raw__(),
         %{relation: relation} <- Enum.find(entries, &(Map.get(&1, :name) == name)) do
      case relation do
        [] -> nil
        [value] -> value
      end
    else
      _ -> nil
    end
  end

  defp resolve_existing_relation(nil, _defaults, _inferred_name), do: nil

  defp resolve_existing_relation(%Favn.RelationRef{} = existing, relation_defaults, inferred_name) do
    existing
    |> Map.from_struct()
    |> RelationResolver.resolve_explicit_relation!(relation_defaults, inferred_name)
  end

  defp resolve_existing_relation(existing, relation_defaults, inferred_name)
       when is_map(existing) or is_list(existing) or existing == true do
    RelationResolver.resolve_explicit_relation!(existing, relation_defaults, inferred_name)
  end

  defp resolve_relation_inputs(asset, relation_defaults)
       when is_map(asset) and is_map(relation_defaults) do
    relation_inputs =
      asset
      |> Map.get(:relation_inputs, [])
      |> Enum.map(fn
        %Favn.Asset.RelationInput{relation_ref: %Favn.RelationRef{} = relation_ref} = input ->
          resolved_relation =
            resolve_existing_relation(relation_ref, relation_defaults, relation_ref.name)

          %{input | relation_ref: resolved_relation}

        other ->
          other
      end)

    Map.put(asset, :relation_inputs, relation_inputs)
  end

  defp validate_asset(asset) when is_map(asset) do
    asset_module = Module.concat(Favn, Asset)

    with {:module, ^asset_module} <- Code.ensure_loaded(asset_module),
         true <- function_exported?(asset_module, :validate!, 1) do
      {:ok, asset_module.validate!(asset)}
    else
      _ -> {:error, :invalid_compiled_assets}
    end
  end

  defp namespace_defaults(module) when is_atom(module) do
    namespace_module = Module.concat(Favn, Namespace)

    with {:module, ^namespace_module} <- Code.ensure_loaded(namespace_module),
         true <- function_exported?(namespace_module, :resolve_relation, 1) do
      namespace_module.resolve_relation(module)
    else
      _ -> %{}
    end
  end
end
