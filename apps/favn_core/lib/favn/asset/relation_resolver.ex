defmodule Favn.Asset.RelationResolver do
  @moduledoc false

  alias Favn.RelationRef

  @spec inferred_relation_name_for_asset(term()) :: atom() | binary()
  def inferred_relation_name_for_asset(%{module: module, name: :asset}) when is_atom(module) do
    if function_exported?(module, :__favn_single_asset__, 0) do
      inferred_relation_name_for_module(module)
    else
      :asset
    end
  end

  def inferred_relation_name_for_asset(%{name: name}) when is_atom(name), do: name
  def inferred_relation_name_for_asset(_other), do: :asset

  @spec inferred_relation_name_for_module(module()) :: binary()
  def inferred_relation_name_for_module(module) when is_atom(module) do
    module
    |> Module.split()
    |> List.last()
    |> Macro.underscore()
  end

  @spec resolve_explicit_relation!(true | keyword() | map(), map(), atom() | binary()) ::
          RelationRef.t()
  def resolve_explicit_relation!(true, defaults, inferred_name) when is_map(defaults) do
    RelationRef.new!(Map.put(defaults, :name, inferred_name))
  end

  def resolve_explicit_relation!(attrs, defaults, inferred_name)
      when is_list(attrs) and is_map(defaults) do
    if Keyword.keyword?(attrs) do
      attrs
      |> Map.new()
      |> resolve_relation_attrs!(defaults, inferred_name)
    else
      raise_invalid_relation_value!(attrs)
    end
  end

  def resolve_explicit_relation!(attrs, defaults, inferred_name)
      when is_map(attrs) and is_map(defaults) do
    resolve_relation_attrs!(attrs, defaults, inferred_name)
  end

  def resolve_explicit_relation!(other, _defaults, _inferred_name),
    do: raise_invalid_relation_value!(other)

  @spec resolve_relation_attrs!(map(), map(), atom() | binary()) :: RelationRef.t()
  def resolve_relation_attrs!(attrs, defaults, inferred_name)
      when is_map(attrs) and is_map(defaults) do
    attrs = drop_nil_values(attrs)

    attrs
    |> maybe_put_inferred_name(inferred_name)
    |> merge_relation_attrs(defaults)
    |> RelationRef.new!()
  end

  @spec ensure_unique_relation_owners!([map()]) :: :ok
  def ensure_unique_relation_owners!(assets) when is_list(assets) do
    assets
    |> Enum.reject(&is_nil(&1.relation))
    |> Enum.group_by(& &1.relation)
    |> Enum.each(fn {relation_ref, owners} ->
      case owners do
        [_single] -> :ok
        _many -> raise ArgumentError, "duplicate relation #{inspect(relation_ref)}"
      end
    end)

    :ok
  end

  defp merge_relation_attrs(attrs, defaults) do
    defaults
    |> maybe_drop_default_key(attrs, [:catalog], [:database, "database"])
    |> maybe_drop_default_key(attrs, [:name], [:table, "table", :name, "name"])
    |> Map.merge(attrs)
  end

  defp maybe_put_inferred_name(attrs, inferred_name) do
    if has_explicit_name?(attrs) do
      attrs
    else
      Map.put(attrs, :name, inferred_name)
    end
  end

  defp has_explicit_name?(attrs) do
    Enum.any?([:name, "name", :table, "table"], &Map.has_key?(attrs, &1))
  end

  defp maybe_drop_default_key(defaults, attrs, canonical_keys, authored_keys) do
    if Enum.any?(authored_keys, &Map.has_key?(attrs, &1)) do
      Enum.reduce(canonical_keys, defaults, &Map.delete(&2, &1))
    else
      defaults
    end
  end

  defp raise_invalid_relation_value!(value) do
    raise ArgumentError,
          "invalid @relation value #{inspect(value)}; expected true, a keyword list, or a map"
  end

  defp drop_nil_values(attrs) when is_map(attrs) do
    attrs
    |> Enum.reject(fn {_key, value} -> is_nil(value) end)
    |> Map.new()
  end
end
