defmodule Favn.Namespace do
  @moduledoc """
  Public helper for inherited relation defaults.

  Use `Favn.Namespace` to declare relation defaults once on parent modules, then
  let `Favn.Asset`, `Favn.SQLAsset`, `Favn.MultiAsset`, `Favn.Assets`, and
  `Favn.Source` inherit them.

  ## When to use it

  Use this module when many assets share the same `connection`, `catalog`, or
  `schema` and you want those values derived from module nesting instead of
  repeated in every asset.

  ## Example

      defmodule MyApp.Warehouse do
        use Favn.Namespace, relation: [connection: :warehouse]
      end

      defmodule MyApp.Warehouse.Raw do
        use Favn.Namespace, relation: [catalog: "raw"]
      end

      defmodule MyApp.Warehouse.Raw.Sales do
        use Favn.Namespace, relation: [schema: "sales"]
      end

  ## See also

  - `Favn.Asset`
  - `Favn.SQLAsset`
  - `Favn.Source`
  """

  @supported_keys [:connection, :catalog, :schema]

  @doc false
  defmacro __using__(opts) do
    quote bind_quoted: [opts: opts] do
      @favn_namespace_config Favn.Namespace.normalize_config!(opts)

      @doc false
      @spec __favn_namespace_config__() :: map()
      def __favn_namespace_config__, do: @favn_namespace_config
    end
  end

  @doc """
  Resolve relation defaults for a module by merging ancestor namespaces.

  Returns a map with `:connection`, `:catalog`, and `:schema` keys for relation construction.
  """
  @spec resolve_relation(module()) :: map()
  def resolve_relation(module) when is_atom(module) do
    module
    |> ancestors()
    |> Enum.reduce(%{}, fn ancestor, acc ->
      case namespace_config(ancestor) do
        nil -> acc
        config -> Map.merge(acc, config)
      end
    end)
  end

  @doc false
  @spec normalize_config!(keyword() | map()) :: map()
  def normalize_config!(opts) when is_list(opts) do
    if Keyword.keyword?(opts) do
      opts |> Map.new() |> normalize_config!()
    else
      raise ArgumentError,
            "namespace config must be a keyword list or map, got: #{inspect(opts)}"
    end
  end

  def normalize_config!(opts) when is_map(opts) do
    relation_defaults = normalize_relation_defaults!(Map.get(opts, :relation, %{}))
    validate_no_legacy_keys!(Map.delete(opts, :relation))
    relation_defaults
  end

  def normalize_config!(opts) do
    raise ArgumentError, "namespace config must be a keyword list or map, got: #{inspect(opts)}"
  end

  defp normalize_relation_defaults!(defaults) when defaults in [%{}, []], do: %{}

  defp normalize_relation_defaults!(defaults) when is_map(defaults) do
    Enum.reduce(defaults, %{}, fn {key, value}, acc ->
      canonical_key = normalize_key!(key)
      Map.put(acc, canonical_key, normalize_value!(canonical_key, value))
    end)
  end

  defp normalize_relation_defaults!(defaults) when is_list(defaults) do
    if Keyword.keyword?(defaults) do
      defaults
      |> Map.new()
      |> normalize_relation_defaults!()
    else
      raise ArgumentError,
            "namespace relation config must be a keyword list or map, got: #{inspect(defaults)}"
    end
  end

  defp normalize_relation_defaults!(defaults) do
    raise ArgumentError,
          "namespace relation config must be a keyword list or map, got: #{inspect(defaults)}"
  end

  defp validate_no_legacy_keys!(opts_without_relation) when map_size(opts_without_relation) == 0,
    do: :ok

  defp validate_no_legacy_keys!(opts_without_relation) do
    raise ArgumentError,
          "namespace config contains unsupported key(s) #{inspect(Map.keys(opts_without_relation))}; use relation: [connection: ..., catalog: ..., schema: ...]"
  end

  defp namespace_config(module) do
    cond do
      Module.open?(module) ->
        Module.get_attribute(module, :favn_namespace_config)

      match?({:module, _}, Code.ensure_loaded(module)) and
          function_exported?(module, :__favn_namespace_config__, 0) ->
        module.__favn_namespace_config__()

      true ->
        nil
    end
  end

  defp ancestors(module) do
    parts = Module.split(module)

    1..length(parts)
    |> Enum.map(fn index -> Module.concat(Enum.take(parts, index)) end)
  end

  defp normalize_key!(key) when key in @supported_keys, do: key

  defp normalize_key!(key) do
    raise ArgumentError,
          "namespace config contains unsupported key #{inspect(key)}; allowed keys: #{@supported_keys |> inspect()}"
  end

  defp normalize_value!(:connection, value) when is_atom(value), do: value

  defp normalize_value!(field, value) when field in [:catalog, :schema] and is_binary(value),
    do: value

  defp normalize_value!(field, value) when field in [:catalog, :schema] and is_atom(value),
    do: Atom.to_string(value)

  defp normalize_value!(field, value) do
    raise ArgumentError,
          "namespace config #{field} has invalid value #{inspect(value)}"
  end
end
