defmodule Favn.Namespace do
  @moduledoc """
  Namespace config carrier for inherited asset relation defaults.

  Phase 1 supports `:connection`, `:catalog`, and `:schema` inheritance.
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
  Resolve namespace config for a module by merging ancestor namespaces.
  """
  @spec resolve(module()) :: map()
  def resolve(module) when is_atom(module) do
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
    Enum.reduce(opts, %{}, fn {key, value}, acc ->
      canonical_key = normalize_key!(key)
      Map.put(acc, canonical_key, normalize_value!(canonical_key, value))
    end)
  end

  def normalize_config!(opts) do
    raise ArgumentError, "namespace config must be a keyword list or map, got: #{inspect(opts)}"
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
