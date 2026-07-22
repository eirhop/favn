defmodule Favn.Namespace.Config do
  @moduledoc false

  alias Favn.RuntimeConfig.Bundle
  alias Favn.SQL.SessionRequirements

  @additive_fields [:settings, :meta, :runtime_config]
  @scalar_fields [:runtime_inputs, :freshness, :window, :materialized]
  @relation_keys [:connection, :catalog, :schema]

  @type inherited_value :: :unset | {:set, term()}

  @type t :: %__MODULE__{
          relation: map(),
          resources: [String.t()],
          settings: [term()],
          meta: [term()],
          runtime_config: [Bundle.t()],
          runtime_inputs: inherited_value(),
          freshness: inherited_value(),
          window: inherited_value(),
          materialized: inherited_value()
        }

  defstruct relation: %{},
            resources: [],
            settings: [],
            meta: [],
            runtime_config: [],
            runtime_inputs: :unset,
            freshness: :unset,
            window: :unset,
            materialized: :unset

  @spec new!(map()) :: t()
  def new!(declarations) when is_map(declarations) do
    %__MODULE__{
      relation: normalize_relation!(Map.get(declarations, :relation, [])),
      resources: normalize_resources!(Map.get(declarations, :resources, [])),
      settings: declaration_list!(declarations, :settings),
      meta: declaration_list!(declarations, :meta),
      runtime_config: normalize_runtime_config!(declarations),
      runtime_inputs: scalar!(declarations, :runtime_inputs),
      freshness: scalar!(declarations, :freshness),
      window: scalar!(declarations, :window),
      materialized: scalar!(declarations, :materialized)
    }
  end

  def new!(value) do
    raise ArgumentError, "namespace declarations must be a map, got: #{inspect(value)}"
  end

  @spec merge(t(), t()) :: t()
  def merge(%__MODULE__{} = parent, %__MODULE__{} = child) do
    %__MODULE__{
      relation: Map.merge(parent.relation, child.relation),
      resources: parent.resources ++ child.resources,
      settings: parent.settings ++ child.settings,
      meta: parent.meta ++ child.meta,
      runtime_config: parent.runtime_config ++ child.runtime_config,
      runtime_inputs: closest(parent.runtime_inputs, child.runtime_inputs),
      freshness: closest(parent.freshness, child.freshness),
      window: closest(parent.window, child.window),
      materialized: closest(parent.materialized, child.materialized)
    }
  end

  @spec finalize(t()) :: t()
  def finalize(%__MODULE__{} = config) do
    %{config | resources: SessionRequirements.normalize_resources!(config.resources)}
  end

  @spec effective_declarations(t(), atom(), [term()]) :: [term()]
  def effective_declarations(%__MODULE__{} = config, field, local)
      when field in @additive_fields and is_list(local) do
    Map.fetch!(config, field) ++ local
  end

  def effective_declarations(%__MODULE__{} = config, field, local)
      when field in @scalar_fields and is_list(local) do
    case local do
      [] -> inherited_declarations(Map.fetch!(config, field))
      declarations -> declarations
    end
  end

  defp declaration_list!(declarations, field) do
    case Map.get(declarations, field, []) do
      values when is_list(values) ->
        values

      value ->
        raise ArgumentError,
              "namespace #{field} declarations must be a list, got: #{inspect(value)}"
    end
  end

  defp normalize_runtime_config!(declarations) do
    declarations
    |> declaration_list!(:runtime_config)
    |> Enum.map(&Bundle.validate!/1)
  end

  defp normalize_relation!([]), do: %{}
  defp normalize_relation!([value]), do: normalize_relation_defaults!(value)

  defp normalize_relation!([_first, _second | _rest]) do
    raise ArgumentError, "multiple namespace relation declarations are not allowed"
  end

  defp normalize_relation!(value) do
    raise ArgumentError, "namespace relation declarations must be a list, got: #{inspect(value)}"
  end

  defp normalize_relation_defaults!(defaults) when defaults in [%{}, []], do: %{}

  defp normalize_relation_defaults!(defaults) when is_list(defaults) do
    if Keyword.keyword?(defaults) do
      defaults
      |> Map.new()
      |> normalize_relation_defaults!()
    else
      raise ArgumentError,
            "namespace relation must be a keyword list or map, got: #{inspect(defaults)}"
    end
  end

  defp normalize_relation_defaults!(defaults) when is_map(defaults) do
    Enum.reduce(defaults, %{}, fn {key, value}, acc ->
      canonical_key = normalize_relation_key!(key)
      Map.put(acc, canonical_key, normalize_relation_value!(canonical_key, value))
    end)
  end

  defp normalize_relation_defaults!(defaults) do
    raise ArgumentError,
          "namespace relation must be a keyword list or map, got: #{inspect(defaults)}"
  end

  defp normalize_resources!(declarations) when is_list(declarations) do
    declarations
    |> Enum.flat_map(fn
      resources when is_list(resources) -> resources
      value -> raise ArgumentError, "namespace resources must be lists, got: #{inspect(value)}"
    end)
    |> SessionRequirements.normalize_resources!()
  rescue
    error in ArgumentError ->
      reraise ArgumentError.exception("namespace resources are invalid: #{error.message}"),
              __STACKTRACE__
  end

  defp normalize_resources!(value) do
    raise ArgumentError, "namespace resource declarations must be a list, got: #{inspect(value)}"
  end

  defp scalar!(declarations, field) do
    case declaration_list!(declarations, field) do
      [] ->
        :unset

      [value] ->
        {:set, value}

      [_first, _second | _rest] ->
        raise ArgumentError, "multiple namespace #{field} declarations are not allowed"
    end
  end

  defp closest(parent, :unset), do: parent
  defp closest(_parent, {:set, _value} = child), do: child

  defp inherited_declarations(:unset), do: []
  defp inherited_declarations({:set, value}), do: [value]

  defp normalize_relation_key!(key) when key in @relation_keys, do: key

  defp normalize_relation_key!(key) do
    raise ArgumentError,
          "namespace relation contains unsupported key #{inspect(key)}; allowed keys: #{inspect(@relation_keys)}"
  end

  defp normalize_relation_value!(:connection, value) when is_atom(value), do: value

  defp normalize_relation_value!(field, value)
       when field in [:catalog, :schema] and is_binary(value),
       do: value

  defp normalize_relation_value!(field, value)
       when field in [:catalog, :schema] and is_atom(value),
       do: Atom.to_string(value)

  defp normalize_relation_value!(field, value) do
    raise ArgumentError, "namespace relation #{field} has invalid value #{inspect(value)}"
  end
end
