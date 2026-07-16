defmodule Favn.Namespace do
  @moduledoc """
  Public helper for inherited relation defaults, SQL session resources, and
  runtime-config selection.

  Use `Favn.Namespace` to declare relation defaults once on parent modules, then
  let `Favn.Asset`, `Favn.SQLAsset`, `Favn.MultiAsset`, and
  `Favn.Source` inherit them. Runtime-config bundle selection applies only to
  descendant `Favn.Asset` and `Favn.MultiAsset` executable Elixir assets.

  ## When to use it

  Use this module when many assets share the same warehouse connection, catalog,
  schema, or SQL session resources and you want those values inherited from
  explicit namespace modules instead of repeated in every asset.

  The recommended lakehouse convention is one namespace module per level:

  - `lakehouse.ex` sets the connection only. The connection is the server/session/auth boundary.
  - `lakehouse/raw.ex` and `lakehouse/mart.ex` set phase catalogs.
  - `lakehouse/raw/sales.ex` and similar modules set segment/domain schemas.
  - leaf modules under those folders define assets and infer table/view names.

  Use `catalog` for physical databases or lakehouse phases such as
  raw/intermediate/mart. Use `schema` for domains or segments such as sales and
  finance. Do not use catalog and schema interchangeably in new projects.

  ## Recommended project shape

      lib/my_app/
        connections/important_lakehouse.ex
        lakehouse.ex
        lakehouse/raw.ex
        lakehouse/raw/sales.ex
        lakehouse/raw/sales/orders.ex
        lakehouse/mart.ex
        lakehouse/mart/sales.ex
        lakehouse/mart/sales/order_summary.ex
        lakehouse/mart/sales/order_summary.sql
        integrations/shopify.ex
        pipelines/daily_sales.ex
        triggers/schedules.ex
        sql/calendar.ex

  The lakehouse tree should mirror namespaces and assets. Keep connection
  providers, integration clients, pipelines, triggers, and reusable SQL outside `lakehouse/` unless the
  project has a stronger documented convention. Keep asset-specific logic near
  the asset; move code away from the asset only when it is transport-specific or
  genuinely reused by multiple assets.

  ## Example

      # lib/my_app/lakehouse.ex
      defmodule MyApp.Lakehouse do
        use Favn.Namespace, relation: [connection: :important_lakehouse]
      end

      # lib/my_app/lakehouse/raw.ex
      defmodule MyApp.Lakehouse.Raw do
        use Favn.Namespace,
          relation: [catalog: "raw"],
          resources: [:azure_extension]
      end

      # lib/my_app/lakehouse/raw/sales.ex
      defmodule MyApp.Lakehouse.Raw.Sales do
        use Favn.Namespace, relation: [schema: "sales"]
      end

      # lib/my_app/lakehouse/raw/sales/orders.ex
      defmodule MyApp.Lakehouse.Raw.Sales.Orders do
        use Favn.Asset

        relation true
        def asset(_ctx), do: :ok
      end

  ## Supported options

  `use Favn.Namespace` accepts:

  - `relation: [connection: ...]` for a root lakehouse namespace
  - `relation: [catalog: ...]` for database or phase namespaces such as raw or mart
  - `relation: [schema: ...]` for segment/domain namespaces such as sales or finance
  - `resources: [...]` for additive named SQL session resources inherited by
    descendant `Favn.SQLAsset` modules
  - `runtime_config: [bundle, ...]` for explicit descendant Elixir asset requirements

  Supported relation keys:

  - `connection`: atom
  - `catalog`: string or atom
  - `schema`: string or atom

  Runtime config bundles may also be selected for descendant Elixir assets:

      use Favn.Namespace,
        relation: [schema: "sales"],
        runtime_config: [MyApp.RuntimeConfigs.github()]

  Namespaces select reusable bundles; they do not define or resolve values and
  are not a global configuration registry. Avoid selecting secret bundles at a
  broad root namespace unless every descendant executable Elixir asset needs
  them.

  Resource inheritance is additive from root to leaf, unlike relation defaults,
  which override by key. Names normalize to lowercase snake_case strings in the
  manifest. A namespace resource applies only to descendant SQL assets. Put a
  resource on a broad namespace only when every descendant SQL asset needs that
  physical-session capability; otherwise use local `resources [...]` on the
  leaf asset. Resources are configured as trusted native DuckDB SQL files; read
  the HexDocs guide
  [DuckDB Session Scripts And Resources](duckdb-session-scripts.html).

  ## See also

  - `Favn.Asset`
  - `Favn.SQLAsset`
  - `Favn.Source`
  """

  alias Favn.RuntimeConfig.Bundle
  alias Favn.SQL.SessionRequirements

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
        config -> Map.merge(acc, config.relation)
      end
    end)
  end

  @doc """
  Resolves runtime configuration bundles selected by ancestor namespaces.

  Bundles are returned from root to leaf and remain unresolved.
  """
  @spec resolve_runtime_config(module()) :: [Bundle.t()]
  def resolve_runtime_config(module) when is_atom(module) do
    module
    |> ancestors()
    |> Enum.flat_map(fn ancestor ->
      case namespace_config(ancestor) do
        nil -> []
        config -> config.runtime_config
      end
    end)
  end

  @doc """
  Resolves SQL session resources selected by ancestor namespaces.

  Resources are inherited additively from root to leaf, normalized to stable
  strings, deduplicated, and sorted. They apply only when a descendant
  `Favn.SQLAsset` compiles its session requirements.
  """
  @spec resolve_resources(module()) :: [String.t()]
  def resolve_resources(module) when is_atom(module) do
    module
    |> ancestors()
    |> Enum.flat_map(fn ancestor ->
      case namespace_config(ancestor) do
        nil -> []
        config -> config.resources
      end
    end)
    |> SessionRequirements.normalize_resources!()
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
    runtime_config = normalize_runtime_config!(Map.get(opts, :runtime_config, []))
    resources = normalize_resources!(Map.get(opts, :resources, []))
    validate_no_legacy_keys!(Map.drop(opts, [:relation, :runtime_config, :resources]))
    %{relation: relation_defaults, runtime_config: runtime_config, resources: resources}
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

  defp normalize_runtime_config!(bundles) when is_list(bundles) do
    Enum.map(bundles, &Bundle.validate!/1)
  end

  defp normalize_runtime_config!(_other) do
    raise ArgumentError,
          "namespace runtime_config must be a list of Favn.RuntimeConfig.Bundle values"
  end

  defp normalize_resources!(resources) do
    SessionRequirements.normalize_resources!(resources)
  rescue
    error in ArgumentError ->
      raise ArgumentError, "namespace resources are invalid: #{error.message}"
  end

  defp validate_no_legacy_keys!(opts_without_relation) when map_size(opts_without_relation) == 0,
    do: :ok

  defp validate_no_legacy_keys!(opts_without_relation) do
    raise ArgumentError,
          "namespace config contains unsupported key(s) #{inspect(Map.keys(opts_without_relation))}; supported keys are :relation, :runtime_config, and :resources"
  end

  defp namespace_config(module) do
    cond do
      module_open?(module) ->
        Module.get_attribute(module, :favn_namespace_config)

      match?({:module, _}, ensure_namespace_module(module)) and
          function_exported?(module, :__favn_namespace_config__, 0) ->
        module.__favn_namespace_config__()

      true ->
        nil
    end
  end

  defp module_open?(module) when is_atom(module) do
    Module.open?(module)
  rescue
    ArgumentError -> false
  end

  # Namespace inheritance is used during DSL compilation, so same-project
  # ancestor modules may exist but not be compiled yet under parallel compile.
  # In some compile contexts `Code.can_await_module_compilation?/0` is false,
  # so `ensure_compiled/1` cannot be used directly. We still need to tolerate
  # parent/child compile-order races, so we poll `ensure_loaded/1` briefly.
  defp ensure_namespace_module(module) when is_atom(module) do
    if Code.can_await_module_compilation?() do
      Code.ensure_compiled(module)
    else
      await_loaded_module(module)
    end
  end

  @namespace_load_wait_ms 500
  @namespace_load_poll_ms 10

  defp await_loaded_module(module) when is_atom(module) do
    deadline = System.monotonic_time(:millisecond) + @namespace_load_wait_ms
    await_loaded_module(module, deadline)
  end

  defp await_loaded_module(module, deadline_ms) when is_atom(module) do
    case Code.ensure_loaded(module) do
      {:module, _loaded} = loaded ->
        loaded

      {:error, _reason} = error ->
        if System.monotonic_time(:millisecond) < deadline_ms do
          Process.sleep(@namespace_load_poll_ms)
          await_loaded_module(module, deadline_ms)
        else
          error
        end
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
