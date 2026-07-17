defmodule Favn.Namespace do
  @moduledoc """
  Declares inherited defaults for descendant asset and source modules.

  Namespace modules are structural. They use the same declaration syntax as
  assets, while executable definitions remain in descendant `Favn.Asset`,
  `Favn.MultiAsset`, `Favn.SQLAsset`, or `Favn.Source` modules.

      defmodule MyApp.Lakehouse do
        use Favn.Namespace

        relation(connection: :important_lakehouse)
        settings(environment: "production")
        meta(owner: "data-platform")
      end

      defmodule MyApp.Lakehouse.Raw do
        use Favn.Namespace

        relation(catalog: "raw")
        resources([:azure_extension])
      end

      defmodule MyApp.Lakehouse.Raw.Sales do
        use Favn.Namespace

        relation(schema: "sales")
        runtime_config(MyApp.RuntimeConfigs.storage())
        runtime_inputs(MyApp.Lakehouse.Raw.Inputs)
        freshness({:daily, timezone: "Etc/UTC"})
      end

      defmodule MyApp.Lakehouse.Raw.Sales.Orders do
        use Favn.SQLAsset

        settings(dataset: "orders")
        materialized(:table)
        relation(true)
        query(file: "orders.sql")
      end

  Descendants are discovered from normal Elixir module ancestry. An asset does
  not also `use Favn.Namespace`; combining namespace and asset/source DSLs in
  one module is rejected so every declaration has one clear owner.

  ## Resolution

  Favn resolves namespace modules from root to leaf and then applies the leaf
  asset declarations:

  - `relation` merges by key.
  - `resources` combines additively for SQL assets.
  - `settings` and `meta` shallow-merge, with the closest value winning for the
    same key.
  - `runtime_config` bundles combine through the normal requirement conflict
    validation.
  - `runtime_inputs`, `freshness`, `window`, and `materialized` use the closest
    declaration. `nil` clears the optional scalar declarations.

  `Favn.MultiAsset` adds its module declarations after namespace defaults and
  child declarations last. Each asset kind consumes the declarations already
  supported by its public DSL. Metadata also applies to `Favn.Source`.

  SQL runtime configuration still has an explicit consumer: a SQL asset with
  effective runtime requirements must have an effective `runtime_inputs`
  resolver. Resolved values remain runner-local and the resolver converts only
  selected values into bounded SQL parameters.

  Keep dependencies, contracts, checks, queries, and executable functions on
  leaf modules. Put a default on the narrowest namespace whose compatible
  descendants share it.

  ## Recommended project shape

      lib/my_app/
        connections/important_lakehouse.ex
        lakehouse.ex
        lakehouse/raw.ex
        lakehouse/raw/sales.ex
        lakehouse/raw/sales/orders.ex
        lakehouse/raw/sales/orders.sql
        lakehouse/mart.ex
        lakehouse/mart/sales.ex
        lakehouse/mart/sales/order_summary.ex

  Use `catalog` for databases or lakehouse phases such as raw and mart. Use
  `schema` for domains such as sales and finance. Keep connection providers,
  integration clients, pipelines, triggers, and reusable SQL outside the
  lakehouse tree unless the project has a stronger documented convention.

  ## See also

  - `Favn.Asset`
  - `Favn.MultiAsset`
  - `Favn.SQLAsset`
  - `Favn.Source`
  """

  alias Favn.DSL.AssetDeclarations
  alias Favn.DSL.Compiler, as: DSLCompiler
  alias Favn.Namespace.Config

  @declarations [
    :settings,
    :meta,
    :runtime_config,
    :runtime_inputs,
    :freshness,
    :window,
    :materialized,
    :relation,
    :resources
  ]

  @doc false
  defmacro __using__(opts) do
    env = __CALLER__

    if opts != [] do
      DSLCompiler.compile_error!(
        env.file,
        env.line,
        "Favn.Namespace accepts declarations as macros after `use Favn.Namespace`; " <>
          "replace namespace options with relation(...), resources(...), settings(...), " <>
          "meta(...), runtime_config(...), runtime_inputs(...), freshness(...), " <>
          "window(...), or materialized(...)"
      )
    end

    quote bind_quoted: [file: env.file, line: env.line, declarations: @declarations] do
      Favn.DSL.AssetDeclarations.claim_module!(__MODULE__, :namespace, file, line)
      Favn.DSL.AssetDeclarations.register!(__MODULE__, declarations)
      Module.register_attribute(__MODULE__, :favn_namespace_config, persist: false)

      import Favn.DSL.AssetDeclarations,
        only: [
          settings: 1,
          meta: 1,
          freshness: 1,
          window: 1,
          relation: 1,
          runtime_config: 1,
          runtime_config: 2,
          env!: 1,
          env!: 2,
          secret_env!: 1,
          secret_env!: 2
        ]

      import Favn.Namespace, only: [materialized: 1, resources: 1, runtime_inputs: 1]

      @before_compile Favn.Namespace
    end
  end

  @doc "Declares the inherited SQL materialization strategy."
  defmacro materialized(value) do
    quote do
      Favn.DSL.AssetDeclarations.put(__MODULE__, :materialized, unquote(value))
    end
  end

  @doc "Declares inherited named SQL session resources."
  defmacro resources(values) do
    quote do
      Favn.DSL.AssetDeclarations.put(__MODULE__, :resources, unquote(values))
    end
  end

  @doc "Declares the inherited SQL runtime-input resolver."
  defmacro runtime_inputs(module) do
    quote do
      Favn.DSL.AssetDeclarations.put(__MODULE__, :runtime_inputs, unquote(module))
    end
  end

  @doc false
  defmacro __before_compile__(env) do
    declarations = Map.new(@declarations, &{&1, AssetDeclarations.values(env.module, &1)})

    config =
      try do
        Config.new!(declarations)
      rescue
        error in ArgumentError ->
          DSLCompiler.compile_error!(env.file, env.line, error.message)
      end

    Module.put_attribute(env.module, :favn_namespace_config, config)

    quote do
      @doc false
      @spec __favn_namespace_config__() :: Favn.Namespace.Config.t()
      def __favn_namespace_config__, do: unquote(Macro.escape(config))
    end
  end

  @doc "Resolves relation defaults selected by ancestor namespaces."
  @spec resolve_relation(module()) :: map()
  def resolve_relation(module) when is_atom(module), do: resolve(module).relation

  @doc "Resolves runtime-configuration bundles selected by ancestor namespaces."
  @spec resolve_runtime_config(module()) :: [Favn.RuntimeConfig.Bundle.t()]
  def resolve_runtime_config(module) when is_atom(module), do: resolve(module).runtime_config

  @doc "Resolves named SQL session resources selected by ancestor namespaces."
  @spec resolve_resources(module()) :: [String.t()]
  def resolve_resources(module) when is_atom(module), do: resolve(module).resources

  @doc false
  @spec effective_declarations(Config.t(), atom(), [term()]) :: [term()]
  defdelegate effective_declarations(config, field, local), to: Config

  @doc false
  @spec resolve(module()) :: Config.t()
  def resolve(module) when is_atom(module) do
    module
    |> ancestors()
    |> Enum.reduce(Config.new!(%{}), fn ancestor, acc ->
      case namespace_config(ancestor, module) do
        nil ->
          acc

        %Config{} = config ->
          Config.merge(acc, config)
      end
    end)
    |> Config.finalize()
  end

  defp namespace_config(module, target) do
    cond do
      module == target and module_open?(module) ->
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

  defp ensure_namespace_module(module) when is_atom(module) do
    if Code.can_await_module_compilation?() do
      Code.ensure_compiled(module)
    else
      Code.ensure_loaded(module)
    end
  end

  defp ancestors(module) do
    parts = Module.split(module)

    1..length(parts)
    |> Enum.map(fn index -> Module.concat(Enum.take(parts, index)) end)
  end
end
