defmodule Favn.SQLAsset do
  @moduledoc """
  Preferred single-asset SQL DSL.

  Use this module when one module should define one SQL asset. Like
  `Favn.Asset`, it compiles to one canonical `%Favn.Asset{}` with ref
  `{Module, :asset}`, but the authored body comes from `query/1` instead of a
  handwritten `asset/1` function.

  ## When to use it

  Use `Favn.SQLAsset` when:

  - the asset body is primarily SQL
  - you want Favn-aware relation references, placeholders, and reusable SQL
  - you want the SQL asset to participate in the same dependency and runtime model as Elixir assets

  ## Minimal example

      defmodule MyApp.Gold.Sales.FctOrders do
        use Favn.Namespace, relation: [connection: :warehouse, catalog: "gold", schema: "sales"]
        use Favn.SQLAsset

        @doc "Build the gold orders fact table"
        @meta owner: "analytics", category: :sales, tags: [:gold]
        @window Favn.Window.daily(lookback: 1)
        @materialized {:incremental, strategy: :delete_insert, window_column: :order_date}

        query do
          ~SQL\"""
          select *
          from silver.sales.stg_orders
          where order_date >= @window_start
            and order_date < @window_end
          \"""
        end
      end

  ## Authoring contract

  - define exactly one `query/1` declaration
  - declare exactly one `@materialized`
  - attach `@doc`, `@meta`, `@depends`, `@window`, `@materialized`, and optional `@relation` before `query`
  - use `~SQL` for inline SQL bodies
  - use `query file: "..."` for file-backed SQL loaded at compile time

  ## Supported attributes

  - `@doc`: asset documentation
  - `@meta`: keyword or map metadata such as `owner`, `category`, and `tags`
  - `@depends`: repeatable dependency declaration
  - `@window`: one `Favn.Window.*` spec
  - `@relation`: optional owned relation declaration
  - `@materialized`: required SQL materialization strategy

  `@depends` supports:

  - `Other.SingleAssetModule`
  - `{Other.MultiAssetModule, :asset_name}`

  `@materialized` currently supports:

  - `:table`
  - `:view`
  - `{:incremental, strategy: :append}`
  - `{:incremental, strategy: :delete_insert, window_column: :column_name}`

  Incremental notes:

  - incremental materialization requires `@window`
  - `:append` does not accept `:window_column`
  - `:delete_insert` requires `:window_column`
  - `:merge`, `:replace`, and `unique_key` are not supported in v0.4

  ## Dependency inference

  Relation-style references such as `silver.sales.stg_orders` are the preferred
  way to reference upstream SQL inputs.

  When a relation reference resolves to an owned asset relation in the same
  connection, Favn infers the dependency automatically. Use `@depends` when the
  dependency is not visible in the SQL body, when you need a non-SQL upstream,
  or when the relation cannot be resolved from owned asset metadata.

  ## What gets compiled

  The DSL keeps both authored SQL and normalized SQL IR so Favn can validate
  reusable SQL definitions, placeholders, and typed relation usage at compile
  time. The final public output is still one canonical `%Favn.Asset{}` plus
  SQL-specific definition metadata used by rendering and execution.

  ## Runtime context notes

  The generated `asset/1` calls into the SQL runtime automatically. Runtime
  inputs such as window bounds and explicit `params` are resolved during render
  and execution, not inside user-authored Elixir code.

  ## Common mistakes

  - defining more than one query
  - forgetting `@materialized`
  - using interpolation inside `~SQL`
  - using invalid `@depends`, `@window`, or `@relation` values
  - expecting `asset/1` to be user-defined in a `Favn.SQLAsset` module

  ## See also

  - `Favn.SQL`
  - `Favn.Window`
  - `Favn.Connection`
  """

  alias Favn.Asset
  alias Favn.Asset.RelationResolver
  alias Favn.DSL.Compiler, as: DSLCompiler
  alias Favn.Namespace
  alias Favn.Ref
  alias Favn.RelationRef
  alias Favn.SQL
  alias Favn.SQL.Definition, as: SQLDefinition
  alias Favn.SQL.Source
  alias Favn.SQL.Template
  alias Favn.SQLAsset.Definition
  alias Favn.SQLAsset.Materialization
  alias Favn.SQLAsset.RelationUsage
  alias Favn.Window.Spec

  @doc false
  defmacro __using__(_opts) do
    quote do
      Module.register_attribute(__MODULE__, :depends, accumulate: true)
      Module.register_attribute(__MODULE__, :meta, persist: false)
      Module.register_attribute(__MODULE__, :relation, accumulate: true)
      Module.register_attribute(__MODULE__, :window, accumulate: true)
      Module.register_attribute(__MODULE__, :materialized, accumulate: true)
      Module.register_attribute(__MODULE__, :favn_sql_asset_raw, persist: false)
      Module.register_attribute(__MODULE__, :favn_sql_imports, accumulate: true)

      @on_definition Favn.SQLAsset
      @before_compile Favn.SQLAsset

      import Favn.SQLAsset, only: [query: 1]
      import Favn.SQL, only: [sigil_SQL: 2]
    end
  end

  @doc false
  def __on_definition__(env, kind, name, args, _guards, _body) do
    arity = length(args || [])

    generated_definition? =
      Module.get_attribute(env.module, :favn_sql_asset_generating) == true and
        kind == :def and
        name in [
          :__favn_asset_compiler__,
          :__favn_assets_raw__,
          :__favn_sql_asset_definition__,
          :__favn_single_asset__,
          :asset
        ]

    if generated_definition? do
      :ok
    else
      case {kind, name, arity} do
        {kind, :asset, _arity} when kind in [:def, :defp] ->
          DSLCompiler.compile_error!(
            env.file,
            env.line,
            "Favn.SQLAsset reserves asset/1 and generates it automatically"
          )

        {kind, _name, _arity} when kind in [:def, :defp] ->
          validate_no_stray_asset_attributes!(env, kind, name, arity)

        _ ->
          :ok
      end
    end
  end

  @doc """
  Declares the SQL body for a `Favn.SQLAsset`.

  Use either an inline `~SQL` body or `file: "..."`. Each module may declare
  exactly one query.

  Supported forms:

  - `query do ... end`
  - `query file: "path/to/query.sql"`

  ## Examples

      query do
        ~SQL\"""
        select *
        from raw.sales.orders
        \"""
      end

      query file: "sql/fct_orders.sql"
  """
  defmacro query(do: body) do
    raw_definition = Module.get_attribute(__CALLER__.module, :favn_sql_asset_raw)

    if raw_definition do
      DSLCompiler.compile_error!(
        __CALLER__.file,
        __CALLER__.line,
        "Favn.SQLAsset modules can define only one query body"
      )
    end

    sql = extract_sql!(body, __CALLER__)

    Module.put_attribute(__CALLER__.module, :favn_sql_asset_raw, %{
      module: __CALLER__.module,
      file: DSLCompiler.normalize_file(__CALLER__.file),
      line: __CALLER__.line,
      sql_file: DSLCompiler.normalize_file(__CALLER__.file),
      sql_line: __CALLER__.line,
      sql: sql
    })

    quote do
      :ok
    end
  end

  defmacro query(file: path) when is_binary(path) do
    raw_definition = Module.get_attribute(__CALLER__.module, :favn_sql_asset_raw)

    if raw_definition do
      DSLCompiler.compile_error!(
        __CALLER__.file,
        __CALLER__.line,
        "Favn.SQLAsset modules can define only one query body"
      )
    end

    source = Source.load_file!(__CALLER__, path, owner: "query")

    Module.put_attribute(__CALLER__.module, :favn_sql_asset_raw, %{
      module: __CALLER__.module,
      file: DSLCompiler.normalize_file(__CALLER__.file),
      line: __CALLER__.line,
      sql_file: source.sql_file,
      sql_line: source.sql_line,
      sql: source.sql
    })

    quote do
      :ok
    end
  end

  defmacro query(opts) do
    DSLCompiler.compile_error!(
      __CALLER__.file,
      __CALLER__.line,
      "query expects either a do block or file: \"path/to/query.sql\", got: #{Macro.to_string(opts)}"
    )
  end

  @doc false
  defmacro __before_compile__(env) do
    base_definition = Module.get_attribute(env.module, :favn_sql_asset_raw)

    if is_nil(base_definition) do
      DSLCompiler.compile_error!(
        env.file,
        env.line,
        "Favn.SQLAsset modules must define exactly one query body"
      )
    end

    raw_definition =
      base_definition
      |> Map.put(:doc, DSLCompiler.normalize_doc(Module.get_attribute(env.module, :doc)))
      |> Map.put(
        :depends,
        env.module |> DSLCompiler.fetch_accum_attribute(:depends) |> Enum.reverse()
      )
      |> Map.put(:meta, Module.get_attribute(env.module, :meta))
      |> Map.put(
        :window,
        env.module |> DSLCompiler.fetch_accum_attribute(:window) |> Enum.reverse()
      )
      |> Map.put(
        :relation,
        env.module |> DSLCompiler.fetch_accum_attribute(:relation) |> Enum.reverse()
      )
      |> Map.put(
        :materialized,
        env.module |> DSLCompiler.fetch_accum_attribute(:materialized) |> Enum.reverse()
      )
      |> Map.put(
        :sql_imports,
        env.module |> DSLCompiler.fetch_accum_attribute(:favn_sql_imports) |> Enum.reverse()
      )

    definition = build_definition!(raw_definition)

    Module.put_attribute(env.module, :favn_sql_asset_generating, true)

    quote do
      @doc false
      @spec __favn_asset_compiler__() :: module()
      def __favn_asset_compiler__, do: Favn.SQLAsset.Compiler

      @doc false
      def __favn_assets_raw__, do: [unquote(Macro.escape(raw_definition))]

      @doc false
      @spec __favn_sql_asset_definition__() :: Favn.SQLAsset.Definition.t()
      def __favn_sql_asset_definition__, do: unquote(Macro.escape(definition))

      @doc false
      def __favn_single_asset__, do: true

      @doc false
      @spec asset(map()) :: Favn.Asset.return_value()
      def asset(ctx), do: Favn.SQLAsset.runtime_asset(__MODULE__, ctx)
    end
  end

  @doc false
  @spec runtime_asset(module(), map()) :: Favn.Asset.return_value()
  def runtime_asset(module, ctx) when is_atom(module) and is_map(ctx) do
    runtime_module = Favn.SQLAsset.Runtime

    with {:module, ^runtime_module} <- Code.ensure_loaded(runtime_module),
         true <- function_exported?(runtime_module, :run, 2) do
      :erlang.apply(runtime_module, :run, [module, struct(Favn.Run.Context, ctx)])
    else
      _ -> {:error, :runtime_not_available}
    end
  end

  defp build_definition!(raw_definition) do
    depends_on = normalize_depends!(raw_definition.depends, raw_definition)
    meta = normalize_meta!(raw_definition.meta, raw_definition)
    window_spec = normalize_window!(raw_definition.window, raw_definition)

    materialization =
      normalize_materialized!(raw_definition.materialized, window_spec, raw_definition)

    relation =
      normalize_relation!(
        raw_definition,
        RelationResolver.inferred_relation_name_for_module(raw_definition.module)
      )

    known_definitions = fetch_sql_definitions!(raw_definition)

    template =
      Template.compile!(raw_definition.sql,
        known_definitions: known_definitions,
        file: raw_definition.sql_file,
        line: raw_definition.sql_line,
        module: raw_definition.module,
        scope: :query,
        local_args: [],
        enforce_query_root: true
      )

    relation_inputs =
      RelationUsage.collect(raw_definition.module, template, Map.values(known_definitions))

    asset = %Asset{
      module: raw_definition.module,
      name: :asset,
      entrypoint: :asset,
      ref: Ref.new(raw_definition.module, :asset),
      arity: 1,
      type: :sql,
      title: nil,
      doc: raw_definition.doc,
      file: raw_definition.file,
      line: raw_definition.line,
      meta: meta,
      depends_on: depends_on,
      config: %{},
      relation_inputs: relation_inputs,
      window_spec: window_spec,
      relation: relation,
      materialization: materialization
    }

    definition = %Definition{
      module: raw_definition.module,
      asset: asset,
      sql: raw_definition.sql,
      template: template,
      relation_inputs: relation_inputs,
      sql_definitions: Map.values(known_definitions),
      materialization: materialization,
      raw_asset: raw_definition
    }

    try do
      _ = Asset.validate!(asset)
      definition
    rescue
      error in ArgumentError ->
        DSLCompiler.compile_error!(raw_definition.file, raw_definition.line, error.message)
    end
  end

  defp normalize_depends!(depends, raw_definition) do
    Enum.map(depends, fn
      module when is_atom(module) ->
        Ref.new(module, :asset)

      {module, name} when is_atom(module) and is_atom(name) ->
        if DSLCompiler.module_atom?(module) do
          Ref.new(module, name)
        else
          DSLCompiler.compile_error!(
            raw_definition.file,
            raw_definition.line,
            "invalid @depends entry #{inspect({module, name})}; expected Module or {Module, :asset_name}"
          )
        end

      dependency ->
        DSLCompiler.compile_error!(
          raw_definition.file,
          raw_definition.line,
          "invalid @depends entry #{inspect(dependency)}; expected Module or {Module, :asset_name}"
        )
    end)
  end

  defp normalize_meta!(meta, raw_definition) do
    Asset.normalize_meta!(meta)
  rescue
    error in ArgumentError ->
      DSLCompiler.compile_error!(raw_definition.file, raw_definition.line, error.message)
  end

  defp normalize_window!([], _raw_definition), do: nil
  defp normalize_window!([%Spec{} = spec], _raw_definition), do: spec

  defp normalize_window!([_a, _b | _rest], raw_definition) do
    DSLCompiler.compile_error!(
      raw_definition.file,
      raw_definition.line,
      "multiple @window attributes are not allowed; use at most one @window before query"
    )
  end

  defp normalize_window!(value, raw_definition) do
    DSLCompiler.compile_error!(
      raw_definition.file,
      raw_definition.line,
      "invalid @window value #{inspect(value)}; expected Favn.Window spec like Favn.Window.daily()"
    )
  end

  defp normalize_materialized!([value], window_spec, raw_definition) do
    value
    |> Materialization.normalize!()
    |> validate_incremental_materialized!(window_spec, raw_definition)
  rescue
    error in ArgumentError ->
      DSLCompiler.compile_error!(raw_definition.file, raw_definition.line, error.message)
  end

  defp normalize_materialized!([], _window_spec, raw_definition) do
    DSLCompiler.compile_error!(
      raw_definition.file,
      raw_definition.line,
      "Favn.SQLAsset requires one @materialized attribute"
    )
  end

  defp normalize_materialized!([_a, _b | _rest], _window_spec, raw_definition) do
    DSLCompiler.compile_error!(
      raw_definition.file,
      raw_definition.line,
      "multiple @materialized attributes are not allowed; use exactly one @materialized before query"
    )
  end

  defp validate_incremental_materialized!(
         {:incremental, opts} = materialization,
         window_spec,
         raw_definition
       ) do
    strategy = Keyword.fetch!(opts, :strategy)

    if is_nil(window_spec) do
      DSLCompiler.compile_error!(
        raw_definition.file,
        raw_definition.line,
        "incremental SQL materialization requires @window"
      )
    end

    if Keyword.has_key?(opts, :unique_key) do
      DSLCompiler.compile_error!(
        raw_definition.file,
        raw_definition.line,
        "incremental materialization unique_key is reserved for future :merge semantics and is not supported in Phase 4b"
      )
    end

    case strategy do
      :append ->
        if Keyword.has_key?(opts, :window_column) do
          DSLCompiler.compile_error!(
            raw_definition.file,
            raw_definition.line,
            "incremental :append does not accept :window_column"
          )
        end

      :delete_insert ->
        case Keyword.fetch(opts, :window_column) do
          {:ok, _column} ->
            :ok

          :error ->
            DSLCompiler.compile_error!(
              raw_definition.file,
              raw_definition.line,
              "incremental :delete_insert requires :window_column"
            )
        end

      :merge ->
        DSLCompiler.compile_error!(
          raw_definition.file,
          raw_definition.line,
          "incremental strategy :merge is not supported in Phase 4b"
        )

      :replace ->
        DSLCompiler.compile_error!(
          raw_definition.file,
          raw_definition.line,
          "incremental strategy :replace is not supported in Phase 4b"
        )
    end

    materialization
  end

  defp validate_incremental_materialized!(materialization, _window_spec, _raw_definition),
    do: materialization

  defp normalize_relation!(raw_definition, inferred_name) do
    defaults = Namespace.resolve_relation(raw_definition.module)

    relation_attrs =
      case raw_definition.relation do
        [] ->
          %{}

        [true] ->
          %{}

        [attrs] when is_list(attrs) ->
          if Keyword.keyword?(attrs) do
            Map.new(attrs)
          else
            DSLCompiler.compile_error!(
              raw_definition.file,
              raw_definition.line,
              "invalid @relation value #{inspect(attrs)}; expected true, a keyword list, or a map"
            )
          end

        [attrs] when is_map(attrs) ->
          attrs

        [_a, _b | _rest] ->
          DSLCompiler.compile_error!(
            raw_definition.file,
            raw_definition.line,
            "multiple @relation attributes are not allowed; use at most one @relation before query do ... end"
          )

        [other] ->
          DSLCompiler.compile_error!(
            raw_definition.file,
            raw_definition.line,
            "invalid @relation value #{inspect(other)}; expected true, a keyword list, or a map"
          )
      end

    relation_attrs
    |> RelationResolver.resolve_relation_attrs!(defaults, inferred_name)
    |> ensure_sql_relation!(raw_definition)
  rescue
    error in ArgumentError ->
      DSLCompiler.compile_error!(raw_definition.file, raw_definition.line, error.message)
  end

  defp ensure_sql_relation!(%RelationRef{connection: nil}, raw_definition) do
    DSLCompiler.compile_error!(
      raw_definition.file,
      raw_definition.line,
      "SQL assets require a connection through Favn.Namespace or @relation"
    )
  end

  defp ensure_sql_relation!(%RelationRef{} = relation_ref, _raw_definition),
    do: relation_ref

  defp validate_no_stray_asset_attributes!(env, kind, name, arity) do
    depends = DSLCompiler.fetch_accum_attribute(env.module, :depends)
    meta = Module.get_attribute(env.module, :meta)
    window = DSLCompiler.fetch_accum_attribute(env.module, :window)
    relation = DSLCompiler.fetch_accum_attribute(env.module, :relation)
    materialized = DSLCompiler.fetch_accum_attribute(env.module, :materialized)

    if depends != [] or not is_nil(meta) or window != [] or relation != [] or materialized != [] do
      Module.delete_attribute(env.module, :depends)
      Module.delete_attribute(env.module, :meta)
      Module.delete_attribute(env.module, :window)
      Module.delete_attribute(env.module, :relation)
      Module.delete_attribute(env.module, :materialized)

      DSLCompiler.compile_error!(
        env.file,
        env.line,
        "@depends/@meta/@window/@relation/@materialized on #{kind} #{name}/#{arity} requires query immediately below those attributes"
      )
    else
      :ok
    end
  end

  defp extract_sql!(body, env) do
    SQL.extract_sql!(body, env, "query body must contain a ~SQL literal")
  end

  defp fetch_sql_definitions!(raw_definition) do
    raw_definition
    |> Map.get(:sql_imports, [])
    |> Enum.uniq()
    |> Enum.flat_map(fn module ->
      case Code.ensure_compiled(module) do
        {:module, _} ->
          if function_exported?(module, :__favn_sql_definitions__, 0) do
            module.__favn_sql_definitions__()
          else
            DSLCompiler.compile_error!(
              raw_definition.file,
              raw_definition.line,
              "imported SQL provider #{inspect(module)} does not define reusable SQL"
            )
          end

        {:error, _reason} ->
          DSLCompiler.compile_error!(
            raw_definition.file,
            raw_definition.line,
            "imported SQL provider #{inspect(module)} could not be resolved"
          )
      end
    end)
    |> Enum.group_by(fn
      %SQLDefinition{name: name, arity: arity} -> {name, arity}
      other -> other
    end)
    |> Enum.map(fn
      {{name, arity}, [%SQLDefinition{} = definition]} ->
        {{name, arity}, definition}

      {{name, arity}, definitions} ->
        providers = definitions |> Enum.map(&inspect(&1.module)) |> Enum.sort() |> Enum.join(", ")

        DSLCompiler.compile_error!(
          raw_definition.file,
          raw_definition.line,
          "duplicate visible defsql #{name}/#{arity}; conflicting providers: #{providers}"
        )

      {other, _definitions} ->
        DSLCompiler.compile_error!(
          raw_definition.file,
          raw_definition.line,
          "invalid SQL definition import #{inspect(other)}"
        )
    end)
    |> Map.new()
  end
end
