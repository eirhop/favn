defmodule Favn.SQLAsset do
  @moduledoc """
  Preferred single-module SQL asset DSL.

  `Favn.SQLAsset` compiles one SQL-authored module into one canonical
  `%Favn.Asset{}` with ref `{Module, :asset}`.

  SQL bodies are authored with `query do ... end` and a real `~SQL` sigil.
  In the current phase, `~SQL` simply returns a plain SQL string.
  """

  alias Favn.Asset
  alias Favn.Namespace
  alias Favn.Ref
  alias Favn.RelationRef
  alias Favn.SQLAsset.Definition
  alias Favn.SQLAsset.Materialization
  alias Favn.SQLAsset.Runtime
  alias Favn.Window.Spec

  @doc false
  defmacro __using__(_opts) do
    quote do
      Module.register_attribute(__MODULE__, :depends, accumulate: true)
      Module.register_attribute(__MODULE__, :meta, persist: false)
      Module.register_attribute(__MODULE__, :produces, accumulate: true)
      Module.register_attribute(__MODULE__, :window, accumulate: true)
      Module.register_attribute(__MODULE__, :materialized, accumulate: true)
      Module.register_attribute(__MODULE__, :favn_sql_asset_raw, persist: false)

      @on_definition Favn.SQLAsset
      @before_compile Favn.SQLAsset

      import Favn.SQLAsset, only: [query: 1, sigil_SQL: 2]
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
          compile_error!(
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

  @doc false
  defmacro sigil_SQL({:<<>>, _meta, parts}, modifiers) when is_list(modifiers) do
    if modifiers != [] do
      compile_error!(__CALLER__.file, __CALLER__.line, "~SQL sigil does not support modifiers")
    end

    if Enum.all?(parts, &is_binary/1) do
      Enum.join(parts)
    else
      compile_error!(
        __CALLER__.file,
        __CALLER__.line,
        "~SQL sigil does not support interpolation"
      )
    end
  end

  defmacro sigil_SQL(_body, modifiers) do
    if modifiers != [] do
      compile_error!(__CALLER__.file, __CALLER__.line, "~SQL sigil does not support modifiers")
    else
      compile_error!(__CALLER__.file, __CALLER__.line, "~SQL body must be a literal string")
    end
  end

  @doc false
  defmacro query(do: body) do
    raw_definition = Module.get_attribute(__CALLER__.module, :favn_sql_asset_raw)

    if raw_definition do
      compile_error!(
        __CALLER__.file,
        __CALLER__.line,
        "Favn.SQLAsset modules can define only one query body"
      )
    end

    sql = extract_sql!(body, __CALLER__)

    Module.put_attribute(__CALLER__.module, :favn_sql_asset_raw, %{
      module: __CALLER__.module,
      file: normalize_file(__CALLER__.file),
      line: __CALLER__.line,
      sql: sql
    })

    quote do
      :ok
    end
  end

  @doc false
  defmacro __before_compile__(env) do
    base_definition = Module.get_attribute(env.module, :favn_sql_asset_raw)

    if is_nil(base_definition) do
      compile_error!(
        env.file,
        env.line,
        "Favn.SQLAsset modules must define exactly one query body"
      )
    end

    raw_definition =
      base_definition
      |> Map.put(:doc, normalize_doc(Module.get_attribute(env.module, :doc)))
      |> Map.put(:depends, env.module |> fetch_accum_attribute(:depends) |> Enum.reverse())
      |> Map.put(:meta, Module.get_attribute(env.module, :meta))
      |> Map.put(:window, env.module |> fetch_accum_attribute(:window) |> Enum.reverse())
      |> Map.put(:produces, env.module |> fetch_accum_attribute(:produces) |> Enum.reverse())
      |> Map.put(
        :materialized,
        env.module |> fetch_accum_attribute(:materialized) |> Enum.reverse()
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
      @spec asset(Favn.Run.Context.t()) :: Favn.Asset.return_value()
      def asset(ctx), do: Runtime.run(__MODULE__, ctx)
    end
  end

  defp build_definition!(raw_definition) do
    depends_on = normalize_depends!(raw_definition.depends, raw_definition)
    meta = normalize_meta!(raw_definition.meta, raw_definition)
    window_spec = normalize_window!(raw_definition.window, raw_definition)
    materialization = normalize_materialized!(raw_definition.materialized, raw_definition)
    produces = normalize_produces!(raw_definition, inferred_relation_name(raw_definition.module))

    asset = %Asset{
      module: raw_definition.module,
      name: :asset,
      ref: Ref.new(raw_definition.module, :asset),
      arity: 1,
      type: :sql,
      title: nil,
      doc: raw_definition.doc,
      file: raw_definition.file,
      line: raw_definition.line,
      meta: meta,
      depends_on: depends_on,
      window_spec: window_spec,
      produces: produces,
      materialization: materialization
    }

    definition = %Definition{
      module: raw_definition.module,
      asset: asset,
      sql: raw_definition.sql,
      materialization: materialization,
      raw_asset: raw_definition
    }

    try do
      _ = Asset.validate!(asset)
      definition
    rescue
      error in ArgumentError ->
        compile_error!(raw_definition.file, raw_definition.line, error.message)
    end
  end

  defp normalize_depends!(depends, raw_definition) do
    Enum.map(depends, fn
      module when is_atom(module) ->
        Ref.new(module, :asset)

      {module, name} when is_atom(module) and is_atom(name) ->
        if module_atom?(module) do
          Ref.new(module, name)
        else
          compile_error!(
            raw_definition.file,
            raw_definition.line,
            "invalid @depends entry #{inspect({module, name})}; expected Module or {Module, :asset_name}"
          )
        end

      dependency ->
        compile_error!(
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
      compile_error!(raw_definition.file, raw_definition.line, error.message)
  end

  defp normalize_window!([], _raw_definition), do: nil
  defp normalize_window!([%Spec{} = spec], _raw_definition), do: spec

  defp normalize_window!([_a, _b | _rest], raw_definition) do
    compile_error!(
      raw_definition.file,
      raw_definition.line,
      "multiple @window attributes are not allowed; use at most one @window for sql ..."
    )
  end

  defp normalize_window!(value, raw_definition) do
    compile_error!(
      raw_definition.file,
      raw_definition.line,
      "invalid @window value #{inspect(value)}; expected Favn.Window spec like Favn.Window.daily()"
    )
  end

  defp normalize_materialized!([value], raw_definition) do
    Materialization.normalize!(value)
  rescue
    error in ArgumentError ->
      compile_error!(raw_definition.file, raw_definition.line, error.message)
  end

  defp normalize_materialized!([], raw_definition) do
    compile_error!(
      raw_definition.file,
      raw_definition.line,
      "Favn.SQLAsset requires one @materialized attribute"
    )
  end

  defp normalize_materialized!([_a, _b | _rest], raw_definition) do
    compile_error!(
      raw_definition.file,
      raw_definition.line,
      "multiple @materialized attributes are not allowed; use exactly one @materialized for sql ..."
    )
  end

  defp normalize_produces!(raw_definition, inferred_name) do
    defaults = Namespace.resolve(raw_definition.module)

    produces_attrs =
      case raw_definition.produces do
        [] ->
          %{}

        [true] ->
          %{}

        [attrs] when is_list(attrs) ->
          if Keyword.keyword?(attrs) do
            Map.new(attrs)
          else
            compile_error!(
              raw_definition.file,
              raw_definition.line,
              "invalid @produces value #{inspect(attrs)}; expected true, a keyword list, or a map"
            )
          end

        [attrs] when is_map(attrs) ->
          attrs

        [_a, _b | _rest] ->
          compile_error!(
            raw_definition.file,
            raw_definition.line,
            "multiple @produces attributes are not allowed; use at most one @produces for sql ..."
          )

        [other] ->
          compile_error!(
            raw_definition.file,
            raw_definition.line,
            "invalid @produces value #{inspect(other)}; expected true, a keyword list, or a map"
          )
      end

    defaults
    |> maybe_drop_default_key(produces_attrs, [:catalog], [:database, "database"])
    |> maybe_drop_default_key(produces_attrs, [:name], [:table, "table", :name, "name"])
    |> Map.merge(produces_attrs)
    |> maybe_put_inferred_name(inferred_name)
    |> RelationRef.new!()
    |> ensure_sql_relation_connection!(raw_definition)
  rescue
    error in ArgumentError ->
      compile_error!(raw_definition.file, raw_definition.line, error.message)
  end

  defp ensure_sql_relation_connection!(%RelationRef{connection: nil}, raw_definition) do
    compile_error!(
      raw_definition.file,
      raw_definition.line,
      "SQL assets require a connection through Favn.Namespace or @produces"
    )
  end

  defp ensure_sql_relation_connection!(%RelationRef{} = relation_ref, _raw_definition),
    do: relation_ref

  defp validate_no_stray_asset_attributes!(env, kind, name, arity) do
    depends = fetch_accum_attribute(env.module, :depends)
    meta = Module.get_attribute(env.module, :meta)
    window = fetch_accum_attribute(env.module, :window)
    produces = fetch_accum_attribute(env.module, :produces)
    materialized = fetch_accum_attribute(env.module, :materialized)

    if depends != [] or not is_nil(meta) or window != [] or produces != [] or materialized != [] do
      Module.delete_attribute(env.module, :depends)
      Module.delete_attribute(env.module, :meta)
      Module.delete_attribute(env.module, :window)
      Module.delete_attribute(env.module, :produces)
      Module.delete_attribute(env.module, :materialized)

      compile_error!(
        env.file,
        env.line,
        "@depends/@meta/@window/@produces/@materialized on #{kind} #{name}/#{arity} requires sql ... immediately below those attributes"
      )
    else
      :ok
    end
  end

  defp inferred_relation_name(module) do
    module
    |> Module.split()
    |> List.last()
    |> Macro.underscore()
  end

  defp maybe_drop_default_key(defaults, attrs, canonical_keys, authored_keys) do
    if Enum.any?(authored_keys, &Map.has_key?(attrs, &1)) do
      Enum.reduce(canonical_keys, defaults, &Map.delete(&2, &1))
    else
      defaults
    end
  end

  defp maybe_put_inferred_name(attrs, inferred_name) do
    if Enum.any?([:name, "name", :table, "table"], &Map.has_key?(attrs, &1)) do
      attrs
    else
      Map.put(attrs, :name, inferred_name)
    end
  end

  defp extract_sql!(body, env) do
    case body do
      binary when is_binary(binary) ->
        binary

      {:sigil_SQL, _meta, [parts_ast, modifiers]} ->
        extract_sigil_sql!(parts_ast, modifiers, env)

      _ ->
        compile_error!(
          env.file,
          env.line,
          "query body must contain a ~SQL literal"
        )
    end
  end

  defp extract_sigil_sql!(_parts_ast, modifiers, env) when modifiers != [] do
    compile_error!(env.file, env.line, "~SQL sigil does not support modifiers")
  end

  defp extract_sigil_sql!({:<<>>, _meta, parts}, [], env) do
    if Enum.all?(parts, &is_binary/1) do
      Enum.join(parts)
    else
      compile_error!(env.file, env.line, "~SQL sigil does not support interpolation")
    end
  end

  defp extract_sigil_sql!(_parts_ast, [], env) do
    compile_error!(env.file, env.line, "query body must contain a ~SQL literal")
  end

  defp normalize_doc({_line, false}), do: nil
  defp normalize_doc({_line, doc}) when is_binary(doc), do: doc
  defp normalize_doc(false), do: nil
  defp normalize_doc(doc) when is_binary(doc), do: doc
  defp normalize_doc(_), do: nil

  defp fetch_accum_attribute(module, attribute) do
    module
    |> Module.get_attribute(attribute)
    |> List.wrap()
  end

  defp normalize_file(file) do
    file
    |> to_string()
    |> Path.relative_to_cwd()
  end

  defp compile_error!(file, line, description) do
    raise CompileError, file: file, line: line, description: description
  end

  defp module_atom?(module) when is_atom(module) do
    module
    |> Atom.to_string()
    |> String.starts_with?("Elixir.")
  end
end
