defmodule Favn.SQL do
  @moduledoc """
  Reusable SQL authoring DSL.

  This module provides compile-time reusable SQL definitions (`defsql`) and the
  `~SQL` sigil used by `Favn.SQLAsset`.

  ## When to use it

  Use `Favn.SQL` when several SQL assets share SQL fragments, CTE builders, or
  parameterized reusable SQL definitions.

  Use `Favn.SQLAsset` when authoring a concrete runnable SQL asset.
  Asset-specific SQL should stay in or next to the asset module; this module is
  for SQL reused by multiple assets.

  ## Features

  - `~SQL` for interpolation-free SQL literals
  - `defsql` for reusable SQL definitions with named arguments
  - compile-time validation of imported SQL definitions
  - compile-time template analysis before runtime execution exists

  ## Example

      defmodule MyApp.SQL.Reporting do
        use Favn.SQL

        defsql orders_in_window(start_at, end_at) do
          ~SQL"select * from raw.orders where inserted_at >= @start_at and inserted_at < @end_at"
        end
      end

      defmodule MyApp.Warehouse.Mart.OrderSummary do
        use MyApp.SQL.Reporting
        use Favn.SQLAsset

        @materialized :view

        query do
          ~SQL"select * from orders_in_window(@window_start, @window_end)"
        end
      end

  ## Rules

  - keep `defsql` definitions small and composable
  - prefer `~SQL` literals over string interpolation
  - keep runnable asset concerns such as `@materialized` and `@window` in
    `Favn.SQLAsset`, not in the reusable SQL provider module

  ## See also

  - `Favn.SQLAsset`
  - `Favn.Connection`
  - `Favn.Window`
  """

  alias Favn.DSL.Compiler, as: DSLCompiler
  alias Favn.SQL.Definition
  alias Favn.SQL.Definition.Param
  alias Favn.SQL.Source
  alias Favn.SQL.Template

  @type opts :: keyword()

  @doc false
  defmacro __using__(_opts) do
    quote do
      Module.register_attribute(__MODULE__, :favn_sql_raw_definitions, accumulate: true)
      Module.register_attribute(__MODULE__, :favn_sql_imports, accumulate: true)

      @before_compile Favn.SQL

      import Favn.SQL, only: [defsql: 2, sigil_SQL: 2]
    end
  end

  @doc """
  Canonical SQL sigil for Favn SQL authoring.

  Use this for literal SQL only. Interpolation is intentionally rejected so the
  compiler can analyze the full SQL body deterministically.
  """
  defmacro sigil_SQL({:<<>>, _meta, parts}, modifiers) when is_list(modifiers) do
    if modifiers != [] do
      DSLCompiler.compile_error!(
        __CALLER__.file,
        __CALLER__.line,
        "~SQL sigil does not support modifiers"
      )
    end

    if Enum.all?(parts, &is_binary/1) do
      Enum.join(parts)
    else
      DSLCompiler.compile_error!(
        __CALLER__.file,
        __CALLER__.line,
        "~SQL sigil does not support interpolation"
      )
    end
  end

  defmacro sigil_SQL(_body, modifiers) do
    if modifiers != [] do
      DSLCompiler.compile_error!(
        __CALLER__.file,
        __CALLER__.line,
        "~SQL sigil does not support modifiers"
      )
    else
      DSLCompiler.compile_error!(
        __CALLER__.file,
        __CALLER__.line,
        "~SQL body must be a literal string"
      )
    end
  end

  @doc """
  Defines reusable SQL that can be referenced from SQL assets or other reusable
  SQL definitions.

  Use this when you want one shared SQL definition to be imported by many SQL
  assets.

  Supported forms:

  - `defsql name(args...) do ... end`
  - `defsql name(args...), file: "path/to/file.sql"`
  """
  defmacro defsql({name, _meta, args_ast}, do: body)
           when is_atom(name) and (is_list(args_ast) or is_nil(args_ast)) do
    args = normalize_defsql_args!(args_ast || [], __CALLER__)
    validate_reserved_arg_names!(args, __CALLER__)
    sql = extract_sql!(body, __CALLER__, "defsql body must contain a ~SQL literal")
    arity = length(args)

    raw = %{
      module: __CALLER__.module,
      name: name,
      args: args,
      arity: arity,
      sql: sql,
      file: DSLCompiler.normalize_file(__CALLER__.file),
      line: __CALLER__.line,
      sql_file: DSLCompiler.normalize_file(__CALLER__.file),
      sql_line: __CALLER__.line
    }

    quote bind_quoted: [raw: Macro.escape(raw)] do
      @favn_sql_raw_definitions raw
      :ok
    end
  end

  defmacro defsql({name, _meta, args_ast}, file: path)
           when is_atom(name) and (is_list(args_ast) or is_nil(args_ast)) and is_binary(path) do
    args = normalize_defsql_args!(args_ast || [], __CALLER__)
    validate_reserved_arg_names!(args, __CALLER__)
    arity = length(args)

    source = Source.load_file!(__CALLER__, path, owner: "defsql")

    raw = %{
      module: __CALLER__.module,
      name: name,
      args: args,
      arity: arity,
      sql: source.sql,
      file: DSLCompiler.normalize_file(__CALLER__.file),
      line: __CALLER__.line,
      sql_file: source.sql_file,
      sql_line: source.sql_line
    }

    quote bind_quoted: [raw: Macro.escape(raw)] do
      @favn_sql_raw_definitions raw
      :ok
    end
  end

  defmacro defsql(_head, do: _body) do
    DSLCompiler.compile_error!(
      __CALLER__.file,
      __CALLER__.line,
      "defsql expects a function-style head, for example defsql my_macro(arg) do ... end"
    )
  end

  defmacro defsql(_head, file: _path) do
    DSLCompiler.compile_error!(
      __CALLER__.file,
      __CALLER__.line,
      "defsql expects a function-style head, for example defsql my_macro(arg), file: \"sql/my_macro.sql\""
    )
  end

  defmacro defsql(_head, opts) do
    DSLCompiler.compile_error!(
      __CALLER__.file,
      __CALLER__.line,
      "defsql expects either a do block or file: \"path/to/query.sql\", got: #{Macro.to_string(opts)}"
    )
  end

  @doc false
  defmacro __before_compile__(env) do
    provider_module = env.module

    raw_definitions =
      env.module
      |> Module.get_attribute(:favn_sql_raw_definitions)
      |> List.wrap()
      |> Enum.reverse()

    ensure_unique_definition_keys!(raw_definitions)

    imports =
      env.module
      |> Module.get_attribute(:favn_sql_imports)
      |> List.wrap()
      |> Enum.reverse()
      |> Enum.uniq()

    definitions = build_sql_definitions!(env.module, raw_definitions, imports)

    using_ast =
      quote do
        if not Module.has_attribute?(__MODULE__, :favn_sql_imports) do
          Module.register_attribute(__MODULE__, :favn_sql_imports, accumulate: true)
        end

        @favn_sql_imports unquote(provider_module)
        import Favn.SQL, only: [sigil_SQL: 2]
      end

    quote do
      @doc false
      @spec __favn_sql_definitions__() :: [Favn.SQL.Definition.t()]
      def __favn_sql_definitions__, do: unquote(Macro.escape(definitions))

      @doc false
      defmacro __using__(_opts), do: unquote(Macro.escape(using_ast))
    end
  end

  @spec imported_definitions(module()) :: [Definition.t()]
  def imported_definitions(module) when is_atom(module) do
    if function_exported?(module, :module_info, 1) do
      module.module_info(:attributes)
      |> Keyword.get(:favn_sql_imports, [])
      |> List.flatten()
      |> Enum.uniq()
      |> Enum.flat_map(fn imported_module ->
        if function_exported?(imported_module, :__favn_sql_definitions__, 0) do
          imported_module.__favn_sql_definitions__()
        else
          []
        end
      end)
    else
      []
    end
  end

  defp build_sql_definitions!(module, raw_definitions, imports) do
    imported_definitions = fetch_imported_definitions!(imports)

    provisional =
      Enum.map(raw_definitions, fn raw ->
        inferred_root_kind = Template.infer_root_kind!(raw.sql, file: raw.file, line: raw.line)

        %Definition{
          module: module,
          name: raw.name,
          arity: raw.arity,
          params:
            Enum.with_index(raw.args, fn name, index -> %Param{name: name, index: index} end),
          shape: if(inferred_root_kind == :query, do: :relation, else: :expression),
          sql: raw.sql,
          template: nil,
          file: raw.sql_file,
          line: raw.sql_line,
          declared_file: raw.file,
          declared_line: raw.line
        }
      end)

    known_definitions =
      build_visible_definition_catalog!(module, provisional, imported_definitions)

    local_definitions =
      Enum.map(provisional, fn %Definition{} = definition ->
        template =
          Template.compile!(definition.sql,
            known_definitions: known_definitions,
            file: definition.file,
            line: definition.line,
            module: module,
            scope: :definition,
            local_arg_index: Map.new(definition.params, &{&1.name, &1.index}),
            enforce_query_root: false
          )

        %Definition{definition | template: template}
      end)

    visible_definitions =
      build_visible_definition_catalog!(module, local_definitions, imported_definitions)

    detect_definition_cycles!(local_definitions, visible_definitions)
    local_definitions
  end

  defp fetch_imported_definitions!(imports) do
    Enum.flat_map(imports, fn module ->
      case Code.ensure_compiled(module) do
        {:module, _} ->
          if function_exported?(module, :__favn_sql_definitions__, 0) do
            module.__favn_sql_definitions__()
          else
            DSLCompiler.compile_error!(
              "nofile",
              1,
              "imported SQL provider #{inspect(module)} does not define reusable SQL"
            )
          end

        {:error, _reason} ->
          DSLCompiler.compile_error!(
            "nofile",
            1,
            "imported SQL provider #{inspect(module)} could not be resolved"
          )
      end
    end)
  end

  defp normalize_defsql_args!(args_ast, env) do
    Enum.map(args_ast, fn
      {name, _meta, context} when is_atom(name) and is_atom(context) ->
        name

      {name, _meta, nil} when is_atom(name) ->
        name

      other ->
        DSLCompiler.compile_error!(
          env.file,
          env.line,
          "defsql arguments must be plain variable names, got: #{Macro.to_string(other)}"
        )
    end)
  end

  defp validate_reserved_arg_names!(args, env) do
    reserved = MapSet.new(Template.reserved_runtime_inputs())

    Enum.each(args, fn arg ->
      if MapSet.member?(reserved, arg) do
        DSLCompiler.compile_error!(
          env.file,
          env.line,
          "defsql argument @#{arg} is reserved for runtime SQL inputs"
        )
      end
    end)
  end

  defp ensure_unique_definition_keys!(raw_definitions) do
    duplicates =
      raw_definitions
      |> Enum.group_by(&{&1.name, &1.arity})
      |> Enum.filter(fn {_key, entries} -> length(entries) > 1 end)

    if duplicates != [] do
      [{name, arity}, _entries] = hd(duplicates)
      DSLCompiler.compile_error!("nofile", 1, "duplicate defsql #{name}/#{arity}")
    end
  end

  defp build_visible_definition_catalog!(module, local_definitions, imported_definitions) do
    imported_catalog = Map.new(imported_definitions, &{Definition.key(&1), &1})

    local_catalog =
      local_definitions
      |> Enum.filter(&(&1.module == module))
      |> Map.new(&{Definition.key(&1), &1})

    Map.merge(imported_catalog, local_catalog)
  end

  defp detect_definition_cycles!(local_definitions, visible_definitions) do
    local_definitions
    |> Enum.each(fn definition ->
      detect_definition_cycle!(definition, visible_definitions, [])
    end)
  end

  defp detect_definition_cycle!(definition, visible_definitions, stack) do
    key = Definition.key(definition)

    if key in stack do
      cycle = Enum.reverse([key | stack])

      DSLCompiler.compile_error!(
        definition.file,
        definition.line,
        "cyclic defsql dependency detected: #{inspect(cycle)}"
      )
    end

    called = Template.called_definition_keys(definition.template)

    Enum.each(called, fn called_key ->
      case Map.fetch(visible_definitions, called_key) do
        {:ok, called_definition} ->
          detect_definition_cycle!(called_definition, visible_definitions, [key | stack])

        :error ->
          DSLCompiler.compile_error!(
            definition.file,
            definition.line,
            "unknown SQL definition #{elem(called_key, 0)}/#{elem(called_key, 1)}"
          )
      end
    end)
  end

  @spec extract_sql!(Macro.t(), Macro.Env.t(), String.t()) :: String.t()
  def extract_sql!(body, env, error_message) do
    case body do
      {:__block__, _meta, [sql]} -> extract_sql!(sql, env, error_message)
      {:sigil_SQL, _meta, [{:<<>>, _, _parts}, _mods]} -> Macro.expand(body, env)
      {:sigil_SQL, _meta, [binary, _mods]} -> Macro.expand({:sigil_SQL, [], [binary, []]}, env)
      _ -> DSLCompiler.compile_error!(env.file, env.line, error_message)
    end
  end
end
