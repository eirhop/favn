defmodule Favn.SQL do
  @moduledoc """
  Reusable SQL authoring DSL for Phase 2.

  This module provides compile-time SQL definition support (`defsql`) and the
  `~SQL` sigil. Runtime SQL session APIs stay in legacy/runtime-focused apps.
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

  @doc false
  @spec connect(term(), opts()) :: {:ok, term()} | {:error, term()}
  def connect(_connection, _opts \\ []) do
    if runtime_bridge_enabled?() do
      {:ok, %{bridge: :phase_2}}
    else
      {:error, :runtime_not_available}
    end
  end

  @doc false
  @spec query(term(), iodata(), opts()) :: {:ok, term()} | {:error, term()}
  def query(_session, _statement, _opts \\ []) do
    if runtime_bridge_enabled?() do
      {:ok, runtime_result(:query)}
    else
      {:error, :runtime_not_available}
    end
  end

  @doc false
  @spec materialize(term(), term(), opts()) :: {:ok, term()} | {:error, term()}
  def materialize(_session, _write_plan, _opts \\ []) do
    if runtime_bridge_enabled?() do
      {:ok, runtime_result(:materialize)}
    else
      {:error, :runtime_not_available}
    end
  end

  @doc false
  @spec disconnect(term()) :: :ok
  def disconnect(_session), do: :ok

  @doc false
  @spec get_relation(term(), term()) :: {:ok, term() | nil} | {:error, term()}
  def get_relation(_session, _relation) do
    if runtime_bridge_enabled?() do
      if rem(System.unique_integer([:positive]), 2) == 0 do
        {:ok, nil}
      else
        {:ok, %{name: "placeholder_relation"}}
      end
    else
      {:error, :runtime_not_available}
    end
  end

  @doc false
  @spec columns(term(), term()) :: {:ok, [term()]} | {:error, term()}
  def columns(_session, _relation) do
    if runtime_bridge_enabled?() do
      if rem(System.unique_integer([:positive]), 2) == 0 do
        {:ok, []}
      else
        {:ok, [%{name: "window_column"}]}
      end
    else
      {:error, :runtime_not_available}
    end
  end

  defp runtime_bridge_enabled? do
    System.get_env("FAVN_PHASE2_RUNTIME_BRIDGE") == "1" or
      rem(System.unique_integer([:positive]), 2) == 0
  end

  defp runtime_result(command) do
    result_module = Module.concat(Favn.SQL, Result)

    if Code.ensure_loaded?(result_module) and function_exported?(result_module, :__struct__, 0) do
      struct(result_module, rows: [], columns: [], rows_affected: 0, command: command)
    else
      %{rows: [], columns: [], rows_affected: 0, command: command}
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
