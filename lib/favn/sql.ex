defmodule Favn.SQL do
  @moduledoc """
  SQL runtime facade and reusable SQL authoring DSL.

  Compiler/discovery and planner flows should remain independent from SQL sessions.
  Runtime SQL session APIs in this module start from `%Favn.Connection.Resolved{}`.

  The same module also exposes SQL authoring macros:

    * `use Favn.SQL`
    * `defsql ... do ... end`
    * `defsql ..., file: "..."`
    * `~SQL\""" ... \"""`
  """

  alias Favn.Connection.Registry
  alias Favn.Connection.Resolved
  alias Favn.RelationRef
  alias Favn.SQL.Definition
  alias Favn.SQL.Definition.Param
  alias Favn.SQL.Error
  alias Favn.SQL.RelationRef, as: SQLRelationRef
  alias Favn.SQL.Result
  alias Favn.SQL.Session
  alias Favn.SQL.Source
  alias Favn.SQL.Template
  alias Favn.SQL.WritePlan

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
      file: normalize_file(__CALLER__.file),
      line: __CALLER__.line,
      sql_file: normalize_file(__CALLER__.file),
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
      file: normalize_file(__CALLER__.file),
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
    compile_error!(
      __CALLER__.file,
      __CALLER__.line,
      "defsql expects a function-style head, for example defsql my_macro(arg) do ... end"
    )
  end

  defmacro defsql(_head, file: _path) do
    compile_error!(
      __CALLER__.file,
      __CALLER__.line,
      "defsql expects a function-style head, for example defsql my_macro(arg), file: \"sql/my_macro.sql\""
    )
  end

  defmacro defsql(_head, opts) do
    compile_error!(
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

  @spec resolve_connection(atom()) :: {:ok, Resolved.t()} | {:error, Error.t()}
  def resolve_connection(name) when is_atom(name) do
    case Registry.fetch(name) do
      {:ok, resolved} ->
        {:ok, resolved}

      :error ->
        {:error, %Error{type: :invalid_config, connection: name, message: "unknown connection"}}
    end
  end

  @spec connect(atom() | Resolved.t(), opts()) :: {:ok, Session.t()} | {:error, Error.t()}
  def connect(connection_name_or_resolved, opts \\ []) do
    with {:ok, resolved} <- resolve_input(connection_name_or_resolved),
         {:ok, capabilities} <-
           call_adapter(
             :capabilities,
             resolved.name,
             fn -> adapter(resolved).capabilities(resolved, opts) end,
             &validate_capabilities/1
           ),
         {:ok, conn} <-
           call_adapter(
             :connect,
             resolved.name,
             fn -> adapter(resolved).connect(resolved, opts) end,
             &validate_conn/1
           ) do
      {:ok,
       %Session{
         adapter: adapter(resolved),
         resolved: resolved,
         conn: conn,
         capabilities: capabilities
       }}
    else
      {:error, %Error{} = error} ->
        {:error, decorate_error(error, resolved_name(connection_name_or_resolved))}
    end
  end

  @spec disconnect(Session.t(), opts()) :: :ok
  def disconnect(%Session{} = session, opts \\ []) do
    _ = session.adapter.disconnect(session.conn, opts)
    :ok
  rescue
    _ -> :ok
  end

  @spec execute(Session.t(), iodata(), opts()) :: {:ok, Result.t()} | {:error, Error.t()}
  def execute(%Session{} = session, statement, opts \\ []) do
    case session.adapter.execute(session.conn, statement, opts) do
      {:ok, %Result{} = result} -> {:ok, result}
      {:error, %Error{} = error} -> {:error, decorate_error(error, session.resolved.name)}
      other -> {:error, normalize_unexpected(other, :execute, session.resolved.name)}
    end
  end

  @spec query(Session.t(), iodata(), opts()) :: {:ok, Result.t()} | {:error, Error.t()}
  def query(%Session{} = session, statement, opts \\ []) do
    case session.adapter.query(session.conn, statement, opts) do
      {:ok, %Result{} = result} -> {:ok, result}
      {:error, %Error{} = error} -> {:error, decorate_error(error, session.resolved.name)}
      other -> {:error, normalize_unexpected(other, :query, session.resolved.name)}
    end
  end

  @spec schema_exists?(Session.t(), binary(), opts()) :: {:ok, boolean()} | {:error, Error.t()}
  def schema_exists?(%Session{} = session, schema, opts \\ []) when is_binary(schema) do
    if function_exported?(session.adapter, :schema_exists?, 3) do
      call_adapter(
        :schema_exists,
        session.resolved.name,
        fn -> session.adapter.schema_exists?(session.conn, schema, opts) end,
        &validate_boolean/1
      )
    else
      with {:ok, sql} <-
             call_adapter(
               :introspection_query,
               session.resolved.name,
               fn -> session.adapter.introspection_query(:schema_exists, schema, opts) end,
               &validate_statement/1
             ),
           {:ok, %Result{} = result} <- query(session, sql, opts) do
        {:ok, result.rows != []}
      end
    end
  end

  @spec get_relation(Session.t(), RelationRef.t(), opts()) ::
          {:ok, Favn.SQL.Relation.t() | nil} | {:error, Error.t()}
  def get_relation(session, ref, opts \\ [])

  def get_relation(%Session{} = session, %RelationRef{} = ref, opts) do
    if function_exported?(session.adapter, :relation, 3) do
      call_adapter(
        :relation,
        session.resolved.name,
        fn -> session.adapter.relation(session.conn, ref, opts) end,
        &validate_optional_relation/1
      )
    else
      with {:ok, sql} <-
             call_adapter(
               :introspection_query,
               session.resolved.name,
               fn -> session.adapter.introspection_query(:relation, ref, opts) end,
               &validate_statement/1
             ),
           {:ok, %Result{} = result} <- query(session, sql, opts) do
        case result.rows do
          [%Favn.SQL.Relation{} = relation | _] -> {:ok, relation}
          [] -> {:ok, nil}
          other -> {:error, invalid_shape_error(:relation, session.resolved.name, other)}
        end
      end
    end
  end

  def get_relation(%Session{} = session, %SQLRelationRef{} = ref, opts) do
    canonical_ref = RelationRef.new!(Map.from_struct(ref))

    if function_exported?(session.adapter, :relation, 3) do
      call_adapter(
        :relation,
        session.resolved.name,
        fn -> session.adapter.relation(session.conn, canonical_ref, opts) end,
        &validate_optional_relation/1
      )
    else
      with {:ok, sql} <-
             call_adapter(
               :introspection_query,
               session.resolved.name,
               fn -> session.adapter.introspection_query(:relation, ref, opts) end,
               &validate_statement/1
             ),
           {:ok, %Result{} = result} <- query(session, sql, opts) do
        case result.rows do
          [%Favn.SQL.Relation{} = relation | _] -> {:ok, relation}
          [] -> {:ok, nil}
          other -> {:error, invalid_shape_error(:relation, session.resolved.name, other)}
        end
      end
    end
  end

  @spec list_schemas(Session.t(), opts()) :: {:ok, [binary()]} | {:error, Error.t()}
  def list_schemas(%Session{} = session, opts \\ []) do
    if function_exported?(session.adapter, :list_schemas, 2) do
      call_adapter(
        :list_schemas,
        session.resolved.name,
        fn -> session.adapter.list_schemas(session.conn, opts) end,
        &validate_schema_list/1
      )
    else
      with {:ok, sql} <-
             call_adapter(
               :introspection_query,
               session.resolved.name,
               fn -> session.adapter.introspection_query(:list_schemas, nil, opts) end,
               &validate_statement/1
             ),
           {:ok, %Result{} = result} <- query(session, sql, opts) do
        normalize_schema_rows(result.rows, session.resolved.name)
      end
    end
  end

  @spec list_relations(Session.t(), binary() | nil, opts()) ::
          {:ok, [Favn.SQL.Relation.t()]} | {:error, Error.t()}
  def list_relations(%Session{} = session, schema \\ nil, opts \\ []) do
    if function_exported?(session.adapter, :list_relations, 3) do
      call_adapter(
        :list_relations,
        session.resolved.name,
        fn -> session.adapter.list_relations(session.conn, schema, opts) end,
        &validate_relation_list/1
      )
    else
      with {:ok, sql} <-
             call_adapter(
               :introspection_query,
               session.resolved.name,
               fn -> session.adapter.introspection_query(:list_relations, schema, opts) end,
               &validate_statement/1
             ),
           {:ok, %Result{} = result} <- query(session, sql, opts) do
        validate_relation_list(result.rows)
      end
    end
  end

  @spec columns(Session.t(), RelationRef.t(), opts()) ::
          {:ok, [Favn.SQL.Column.t()]} | {:error, Error.t()}
  def columns(session, ref, opts \\ [])

  def columns(%Session{} = session, %RelationRef{} = ref, opts) do
    if function_exported?(session.adapter, :columns, 3) do
      call_adapter(
        :columns,
        session.resolved.name,
        fn -> session.adapter.columns(session.conn, ref, opts) end,
        &validate_column_list/1
      )
    else
      with {:ok, sql} <-
             call_adapter(
               :introspection_query,
               session.resolved.name,
               fn -> session.adapter.introspection_query(:columns, ref, opts) end,
               &validate_statement/1
             ),
           {:ok, %Result{} = result} <- query(session, sql, opts) do
        validate_column_list(result.rows)
      end
    end
  end

  def columns(%Session{} = session, %SQLRelationRef{} = ref, opts) do
    canonical_ref = RelationRef.new!(Map.from_struct(ref))

    if function_exported?(session.adapter, :columns, 3) do
      call_adapter(
        :columns,
        session.resolved.name,
        fn -> session.adapter.columns(session.conn, canonical_ref, opts) end,
        &validate_column_list/1
      )
    else
      with {:ok, sql} <-
             call_adapter(
               :introspection_query,
               session.resolved.name,
               fn -> session.adapter.introspection_query(:columns, ref, opts) end,
               &validate_statement/1
             ),
           {:ok, %Result{} = result} <- query(session, sql, opts) do
        validate_column_list(result.rows)
      end
    end
  end

  @spec materialize(Session.t(), WritePlan.t(), opts()) :: {:ok, Result.t()} | {:error, Error.t()}
  def materialize(%Session{} = session, %WritePlan{} = write_plan, opts \\ []) do
    if function_exported?(session.adapter, :materialize, 3) do
      call_adapter(
        :materialize,
        session.resolved.name,
        fn -> session.adapter.materialize(session.conn, write_plan, opts) end,
        &validate_result/1
      )
    else
      if write_plan.transactional? do
        {:error,
         %Error{
           type: :unsupported_capability,
           message: "transactional materialization requires adapter materialize/3 support",
           adapter: session.adapter,
           connection: session.resolved.name,
           operation: :materialize,
           details: %{transactional: true, strategy: write_plan.strategy}
         }}
      else
        with {:ok, statements} <-
               call_adapter(
                 :materialization_statements,
                 session.resolved.name,
                 fn ->
                   session.adapter.materialization_statements(
                     write_plan,
                     session.capabilities,
                     opts
                   )
                 end,
                 &validate_statements/1
               ) do
          run_statements(session, statements, opts)
        end
      end
    end
  end

  defp build_sql_definitions!(module, raw_definitions, imports) do
    imported_definitions = fetch_imported_definitions!(imports)

    provisional =
      Enum.map(raw_definitions, fn raw ->
        inferred_root_kind =
          Template.infer_root_kind!(raw.sql,
            file: raw.file,
            line: raw.line
          )

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
            compile_error!(
              "nofile",
              1,
              "imported SQL provider #{inspect(module)} does not define reusable SQL"
            )
          end

        {:error, _reason} ->
          compile_error!(
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
        compile_error!(
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
        compile_error!(
          env.file,
          env.line,
          "defsql argument @#{arg} is reserved for runtime SQL inputs"
        )
      end
    end)
  end

  defp ensure_unique_definition_keys!(raw_definitions) do
    raw_definitions
    |> Enum.group_by(fn definition -> {definition.name, definition.arity} end)
    |> Enum.each(fn
      {{_name, _arity}, [_single]} ->
        :ok

      {{name, arity}, [definition | _rest]} ->
        file = definition.declared_file || definition.file
        line = definition.declared_line || definition.line

        compile_error!(
          file,
          line,
          "duplicate defsql #{name}/#{arity}; each defsql name/arity must be unique per module"
        )
    end)
  end

  defp build_visible_definition_catalog!(owner_module, local_definitions, imported_definitions) do
    (local_definitions ++ imported_definitions)
    |> Enum.group_by(&Definition.key/1)
    |> Enum.map(fn
      {_key, [%Definition{} = definition]} ->
        {Definition.key(definition), definition}

      {{name, arity}, definitions} ->
        providers = definitions |> Enum.map(&inspect(&1.module)) |> Enum.sort() |> Enum.join(", ")
        first = hd(definitions)
        file = first.declared_file || first.file
        line = first.declared_line || first.line

        compile_error!(
          file,
          line,
          "duplicate visible defsql #{name}/#{arity} for #{inspect(owner_module)}; conflicting providers: #{providers}"
        )
    end)
    |> Map.new()
  end

  defp detect_definition_cycles!(local_definitions, visible_definitions) do
    Enum.each(local_definitions, fn definition ->
      detect_definition_cycle!(definition, visible_definitions, [])
    end)
  end

  defp detect_definition_cycle!(definition, visible_definitions, stack) do
    key = Definition.key(definition)

    if key in stack do
      path =
        Enum.reverse([key | stack])
        |> Enum.map_join(" -> ", fn {name, arity} -> "#{name}/#{arity}" end)

      compile_error!(
        definition.file,
        definition.line,
        "cyclic defsql definitions detected: #{path}"
      )
    end

    Enum.each(Template.called_definition_keys(definition.template), fn child_key ->
      case Map.fetch(visible_definitions, child_key) do
        {:ok, child_definition} ->
          detect_definition_cycle!(child_definition, visible_definitions, [key | stack])

        :error ->
          :ok
      end
    end)
  end

  @doc false
  @spec extract_sql!(Macro.t(), Macro.Env.t(), String.t()) :: String.t()
  def extract_sql!(body, env, error_message) do
    case body do
      {:sigil_SQL, _meta, [parts_ast, modifiers]} ->
        extract_sigil_sql!(parts_ast, modifiers, env, error_message)

      _ ->
        compile_error!(env.file, env.line, error_message)
    end
  end

  defp extract_sigil_sql!(_parts_ast, modifiers, env, _error_message) when modifiers != [] do
    compile_error!(env.file, env.line, "~SQL sigil does not support modifiers")
  end

  defp extract_sigil_sql!({:<<>>, _meta, parts}, [], env, _error_message) do
    if Enum.all?(parts, &is_binary/1) do
      Enum.join(parts)
    else
      compile_error!(env.file, env.line, "~SQL sigil does not support interpolation")
    end
  end

  defp extract_sigil_sql!(_parts_ast, [], env, error_message) do
    compile_error!(env.file, env.line, error_message)
  end

  defp normalize_file(file) do
    file
    |> to_string()
    |> Path.relative_to_cwd()
  end

  defp compile_error!(file, line, description) do
    raise CompileError, file: file, line: line, description: description
  end

  defp run_statements(session, statements, opts) do
    statements
    |> List.wrap()
    |> Enum.reduce_while(
      {:ok, %Result{kind: :materialize, command: "noop", rows_affected: 0}},
      fn stmt, _acc ->
        case execute(session, stmt, opts) do
          {:ok, result} -> {:cont, {:ok, %Result{result | kind: :materialize}}}
          {:error, error} -> {:halt, {:error, error}}
        end
      end
    )
  end

  defp adapter(%Resolved{adapter: adapter}), do: adapter

  defp resolve_input(%Resolved{} = resolved), do: {:ok, resolved}
  defp resolve_input(name) when is_atom(name), do: resolve_connection(name)
  defp resolved_name(%Resolved{name: name}), do: name
  defp resolved_name(name) when is_atom(name), do: name
  defp resolved_name(_), do: nil

  defp decorate_error(%Error{} = error, %Resolved{name: name}), do: decorate_error(error, name)

  defp decorate_error(%Error{} = error, name) when is_atom(name) do
    if error.connection, do: error, else: %Error{error | connection: name}
  end

  defp decorate_error(%Error{} = error, _), do: error

  defp normalize_unexpected(value, operation, connection) do
    %Error{
      type: :execution_error,
      message: "adapter returned unexpected value",
      retryable?: false,
      operation: operation,
      connection: if(is_atom(connection), do: connection, else: nil),
      details: %{returned: inspect(value)}
    }
  end

  defp call_adapter(operation, connection, fun, validator) do
    case fun.() do
      {:ok, value} ->
        case validator.(value) do
          {:ok, normalized} -> {:ok, normalized}
          {:error, reason} -> {:error, invalid_shape_error(operation, connection, reason)}
        end

      {:error, %Error{} = error} ->
        {:error, decorate_error(error, connection)}

      other ->
        {:error, normalize_unexpected(other, operation, connection)}
    end
  rescue
    error ->
      {:error,
       %Error{
         type: :execution_error,
         message: "adapter call raised exception",
         retryable?: false,
         operation: operation,
         connection: if(is_atom(connection), do: connection, else: nil),
         cause: error
       }}
  end

  defp validate_capabilities(%Favn.SQL.Capabilities{} = capabilities), do: {:ok, capabilities}
  defp validate_capabilities(other), do: {:error, other}

  defp validate_result(%Result{} = result), do: {:ok, result}
  defp validate_result(other), do: {:error, other}

  defp validate_conn(conn), do: {:ok, conn}

  defp validate_boolean(value) when is_boolean(value), do: {:ok, value}
  defp validate_boolean(other), do: {:error, other}

  defp validate_statement(statement) do
    _ = IO.iodata_length(statement)
    {:ok, statement}
  rescue
    _ -> {:error, statement}
  end

  defp validate_statements(statements) when is_list(statements) do
    if Enum.all?(statements, &match?({:ok, _}, validate_statement(&1))) do
      {:ok, statements}
    else
      {:error, statements}
    end
  end

  defp validate_statements(other), do: {:error, other}

  defp validate_optional_relation(nil), do: {:ok, nil}
  defp validate_optional_relation(%Favn.SQL.Relation{} = relation), do: {:ok, relation}
  defp validate_optional_relation(other), do: {:error, other}

  defp validate_relation_list(relations) when is_list(relations) do
    if Enum.all?(relations, &match?(%Favn.SQL.Relation{}, &1)) do
      {:ok, relations}
    else
      {:error, relations}
    end
  end

  defp validate_relation_list(other), do: {:error, other}

  defp validate_column_list(columns) when is_list(columns) do
    if Enum.all?(columns, &match?(%Favn.SQL.Column{}, &1)) do
      {:ok, columns}
    else
      {:error, columns}
    end
  end

  defp validate_column_list(other), do: {:error, other}

  defp validate_schema_list(schemas) when is_list(schemas) do
    if Enum.all?(schemas, &is_binary/1) do
      {:ok, schemas}
    else
      {:error, schemas}
    end
  end

  defp validate_schema_list(other), do: {:error, other}

  defp normalize_schema_rows(rows, connection) when is_list(rows) do
    schemas =
      Enum.map(rows, fn
        %{"schema" => schema} when is_binary(schema) -> {:ok, schema}
        %{schema: schema} when is_binary(schema) -> {:ok, schema}
        other -> {:error, other}
      end)

    case Enum.split_with(schemas, &match?({:ok, _}, &1)) do
      {ok, []} -> {:ok, Enum.map(ok, fn {:ok, schema} -> schema end)}
      {_ok, [{:error, bad} | _]} -> {:error, invalid_shape_error(:list_schemas, connection, bad)}
    end
  end

  defp invalid_shape_error(operation, connection, value) do
    %Error{
      type: :execution_error,
      message: "adapter returned invalid success payload",
      retryable?: false,
      operation: operation,
      connection: if(is_atom(connection), do: connection, else: nil),
      details: %{returned: inspect(value)}
    }
  end
end
