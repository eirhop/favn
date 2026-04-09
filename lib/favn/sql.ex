defmodule Favn.SQL do
  @moduledoc """
  Internal SQL facade used at runtime for adapter orchestration.

  Compiler/discovery and planner flows should remain independent from SQL sessions.
  This facade starts from `%Favn.Connection.Resolved{}` and is runtime-only.
  """

  alias Favn.Connection.Registry
  alias Favn.Connection.Resolved
  alias Favn.SQL.{Error, RelationRef, Result, Session, WritePlan}

  @type opts :: keyword()

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
         {:ok, conn} <- adapter(resolved).connect(resolved, opts) do
      {:ok,
       %Session{
         adapter: adapter(resolved),
         resolved: resolved,
         conn: conn,
         capabilities: capabilities
       }}
    else
      {:error, %Error{} = error} -> {:error, decorate_error(error, connection_name_or_resolved)}
      other -> {:error, normalize_unexpected(other, :connect, connection_name_or_resolved)}
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
  def get_relation(%Session{} = session, %RelationRef{} = ref, opts \\ []) do
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
  def columns(%Session{} = session, %RelationRef{} = ref, opts \\ []) do
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

  defp validate_boolean(value) when is_boolean(value), do: {:ok, value}
  defp validate_boolean(other), do: {:error, other}

  defp validate_statement(statement) do
    if IO.iodata_length(statement) >= 0 do
      {:ok, statement}
    else
      {:error, statement}
    end
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

  defp normalize_schema_rows(other, connection),
    do: {:error, invalid_shape_error(:list_schemas, connection, other)}

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
