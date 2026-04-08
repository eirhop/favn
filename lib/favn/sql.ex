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
         {:ok, capabilities} <- adapter(resolved).capabilities(resolved, opts),
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
      session.adapter.schema_exists?(session.conn, schema, opts)
    else
      with {:ok, sql} <- session.adapter.introspection_query(:schema_exists, schema, opts),
           {:ok, %Result{} = result} <- query(session, sql, opts) do
        {:ok, result.rows != []}
      end
    end
  end

  @spec get_relation(Session.t(), RelationRef.t(), opts()) ::
          {:ok, Favn.SQL.Relation.t() | nil} | {:error, Error.t()}
  def get_relation(%Session{} = session, %RelationRef{} = ref, opts \\ []) do
    if function_exported?(session.adapter, :relation, 3) do
      session.adapter.relation(session.conn, ref, opts)
    else
      with {:ok, sql} <- session.adapter.introspection_query(:relation, ref, opts),
           {:ok, %Result{} = result} <- query(session, sql, opts) do
        case result.rows do
          [relation | _] -> {:ok, relation}
          [] -> {:ok, nil}
        end
      end
    end
  end

  @spec list_schemas(Session.t(), opts()) :: {:ok, [binary()]} | {:error, Error.t()}
  def list_schemas(%Session{} = session, opts \\ []) do
    if function_exported?(session.adapter, :list_schemas, 2) do
      session.adapter.list_schemas(session.conn, opts)
    else
      with {:ok, sql} <- session.adapter.introspection_query(:list_schemas, nil, opts),
           {:ok, %Result{} = result} <- query(session, sql, opts) do
        {:ok, Enum.map(result.rows, &Map.get(&1, "schema"))}
      end
    end
  end

  @spec list_relations(Session.t(), binary() | nil, opts()) ::
          {:ok, [Favn.SQL.Relation.t()]} | {:error, Error.t()}
  def list_relations(%Session{} = session, schema \\ nil, opts \\ []) do
    if function_exported?(session.adapter, :list_relations, 3) do
      session.adapter.list_relations(session.conn, schema, opts)
    else
      with {:ok, sql} <- session.adapter.introspection_query(:list_relations, schema, opts),
           {:ok, %Result{} = result} <- query(session, sql, opts) do
        {:ok, result.rows}
      end
    end
  end

  @spec columns(Session.t(), RelationRef.t(), opts()) ::
          {:ok, [Favn.SQL.Column.t()]} | {:error, Error.t()}
  def columns(%Session{} = session, %RelationRef{} = ref, opts \\ []) do
    if function_exported?(session.adapter, :columns, 3) do
      session.adapter.columns(session.conn, ref, opts)
    else
      with {:ok, sql} <- session.adapter.introspection_query(:columns, ref, opts),
           {:ok, %Result{} = result} <- query(session, sql, opts) do
        {:ok, result.rows}
      end
    end
  end

  @spec materialize(Session.t(), WritePlan.t(), opts()) :: {:ok, Result.t()} | {:error, Error.t()}
  def materialize(%Session{} = session, %WritePlan{} = write_plan, opts \\ []) do
    if function_exported?(session.adapter, :materialize, 3) do
      session.adapter.materialize(session.conn, write_plan, opts)
    else
      with {:ok, statements} <-
             session.adapter.materialization_statements(write_plan, session.capabilities, opts) do
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
end
