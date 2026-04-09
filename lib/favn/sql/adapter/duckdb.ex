defmodule Favn.SQL.Adapter.DuckDB do
  @moduledoc """
  DuckDB implementation of `Favn.SQL.Adapter` backed by `duckdbex`.

  This module is internal to Favn runtime SQL execution and must not be exposed
  as an external API contract.
  """

  @behaviour Favn.SQL.Adapter

  alias Favn.Connection.Resolved
  alias Favn.SQL.{Capabilities, Column, Error, Relation, RelationRef, Result, WritePlan}

  defmodule Conn do
    @moduledoc false

    @enforce_keys [:db_ref, :conn_ref]
    defstruct [:db_ref, :conn_ref]

    @type t :: %__MODULE__{db_ref: reference(), conn_ref: reference()}
  end

  @type opts :: keyword()

  @impl true
  @spec connect(Resolved.t(), opts()) :: {:ok, Conn.t()} | {:error, Error.t()}
  def connect(%Resolved{config: config} = resolved, _opts) do
    database = Map.get(config, :database)

    with {:ok, db_ref} <- open_database(database),
         {:ok, conn_ref} <- Duckdbex.connection(db_ref) do
      {:ok, %Conn{db_ref: db_ref, conn_ref: conn_ref}}
    else
      {:error, reason} -> {:error, normalize_error(:connect, resolved.name, reason)}
    end
  end

  @impl true
  @spec disconnect(Conn.t(), opts()) :: :ok
  def disconnect(%Conn{} = conn, _opts) do
    _ = safe_release(conn.conn_ref)
    _ = safe_release(conn.db_ref)
    :ok
  end

  @impl true
  @spec capabilities(Resolved.t(), opts()) :: {:ok, Capabilities.t()}
  def capabilities(%Resolved{}, _opts) do
    {:ok,
     %Capabilities{
       relation_types: [:table, :view],
       replace_view: :supported,
       replace_table: :supported,
       transactions: :supported,
       merge: :unsupported,
       materialized_views: :unsupported,
       relation_comments: :unsupported,
       column_comments: :unsupported,
       metadata_timestamps: :unsupported,
       query_tracking: :unsupported,
       extensions: %{
         bundled_in_amalgamation: [:csv, :parquet],
         not_bundled_in_amalgamation: [
           :json,
           :httpfs,
           :sqlite_scanner,
           :postgres_scanner,
           :substrait
         ]
       }
     }}
  end

  @impl true
  @spec execute(Conn.t(), iodata(), opts()) :: {:ok, Result.t()} | {:error, Error.t()}
  def execute(%Conn{} = conn, statement, opts) do
    params = Keyword.get(opts, :params, [])

    with {:ok, result_ref} <- run_query(conn, statement, params),
         {:ok, rows, columns} <- fetch_rows(result_ref) do
      {:ok,
       %Result{
         kind: :execute,
         command: IO.iodata_to_binary(statement),
         rows_affected: nil,
         rows: rows,
         columns: columns,
         metadata: %{}
       }}
    else
      {:error, reason} -> {:error, normalize_error(:execute, nil, reason)}
    end
  end

  @impl true
  @spec query(Conn.t(), iodata(), opts()) :: {:ok, Result.t()} | {:error, Error.t()}
  def query(%Conn{} = conn, statement, opts) do
    params = Keyword.get(opts, :params, [])

    with {:ok, result_ref} <- run_query(conn, statement, params),
         {:ok, rows, columns} <- fetch_rows(result_ref) do
      {:ok,
       %Result{
         kind: :query,
         command: IO.iodata_to_binary(statement),
         rows_affected: nil,
         rows: rows,
         columns: columns,
         metadata: %{}
       }}
    else
      {:error, reason} -> {:error, normalize_error(:query, nil, reason)}
    end
  end

  @impl true
  @spec introspection_query(Favn.SQL.Adapter.introspection_kind(), term(), opts()) ::
          {:ok, iodata()} | {:error, Error.t()}
  def introspection_query(:schema_exists, schema, _opts) when is_binary(schema) do
    {:ok,
     [
       "SELECT schema_name AS schema FROM information_schema.schemata WHERE schema_name = ",
       quote_literal(schema),
       " LIMIT 1"
     ]}
  end

  def introspection_query(:relation, %RelationRef{} = ref, _opts) do
    {:ok,
     [
       relation_introspection_base(ref),
       " AND table_name = ",
       quote_literal(ref.name),
       " LIMIT 1"
     ]}
  end

  def introspection_query(:list_schemas, _payload, _opts) do
    {:ok, "SELECT schema_name AS schema FROM information_schema.schemata ORDER BY schema_name"}
  end

  def introspection_query(:list_relations, schema, _opts) do
    base =
      if is_binary(schema) do
        [
          "SELECT table_catalog, table_schema, table_name, table_type FROM information_schema.tables",
          " WHERE table_schema = ",
          quote_literal(schema),
          " ORDER BY table_catalog, table_schema, table_name"
        ]
      else
        "SELECT table_catalog, table_schema, table_name, table_type FROM information_schema.tables ORDER BY table_catalog, table_schema, table_name"
      end

    {:ok, base}
  end

  def introspection_query(:columns, %RelationRef{} = ref, _opts) do
    {:ok,
     [
       "SELECT column_name, ordinal_position, data_type, is_nullable, column_default ",
       "FROM information_schema.columns WHERE table_name = ",
       quote_literal(ref.name),
       " AND table_schema = ",
       quote_literal(ref.schema || "main"),
       " ORDER BY ordinal_position"
     ]}
  end

  def introspection_query(_kind, _payload, _opts) do
    {:error,
     %Error{
       type: :unsupported_capability,
       message: "unsupported introspection query",
       retryable?: false
     }}
  end

  @impl true
  @spec materialization_statements(WritePlan.t(), Capabilities.t(), opts()) ::
          {:ok, [iodata()]} | {:error, Error.t()}
  def materialization_statements(%WritePlan{} = plan, _caps, _opts) do
    target = qualified_relation(plan.target)

    statements =
      case plan.materialization do
        :view ->
          [create_view_statement(target, plan)]

        :table ->
          [create_table_statement(target, plan)]

        :incremental ->
          incremental_statements(target, plan)
      end

    {:ok, plan.pre_statements ++ statements ++ plan.post_statements}
  rescue
    _ ->
      {:error,
       %Error{
         type: :execution_error,
         message: "failed to build materialization statements",
         retryable?: false,
         operation: :materialization_statements
       }}
  end

  @impl true
  @spec ping(Conn.t(), opts()) :: :ok | {:error, Error.t()}
  def ping(%Conn{} = conn, _opts) do
    case run_query(conn, "SELECT 1", []) do
      {:ok, result_ref} ->
        _ = safe_release(result_ref)
        :ok

      {:error, reason} ->
        {:error, normalize_error(:ping, nil, reason)}
    end
  end

  @impl true
  @spec schema_exists?(Conn.t(), binary(), opts()) :: {:ok, boolean()} | {:error, Error.t()}
  def schema_exists?(%Conn{} = conn, schema, _opts) when is_binary(schema) do
    sql =
      "SELECT 1 FROM information_schema.schemata WHERE schema_name = #{quote_literal(schema)} LIMIT 1"

    with {:ok, result} <- query(conn, sql, []),
         do: {:ok, result.rows != []}
  end

  @impl true
  @spec relation(Conn.t(), RelationRef.t(), opts()) ::
          {:ok, Relation.t() | nil} | {:error, Error.t()}
  def relation(%Conn{} = conn, %RelationRef{} = ref, _opts) do
    sql = [
      relation_introspection_base(ref),
      " AND table_name = ",
      quote_literal(ref.name),
      " LIMIT 1"
    ]

    with {:ok, result} <- query(conn, sql, []),
         do: {:ok, List.first(Enum.map(result.rows, &row_to_relation/1))}
  end

  @impl true
  @spec list_schemas(Conn.t(), opts()) :: {:ok, [binary()]} | {:error, Error.t()}
  def list_schemas(%Conn{} = conn, _opts) do
    with {:ok, result} <-
           query(conn, "SELECT schema_name AS schema FROM information_schema.schemata", []),
         do: {:ok, Enum.map(result.rows, &Map.get(&1, "schema"))}
  end

  @impl true
  @spec list_relations(Conn.t(), binary() | nil, opts()) ::
          {:ok, [Relation.t()]} | {:error, Error.t()}
  def list_relations(%Conn{} = conn, schema, _opts) do
    {:ok, sql} = introspection_query(:list_relations, schema, [])

    with {:ok, result} <- query(conn, sql, []),
         do: {:ok, Enum.map(result.rows, &row_to_relation/1)}
  end

  @impl true
  @spec columns(Conn.t(), RelationRef.t(), opts()) :: {:ok, [Column.t()]} | {:error, Error.t()}
  def columns(%Conn{} = conn, %RelationRef{} = ref, _opts) do
    {:ok, sql} = introspection_query(:columns, ref, [])

    with {:ok, result} <- query(conn, sql, []),
         do: {:ok, Enum.map(result.rows, &row_to_column/1)}
  end

  @impl true
  @spec transaction(Conn.t(), (Conn.t() -> {:ok, term()} | {:error, Error.t()}), opts()) ::
          {:ok, term()} | {:error, Error.t()}
  def transaction(%Conn{} = conn, fun, _opts) when is_function(fun, 1) do
    with :ok <- tx_begin(conn),
         {:ok, value} <- fun.(conn),
         :ok <- tx_commit(conn) do
      {:ok, value}
    else
      {:error, %Error{} = error} ->
        _ = tx_rollback(conn)
        {:error, error}

      {:error, reason} ->
        _ = tx_rollback(conn)
        {:error, normalize_error(:transaction, nil, reason)}
    end
  end

  @impl true
  @spec materialize(Conn.t(), WritePlan.t(), opts()) :: {:ok, Result.t()} | {:error, Error.t()}
  def materialize(%Conn{} = conn, %WritePlan{} = plan, opts) do
    rows =
      plan.options
      |> Map.get(:appender_rows, Keyword.get(opts, :appender_rows, []))
      |> List.wrap()

    if plan.materialization == :table and rows != [] do
      appender_materialize(conn, plan, rows)
    else
      with {:ok, statements} <- materialization_statements(plan, %Capabilities{}, opts) do
        Enum.reduce_while(statements, {:ok, 0}, fn statement, {:ok, count} ->
          case execute(conn, statement, opts) do
            {:ok, _} -> {:cont, {:ok, count + 1}}
            {:error, error} -> {:halt, {:error, error}}
          end
        end)
        |> case do
          {:ok, _} -> {:ok, %Result{kind: :materialize, command: "sql", rows_affected: nil}}
          {:error, error} -> {:error, error}
        end
      end
    end
  end

  defp appender_materialize(%Conn{} = conn, %WritePlan{} = plan, rows) do
    target = qualified_relation(plan.target)

    with {:ok, _} <- execute(conn, create_table_for_appender(target, plan.select_sql), []),
         {:ok, appender} <- open_appender(conn, plan),
         :ok <- Duckdbex.appender_add_rows(appender, rows),
         :ok <- Duckdbex.appender_flush(appender),
         :ok <- Duckdbex.appender_close(appender) do
      {:ok,
       %Result{
         kind: :materialize,
         command: "appender",
         rows_affected: length(rows),
         metadata: %{strategy: :appender}
       }}
    else
      {:error, reason} -> {:error, normalize_error(:materialize, nil, reason)}
    end
  end

  defp open_database(nil), do: Duckdbex.open()
  defp open_database(":memory:"), do: Duckdbex.open()
  defp open_database(path) when is_binary(path), do: Duckdbex.open(path)
  defp open_database(_), do: {:error, :invalid_database}

  defp run_query(%Conn{conn_ref: conn_ref}, statement, []),
    do: Duckdbex.query(conn_ref, to_string(statement))

  defp run_query(%Conn{conn_ref: conn_ref}, statement, params),
    do: Duckdbex.query(conn_ref, to_string(statement), params)

  defp fetch_rows(result_ref) do
    columns =
      case Duckdbex.columns(result_ref) do
        cols when is_list(cols) -> Enum.map(cols, &to_string/1)
        _ -> []
      end

    rows =
      case Duckdbex.fetch_all(result_ref) do
        rows when is_list(rows) -> Enum.map(rows, &normalize_row(&1, columns))
        other -> other
      end

    _ = safe_release(result_ref)

    if is_list(rows) do
      {:ok, rows, columns}
    else
      {:error, rows}
    end
  end

  defp normalize_row(row, _columns) when is_map(row), do: row
  defp normalize_row(row, _columns) when is_list(row) and Keyword.keyword?(row), do: Map.new(row)

  defp normalize_row(row, columns) when is_list(row) do
    columns
    |> Enum.zip(row)
    |> Map.new()
  end

  defp normalize_row(other, _columns), do: %{"value" => other}

  defp tx_begin(%Conn{conn_ref: conn_ref}) do
    case Duckdbex.begin_transaction(conn_ref) do
      :ok -> :ok
      {:error, reason} -> {:error, normalize_error(:transaction, nil, reason)}
    end
  end

  defp tx_commit(%Conn{conn_ref: conn_ref}) do
    case Duckdbex.commit(conn_ref) do
      :ok -> :ok
      {:error, reason} -> {:error, normalize_error(:transaction, nil, reason)}
    end
  end

  defp tx_rollback(%Conn{conn_ref: conn_ref}) do
    case Duckdbex.rollback(conn_ref) do
      :ok -> :ok
      {:error, _reason} -> :ok
    end
  end

  defp create_view_statement(target, %WritePlan{replace?: true, select_sql: sql}),
    do: ["CREATE OR REPLACE VIEW ", target, " AS ", sql]

  defp create_view_statement(target, %WritePlan{if_not_exists?: true, select_sql: sql}),
    do: ["CREATE VIEW IF NOT EXISTS ", target, " AS ", sql]

  defp create_view_statement(target, %WritePlan{select_sql: sql}),
    do: ["CREATE VIEW ", target, " AS ", sql]

  defp create_table_statement(target, %WritePlan{replace?: true, select_sql: sql}),
    do: ["CREATE OR REPLACE TABLE ", target, " AS ", sql]

  defp create_table_statement(target, %WritePlan{if_not_exists?: true, select_sql: sql}),
    do: ["CREATE TABLE IF NOT EXISTS ", target, " AS ", sql]

  defp create_table_statement(target, %WritePlan{select_sql: sql}),
    do: ["CREATE TABLE ", target, " AS ", sql]

  defp incremental_statements(target, %WritePlan{strategy: :append, select_sql: sql}),
    do: [["INSERT INTO ", target, " ", sql]]

  defp incremental_statements(_target, %WritePlan{strategy: strategy}) do
    raise ArgumentError, "unsupported incremental strategy for DuckDB: #{inspect(strategy)}"
  end

  defp create_table_for_appender(target, select_sql),
    do: ["CREATE OR REPLACE TABLE ", target, " AS SELECT * FROM (", select_sql, ") LIMIT 0"]

  defp open_appender(%Conn{conn_ref: conn_ref}, %WritePlan{
         target: %Relation{schema: nil, name: name}
       }),
       do: Duckdbex.appender(conn_ref, name)

  defp open_appender(%Conn{conn_ref: conn_ref}, %WritePlan{
         target: %Relation{schema: schema, name: name}
       }),
       do: Duckdbex.appender(conn_ref, schema, name)

  defp relation_introspection_base(%RelationRef{} = ref) do
    schema = ref.schema || "main"

    [
      "SELECT table_catalog, table_schema, table_name, table_type FROM information_schema.tables",
      " WHERE table_schema = ",
      quote_literal(schema)
    ]
  end

  defp row_to_relation(row) do
    %Relation{
      catalog: Map.get(row, "table_catalog"),
      schema: Map.get(row, "table_schema"),
      name: Map.get(row, "table_name"),
      type: relation_type(Map.get(row, "table_type")),
      metadata: %{}
    }
  end

  defp row_to_column(row) do
    nullable? =
      case Map.get(row, "is_nullable") do
        "YES" -> true
        "NO" -> false
        value when is_boolean(value) -> value
        _ -> nil
      end

    %Column{
      name: Map.get(row, "column_name"),
      position: normalize_integer(Map.get(row, "ordinal_position")),
      data_type: Map.get(row, "data_type"),
      nullable?: nullable?,
      default: Map.get(row, "column_default")
    }
  end

  defp relation_type("BASE TABLE"), do: :table
  defp relation_type("VIEW"), do: :view
  defp relation_type(_), do: :unknown

  defp normalize_integer(value) when is_integer(value), do: value

  defp normalize_integer(value) when is_binary(value) do
    case Integer.parse(value) do
      {int, ""} -> int
      _ -> nil
    end
  end

  defp normalize_integer(_), do: nil

  defp qualified_relation(%Relation{schema: nil, name: name}), do: quote_ident(name)

  defp qualified_relation(%Relation{schema: schema, name: name}),
    do: [quote_ident(schema), ".", quote_ident(name)]

  defp quote_ident(identifier), do: ["\"", String.replace(identifier, "\"", "\"\""), "\""]
  defp quote_literal(value), do: ["'", String.replace(value, "'", "''"), "'"]

  defp safe_release(resource) do
    case Duckdbex.release(resource) do
      :ok -> :ok
      {:error, _} -> :ok
    end
  rescue
    _ -> :ok
  end

  defp normalize_error(operation, connection, reason) do
    message = if is_binary(reason), do: reason, else: "duckdb operation failed"

    %Error{
      type: error_type(reason),
      message: message,
      retryable?: retryable_reason?(reason),
      adapter: __MODULE__,
      operation: operation,
      connection: connection,
      details: %{reason: reason}
    }
  end

  defp error_type(:invalid_database), do: :invalid_config
  defp error_type(reason) when reason in [:invalid_argument, :syntax_error], do: :execution_error
  defp error_type(_), do: :connection_error

  defp retryable_reason?(reason) when reason in [:busy, :locked], do: true
  defp retryable_reason?(_), do: false
end
