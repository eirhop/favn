defmodule Favn.SQL.Adapter.DuckDB do
  @moduledoc """
  DuckDB implementation of `Favn.SQL.Adapter` backed by `duckdbex`.

  This module is internal to Favn runtime SQL execution and must not be exposed
  as an external API contract.
  """

  @behaviour Favn.SQL.Adapter

  alias Favn.Connection.Resolved
  alias Favn.RelationRef
  alias Favn.SQL.Adapter.DuckDB.Client
  alias Favn.SQL.{Capabilities, Column, Error, Relation, Result, WritePlan}

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
    with {:ok, db_ref} <- Client.open(Map.get(config, :database)),
         {:ok, conn_ref} <- create_connection(db_ref) do
      {:ok, %Conn{db_ref: db_ref, conn_ref: conn_ref}}
    else
      {:error, reason} -> {:error, normalize_error(:connect, resolved.name, reason)}
    end
  end

  defp create_connection(db_ref) do
    case Client.connection(db_ref) do
      {:ok, conn_ref} ->
        {:ok, conn_ref}

      {:error, reason} ->
        _ = safe_release(db_ref)
        {:error, reason}
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
    run_and_fetch(conn, statement, opts, :execute)
  end

  @impl true
  @spec query(Conn.t(), iodata(), opts()) :: {:ok, Result.t()} | {:error, Error.t()}
  def query(%Conn{} = conn, statement, opts) do
    run_and_fetch(conn, statement, opts, :query)
  end

  defp run_and_fetch(%Conn{} = conn, statement, opts, kind) do
    params = Keyword.get(opts, :params, [])

    with {:ok, result_ref} <- run_query(conn, statement, params),
         {:ok, rows, columns} <- fetch_rows(result_ref) do
      {:ok,
       %Result{
         kind: kind,
         command: IO.iodata_to_binary(statement),
         rows_affected: nil,
         rows: rows,
         columns: columns,
         metadata: %{}
       }}
    else
      {:error, reason} -> {:error, normalize_error(kind, nil, reason)}
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
        :view -> [create_view_statement(target, plan)]
        :table -> [create_table_statement(target, plan)]
        :incremental -> incremental_statements(target, plan)
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
    rows = appender_rows(plan, opts)

    if plan.materialization == :table and rows != [] do
      appender_materialize(conn, plan, rows)
    else
      run_plan_materialization(conn, plan, opts)
    end
  end

  defp run_plan_materialization(%Conn{} = conn, %WritePlan{transactional?: true} = plan, opts) do
    case transaction(
           conn,
           fn tx_conn -> run_materialization_statements(tx_conn, plan, opts) end,
           []
         ) do
      {:ok, result} ->
        {:ok, result}

      {:error, reason} ->
        {:error, normalize_error(:materialize, nil, reason)}
    end
  end

  defp run_plan_materialization(%Conn{} = conn, %WritePlan{} = plan, opts) do
    run_materialization_statements(conn, plan, opts)
  end

  defp run_materialization_statements(%Conn{} = conn, %WritePlan{} = plan, opts) do
    params = Keyword.get(opts, :params, [])

    with {:ok, statements} <- materialization_statements(plan, %Capabilities{}, opts) do
      Enum.reduce_while(statements, {:ok, 0}, fn statement, {:ok, count} ->
        statement_params = statement_params(plan, statement, params)

        case execute(conn, statement, params: statement_params) do
          {:ok, _} -> {:cont, {:ok, count + 1}}
          {:error, error} -> {:halt, {:error, error}}
        end
      end)
      |> case do
        {:ok, _} ->
          {:ok,
           %Result{
             kind: :materialize,
             command: "sql",
             rows_affected: nil,
             metadata: %{mode: plan.mode || :materialize, strategy: plan.strategy}
           }}

        {:error, error} ->
          {:error, error}
      end
    end
  end

  defp appender_materialize(%Conn{} = conn, %WritePlan{} = plan, rows) do
    with {:ok, statements} <- materialization_statements(plan, %Capabilities{}, []),
         {:ok, pre, post} <- split_materialization_statements(plan, statements),
         {:ok, _} <- execute_statements(conn, pre),
         {:ok, _} <- execute(conn, appender_table_statement(plan), []),
         :ok <- append_rows(conn, plan.target, rows),
         {:ok, _} <- execute_statements(conn, post) do
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

  defp split_materialization_statements(%WritePlan{} = plan, statements) do
    pre_count = length(plan.pre_statements)
    post_count = length(plan.post_statements)

    {pre, rest} = Enum.split(statements, pre_count)
    main_count = max(length(rest) - post_count, 0)
    {_main, post} = Enum.split(rest, main_count)

    {:ok, pre, post}
  end

  defp execute_statements(_conn, []), do: {:ok, :noop}

  defp execute_statements(conn, statements) do
    Enum.reduce_while(statements, {:ok, :ok}, fn statement, _acc ->
      case execute(conn, statement, []) do
        {:ok, _} -> {:cont, {:ok, :ok}}
        {:error, error} -> {:halt, {:error, error}}
      end
    end)
  end

  defp appender_table_statement(%WritePlan{} = plan) do
    target = qualified_relation(plan.target)

    empty_plan = %WritePlan{plan | select_sql: ["SELECT * FROM (", plan.select_sql, ") LIMIT 0"]}

    create_table_statement(target, empty_plan)
  end

  defp appender_rows(%WritePlan{} = plan, opts) do
    plan.options
    |> Map.get(:appender_rows, Keyword.get(opts, :appender_rows, []))
    |> List.wrap()
  end

  defp run_query(%Conn{conn_ref: conn_ref}, statement, params) do
    Client.query(conn_ref, IO.iodata_to_binary(statement), params)
  end

  defp fetch_rows(result_ref) do
    columns =
      case Client.columns(result_ref) do
        cols when is_list(cols) -> Enum.map(cols, &to_string/1)
        _ -> []
      end

    rows =
      case Client.fetch_all(result_ref) do
        rows when is_list(rows) -> Enum.map(rows, &normalize_row(&1, columns))
        other -> other
      end

    _ = safe_release(result_ref)

    if is_list(rows), do: {:ok, rows, columns}, else: {:error, rows}
  end

  defp normalize_row(row, _columns) when is_map(row) do
    row
    |> Enum.map(fn {k, v} -> {to_string(k), v} end)
    |> Map.new()
  end

  defp normalize_row(row, columns) when is_list(row) do
    if Keyword.keyword?(row) do
      row
      |> Enum.map(fn {k, v} -> {to_string(k), v} end)
      |> Map.new()
    else
      columns
      |> Enum.zip(row)
      |> Map.new()
    end
  end

  defp normalize_row(other, _columns), do: %{"value" => other}

  defp tx_begin(%Conn{conn_ref: conn_ref}) do
    case Client.begin_transaction(conn_ref) do
      :ok -> :ok
      {:error, reason} -> {:error, normalize_error(:transaction, nil, reason)}
    end
  end

  defp tx_commit(%Conn{conn_ref: conn_ref}) do
    case Client.commit(conn_ref) do
      :ok -> :ok
      {:error, reason} -> {:error, normalize_error(:transaction, nil, reason)}
    end
  end

  defp tx_rollback(%Conn{conn_ref: conn_ref}) do
    case Client.rollback(conn_ref) do
      :ok -> :ok
      {:error, _reason} -> :ok
    end
  end

  defp create_view_statement(target, %WritePlan{replace_existing?: true, select_sql: sql}),
    do: ["CREATE OR REPLACE VIEW ", target, " AS ", sql]

  defp create_view_statement(target, %WritePlan{replace?: true, select_sql: sql}),
    do: ["CREATE OR REPLACE VIEW ", target, " AS ", sql]

  defp create_view_statement(target, %WritePlan{if_not_exists?: true, select_sql: sql}),
    do: ["CREATE VIEW IF NOT EXISTS ", target, " AS ", sql]

  defp create_view_statement(target, %WritePlan{select_sql: sql}),
    do: ["CREATE VIEW ", target, " AS ", sql]

  defp create_table_statement(target, %WritePlan{replace_existing?: true, select_sql: sql}),
    do: ["CREATE OR REPLACE TABLE ", target, " AS ", sql]

  defp create_table_statement(target, %WritePlan{replace?: true, select_sql: sql}),
    do: ["CREATE OR REPLACE TABLE ", target, " AS ", sql]

  defp create_table_statement(target, %WritePlan{if_not_exists?: true, select_sql: sql}),
    do: ["CREATE TABLE IF NOT EXISTS ", target, " AS ", sql]

  defp create_table_statement(target, %WritePlan{select_sql: sql}),
    do: ["CREATE TABLE ", target, " AS ", sql]

  defp incremental_statements(target, %WritePlan{mode: :bootstrap} = plan),
    do: [create_table_statement(target, plan)]

  defp incremental_statements(target, %WritePlan{strategy: :append, select_sql: sql}),
    do: [["INSERT INTO ", target, " ", sql]]

  defp incremental_statements(
         target,
         %WritePlan{strategy: :delete_insert, select_sql: sql, window: window, options: options}
       ) do
    column = normalize_window_column(options)

    [
      [
        "DELETE FROM ",
        target,
        " WHERE ",
        quote_ident(column),
        " >= TIMESTAMP ",
        quote_literal(DateTime.to_iso8601(window.start_at)),
        " AND ",
        quote_ident(column),
        " < TIMESTAMP ",
        quote_literal(DateTime.to_iso8601(window.end_at))
      ],
      ["INSERT INTO ", target, " ", sql]
    ]
  end

  defp incremental_statements(_target, %WritePlan{strategy: strategy}) do
    raise ArgumentError, "unsupported incremental strategy for DuckDB: #{inspect(strategy)}"
  end

  defp statement_params(
         %WritePlan{materialization: :incremental, strategy: :delete_insert, mode: :incremental},
         statement,
         params
       ) do
    if IO.iodata_to_binary(statement) |> String.starts_with?("DELETE FROM") do
      []
    else
      params
    end
  end

  defp statement_params(_plan, _statement, params), do: params

  defp normalize_window_column(options) do
    options
    |> Map.fetch!(:window_column)
    |> to_string()
  end

  defp open_appender(%Conn{conn_ref: conn_ref}, %Relation{name: name, schema: schema}) do
    Client.appender(conn_ref, name, schema)
  end

  defp append_rows(%Conn{} = conn, %Relation{} = target, rows) do
    with {:ok, appender} <- open_appender(conn, target) do
      result =
        with :ok <- Client.appender_add_rows(appender, rows), do: Client.appender_flush(appender)

      case {result, close_appender(appender)} do
        {:ok, :ok} -> :ok
        {{:error, reason}, _close_result} -> {:error, reason}
        {:ok, {:error, reason}} -> {:error, reason}
      end
    end
  end

  defp close_appender(appender) do
    case Client.appender_close(appender) do
      :ok ->
        :ok

      {:error, reason} ->
        _ = safe_release(appender)
        {:error, reason}
    end
  rescue
    _ ->
      _ = safe_release(appender)
      {:error, :appender_close_failed}
  end

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
    _ = Client.release(resource)
    :ok
  rescue
    _ -> :ok
  end

  defp normalize_error(operation, connection, reason) do
    %Error{
      type: error_type(operation, reason),
      message: error_message(reason),
      retryable?: retryable_reason?(reason),
      adapter: __MODULE__,
      operation: operation,
      connection: connection,
      details: %{reason: reason}
    }
  end

  defp error_type(:connect, :invalid_database), do: :invalid_config
  defp error_type(:connect, _), do: :connection_error
  defp error_type(:ping, _), do: :connection_error
  defp error_type(:transaction, _), do: :execution_error

  defp error_type(_, reason) when reason in [:invalid_argument, :syntax_error],
    do: :execution_error

  defp error_type(_, {:error, _}), do: :execution_error
  defp error_type(_, _), do: :execution_error

  defp error_message(reason) when is_binary(reason), do: reason
  defp error_message({:error, message}) when is_binary(message), do: message
  defp error_message(_), do: "duckdb operation failed"

  defp retryable_reason?(reason) when reason in [:busy, :locked], do: true

  defp retryable_reason?("Transaction conflict: cannot update a table that has been altered!"),
    do: true

  defp retryable_reason?(_), do: false
end
