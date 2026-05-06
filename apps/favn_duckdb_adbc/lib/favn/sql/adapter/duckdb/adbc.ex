defmodule Favn.SQL.Adapter.DuckDB.ADBC do
  @moduledoc """
  DuckDB implementation of `Favn.SQL.Adapter` backed by ADBC.

  This adapter is exposed through the public `:favn_duckdb_adbc` plugin and is
  the preferred DuckDB production path. It keeps the existing `Favn.SQLClient`
  API and uses bounded row materialization when returning query results to
  Elixir. Large result sets should be written by DuckDB itself to explicit
  external paths with SQL such as `COPY (...) TO ...`.

  ## DuckDB ADBC driver installation

  Production hosts must have a DuckDB ADBC-capable `libduckdb` installed or use
  the driver installation mechanism supported by the `:adbc` package. See the
  DuckDB ADBC client documentation for supported driver setup:
  https://duckdb.org/docs/stable/clients/adbc.html

  To pin a specific DuckDB build, configure the driver path and entrypoint:

      config :favn, :duckdb_adbc,
        driver: "/opt/duckdb/1.5.2/libduckdb.so",
        entrypoint: "duckdb_adbc_init"

  The gated integration test also honors `DUCKDB_ADBC_DRIVER` with the
  `duckdb_adbc_init` entrypoint.

  ## Result bounds

  Normal `query/3` calls are for bounded read-style SQL such as `SELECT`, `WITH`,
  and `VALUES`. They are wrapped with a `LIMIT` based on the configured row
  limit, and converted results are checked against a byte limit before returning
  to Elixir. Configure plugin defaults with:

      config :favn, :runner_plugins,
        [{FavnDuckdbADBC,
          default_row_limit: 10_000,
          default_result_byte_limit: 20_000_000}]

  Large data movement should stay in DuckDB via `execute/3` and explicit SQL such
  as `COPY TO`, `COPY FROM`, `read_json`, or `read_ndjson` against caller-owned
  paths.
  """

  @behaviour Favn.SQL.Adapter

  alias Favn.Connection.Resolved
  alias Favn.RelationRef
  alias Favn.SQL.Adapter.DuckDB.ADBC.{Bootstrap, Client, ErrorMapper}
  alias Favn.SQL.{Capabilities, Column, ConcurrencyPolicy, Error, Relation, Result, WritePlan}

  defmodule Conn do
    @moduledoc false

    @enforce_keys [:db_ref, :conn_ref, :connection, :client, :max_rows, :max_result_bytes]
    defstruct [:db_ref, :conn_ref, :connection, :client, :max_rows, :max_result_bytes]

    @type t :: %__MODULE__{
            db_ref: term(),
            conn_ref: term(),
            connection: atom() | nil,
            client: module(),
            max_rows: pos_integer(),
            max_result_bytes: pos_integer()
          }
  end

  @type opts :: keyword()

  @production_key :production?
  @storage_key :duckdb_storage
  @local_file_storage :local_file
  @non_local_storage [:external, :ephemeral, :ducklake]

  @impl true
  @spec connect(Resolved.t(), opts()) :: {:ok, Conn.t()} | {:error, Error.t()}
  def connect(%Resolved{config: config} = resolved, opts) do
    client = resolve_client(opts)
    client_opts = Keyword.get(opts, :duckdb_adbc, [])
    max_rows = max_rows(opts)
    max_result_bytes = max_result_bytes(opts)

    with :ok <- validate_production_storage(resolved),
         {:ok, db_ref} <- client.open(Map.get(config || %{}, :database), client_opts),
         {:ok, conn_ref} <- create_connection(client, db_ref) do
      {:ok,
       %Conn{
         db_ref: db_ref,
         conn_ref: conn_ref,
         connection: resolved.name,
         client: client,
         max_rows: max_rows,
         max_result_bytes: max_result_bytes
       }}
    else
      {:error, %Error{} = error} -> {:error, error}
      {:error, reason} -> {:error, normalize_error(:connect, resolved.name, reason)}
    end
  end

  defp create_connection(client, db_ref) do
    case client.connection(db_ref) do
      {:ok, conn_ref} ->
        {:ok, conn_ref}

      {:error, reason} ->
        _ = safe_release(client, db_ref)
        {:error, reason}
    end
  end

  @impl true
  @spec bootstrap(Conn.t(), Resolved.t(), opts()) :: :ok | {:error, Error.t()}
  def bootstrap(%Conn{} = conn, %Resolved{} = resolved, opts),
    do: Bootstrap.run(conn, resolved, opts)

  @spec bootstrap_schema_field() :: Favn.Connection.Definition.field()
  @doc """
  Returns the connection schema field for DuckDB bootstrap runtime config.
  """
  def bootstrap_schema_field, do: Bootstrap.schema_field()

  @spec production_storage_schema_fields() :: [Favn.Connection.Definition.field()]
  @doc """
  Returns connection schema fields for DuckDB production storage validation.
  """
  def production_storage_schema_fields do
    [
      %{key: @production_key, type: :boolean, default: false},
      %{
        key: @storage_key,
        type: {:in, [@local_file_storage | @non_local_storage]},
        default: @local_file_storage
      }
    ]
  end

  @impl true
  @spec disconnect(Conn.t(), opts()) :: :ok
  def disconnect(%Conn{} = conn, _opts) do
    _ = safe_release(conn, conn.conn_ref)
    _ = safe_release(conn, conn.db_ref)
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
       extensions: %{bundled_in_amalgamation: [:csv, :parquet], duckdb_adbc: :supported}
     }}
  end

  @impl true
  @spec diagnostics(Resolved.t(), opts()) :: {:ok, map()} | {:error, map()}
  def diagnostics(%Resolved{config: config} = resolved, opts) do
    config = config || %{}
    storage = diagnostics_storage(config)

    with :ok <- validate_production_storage(resolved),
         {:ok, preflight} <- diagnostics_preflight(resolved, opts) do
      {:ok, %{status: :ok, adapter: __MODULE__, storage: storage, preflight: preflight}}
    else
      {:error, %Error{} = error} ->
        {:error, diagnostics_error(error, storage)}

      {:error, map} when is_map(map) ->
        {:error, Map.put_new(map, :storage, storage)}
    end
  end

  defp diagnostics_storage(config) do
    %{
      production?: Map.get(config, @production_key, false),
      mode: Map.get(config, @storage_key, @local_file_storage),
      database_path: :redacted
    }
  end

  defp diagnostics_preflight(%Resolved{} = resolved, opts) do
    case connect(resolved, opts) do
      {:ok, %Conn{} = conn} ->
        try do
          run_diagnostics_preflight(conn, resolved, opts)
        after
          _ = disconnect(conn, [])
        end

      {:error, %Error{} = error} ->
        {:error, diagnostics_error(error, diagnostics_storage(resolved.config), :connect)}
    end
  end

  defp run_diagnostics_preflight(%Conn{} = conn, %Resolved{} = resolved, opts) do
    with :ok <- bootstrap(conn, resolved, opts),
         :ok <- ping(conn, []),
         {:ok, version} <- duckdb_version(conn) do
      {:ok,
       %{
         driver: diagnostics_driver(opts),
         connect?: true,
         ping?: true,
         bootstrap?: true,
         duckdb_version: version
       }}
    else
      {:error, %Error{} = error} ->
        {:error, diagnostics_error(error, diagnostics_storage(resolved.config))}
    end
  end

  defp duckdb_version(%Conn{} = conn) do
    with {:ok, result} <- query(conn, "SELECT version() AS duckdb_version", []) do
      version =
        result.rows
        |> List.first(%{})
        |> Map.get("duckdb_version")

      {:ok, version}
    end
  end

  defp diagnostics_driver(opts) do
    configured = FavnDuckdbADBC.Runtime.driver_opts()
    connect_opts = Keyword.get(opts, :duckdb_adbc, [])
    driver = Keyword.get(connect_opts, :driver, Keyword.get(configured, :driver, :duckdb))
    entrypoint = Keyword.get(connect_opts, :entrypoint, Keyword.get(configured, :entrypoint))

    %{
      driver: if(is_binary(driver), do: :redacted_path, else: driver),
      entrypoint: entrypoint
    }
  end

  defp diagnostics_error(%Error{} = error, storage, stage \\ nil) do
    redacted = Error.redact(error)

    %{
      status: :unavailable,
      adapter: __MODULE__,
      message: redacted.message,
      storage: storage,
      preflight: %{
        stage: stage || redacted.operation,
        error_type: redacted.type,
        retryable?: redacted.retryable?,
        details: redacted.details
      }
    }
  end

  @impl true
  @spec default_concurrency_policy(Resolved.t()) :: ConcurrencyPolicy.t()
  def default_concurrency_policy(%Resolved{config: %{database: database}})
      when is_binary(database) and database not in [":memory:", ""] do
    %ConcurrencyPolicy{
      limit: 1,
      scope: {:duckdb_adbc_database, Path.expand(database)},
      applies_to: :all
    }
  end

  def default_concurrency_policy(%Resolved{} = resolved) do
    %ConcurrencyPolicy{ConcurrencyPolicy.single_writer(resolved) | applies_to: :all}
  end

  @impl true
  @spec execute(Conn.t(), iodata(), opts()) :: {:ok, Result.t()} | {:error, Error.t()}
  def execute(%Conn{} = conn, statement, opts) do
    params = Keyword.get(opts, :params, [])
    sql = IO.iodata_to_binary(statement)

    case conn.client.execute(conn.conn_ref, sql, params) do
      {:ok, rows_affected} ->
        {:ok,
         %Result{
           kind: :execute,
           command: sql,
           rows_affected: rows_affected,
           rows: [],
           columns: [],
           metadata: %{}
         }}

      {:error, reason} ->
        {:error, normalize_error(:execute, conn.connection, reason)}
    end
  end

  @impl true
  @spec query(Conn.t(), iodata(), opts()) :: {:ok, Result.t()} | {:error, Error.t()}
  def query(%Conn{} = conn, statement, opts) do
    params = Keyword.get(opts, :params, [])
    sql = IO.iodata_to_binary(statement)

    case conn.client.query(conn.conn_ref, bounded_query_sql(sql, conn.max_rows), params) do
      {:ok, result_ref} ->
        build_query_result(conn, result_ref, sql)

      {:error, reason} ->
        {:error, normalize_error(:query, conn.connection, reason)}
    end
  end

  defp build_query_result(%Conn{} = conn, result_ref, sql) do
    with {:ok, columns} <- fetch_columns(conn, result_ref),
         {:ok, rows} <- fetch_all_rows(conn, result_ref) do
      {:ok,
       %Result{
         kind: :query,
         command: sql,
         rows_affected: nil,
         rows: rows,
         columns: columns,
         metadata: %{row_limit: conn.max_rows, result_byte_limit: conn.max_result_bytes}
       }}
    else
      {:error, reason} -> {:error, normalize_error(:query, conn.connection, reason)}
    end
  after
    _ = safe_release(conn, result_ref)
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
     [relation_introspection_base(ref), " AND table_name = ", quote_literal(ref.name), " LIMIT 1"]}
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

    {:ok,
     schema_setup_statements(plan) ++ plan.pre_statements ++ statements ++ plan.post_statements}
  rescue
    _error ->
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
    case query(conn, "SELECT 1", []) do
      {:ok, _result} -> :ok
      {:error, %Error{} = error} -> {:error, %Error{error | operation: :ping}}
    end
  end

  @impl true
  @spec schema_exists?(Conn.t(), binary(), opts()) :: {:ok, boolean()} | {:error, Error.t()}
  def schema_exists?(%Conn{} = conn, schema, _opts) when is_binary(schema) do
    sql =
      "SELECT 1 FROM information_schema.schemata WHERE schema_name = #{IO.iodata_to_binary(quote_literal(schema))} LIMIT 1"

    with {:ok, result} <- query(conn, sql, []), do: {:ok, result.rows != []}
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
           query(conn, "SELECT schema_name AS schema FROM information_schema.schemata", []) do
      {:ok, Enum.map(result.rows, &Map.get(&1, "schema"))}
    end
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

    with {:ok, result} <- query(conn, sql, []), do: {:ok, Enum.map(result.rows, &row_to_column/1)}
  end

  @impl true
  @spec row_count(Conn.t(), RelationRef.t(), opts()) ::
          {:ok, non_neg_integer()} | {:error, Error.t()}
  def row_count(%Conn{} = conn, %RelationRef{} = ref, _opts) do
    sql = ["SELECT count(*) AS row_count FROM ", qualified_relation_ref(ref)]

    with {:ok, result} <- query(conn, sql, []) do
      count = result.rows |> List.first(%{}) |> Map.get("row_count", 0) |> normalize_integer()
      {:ok, count || 0}
    end
  end

  @impl true
  @spec sample(Conn.t(), RelationRef.t(), opts()) :: {:ok, Result.t()} | {:error, Error.t()}
  def sample(%Conn{} = conn, %RelationRef{} = ref, opts) do
    limit = opts |> Keyword.get(:limit, 20) |> clamp_sample_limit()
    sql = ["SELECT * FROM ", qualified_relation_ref(ref), " LIMIT ", Integer.to_string(limit)]
    query(conn, sql, [])
  end

  @impl true
  @spec table_metadata(Conn.t(), RelationRef.t(), opts()) :: {:ok, map()} | {:error, Error.t()}
  def table_metadata(%Conn{} = conn, %RelationRef{} = ref, _opts) do
    with {:ok, relation} <- relation(conn, ref, []) do
      metadata =
        case relation do
          %Relation{} = relation ->
            %{
              type: relation.type,
              catalog: relation.catalog,
              schema: relation.schema,
              name: relation.name
            }

          nil ->
            %{}
        end

      {:ok, metadata}
    end
  end

  @impl true
  @spec transaction(Conn.t(), (Conn.t() -> {:ok, term()} | {:error, Error.t()}), opts()) ::
          {:ok, term()} | {:error, Error.t()}
  def transaction(%Conn{} = conn, fun, _opts) when is_function(fun, 1) do
    case tx_begin(conn) do
      :ok -> run_transaction(conn, fun)
      {:error, %Error{} = error} -> {:error, error}
    end
  end

  @impl true
  @spec materialize(Conn.t(), WritePlan.t(), opts()) :: {:ok, Result.t()} | {:error, Error.t()}
  def materialize(%Conn{} = conn, %WritePlan{} = plan, opts) do
    rows = appender_rows(plan, opts)

    if plan.materialization == :table and rows != [] do
      bulk_insert_materialize(conn, plan, rows)
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
      {:ok, result} -> {:ok, result}
      {:error, %Error{} = error} -> {:error, materialize_error(error, conn)}
    end
  end

  defp run_plan_materialization(%Conn{} = conn, %WritePlan{} = plan, opts),
    do: run_materialization_statements(conn, plan, opts)

  defp run_materialization_statements(%Conn{} = conn, %WritePlan{} = plan, opts) do
    params = Keyword.get(opts, :params, [])

    with {:ok, statements} <- materialization_statements(plan, %Capabilities{}, opts) do
      schema_setup_count = length(schema_setup_statements(plan))

      statements
      |> Enum.with_index()
      |> Enum.reduce_while({:ok, 0}, fn {statement, index}, {:ok, count} ->
        statement_params =
          materialization_statement_params(plan, statement, params, index, schema_setup_count)

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

  defp bulk_insert_materialize(%Conn{} = conn, %WritePlan{} = plan, rows) do
    transaction(
      conn,
      fn tx_conn ->
        with {:ok, statements} <- materialization_statements(plan, %Capabilities{}, []),
             {:ok, pre, post} <- split_materialization_statements(plan, statements),
             {:ok, _} <- execute_statements(tx_conn, pre),
             {:ok, _} <- execute(tx_conn, bulk_insert_table_statement(plan), []),
             {:ok, rows_affected} <- bulk_insert_rows(tx_conn, plan.target, rows),
             {:ok, _} <- execute_statements(tx_conn, post) do
          {:ok,
           %Result{
             kind: :materialize,
             command: "bulk_insert",
             rows_affected: rows_affected,
             metadata: %{strategy: :adbc_bulk_insert}
           }}
        end
      end,
      []
    )
    |> case do
      {:ok, result} -> {:ok, result}
      {:error, %Error{} = error} -> {:error, materialize_error(error, conn)}
    end
  end

  defp split_materialization_statements(%WritePlan{} = plan, statements) do
    pre_count = length(schema_setup_statements(plan)) + length(plan.pre_statements)
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

  defp bulk_insert_table_statement(%WritePlan{} = plan) do
    target = qualified_relation(plan.target)
    empty_plan = %WritePlan{plan | select_sql: ["SELECT * FROM (", plan.select_sql, ") LIMIT 0"]}
    create_table_statement(target, empty_plan)
  end

  defp bulk_insert_rows(%Conn{} = conn, %Relation{name: name, schema: schema}, rows) do
    opts = [table: name, mode: :append]

    opts =
      if is_binary(schema) and schema != "", do: Keyword.put(opts, :schema, schema), else: opts

    case conn.client.bulk_insert(conn.conn_ref, rows, opts) do
      {:ok, rows_affected} -> {:ok, rows_affected}
      {:error, reason} -> {:error, normalize_error(:materialize, conn.connection, reason)}
    end
  end

  defp schema_setup_statements(%WritePlan{target: %Relation{schema: schema}})
       when is_binary(schema) and schema not in ["", "main"] do
    [["CREATE SCHEMA IF NOT EXISTS ", quote_ident(schema)]]
  end

  defp schema_setup_statements(%WritePlan{}), do: []

  defp appender_rows(%WritePlan{} = plan, opts) do
    plan.options |> Map.get(:appender_rows, Keyword.get(opts, :appender_rows, [])) |> List.wrap()
  end

  defp fetch_columns(%Conn{} = conn, result_ref) do
    case conn.client.columns(result_ref) do
      {:error, reason} -> {:error, reason}
      cols when is_list(cols) -> {:ok, Enum.map(cols, &to_string/1)}
      _other -> {:ok, []}
    end
  end

  defp fetch_all_rows(%Conn{} = conn, result_ref) do
    case conn.client.fetch_all(result_ref, conn.max_rows, conn.max_result_bytes, bounded?: true) do
      {:error, reason} -> {:error, reason}
      rows when is_list(rows) -> {:ok, Enum.map(rows, &normalize_row/1)}
      other -> {:error, other}
    end
  end

  defp normalize_row(row) when is_map(row),
    do: row |> Enum.map(fn {k, v} -> {to_string(k), v} end) |> Map.new()

  defp normalize_row(row) when is_list(row) do
    if Keyword.keyword?(row) do
      row
      |> Enum.map(fn {k, v} -> {to_string(k), v} end)
      |> Map.new()
    else
      %{"value" => row}
    end
  end

  defp normalize_row(other), do: %{"value" => other}

  defp tx_begin(%Conn{conn_ref: conn_ref, client: client} = conn) do
    case client.begin_transaction(conn_ref) do
      :ok ->
        :ok

      {:error, reason} ->
        {:error,
         normalize_error(:transaction, conn.connection, reason) |> transaction_stage_error(:begin)}
    end
  end

  defp tx_commit(%Conn{conn_ref: conn_ref, client: client} = conn) do
    case client.commit(conn_ref) do
      :ok ->
        :ok

      {:error, reason} ->
        {:error,
         normalize_error(:transaction, conn.connection, reason)
         |> transaction_stage_error(:commit)}
    end
  end

  defp tx_rollback(%Conn{conn_ref: conn_ref, client: client}), do: client.rollback(conn_ref)

  defp run_transaction(%Conn{} = conn, fun) do
    case fun.(conn) do
      {:ok, value} ->
        case tx_commit(conn) do
          :ok -> {:ok, value}
          {:error, %Error{} = error} -> finalize_transaction_failure(conn, error)
        end

      {:error, %Error{} = error} ->
        finalize_transaction_failure(conn, transaction_stage_error(error, :body))

      {:error, reason} ->
        error =
          normalize_error(:transaction, conn.connection, reason) |> transaction_stage_error(:body)

        finalize_transaction_failure(conn, error)

      other ->
        error =
          normalize_error(:transaction, conn.connection, {:invalid_transaction_result, other})
          |> transaction_stage_error(:body)

        finalize_transaction_failure(conn, error)
    end
  rescue
    error ->
      raised = %Error{
        type: :execution_error,
        message: "transaction body raised exception",
        retryable?: false,
        adapter: __MODULE__,
        operation: :transaction,
        connection: conn.connection,
        details: %{
          classification: :execution,
          transaction_stage: :body,
          exception: Exception.format(:error, error, __STACKTRACE__)
        },
        cause: error
      }

      finalize_transaction_failure(conn, raised)
  end

  defp finalize_transaction_failure(%Conn{} = conn, %Error{} = error) do
    case tx_rollback(conn) do
      :ok -> {:error, error}
      {:error, reason} -> {:error, ErrorMapper.rollback_failure(error, reason)}
    end
  end

  defp transaction_stage_error(%Error{} = error, stage),
    do: %Error{error | details: Map.put(error.details || %{}, :transaction_stage, stage)}

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

  defp incremental_statements(target, %WritePlan{
         strategy: :delete_insert,
         select_sql: sql,
         window: window,
         options: options
       }) do
    column = options |> Map.fetch!(:window_column) |> to_string()

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
    raise ArgumentError, "unsupported incremental strategy for DuckDB ADBC: #{inspect(strategy)}"
  end

  defp statement_params(
         %WritePlan{materialization: :incremental, strategy: :delete_insert, mode: :incremental},
         statement,
         params
       ) do
    if IO.iodata_to_binary(statement) |> String.starts_with?("DELETE FROM"), do: [], else: params
  end

  defp statement_params(_plan, _statement, params), do: params

  defp materialization_statement_params(_plan, _statement, _params, index, schema_setup_count)
       when index < schema_setup_count, do: []

  defp materialization_statement_params(plan, statement, params, _index, _schema_setup_count),
    do: statement_params(plan, statement, params)

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
        _other -> nil
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
  defp relation_type(_other), do: :unknown

  defp normalize_integer(value) when is_integer(value), do: value

  defp normalize_integer(value) when is_binary(value) do
    case Integer.parse(value) do
      {int, ""} -> int
      _other -> nil
    end
  end

  defp normalize_integer(_value), do: nil

  defp qualified_relation(%Relation{schema: nil, name: name}), do: quote_ident(name)

  defp qualified_relation(%Relation{schema: schema, name: name}),
    do: [quote_ident(schema), ".", quote_ident(name)]

  defp qualified_relation_ref(%RelationRef{catalog: nil, schema: nil, name: name}),
    do: quote_ident(name)

  defp qualified_relation_ref(%RelationRef{catalog: nil, schema: schema, name: name}),
    do: [quote_ident(schema), ".", quote_ident(name)]

  defp qualified_relation_ref(%RelationRef{catalog: catalog, schema: nil, name: name}),
    do: [quote_ident(catalog), ".", quote_ident(name)]

  defp qualified_relation_ref(%RelationRef{catalog: catalog, schema: schema, name: name}),
    do: [quote_ident(catalog), ".", quote_ident(schema), ".", quote_ident(name)]

  defp clamp_sample_limit(limit) when is_integer(limit) and limit >= 0 and limit <= 20, do: limit
  defp clamp_sample_limit(limit) when is_integer(limit) and limit > 20, do: 20
  defp clamp_sample_limit(_limit), do: 20

  defp quote_ident(identifier),
    do: ["\"", String.replace(to_string(identifier), "\"", "\"\""), "\""]

  defp quote_literal(value), do: ["'", String.replace(to_string(value), "'", "''"), "'"]

  defp safe_release(%Conn{client: client}, resource), do: safe_release(client, resource)

  defp safe_release(client, resource) do
    _ = client.release(resource)
    :ok
  rescue
    _error -> :ok
  end

  defp validate_production_storage(%Resolved{config: config} = resolved) do
    config = config || %{}

    if Map.get(config, @production_key, false) do
      with :ok <- validate_storage_mode(config, resolved) do
        if local_file_storage?(config),
          do: validate_local_file_database(Map.get(config, :database), resolved),
          else: :ok
      end
    else
      :ok
    end
  end

  defp validate_storage_mode(config, resolved) do
    storage = Map.get(config, @storage_key, @local_file_storage)

    if storage in [@local_file_storage | @non_local_storage] do
      :ok
    else
      production_storage_error(
        resolved,
        :invalid_storage_mode,
        "production DuckDB storage mode must be :local_file, :external, :ephemeral, or :ducklake"
      )
    end
  end

  defp local_file_storage?(config),
    do: Map.get(config, @storage_key, @local_file_storage) == @local_file_storage

  defp validate_local_file_database(nil, resolved),
    do:
      production_storage_error(
        resolved,
        :missing_database,
        "production DuckDB local-file storage requires an absolute :database path"
      )

  defp validate_local_file_database(":memory:", resolved),
    do:
      production_storage_error(
        resolved,
        :memory_database,
        "production DuckDB local-file storage cannot use :memory:"
      )

  defp validate_local_file_database(database, resolved) when is_binary(database) do
    trimmed_database = String.trim(database)

    cond do
      trimmed_database == "" ->
        production_storage_error(
          resolved,
          :blank_database,
          "production DuckDB local-file storage requires a non-blank :database path"
        )

      trimmed_database != database ->
        production_storage_error(
          resolved,
          :invalid_database,
          "production DuckDB local-file storage requires an exact absolute :database path"
        )

      Path.type(database) != :absolute ->
        production_storage_error(
          resolved,
          :relative_database,
          "production DuckDB local-file storage requires an absolute :database path"
        )

      true ->
        validate_database_parent(database, resolved)
    end
  end

  defp validate_local_file_database(_database, resolved),
    do:
      production_storage_error(
        resolved,
        :invalid_database,
        "production DuckDB local-file storage requires an absolute :database path"
      )

  defp validate_database_parent(database, resolved) do
    parent = Path.dirname(database)

    cond do
      not File.dir?(parent) ->
        production_storage_error(
          resolved,
          :missing_parent_directory,
          "production DuckDB database parent directory must exist",
          %{parent: parent}
        )

      not writable_directory?(parent) ->
        production_storage_error(
          resolved,
          :unwritable_parent_directory,
          "production DuckDB database parent directory must be writable",
          %{parent: parent}
        )

      true ->
        :ok
    end
  end

  defp writable_directory?(directory) do
    path =
      Path.join(directory, ".favn_duckdb_adbc_write_test_#{System.unique_integer([:positive])}")

    case File.open(path, [:write, :exclusive]) do
      {:ok, io} ->
        File.close(io)
        File.rm(path)
        true

      {:error, _reason} ->
        false
    end
  end

  defp production_storage_error(%Resolved{} = resolved, reason, message, extra_details \\ %{}) do
    {:error,
     %Error{
       type: :invalid_config,
       message: message,
       retryable?: false,
       adapter: __MODULE__,
       operation: :connect,
       connection: resolved.name,
       details: Map.merge(%{classification: :invalid_config, reason: reason}, extra_details)
     }}
  end

  defp resolve_client(opts) do
    candidate = Keyword.get(opts, :duckdb_adbc_client, Client.default())
    if is_atom(candidate), do: candidate, else: Client.default()
  end

  defp max_rows(opts) do
    case Keyword.get(opts, :max_rows, FavnDuckdbADBC.Runtime.default_row_limit()) do
      limit when is_integer(limit) and limit > 0 -> limit
      _other -> FavnDuckdbADBC.Runtime.default_row_limit()
    end
  end

  defp max_result_bytes(opts) do
    case Keyword.get(opts, :max_result_bytes, FavnDuckdbADBC.Runtime.default_result_byte_limit()) do
      limit when is_integer(limit) and limit > 0 -> limit
      _other -> FavnDuckdbADBC.Runtime.default_result_byte_limit()
    end
  end

  defp bounded_query_sql(sql, max_rows) do
    sanitized_sql = sql |> String.trim() |> String.trim_trailing(";")
    limit = Integer.to_string(max_rows + 1)

    ["SELECT * FROM (", sanitized_sql, ") AS favn_adbc_bounded_result LIMIT ", limit]
    |> IO.iodata_to_binary()
  end

  defp materialize_error(%Error{} = error, %Conn{} = conn),
    do: %Error{error | operation: :materialize, connection: error.connection || conn.connection}

  defp normalize_error(operation, connection, reason),
    do: ErrorMapper.normalize(operation, connection, reason)
end
