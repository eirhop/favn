defmodule Favn.SQL.Adapter.DuckDB do
  @moduledoc """
  DuckDB implementation of `Favn.SQL.Adapter` backed by `duckdbex`.

  Most functions in this module are internal runtime adapter callbacks. The
  supported authoring helpers are `config_schema_fields/0` and
  `production_storage_schema_fields/0`, which can be used in a
  `Favn.Connection.Definition` when a DuckDB connection needs session setup or
  production storage validation before `Favn.SQLClient` or `Favn.SQLAsset`
  execution.

  Bulk ingestion paths should prefer the DuckDB Appender path for substantial
  insert workloads instead of repeated prepared inserts.

  DuckDB sessions are poolable and therefore use Favn's runner-local SQL session
  pool by default. Disable per connection with `pool: [enabled: false]`, or tune
  local idle retention with:

      pool: [enabled: true, max_idle_per_key: 1, idle_timeout_ms: 300_000]

  Pooling is local to one runner BEAM, checked-out sessions are exclusive, and
  reuse is keyed by connection/config, required catalog set, and adapter
  fingerprint. It does not increase catalog/write concurrency or replace
  DuckLake metadata capacity controls such as conservative `write_concurrency`,
  PgBouncer, or scaling the PostgreSQL metadata database.

  ## Consumer setup

  This adapter is exposed through the public `:favn_duckdb` package/plugin. Add
  `:favn_duckdb` when a consumer project needs DuckDB-backed SQL assets,
  `Favn.SQLClient` DuckDB connections, or DuckDB/DuckLake bootstrap support.

  Do not add internal apps such as `:favn_sql_runtime` or `:favn_runner`
  directly to consumer `mix.exs` files for DuckDB support.

  ## DuckLake bootstrap example

      defmodule MyApp.Connections.Warehouse do
        @behaviour Favn.Connection

        @impl true
        def definition do
          %Favn.Connection.Definition{
            name: :warehouse,
            adapter: Favn.SQL.Adapter.DuckDB,
            config_schema: Favn.SQL.Adapter.DuckDB.config_schema_fields()
          }
        end
      end

      config :favn,
        connections: [
          warehouse: [
            open: [database: ":memory:"],
            duckdb: [
              load: [:ducklake, :postgres, :azure],
              secrets: [
                azure_adls: [
                  type: :azure,
                  provider: :credential_chain,
                  account_name: Favn.RuntimeConfig.Ref.env!("AZURE_STORAGE_ACCOUNT"),
                  chain: [:cli, :env],
                  scope: Favn.RuntimeConfig.Ref.env!("DUCKLAKE_DATA_SCOPE")
                ],
                lakehouse_meta: [
                  type: :postgres,
                  host: Favn.RuntimeConfig.Ref.env!("DUCKLAKE_POSTGRES_HOST"),
                  port: 5432,
                  database: Favn.RuntimeConfig.Ref.env!("DUCKLAKE_POSTGRES_DATABASE"),
                  user: Favn.RuntimeConfig.Ref.env!("DUCKLAKE_POSTGRES_USER"),
                  auth: [
                    type: :azure_postgres_entra,
                    provider: :managed_identity,
                    client_id: Favn.RuntimeConfig.Ref.env!("AZURE_CLIENT_ID", required?: false),
                    endpoint: :auto
                  ],
                  sslmode: :require
                ]
              ],
              attach: [
                lake: [
                  type: :ducklake,
                  metadata: "ducklake:postgres:",
                  meta_secret: :lakehouse_meta,
                  data_path: Favn.RuntimeConfig.Ref.env!("DUCKLAKE_DATA_PATH"),
                  write_concurrency: :unlimited
                ]
              ],
              use: :lake
            ]
          ]
        ]

  Bootstrap failures return `Favn.SQL.Error` values with `operation: :bootstrap`,
  the failing step id, and redacted diagnostics.

  ## Supported DuckLake bootstrap secrets

  DuckDB bootstrap is intentionally DuckDB-specific. It can load extensions,
  create temporary DuckDB secrets, attach DuckDB/DuckLake catalogs, and set the
  active catalog with `USE`.

  Azure ADLS secrets support DuckDB's `credential_chain` provider with optional
  `:chain` and `:scope` fields. Chain values must be one or more of `:cli`,
  `:managed_identity`, `:workload_identity`, `:env`, or `:default`; they render
  as DuckDB's semicolon-separated `CHAIN` value. Scoped Azure secrets must use a
  trailing slash, for example `abfss://container@account.dfs.core.windows.net/path/`.

  PostgreSQL metadata catalog credentials are supplied as DuckDB Postgres secrets
  and then referenced from DuckLake attach config with `meta_secret: :secret_name`.

  PostgreSQL secrets accept either `:password` or Azure PostgreSQL Entra `:auth`,
  not both. Azure auth supports managed identity and Azure CLI dogfooding:

      auth: [
        type: :azure_postgres_entra,
        provider: :managed_identity,
        client_id: Favn.RuntimeConfig.Ref.env!("AZURE_CLIENT_ID", required?: false),
        endpoint: :auto
      ]

      auth: [type: :azure_postgres_entra, provider: :azure_cli]

  Managed identity `endpoint: :auto` uses Azure App Service managed identity when
  `IDENTITY_ENDPOINT` and `IDENTITY_HEADER` are present, otherwise IMDS. The
  PostgreSQL `:user` must be the PostgreSQL role created for the Entra principal
  or managed identity, for example with
  `select * from pgaadauth_create_principal('<identity_name>', false, false);`.
  Tokens are fetched during DuckDB bootstrap immediately before the temporary
  `CREATE SECRET` statement, injected as `PASSWORD`, never cached or persisted,
  and require reconnect/rebootstrap after expiry.

  Generated bootstrap SQL uses temporary DuckDB secrets (`CREATE SECRET`) only.
  """

  @behaviour Favn.SQL.Adapter

  alias Favn.Connection.Resolved
  alias Favn.RelationRef
  alias Favn.SQL.Adapter.DuckDB.{Bootstrap, Client, ErrorMapper}
  alias Favn.SQL.{Capabilities, Column, ConcurrencyPolicy, Error, Relation, Result, WritePlan}

  defmodule Conn do
    @moduledoc false

    @enforce_keys [:db_ref, :conn_ref, :connection, :client]
    defstruct [:db_ref, :conn_ref, :connection, :client]

    @type t :: %__MODULE__{
            db_ref: reference(),
            conn_ref: reference(),
            connection: atom() | nil,
            client: module()
          }
  end

  @type opts :: keyword()

  @production_key :production?
  @storage_key :duckdb_storage
  @local_file_storage :local_file
  @non_local_storage [:external, :ephemeral, :ducklake]

  @impl true
  @spec connect(Resolved.t(), opts()) :: {:ok, Conn.t()} | {:error, Error.t()}
  def connect(%Resolved{} = resolved, opts) do
    client = resolve_client(opts)

    with :ok <- validate_production_storage(resolved),
         {:ok, database} <- Bootstrap.database(resolved),
         {:ok, db_ref} <- client.open(database),
         {:ok, conn_ref} <- create_connection(client, db_ref) do
      {:ok, %Conn{db_ref: db_ref, conn_ref: conn_ref, connection: resolved.name, client: client}}
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
  Returns the legacy single schema field for `duckdb: [...]` runtime config.

  Prefer `config_schema_fields/0` so `open` and `duckdb` are validated together.
  """
  @deprecated "Use config_schema_fields/0 so open and duckdb config are validated together"
  def bootstrap_schema_field, do: Bootstrap.schema_field()

  @spec config_schema_fields() :: [Favn.Connection.Definition.field()]
  @doc """
  Returns DuckDB runtime config schema fields for `open: [...]` and `duckdb: [...]`.
  """
  def config_schema_fields, do: Bootstrap.config_schema_fields()

  @spec production_storage_schema_fields() :: [Favn.Connection.Definition.field()]
  @doc """
  Returns connection schema fields for DuckDB production storage validation.

  Production DuckDB connections default to durable local-file storage and must
  use an explicit absolute `open.database` path whose parent directory already
  exists and is writable. Set `:duckdb_storage` to `:external`, `:ephemeral`, or
  `:ducklake` for production connections that intentionally do not use local-file
  durable storage.
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
  @spec poolable?(Resolved.t(), opts()) :: true
  def poolable?(%Resolved{}, _opts), do: true

  @impl true
  @spec pool_fingerprint(Resolved.t(), opts()) :: map()
  def pool_fingerprint(%Resolved{}, opts) do
    %{
      adapter: __MODULE__,
      client: resolve_client(opts),
      duckdbex: application_vsn(:duckdbex),
      runtime: FavnDuckdb.Runtime.execution_mode()
    }
  end

  @impl true
  @spec validate_session(Conn.t(), opts()) :: :ok | {:error, Error.t()}
  def validate_session(%Conn{} = conn, opts), do: ping(conn, opts)

  @impl true
  @spec reset_session(Conn.t(), Resolved.t(), opts()) :: :ok | {:error, Error.t()}
  def reset_session(%Conn{} = conn, %Resolved{} = resolved, opts) do
    with :ok <- rollback_for_pool_reset(conn),
         :ok <- restore_use_catalog(conn, resolved, opts) do
      :ok
    end
  end

  @impl true
  @spec classify_error(term(), opts()) :: Favn.SQL.Adapter.error_classification()
  def classify_error(reason, opts), do: ErrorMapper.classify(reason, opts)

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
  @spec diagnostics(Resolved.t(), opts()) :: {:ok, map()} | {:error, map()}
  def diagnostics(%Resolved{config: config} = resolved, _opts) do
    config = config || %{}

    case validate_production_storage(resolved) do
      :ok ->
        {:ok,
         %{
           status: :ok,
           adapter: __MODULE__,
           storage: %{
             production?: Map.get(config, @production_key, false),
             mode: Map.get(config, @storage_key, @local_file_storage),
             database_path: :redacted
           }
         }}

      {:error, %Error{} = error} ->
        {:error,
         %{
           status: :invalid_config,
           adapter: __MODULE__,
           message: error.message,
           details: Map.drop(error.details, [:database, :parent])
         }}
    end
  end

  @impl true
  @spec default_concurrency_policy(Resolved.t()) :: ConcurrencyPolicy.t()
  def default_concurrency_policy(%Resolved{config: %{mode: :ducklake}} = resolved) do
    ConcurrencyPolicy.unlimited(resolved)
  end

  def default_concurrency_policy(%Resolved{config: %{open: %{database: database}}})
      when is_binary(database) and database not in [":memory:", ""] do
    %ConcurrencyPolicy{
      limit: 1,
      scope: {:duckdb_database, Path.expand(database)},
      applies_to: :all
    }
  end

  def default_concurrency_policy(%Resolved{} = resolved = %Resolved{config: %{open: open}})
      when is_list(open) do
    default_concurrency_policy(%Resolved{resolved | config: %{open: Map.new(open)}})
  end

  def default_concurrency_policy(%Resolved{config: %{database: database}})
      when is_binary(database) and database not in [":memory:", ""] do
    %ConcurrencyPolicy{
      limit: 1,
      scope: {:duckdb_database, Path.expand(database)},
      applies_to: :all
    }
  end

  def default_concurrency_policy(%Resolved{} = resolved) do
    %ConcurrencyPolicy{ConcurrencyPolicy.single_writer(resolved) | applies_to: :all}
  end

  @impl true
  @spec concurrency_policies(Resolved.t()) :: {:ok, [ConcurrencyPolicy.t()]} | {:error, Error.t()}
  def concurrency_policies(%Resolved{} = resolved) do
    case catalog_write_policies(resolved) do
      [] ->
        {:ok, [default_concurrency_policy(resolved)]}

      catalog_policies ->
        default = %ConcurrencyPolicy{
          ConcurrencyPolicy.single_writer(resolved)
          | applies_to: :writes
        }

        {:ok, [default | catalog_policies]}
    end
  end

  defp catalog_write_policies(%Resolved{} = resolved) do
    secrets = duckdb_secret_config(resolved)

    resolved
    |> duckdb_attach_config()
    |> Enum.map(fn {catalog, attach_config} ->
      catalog_write_policy(resolved, catalog, attach_config, secrets)
    end)
    |> normalize_shared_policy_limits()
  end

  defp catalog_write_policy(%Resolved{} = resolved, catalog, attach_config, secrets) do
    policy =
      ConcurrencyPolicy.catalog(resolved, catalog, catalog_write_concurrency(attach_config))

    %{policy | scope: catalog_policy_scope(resolved, catalog, attach_config, secrets)}
  end

  defp normalize_shared_policy_limits(policies) do
    limits_by_scope =
      policies
      |> Enum.group_by(& &1.scope)
      |> Map.new(fn {scope, scoped_policies} ->
        {scope, strictest_policy_limit(Enum.map(scoped_policies, & &1.limit))}
      end)

    Enum.map(policies, fn policy ->
      %{policy | limit: Map.fetch!(limits_by_scope, policy.scope)}
    end)
  end

  defp strictest_policy_limit(limits) do
    limits
    |> Enum.reject(&(&1 == :unlimited))
    |> case do
      [] -> :unlimited
      finite -> Enum.min(finite)
    end
  end

  defp duckdb_attach_config(%Resolved{config: %{duckdb: duckdb}}) when is_map(duckdb),
    do: normalize_attach_entries(Map.get(duckdb, :attach, []))

  defp duckdb_attach_config(%Resolved{config: %{duckdb: duckdb}}) when is_list(duckdb),
    do: normalize_attach_entries(Keyword.get(duckdb, :attach, []))

  defp duckdb_attach_config(_resolved), do: []

  defp duckdb_secret_config(%Resolved{config: %{duckdb: duckdb}}) when is_map(duckdb),
    do: normalize_secret_entries(Map.get(duckdb, :secrets, []))

  defp duckdb_secret_config(%Resolved{config: %{duckdb: duckdb}}) when is_list(duckdb),
    do: normalize_secret_entries(Keyword.get(duckdb, :secrets, []))

  defp duckdb_secret_config(_resolved), do: %{}

  defp normalize_attach_entries(entries) when is_map(entries) do
    Enum.map(entries, fn {catalog, config} -> {to_string(catalog), config} end)
  end

  defp normalize_attach_entries(entries) when is_list(entries) do
    if Keyword.keyword?(entries) do
      Enum.map(entries, fn {catalog, config} -> {to_string(catalog), config} end)
    else
      []
    end
  end

  defp normalize_attach_entries(_entries), do: []

  defp normalize_secret_entries(entries) when is_map(entries) do
    Map.new(entries, fn {name, config} -> {to_string(name), config} end)
  end

  defp normalize_secret_entries(entries) when is_list(entries) do
    if Keyword.keyword?(entries) do
      Map.new(entries, fn {name, config} -> {to_string(name), config} end)
    else
      %{}
    end
  end

  defp normalize_secret_entries(_entries), do: %{}

  @impl true
  @spec configured_catalogs(Resolved.t()) :: {:ok, [String.t()]}
  def configured_catalogs(%Resolved{} = resolved) do
    catalogs =
      resolved
      |> duckdb_attach_config()
      |> Enum.map(fn {catalog, _config} -> catalog end)

    {:ok, catalogs}
  end

  @impl true
  @spec default_catalog(Resolved.t()) :: {:ok, String.t() | nil}
  def default_catalog(%Resolved{} = resolved), do: {:ok, duckdb_use_catalog(resolved)}

  defp duckdb_use_catalog(%Resolved{config: %{duckdb: duckdb}}) when is_map(duckdb) do
    duckdb |> Map.get(:use) |> normalize_catalog_name()
  end

  defp duckdb_use_catalog(%Resolved{config: %{duckdb: duckdb}}) when is_list(duckdb) do
    duckdb |> Keyword.get(:use) |> normalize_catalog_name()
  end

  defp duckdb_use_catalog(_resolved), do: nil

  defp normalize_catalog_name(nil), do: nil
  defp normalize_catalog_name(catalog) when is_atom(catalog), do: Atom.to_string(catalog)
  defp normalize_catalog_name(catalog) when is_binary(catalog), do: catalog
  defp normalize_catalog_name(_catalog), do: nil

  defp catalog_write_concurrency(config) when is_map(config) do
    config
    |> Map.get(:write_concurrency, default_catalog_write_concurrency(Map.get(config, :type)))
    |> normalize_catalog_write_concurrency()
  end

  defp catalog_write_concurrency(config) when is_list(config) do
    config
    |> Keyword.get(
      :write_concurrency,
      default_catalog_write_concurrency(Keyword.get(config, :type))
    )
    |> normalize_catalog_write_concurrency()
  end

  defp catalog_write_concurrency(_config), do: 1

  defp default_catalog_write_concurrency(:ducklake), do: :unlimited
  defp default_catalog_write_concurrency("ducklake"), do: :unlimited
  defp default_catalog_write_concurrency(_type), do: 1

  defp normalize_catalog_write_concurrency(:unlimited), do: :unlimited
  defp normalize_catalog_write_concurrency(:single), do: 1
  defp normalize_catalog_write_concurrency(value) when is_integer(value) and value > 0, do: value
  defp normalize_catalog_write_concurrency(_value), do: 1

  defp catalog_policy_scope(%Resolved{} = resolved, catalog, attach_config, secrets) do
    with true <- ducklake_attach?(attach_config),
         {:ok, secret_name} <- attach_meta_secret(attach_config),
         {:ok, secret_config} <- Map.fetch(secrets, secret_name),
         {:ok, scope} <- postgres_metadata_scope(resolved, secret_config) do
      scope
    else
      _ -> {resolved.name, catalog}
    end
  end

  defp ducklake_attach?(config) when is_map(config),
    do: Map.get(config, :type) in [:ducklake, "ducklake"]

  defp ducklake_attach?(config) when is_list(config),
    do: Keyword.get(config, :type) in [:ducklake, "ducklake"]

  defp ducklake_attach?(_config), do: false

  defp attach_meta_secret(config) when is_map(config) do
    case Map.get(config, :meta_secret) do
      nil -> :error
      secret -> {:ok, to_string(secret)}
    end
  end

  defp attach_meta_secret(config) when is_list(config) do
    case Keyword.get(config, :meta_secret) do
      nil -> :error
      secret -> {:ok, to_string(secret)}
    end
  end

  defp attach_meta_secret(_config), do: :error

  defp postgres_metadata_scope(%Resolved{}, secret_config) do
    with true <- postgres_secret?(secret_config),
         {:ok, host} <- secret_value(secret_config, :host),
         {:ok, port} <- secret_value(secret_config, :port) do
      hash =
        {host, port}
        |> :erlang.term_to_binary()
        |> then(&:crypto.hash(:sha256, &1))
        |> Base.encode16(case: :lower)

      {:ok, {:ducklake_postgres_metadata, hash}}
    else
      _ -> :error
    end
  end

  defp postgres_secret?(config) when is_map(config),
    do: Map.get(config, :type) in [:postgres, "postgres"]

  defp postgres_secret?(config) when is_list(config),
    do: Keyword.get(config, :type) in [:postgres, "postgres"]

  defp postgres_secret?(_config), do: false

  defp secret_value(config, key) when is_map(config) do
    case Map.fetch(config, key) do
      {:ok, value} -> {:ok, value}
      :error -> Map.fetch(config, Atom.to_string(key))
    end
  end

  defp secret_value(config, key) when is_list(config) do
    case Keyword.fetch(config, key) do
      {:ok, value} -> {:ok, value}
      :error -> :error
    end
  end

  defp secret_value(_config, _key), do: :error

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

    case run_query(conn, statement, params) do
      {:ok, result_ref} ->
        case fetch_rows(conn, result_ref) do
          {:ok, rows, columns} ->
            {:ok,
             %Result{
               kind: kind,
               command: IO.iodata_to_binary(statement),
               rows_affected: nil,
               rows: rows,
               columns: columns,
               metadata: %{}
             }}

          {:error, reason} ->
            {:error, normalize_error(kind, conn.connection, reason)}
        end

      {:error, reason} ->
        {:error, normalize_error(kind, conn.connection, reason)}
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

  def introspection_query(:list_relations, %RelationRef{} = ref, _opts) do
    {:ok,
     [
       relation_introspection_base(ref),
       " ORDER BY table_catalog, table_schema, table_name"
     ]}
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
       " AND ",
       relation_schema_filter(ref),
       relation_catalog_filter(ref),
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
        _ = safe_release(conn, result_ref)
        :ok

      {:error, reason} ->
        {:error, normalize_error(:ping, conn.connection, reason)}
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
  @spec row_count(Conn.t(), RelationRef.t(), opts()) ::
          {:ok, non_neg_integer()} | {:error, Error.t()}
  def row_count(%Conn{} = conn, %RelationRef{} = ref, _opts) do
    sql = ["SELECT count(*) AS row_count FROM ", qualified_relation_ref(ref)]

    with {:ok, result} <- query(conn, sql, []) do
      count =
        result.rows
        |> List.first(%{})
        |> Map.get("row_count", 0)
        |> normalize_integer()

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
      :ok ->
        run_transaction(conn, fun)

      {:error, %Error{} = error} ->
        {:error, error}
    end
  end

  @impl true
  @spec materialize(Conn.t(), WritePlan.t(), opts()) :: {:ok, Result.t()} | {:error, Error.t()}
  def materialize(%Conn{} = conn, %WritePlan{} = plan, opts) do
    rows = appender_rows(plan, opts)

    cond do
      plan.materialization == :table and rows != [] and is_binary(plan.target.catalog) ->
        {:error,
         %Error{
           type: :unsupported_capability,
           message: "DuckDB appender materialization does not support catalog-qualified targets",
           retryable?: false,
           operation: :materialize,
           connection: conn.connection,
           details: %{
             catalog: plan.target.catalog,
             schema: plan.target.schema,
             name: plan.target.name
           }
         }}

      plan.materialization == :table and rows != [] ->
        appender_materialize(conn, plan, rows)

      true ->
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

      {:error, %Error{} = error} ->
        {:error, materialize_error(error, conn)}
    end
  end

  defp run_plan_materialization(%Conn{} = conn, %WritePlan{} = plan, opts) do
    run_materialization_statements(conn, plan, opts)
  end

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

  defp appender_materialize(%Conn{} = conn, %WritePlan{} = plan, rows) do
    transaction(
      conn,
      fn tx_conn ->
        with {:ok, statements} <- materialization_statements(plan, %Capabilities{}, []),
             {:ok, pre, post} <- split_materialization_statements(plan, statements),
             {:ok, _} <- execute_statements(tx_conn, pre),
             {:ok, _} <- execute(tx_conn, appender_table_statement(plan), []),
             :ok <- append_rows(tx_conn, plan.target, rows),
             {:ok, _} <- execute_statements(tx_conn, post) do
          {:ok,
           %Result{
             kind: :materialize,
             command: "appender",
             rows_affected: length(rows),
             metadata: %{strategy: :appender}
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

  defp appender_table_statement(%WritePlan{} = plan) do
    target = qualified_relation(plan.target)

    empty_plan = %WritePlan{plan | select_sql: ["SELECT * FROM (", plan.select_sql, ") LIMIT 0"]}

    create_table_statement(target, empty_plan)
  end

  defp schema_setup_statements(%WritePlan{target: %Relation{catalog: catalog, schema: schema}})
       when is_binary(catalog) and catalog != "" and is_binary(schema) and
              schema not in ["", "main"] do
    [["CREATE SCHEMA IF NOT EXISTS ", quote_ident(catalog), ".", quote_ident(schema)]]
  end

  defp schema_setup_statements(%WritePlan{target: %Relation{schema: schema}})
       when is_binary(schema) and schema not in ["", "main"] do
    [["CREATE SCHEMA IF NOT EXISTS ", quote_ident(schema)]]
  end

  defp schema_setup_statements(%WritePlan{}), do: []

  defp appender_rows(%WritePlan{} = plan, opts) do
    plan.options
    |> Map.get(:appender_rows, Keyword.get(opts, :appender_rows, []))
    |> List.wrap()
  end

  defp run_query(%Conn{conn_ref: conn_ref, client: client}, statement, params) do
    client.query(conn_ref, IO.iodata_to_binary(statement), params)
  end

  defp fetch_rows(%Conn{} = conn, result_ref) do
    case fetch_columns(conn, result_ref) do
      {:ok, columns} -> fetch_all_rows(conn, result_ref, columns)
      {:error, reason} -> {:error, reason}
    end
  after
    _ = safe_release(conn, result_ref)
  end

  defp fetch_columns(%Conn{} = conn, result_ref) do
    case conn.client.columns(result_ref) do
      {:error, reason} -> {:error, reason}
      cols when is_list(cols) -> {:ok, Enum.map(cols, &to_string/1)}
      _other -> {:ok, []}
    end
  end

  defp fetch_all_rows(%Conn{} = conn, result_ref, columns) do
    case conn.client.fetch_all(result_ref) do
      rows when is_list(rows) -> {:ok, Enum.map(rows, &normalize_row(&1, columns)), columns}
      other -> {:error, other}
    end
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

  defp tx_begin(%Conn{conn_ref: conn_ref, client: client} = conn) do
    case client.begin_transaction(conn_ref) do
      :ok ->
        :ok

      {:error, reason} ->
        {:error,
         normalize_error(:transaction, conn.connection, reason)
         |> transaction_stage_error(:begin)}
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

  defp tx_rollback(%Conn{conn_ref: conn_ref, client: client}) do
    client.rollback(conn_ref)
  rescue
    error -> {:error, error}
  end

  defp rollback_for_pool_reset(%Conn{} = conn) do
    case tx_rollback(conn) do
      :ok -> :ok
      {:error, reason} -> handle_pool_reset_rollback_error(conn, reason)
    end
  end

  defp handle_pool_reset_rollback_error(%Conn{} = conn, reason) do
    if no_active_transaction?(reason) do
      :ok
    else
      {:error, normalize_error(:reset_session, conn.connection, reason)}
    end
  end

  defp restore_use_catalog(%Conn{} = conn, %Resolved{} = resolved, opts) do
    use_catalog = duckdb_use_catalog(resolved)

    cond do
      is_nil(use_catalog) ->
        :ok

      catalog_required?(use_catalog, Keyword.get(opts, :required_catalogs)) ->
        case execute(conn, ["USE ", quote_ident(use_catalog)], []) do
          {:ok, _result} -> :ok
          {:error, %Error{} = error} -> {:error, %Error{error | operation: :reset_session}}
        end

      true ->
        :ok
    end
  end

  defp catalog_required?(_catalog, nil), do: true
  defp catalog_required?(_catalog, :all), do: true

  defp catalog_required?(catalog, catalogs) when is_list(catalogs) do
    catalog in Enum.map(catalogs, &to_string/1)
  end

  defp catalog_required?(_catalog, _catalogs), do: false

  defp no_active_transaction?(reason) when is_binary(reason) do
    reason = String.downcase(reason)

    String.contains?(reason, "no active transaction") or
      String.contains?(reason, "no transaction") or
      String.contains?(reason, "not in a transaction")
  end

  defp no_active_transaction?({:error, reason}), do: no_active_transaction?(reason)
  defp no_active_transaction?(_reason), do: false

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
          normalize_error(:transaction, conn.connection, reason)
          |> transaction_stage_error(:body)

        finalize_transaction_failure(conn, error)

      other ->
        error =
          normalize_error(:transaction, conn.connection, {:invalid_transaction_result, other})
          |> transaction_stage_error(:body)

        finalize_transaction_failure(conn, error)
    end
  rescue
    error ->
      raised =
        %Error{
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

  defp transaction_stage_error(%Error{} = error, stage) do
    details = Map.put(error.details || %{}, :transaction_stage, stage)
    %Error{error | details: details}
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

  defp materialization_statement_params(_plan, _statement, _params, index, schema_setup_count)
       when index < schema_setup_count,
       do: []

  defp materialization_statement_params(plan, statement, params, _index, _schema_setup_count),
    do: statement_params(plan, statement, params)

  defp normalize_window_column(options) do
    options
    |> Map.fetch!(:window_column)
    |> to_string()
  end

  defp open_appender(%Conn{conn_ref: conn_ref, client: client}, %Relation{
         name: name,
         schema: schema
       }) do
    client.appender(conn_ref, name, schema)
  end

  defp append_rows(%Conn{} = conn, %Relation{} = target, rows) do
    with {:ok, appender} <- open_appender(conn, target) do
      result =
        with :ok <- conn.client.appender_add_rows(appender, rows),
             do: conn.client.appender_flush(appender)

      case {result, close_appender(conn, appender)} do
        {:ok, :ok} -> :ok
        {{:error, reason}, _close_result} -> {:error, reason}
        {:ok, {:error, reason}} -> {:error, reason}
      end
    end
  end

  defp close_appender(%Conn{} = conn, appender) do
    case conn.client.appender_close(appender) do
      :ok ->
        :ok

      {:error, reason} ->
        _ = safe_release(conn, appender)
        {:error, reason}
    end
  rescue
    _ ->
      _ = safe_release(conn, appender)
      {:error, :appender_close_failed}
  end

  defp relation_introspection_base(%RelationRef{} = ref) do
    [
      "SELECT table_catalog, table_schema, table_name, table_type FROM information_schema.tables",
      " WHERE ",
      relation_schema_filter(ref),
      relation_catalog_filter(ref)
    ]
  end

  defp relation_schema_filter(%RelationRef{catalog: catalog, schema: nil})
       when is_binary(catalog) do
    raise ArgumentError,
          "catalog-qualified relations require schema; got catalog #{inspect(catalog)} without schema"
  end

  defp relation_schema_filter(%RelationRef{schema: schema}) do
    ["table_schema = ", quote_literal(schema || "main")]
  end

  defp relation_catalog_filter(%RelationRef{catalog: catalog}) when is_binary(catalog) do
    [" AND table_catalog = ", quote_literal(catalog)]
  end

  defp relation_catalog_filter(%RelationRef{}), do: []

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

  defp qualified_relation(%Relation{catalog: nil, schema: nil, name: name}), do: quote_ident(name)

  defp qualified_relation(%Relation{catalog: nil, schema: schema, name: name}),
    do: [quote_ident(schema), ".", quote_ident(name)]

  defp qualified_relation(%Relation{catalog: catalog, schema: nil, name: name})
       when is_binary(catalog) do
    raise ArgumentError,
          "catalog-qualified relations require schema; got catalog #{inspect(catalog)} and name #{inspect(name)} without schema"
  end

  defp qualified_relation(%Relation{catalog: catalog, schema: schema, name: name}),
    do: [quote_ident(catalog), ".", quote_ident(schema), ".", quote_ident(name)]

  defp qualified_relation_ref(%RelationRef{catalog: nil, schema: nil, name: name}) do
    quote_ident(name)
  end

  defp qualified_relation_ref(%RelationRef{catalog: nil, schema: schema, name: name}) do
    [quote_ident(schema), ".", quote_ident(name)]
  end

  defp qualified_relation_ref(%RelationRef{catalog: catalog, schema: nil, name: name})
       when is_binary(catalog) do
    raise ArgumentError,
          "catalog-qualified relations require schema; got catalog #{inspect(catalog)} and name #{inspect(name)} without schema"
  end

  defp qualified_relation_ref(%RelationRef{catalog: catalog, schema: schema, name: name}) do
    [quote_ident(catalog), ".", quote_ident(schema), ".", quote_ident(name)]
  end

  defp clamp_sample_limit(limit) when is_integer(limit) and limit >= 0 and limit <= 20, do: limit
  defp clamp_sample_limit(limit) when is_integer(limit) and limit > 20, do: 20
  defp clamp_sample_limit(_limit), do: 20

  defp quote_ident(identifier), do: ["\"", String.replace(identifier, "\"", "\"\""), "\""]
  defp quote_literal(value), do: ["'", String.replace(value, "'", "''"), "'"]

  defp safe_release(%Conn{client: client}, resource), do: safe_release(client, resource)

  defp safe_release(client, resource) do
    _ = client.release(resource)
    :ok
  rescue
    _ -> :ok
  end

  defp validate_production_storage(%Resolved{config: config} = resolved) do
    config = config || %{}

    if Map.get(config, @production_key, false) do
      with :ok <- validate_storage_mode(config, resolved) do
        if local_file_storage?(config) do
          config
          |> open_database()
          |> validate_local_file_database(resolved)
        else
          :ok
        end
      end
    else
      :ok
    end
  end

  defp open_database(%{open: %{database: database}}), do: database
  defp open_database(%{open: open}) when is_list(open), do: Keyword.get(open, :database)
  defp open_database(config), do: Map.get(config, :database)

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

  defp local_file_storage?(config) do
    Map.get(config, @storage_key, @local_file_storage) == @local_file_storage
  end

  defp validate_local_file_database(nil, resolved) do
    production_storage_error(
      resolved,
      :missing_database,
      "production DuckDB local-file storage requires an absolute :database path"
    )
  end

  defp validate_local_file_database(":memory:", resolved) do
    production_storage_error(
      resolved,
      :memory_database,
      "production DuckDB local-file storage cannot use :memory:"
    )
  end

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

  defp validate_local_file_database(_database, resolved) do
    production_storage_error(
      resolved,
      :invalid_database,
      "production DuckDB local-file storage requires an absolute :database path"
    )
  end

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
    path = Path.join(directory, ".favn_duckdb_write_test_#{System.unique_integer([:positive])}")

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
    candidate = Keyword.get(opts, :duckdb_client, Client.default())

    if is_atom(candidate), do: candidate, else: Client.default()
  end

  defp application_vsn(app) do
    case Application.spec(app, :vsn) do
      nil -> nil
      vsn -> List.to_string(vsn)
    end
  end

  defp materialize_error(%Error{} = error, %Conn{} = conn) do
    error
    |> Map.put(:operation, :materialize)
    |> Map.put(:connection, error.connection || conn.connection)
  end

  defp normalize_error(operation, connection, reason) do
    ErrorMapper.normalize(operation, connection, reason)
  end
end
