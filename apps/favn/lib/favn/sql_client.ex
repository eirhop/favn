defmodule Favn.SQLClient do
  @moduledoc """
  Public SQL runtime client for running SQL against named `Favn.Connection`
  entries from plain Elixir code.

  Use this module when you want SQL access outside `Favn.SQLAsset` execution,
  for example in tooling, maintenance tasks, one-off inspections, or custom
  application logic that still uses the same Favn connection contracts.

  One important use case is source-system raw landing from an Elixir
  `Favn.Asset`: fetch records with a project-owned source client, then use
  `Favn.SQLClient` to create/update the owned raw relation before downstream SQL
  assets transform it.

  This client resolves connection configuration from your normal `config :favn`
  setup and runs through the same shared SQL runtime contracts that back SQL
  asset execution.

  ## Connection setup reminder

  Before calling `Favn.SQLClient`, configure both:

  - app-scoped discovery with `discovery: [apps: [:my_app], connections: :all]`,
    or explicit `:connection_modules` with modules implementing `Favn.Connection`
  - `:connections` with runtime values for those named connections

  ## Common flow

      {:ok, session} = Favn.SQLClient.connect(:warehouse)
      {:ok, result} = Favn.SQLClient.query(session, "select 1")
      :ok = Favn.SQLClient.disconnect(session)

  ## Raw landing from an asset

      Favn.SQLClient.with_connection(:warehouse, [], fn session ->
        with {:ok, _} <- Favn.SQLClient.execute(session, "create schema if not exists raw"),
             {:ok, _} <- Favn.SQLClient.execute(session, raw_landing_sql()) do
          :ok
        end
      end)

  For source-system assets, declare source IDs and tokens with
  `Favn.Asset.source_config/2`, read them from `ctx.config`, and return only
  redacted or hashed source identity metadata.

  ## Functions and when to use them

  - `connect/2`: open a session for a named connection.
  - `disconnect/1`: close a session and release backend resources.
  - `query/3`: run a read-style SQL statement and return rows/columns.
  - `execute/3`: run a write/command statement and return command metadata.
  - `transaction/3`: run multiple operations inside one backend transaction when
    the adapter supports it.
  - `capabilities/1`: inspect backend capability flags (transactions,
    materializations, etc.).
  - `relation/2`: inspect relation metadata for a table/view reference.
  - `columns/2`: inspect column metadata for a relation reference.
  - `with_required_catalogs/2` and `with_required_catalogs/3`: set a
    process-local default DuckDB/DuckLake catalog scope for nested SQLClient
    calls.
  - `with_connection/3`: run a callback with auto connect/disconnect handling.

  Runner-executed Elixir assets get a process-local default scope from their
  owned relation when opening the same connection. That default does not cross
  process boundaries. If an asset spawns a `Task` that opens SQLClient sessions,
  wrap the task body with `with_required_catalogs/2` or pass
  `required_catalogs: [...]` explicitly.

  DuckDB/DuckLake connections may use runner-local pooling. Disconnecting a
  pooled session can return it to idle storage rather than physically closing it;
  idle sessions keep catalog admission until they are reused or evicted. For
  DuckLake with PostgreSQL metadata, observed deployments can use about three
  PostgreSQL backend connections per concurrent DuckLake writer, so size
  `write_concurrency` with that headroom instead of treating it as a raw
  PostgreSQL connection count.
  """

  alias Favn.RelationRef
  alias Favn.SQL.Client
  alias Favn.SQL.Session

  @type connection_name :: atom()
  @type opts :: keyword()
  @type session :: Session.t()
  @type operation_result :: {:ok, term()} | {:error, term()}
  @type catalog :: atom() | String.t()

  @doc """
  Opens a SQL session for a configured named connection.

  Use this when your code needs to run multiple SQL operations and keep one
  reusable session handle.

  `opts` are forwarded to the underlying SQL adapter. Common adapter options
  include timeout-related values.

  DuckDB-backed connections also accept `required_catalogs: [catalog]` when the
  caller knows which catalog-qualified relations are needed. That lets DuckDB and
  DuckLake bootstrap attach only those catalogs and lets catalog-level admission
  protect bootstrap work before the session is opened. Elixir assets executed by
  the Favn runner inherit their owned relation catalog as a default scope when
  they open that same relation connection without an explicit `:required_catalogs`
  option.

  Public callers should only pass adapter-facing options here. Internal runtime
  routing controls such as `:registry_name` are not accepted by this facade.

  ## Example

      {:ok, session} = Favn.SQLClient.connect(:warehouse)
      {:ok, raw_session} = Favn.SQLClient.connect(:warehouse, required_catalogs: ["raw"])
  """
  @spec connect(connection_name(), opts()) :: {:ok, session()} | {:error, term()}
  def connect(connection, opts \\ [])

  def connect(connection, opts) when is_atom(connection) and is_list(opts) do
    case Keyword.has_key?(opts, :registry_name) do
      true ->
        {:error,
         ArgumentError.exception(
           "Favn.SQLClient.connect/2 does not accept internal option :registry_name"
         )}

      false ->
        Client.connect(connection, opts)
    end
  end

  def connect(connection, _opts) when not is_atom(connection) do
    {:error,
     ArgumentError.exception("SQL connection name must be an atom, got: #{inspect(connection)}")}
  end

  def connect(_connection, opts) do
    {:error,
     ArgumentError.exception("SQL client options must be a keyword list, got: #{inspect(opts)}")}
  end

  @doc """
  Runs a zero-arity callback with a default DuckDB/DuckLake catalog scope.

  This helper is mainly for Elixir assets that spawn child processes. Runner
  defaults are process-local, so a `Task` that opens SQLClient sessions should
  carry the relation scope explicitly:

      Task.async(fn ->
        Favn.SQLClient.with_required_catalogs(ctx.asset.relation, fn ->
          Favn.SQLClient.with_connection(ctx.asset.relation.connection, [], fn session ->
            Favn.SQLClient.execute(session, landing_sql())
          end)
        end)
      end)

  If the relation has no connection or catalog, the callback runs unchanged.
  Invalid relation input returns `{:error, %ArgumentError{}}`.
  """
  @spec with_required_catalogs(RelationRef.input(), (-> result)) ::
          result | {:error, term()}
        when result: var
  def with_required_catalogs(relation_ref, fun) when is_function(fun, 0) do
    case normalize_relation(relation_ref) do
      {:ok, relation} ->
        case relation_scope(relation) do
          {connection, catalogs} ->
            Client.with_default_required_catalogs(connection, catalogs, fun)

          nil ->
            fun.()
        end

      {:error, %ArgumentError{}} = error ->
        error
    end
  end

  @doc """
  Runs a zero-arity callback with default required catalogs for one connection.

  Explicit `required_catalogs: [...]` passed to `connect/2` still wins for that
  call. The default is process-local and scoped only to the callback.
  """
  @spec with_required_catalogs(connection_name(), [catalog()], (-> result)) :: result
        when result: var
  def with_required_catalogs(connection, catalogs, fun)
      when is_atom(connection) and is_list(catalogs) and is_function(fun, 0) do
    with :ok <- validate_catalogs(catalogs) do
      Client.with_default_required_catalogs(connection, catalogs, fun)
    end
  end

  def with_required_catalogs(connection, _catalogs, _fun) when not is_atom(connection) do
    {:error,
     ArgumentError.exception("SQL connection name must be an atom, got: #{inspect(connection)}")}
  end

  def with_required_catalogs(_connection, catalogs, _fun) when not is_list(catalogs) do
    {:error,
     ArgumentError.exception("SQL required catalogs must be a list, got: #{inspect(catalogs)}")}
  end

  def with_required_catalogs(connection, _catalogs, fun) do
    {:error,
     ArgumentError.exception(
       "with_required_catalogs/3 expects a 0-arity callback, got: #{inspect(fun)} for #{inspect(connection)}"
     )}
  end

  @doc """
  Closes a SQL session.

  Use this when you explicitly manage session lifecycle with `connect/2`.

  This function is safe to call from the session owner in cleanup paths and
  returns `:ok` even when the adapter disconnect path raises. Pooled sessions are
  process-affine; calling `disconnect/1` from a non-owner process returns
  `{:error, %Favn.SQL.Error{type: :invalid_checkout_owner}}` and leaves cleanup
  to the owner or pool monitor.
  """
  @spec disconnect(session()) :: :ok | {:error, Favn.SQL.Error.t()}
  defdelegate disconnect(session), to: Client

  @doc """
  Returns capability metadata for the current session backend.

  Use this when logic depends on backend features, such as transaction support
  or materialization behavior.
  """
  @spec capabilities(session()) :: {:ok, Favn.SQL.Capabilities.t()} | {:error, term()}
  defdelegate capabilities(session), to: Client

  @doc """
  Runs a SQL query statement and returns a normalized result.

  Use this for read-style operations where rows/columns are expected.

  `opts` are forwarded to the adapter. Parameters should be passed with
  adapter-appropriate option keys (for example `params: [...]` when supported by
  the selected adapter).

  ## Example

      {:ok, session} = Favn.SQLClient.connect(:warehouse)
      {:ok, result} = Favn.SQLClient.query(session, "select * from orders where id = ?", params: [1])
  """
  @spec query(session(), iodata(), opts()) :: operation_result()
  defdelegate query(session, statement, opts \\ []), to: Client

  @doc """
  Runs a SQL command statement and returns a normalized result.

  Use this for command/write-style operations such as DDL or DML.

  In many backends this differs from `query/3` in intent and result shape,
  especially around row handling and command metadata.
  """
  @spec execute(session(), iodata(), opts()) :: operation_result()
  defdelegate execute(session, statement, opts \\ []), to: Client

  @doc """
  Runs a callback inside one SQL transaction when supported by the adapter.

  Use this when multiple statements should succeed or fail as one unit.

  The callback receives a session bound to the transaction connection handle.
  If the adapter does not expose transaction support, this returns
  `{:error, %Favn.SQL.Error{type: :unsupported_capability}}` instead of silently
  running without transaction guarantees.

  ## Example

      Favn.SQLClient.transaction(session, fn tx_session ->
        with {:ok, _} <- Favn.SQLClient.execute(tx_session, "insert into jobs(id) values (1)"),
             {:ok, _} <- Favn.SQLClient.execute(tx_session, "insert into jobs(id) values (2)") do
          {:ok, :done}
        end
      end)
  """
  @spec transaction(session(), (session() -> operation_result()), opts()) :: operation_result()
  defdelegate transaction(session, fun, opts \\ []), to: Client

  @doc """
  Returns relation metadata for a table/view reference.

  Use this for relation introspection, existence checks, or lightweight schema
  discovery.

  `relation_ref` accepts `%Favn.RelationRef{}`, keyword input, or map input and
  is normalized through `Favn.RelationRef.new!/1`.

  ## Example

      {:ok, relation} = Favn.SQLClient.relation(session, schema: "analytics", name: "orders")
  """
  @spec relation(session(), RelationRef.input()) :: operation_result()
  def relation(session, relation_ref) do
    relation_ref
    |> RelationRef.new!()
    |> then(&Client.relation(session, &1))
  rescue
    error in ArgumentError -> {:error, error}
  end

  @doc """
  Returns column metadata for a relation reference.

  Use this when validating projection compatibility or when building generic
  SQL tooling that depends on column shapes.

  `relation_ref` input is normalized the same way as `relation/2`.

  ## Example

      {:ok, columns} = Favn.SQLClient.columns(session, %{schema: "analytics", name: "orders"})
  """
  @spec columns(session(), RelationRef.input()) :: operation_result()
  def columns(session, relation_ref) do
    relation_ref
    |> RelationRef.new!()
    |> then(&Client.columns(session, &1))
  rescue
    error in ArgumentError -> {:error, error}
  end

  @doc """
  Runs a callback with automatic connect/disconnect handling.

  Use this for one scoped operation where explicit lifecycle management would be
  repetitive.

  The callback return value is returned directly. The session is always
  disconnected in an `after` block.

  ## Example

      Favn.SQLClient.with_connection(:warehouse, [], fn session ->
        Favn.SQLClient.query(session, "select 1")
      end)
  """
  @spec with_connection(connection_name(), opts(), (session() -> operation_result())) ::
          operation_result()
  def with_connection(connection, opts \\ [], fun)

  def with_connection(connection, opts, fun) when is_function(fun, 1) and is_list(opts) do
    case connect(connection, opts) do
      {:ok, session} ->
        try do
          fun.(session)
        after
          disconnect(session)
        end

      {:error, _reason} = error ->
        error
    end
  end

  def with_connection(connection, _opts, _fun) when not is_atom(connection) do
    {:error,
     ArgumentError.exception("SQL connection name must be an atom, got: #{inspect(connection)}")}
  end

  def with_connection(_connection, opts, _fun) when not is_list(opts) do
    {:error,
     ArgumentError.exception("SQL client options must be a keyword list, got: #{inspect(opts)}")}
  end

  def with_connection(connection, _opts, fun) do
    {:error,
     ArgumentError.exception(
       "with_connection/3 expects a 1-arity callback, got: #{inspect(fun)} for #{inspect(connection)}"
     )}
  end

  defp normalize_relation(relation_ref) do
    {:ok, RelationRef.new!(relation_ref)}
  rescue
    error in ArgumentError -> {:error, error}
  end

  defp validate_catalogs(catalogs) do
    if Enum.all?(catalogs, &(is_atom(&1) or is_binary(&1))) do
      :ok
    else
      {:error,
       ArgumentError.exception(
         "SQL required catalogs must contain only atoms or strings, got: #{inspect(catalogs)}"
       )}
    end
  end

  defp relation_scope(%RelationRef{connection: connection, catalog: catalog})
       when is_atom(connection) do
    case catalog_name(catalog) do
      nil -> nil
      catalog -> {connection, [catalog]}
    end
  end

  defp relation_scope(%RelationRef{}), do: nil

  defp catalog_name(catalog) when is_binary(catalog) and catalog != "", do: catalog

  defp catalog_name(catalog) when is_atom(catalog) and not is_nil(catalog),
    do: Atom.to_string(catalog)

  defp catalog_name(_catalog), do: nil
end
