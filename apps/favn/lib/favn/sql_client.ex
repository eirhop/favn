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

  - `:connection_modules` with one or more modules implementing
    `Favn.Connection`
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
  - `with_connection/3`: run a callback with auto connect/disconnect handling.
  """

  alias Favn.RelationRef
  alias Favn.SQL.Client
  alias Favn.SQL.Session

  @type connection_name :: atom()
  @type opts :: keyword()
  @type session :: Session.t()
  @type operation_result :: {:ok, term()} | {:error, term()}

  @doc """
  Opens a SQL session for a configured named connection.

  Use this when your code needs to run multiple SQL operations and keep one
  reusable session handle.

  `opts` are forwarded to the underlying SQL adapter. Common adapter options
  include timeout-related values.

  Public callers should only pass adapter-facing options here. Internal runtime
  routing controls such as `:registry_name` are not accepted by this facade.

  ## Example

      {:ok, session} = Favn.SQLClient.connect(:warehouse)
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
  Closes a SQL session.

  Use this when you explicitly manage session lifecycle with `connect/2`.

  This function is safe to call in cleanup paths and returns `:ok` even when the
  adapter disconnect path raises.
  """
  @spec disconnect(session()) :: :ok
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
end
