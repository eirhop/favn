defmodule Favn.SQLClientTest do
  use ExUnit.Case, async: false

  alias Favn.Connection.Definition
  alias Favn.Connection.Resolved
  alias Favn.RelationRef
  alias Favn.SQL.Result
  alias Favn.SQL.WritePlan

  defmodule TestConnection do
    @behaviour Favn.Connection

    @impl true
    def definition do
      %Definition{
        name: :test_sql,
        adapter: Favn.SQLClientTest.Adapter,
        config_schema: [
          %{key: :database, required: true, type: :string}
        ]
      }
    end
  end

  defmodule NoTransactionConnection do
    @behaviour Favn.Connection

    @impl true
    def definition do
      %Definition{
        name: :no_tx_sql,
        adapter: Favn.SQLClientTest.NoTransactionAdapter,
        config_schema: [
          %{key: :database, required: true, type: :string}
        ]
      }
    end
  end

  defmodule Adapter do
    @behaviour Favn.SQL.Adapter

    @impl true
    def connect(%Resolved{}, opts) do
      reject_internal_registry_opt!(opts)
      conn = {:conn, make_ref()}
      notify({:connect_opts, opts})
      notify({:connect, conn})
      {:ok, conn}
    end

    @impl true
    def disconnect(conn, _opts) do
      notify({:disconnect, conn})
      :ok
    end

    @impl true
    def capabilities(%Resolved{}, opts) do
      reject_internal_registry_opt!(opts)
      {:ok, %Favn.SQL.Capabilities{transactions: :supported}}
    end

    @impl true
    def execute(conn, statement, _opts) do
      notify({:execute, conn, IO.iodata_to_binary(statement)})
      {:ok, %Result{kind: :execute, rows_affected: 1}}
    end

    @impl true
    def query(conn, statement, _opts) do
      notify({:query, conn, IO.iodata_to_binary(statement)})
      {:ok, %Result{kind: :query, command: IO.iodata_to_binary(statement)}}
    end

    @impl true
    def introspection_query(_kind, _payload, _opts), do: {:ok, "SELECT 1"}

    @impl true
    def materialization_statements(%WritePlan{}, %Favn.SQL.Capabilities{}, _opts), do: {:ok, []}

    @impl true
    def materialize(_conn, %WritePlan{}, _opts), do: {:ok, %Result{kind: :materialize}}

    @impl true
    def relation(_conn, %RelationRef{name: name, schema: schema}, _opts),
      do: {:ok, %Favn.SQL.Relation{name: name, schema: schema || "main", type: :table}}

    @impl true
    def columns(_conn, %RelationRef{}, _opts), do: {:ok, [%Favn.SQL.Column{name: "id"}]}

    @impl true
    def transaction(conn, fun, _opts), do: fun.(conn)

    defp reject_internal_registry_opt!(opts) do
      if Keyword.has_key?(opts, :registry_name) do
        raise "adapter should not receive internal :registry_name routing opt"
      end
    end

    defp notify(message) do
      if pid = Application.get_env(:favn, :sql_client_test_pid) do
        send(pid, message)
      end

      :ok
    end
  end

  defmodule NoTransactionAdapter do
    @behaviour Favn.SQL.Adapter

    @impl true
    def connect(%Resolved{}, _opts), do: {:ok, :conn}

    @impl true
    def disconnect(:conn, _opts), do: :ok

    @impl true
    def capabilities(%Resolved{}, _opts),
      do: {:ok, %Favn.SQL.Capabilities{transactions: :unsupported}}

    @impl true
    def execute(:conn, _statement, _opts), do: {:ok, %Result{kind: :execute, rows_affected: 1}}

    @impl true
    def query(:conn, statement, _opts),
      do: {:ok, %Result{kind: :query, command: IO.iodata_to_binary(statement)}}

    @impl true
    def introspection_query(_kind, _payload, _opts), do: {:ok, "SELECT 1"}

    @impl true
    def materialization_statements(%WritePlan{}, %Favn.SQL.Capabilities{}, _opts), do: {:ok, []}
  end

  setup do
    previous_modules = Application.get_env(:favn, :connection_modules)
    previous_connections = Application.get_env(:favn, :connections)
    previous_pid = Application.get_env(:favn, :sql_client_test_pid)

    Application.put_env(:favn, :sql_client_test_pid, self())
    Application.put_env(:favn, :connection_modules, [TestConnection, NoTransactionConnection])

    Application.put_env(:favn, :connections,
      test_sql: [database: ":memory:"],
      no_tx_sql: [database: ":memory:"]
    )

    on_exit(fn ->
      restore_env(:connection_modules, previous_modules)
      restore_env(:connections, previous_connections)
      restore_env(:sql_client_test_pid, previous_pid)
    end)

    :ok
  end

  test "connects and runs SQL operations without runner runtime" do
    assert {:ok, session} = Favn.SQLClient.connect(:test_sql)
    assert {:ok, %Result{kind: :query}} = Favn.SQLClient.query(session, "select 1")

    assert {:ok, %Result{kind: :execute}} =
             Favn.SQLClient.execute(session, "create table t(id int)")

    assert {:ok, %Favn.SQL.Capabilities{transactions: :supported}} =
             Favn.SQLClient.capabilities(session)

    assert {:ok, %Favn.SQL.Relation{name: "orders"}} =
             Favn.SQLClient.relation(session, name: :orders)

    assert {:ok, [%Favn.SQL.Column{name: "id"}]} =
             Favn.SQLClient.columns(session, %{name: "orders"})

    assert :ok = Favn.SQLClient.disconnect(session)
  end

  test "with_connection wraps connect and disconnect" do
    assert {:ok, :done} =
             Favn.SQLClient.with_connection(:test_sql, [], fn session ->
               assert {:ok, %Result{kind: :query}} = Favn.SQLClient.query(session, "select 1")
               {:ok, :done}
             end)
  end

  test "with_connection reuses one adapter connection for multiple operations" do
    assert :ok =
             Favn.SQLClient.with_connection(:test_sql, [], fn session ->
               assert {:ok, %Result{kind: :execute}} =
                        Favn.SQLClient.execute(session, "create schema if not exists raw")

               assert {:ok, %Result{kind: :query}} =
                        Favn.SQLClient.query(session, "describe raw.events")

               assert {:ok, %Result{kind: :execute}} =
                        Favn.SQLClient.execute(session, "insert into raw.events select 1")

               :ok
             end)

    assert_received {:connect, conn}
    assert_received {:execute, ^conn, "create schema if not exists raw"}
    assert_received {:query, ^conn, "describe raw.events"}
    assert_received {:execute, ^conn, "insert into raw.events select 1"}
    assert_received {:disconnect, ^conn}
    refute_received {:connect, _other_conn}
  end

  test "with_connection session retains required catalog scope across operations" do
    assert :ok =
             Favn.SQLClient.with_connection(:test_sql, [required_catalogs: ["raw"]], fn session ->
               assert session.required_catalogs == ["raw"]
               assert {:ok, %Result{kind: :execute}} = Favn.SQLClient.execute(session, "insert 1")
               assert session.required_catalogs == ["raw"]
               assert {:ok, %Result{kind: :query}} = Favn.SQLClient.query(session, "select 1")
               assert session.required_catalogs == ["raw"]
               :ok
             end)

    assert_received {:connect_opts, [required_catalogs: ["raw"]]}
  end

  test "with_required_catalogs scopes SQLClient connects in spawned tasks" do
    relation =
      RelationRef.new!(connection: :test_sql, catalog: :raw, schema: :landing, name: :events)

    task =
      Task.async(fn ->
        Favn.SQLClient.with_required_catalogs(relation, fn ->
          Favn.SQLClient.with_connection(:test_sql, [], fn session ->
            Favn.SQLClient.query(session, "select 1")
          end)
        end)
      end)

    assert {:ok, %Result{kind: :query}} = Task.await(task)
    assert_received {:connect_opts, [required_catalogs: ["raw"]]}
  end

  test "with_required_catalogs does not rescue callback argument errors" do
    relation =
      RelationRef.new!(connection: :test_sql, catalog: :raw, schema: :landing, name: :events)

    assert_raise ArgumentError, "callback failure", fn ->
      Favn.SQLClient.with_required_catalogs(relation, fn ->
        raise ArgumentError, "callback failure"
      end)
    end
  end

  test "with_required_catalogs returns validation errors for invalid public inputs" do
    assert {:error, %ArgumentError{message: connection_message}} =
             Favn.SQLClient.with_required_catalogs("test_sql", [:raw], fn -> :ok end)

    assert connection_message =~ "must be an atom"

    assert {:error, %ArgumentError{message: catalogs_message}} =
             Favn.SQLClient.with_required_catalogs(:test_sql, :raw, fn -> :ok end)

    assert catalogs_message =~ "must be a list"

    assert {:error, %ArgumentError{message: catalog_value_message}} =
             Favn.SQLClient.with_required_catalogs(:test_sql, [123], fn -> :ok end)

    assert catalog_value_message =~ "atoms or strings"

    assert {:error, %ArgumentError{message: callback_message}} =
             Favn.SQLClient.with_required_catalogs(:test_sql, [:raw], :not_a_fun)

    assert callback_message =~ "0-arity callback"
  end

  test "transaction delegates through adapter callback" do
    assert {:ok, session} = Favn.SQLClient.connect(:test_sql)

    assert {:ok, :inside} =
             Favn.SQLClient.transaction(session, fn tx_session ->
               assert {:ok, %Result{kind: :query}} = Favn.SQLClient.query(tx_session, "select 1")
               {:ok, :inside}
             end)
  end

  test "transaction returns unsupported capability when adapter has no transaction callback" do
    assert {:ok, session} = Favn.SQLClient.connect(:no_tx_sql)

    assert {:error, %Favn.SQL.Error{type: :unsupported_capability, operation: :transaction}} =
             Favn.SQLClient.transaction(session, fn _tx_session ->
               {:ok, :inside}
             end)
  end

  test "returns invalid_config for unknown connection" do
    assert {:error, %Favn.SQL.Error{type: :invalid_config, operation: :connect}} =
             Favn.SQLClient.connect(:missing)
  end

  test "rejects internal registry routing opts on the public facade" do
    assert {:error, %ArgumentError{message: message}} =
             Favn.SQLClient.connect(:test_sql, registry_name: FavnRunner.ConnectionRegistry)

    assert message =~ ":registry_name"
  end

  test "returns clearer validation errors for invalid public inputs" do
    assert {:error, %ArgumentError{message: connection_message}} =
             Favn.SQLClient.connect("test_sql")

    assert connection_message =~ "must be an atom"

    assert {:error, %ArgumentError{message: opts_message}} =
             Favn.SQLClient.connect(:test_sql, %{})

    assert opts_message =~ "keyword list"

    assert {:error, %ArgumentError{message: callback_message}} =
             Favn.SQLClient.with_connection(:test_sql, [], :not_a_fun)

    assert callback_message =~ "1-arity callback"
  end

  defp restore_env(key, nil), do: Application.delete_env(:favn, key)
  defp restore_env(key, value), do: Application.put_env(:favn, key, value)
end
