defmodule FavnSQLRuntime.SQLClientBootstrapTest do
  use ExUnit.Case, async: false

  alias Favn.Connection.Registry
  alias Favn.Connection.Resolved
  alias Favn.SQL.Capabilities
  alias Favn.SQL.Client
  alias Favn.SQL.Error
  alias Favn.SQL.Result

  @events_table :favn_sql_client_bootstrap_events

  defmodule AdapterWithBootstrap do
    @behaviour Favn.SQL.Adapter

    @events_table :favn_sql_client_bootstrap_events

    @impl true
    def connect(%Resolved{name: name}, _opts) do
      conn = {:conn, make_ref()}
      record({:connect, name, conn})
      {:ok, conn}
    end

    @impl true
    def bootstrap(conn, %Resolved{name: name}, _opts) do
      record({:bootstrap, name, conn})

      case Application.get_env(:favn, :sql_bootstrap_mode, :ok) do
        :ok ->
          :ok

        :error ->
          {:error,
           %Error{
             type: :connection_error,
             message: "bootstrap failed",
             operation: :bootstrap,
             connection: name
           }}

        :raise ->
          raise "bootstrap boom"

        :exit ->
          exit(:bootstrap_boom)
      end
    end

    @impl true
    def disconnect(conn, _opts) do
      record({:disconnect, conn})
      :ok
    end

    @impl true
    def capabilities(%Resolved{name: name}, _opts) do
      record({:capabilities, name})
      {:ok, %Capabilities{}}
    end

    @impl true
    def execute(conn, statement, _opts) do
      record({:execute, conn, IO.iodata_to_binary(statement)})
      {:ok, %Result{kind: :execute, command: IO.iodata_to_binary(statement)}}
    end

    @impl true
    def query(conn, statement, _opts) do
      record({:query, conn, IO.iodata_to_binary(statement)})
      {:ok, %Result{kind: :query, command: IO.iodata_to_binary(statement), rows: []}}
    end

    @impl true
    def introspection_query(_kind, _payload, _opts), do: {:ok, "SELECT 1"}

    @impl true
    def materialization_statements(_plan, _capabilities, _opts), do: {:ok, []}

    defp record(event) do
      if :ets.whereis(@events_table) != :undefined do
        :ets.insert(@events_table, {System.unique_integer([:positive, :monotonic]), event})
      end
    end
  end

  defmodule AdapterWithoutBootstrap do
    @behaviour Favn.SQL.Adapter

    @events_table :favn_sql_client_bootstrap_events

    @impl true
    def connect(%Resolved{name: name}, _opts) do
      conn = {:conn, make_ref()}
      record({:connect, name, conn})
      {:ok, conn}
    end

    @impl true
    def disconnect(conn, _opts) do
      record({:disconnect, conn})
      :ok
    end

    @impl true
    def capabilities(%Resolved{name: name}, _opts) do
      record({:capabilities, name})
      {:ok, %Capabilities{}}
    end

    @impl true
    def execute(_conn, statement, _opts),
      do: {:ok, %Result{kind: :execute, command: IO.iodata_to_binary(statement)}}

    @impl true
    def query(_conn, statement, _opts),
      do: {:ok, %Result{kind: :query, command: IO.iodata_to_binary(statement), rows: []}}

    @impl true
    def introspection_query(_kind, _payload, _opts), do: {:ok, "SELECT 1"}

    @impl true
    def materialization_statements(_plan, _capabilities, _opts), do: {:ok, []}

    defp record(event) do
      if :ets.whereis(@events_table) != :undefined do
        :ets.insert(@events_table, {System.unique_integer([:positive, :monotonic]), event})
      end
    end
  end

  setup do
    if :ets.whereis(@events_table) != :undefined do
      :ets.delete(@events_table)
    end

    :ets.new(@events_table, [:named_table, :ordered_set, :public])
    Application.put_env(:favn, :sql_bootstrap_mode, :ok)

    registry_name = Module.concat(__MODULE__, "Registry#{System.unique_integer([:positive])}")

    on_exit(fn ->
      Application.delete_env(:favn, :sql_bootstrap_mode)

      if :ets.whereis(@events_table) != :undefined do
        :ets.delete(@events_table)
      end
    end)

    {:ok, registry_name: registry_name}
  end

  test "runs adapter bootstrap before returning a usable session", %{registry_name: registry_name} do
    start_registry(registry_name, AdapterWithBootstrap)

    assert {:ok, session} = Client.connect(:warehouse, registry_name: registry_name)
    assert {:ok, %Result{}} = Client.query(session, "SELECT 1", [])
    Client.disconnect(session)

    assert [
             {:connect, :warehouse, conn},
             {:bootstrap, :warehouse, conn},
             {:capabilities, :warehouse},
             {:query, conn, "SELECT 1"},
             {:disconnect, conn}
           ] = events()
  end

  test "disconnects and releases the session when bootstrap fails", %{registry_name: registry_name} do
    start_registry(registry_name, AdapterWithBootstrap)
    Application.put_env(:favn, :sql_bootstrap_mode, :error)

    assert {:error, %Error{operation: :bootstrap, connection: :warehouse}} =
             Client.connect(:warehouse, registry_name: registry_name)

    assert [
             {:connect, :warehouse, conn},
             {:bootstrap, :warehouse, conn},
             {:disconnect, conn}
           ] = events()
  end

  test "disconnects when bootstrap raises", %{registry_name: registry_name} do
    start_registry(registry_name, AdapterWithBootstrap)
    Application.put_env(:favn, :sql_bootstrap_mode, :raise)

    assert {:error, %Error{operation: :connect}} =
             Client.connect(:warehouse, registry_name: registry_name)

    assert [
             {:connect, :warehouse, conn},
             {:bootstrap, :warehouse, conn},
             {:disconnect, conn}
           ] = events()
  end

  test "disconnects when bootstrap exits", %{registry_name: registry_name} do
    start_registry(registry_name, AdapterWithBootstrap)
    Application.put_env(:favn, :sql_bootstrap_mode, :exit)

    assert {:error, %Error{operation: :connect}} =
             Client.connect(:warehouse, registry_name: registry_name)

    assert [
             {:connect, :warehouse, conn},
             {:bootstrap, :warehouse, conn},
             {:disconnect, conn}
           ] = events()
  end

  test "adapters without bootstrap callback still connect", %{registry_name: registry_name} do
    start_registry(registry_name, AdapterWithoutBootstrap)

    assert {:ok, session} = Client.connect(:warehouse, registry_name: registry_name)
    Client.disconnect(session)

    assert [
             {:connect, :warehouse, conn},
             {:capabilities, :warehouse},
             {:disconnect, conn}
           ] = events()
  end

  defp start_registry(registry_name, adapter) do
    resolved = %Resolved{
      name: :warehouse,
      adapter: adapter,
      module: __MODULE__,
      config: %{},
      secret_fields: []
    }

    start_supervised!({Registry, name: registry_name, connections: %{warehouse: resolved}})
  end

  defp events do
    @events_table
    |> :ets.tab2list()
    |> Enum.sort_by(&elem(&1, 0))
    |> Enum.map(&elem(&1, 1))
  end
end
