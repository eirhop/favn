defmodule FavnRunner.SQLRuntimeBridgeTest do
  use ExUnit.Case, async: false

  alias Favn.Connection.Registry
  alias Favn.Connection.Resolved
  alias Favn.RelationRef
  alias Favn.SQL.Error
  alias Favn.SQL.Relation
  alias Favn.SQL.RuntimeBridge
  alias Favn.SQL.Session
  alias Favn.SQL.WritePlan

  @events_table :favn_runner_sql_runtime_bridge_events

  defmodule Adapter do
    @behaviour Favn.SQL.Adapter

    @events_table :favn_runner_sql_runtime_bridge_events

    @impl true
    def connect(%Resolved{}, _opts) do
      record({:connect})
      {:ok, :conn}
    end

    @impl true
    def disconnect(:conn, _opts) do
      record({:disconnect})
      :ok
    end

    @impl true
    def capabilities(%Resolved{}, _opts),
      do: {:ok, %Favn.SQL.Capabilities{transactions: :supported}}

    @impl true
    def execute(:conn, _statement, _opts), do: {:ok, %Favn.SQL.Result{kind: :execute}}

    @impl true
    def query(:conn, statement, _opts) do
      {:ok, %Favn.SQL.Result{kind: :query, command: IO.iodata_to_binary(statement), rows: []}}
    end

    @impl true
    def introspection_query(_kind, _payload, _opts), do: {:ok, "SELECT 1"}

    @impl true
    def materialization_statements(%WritePlan{}, %Favn.SQL.Capabilities{}, _opts), do: {:ok, []}

    @impl true
    def materialize(:conn, %WritePlan{}, _opts), do: {:ok, %Favn.SQL.Result{kind: :materialize}}

    @impl true
    def relation(:conn, %RelationRef{} = ref, _opts),
      do: {:ok, %Relation{schema: ref.schema || "main", name: ref.name, type: :table}}

    @impl true
    def columns(:conn, _ref, _opts), do: {:ok, [%Favn.SQL.Column{name: "id"}]}

    defp record(event) do
      if :ets.whereis(@events_table) != :undefined do
        :ets.insert(@events_table, {System.unique_integer([:positive]), event})
      end

      :ok
    end
  end

  defmodule DisconnectRaisingAdapter do
    @behaviour Favn.SQL.Adapter

    @impl true
    def connect(%Resolved{}, _opts), do: {:ok, :conn}

    @impl true
    def disconnect(:conn, _opts), do: raise("disconnect failed")

    @impl true
    def capabilities(%Resolved{}, _opts),
      do: {:ok, %Favn.SQL.Capabilities{transactions: :supported}}

    @impl true
    def execute(:conn, _statement, _opts), do: {:ok, %Favn.SQL.Result{kind: :execute}}

    @impl true
    def query(:conn, _statement, _opts), do: {:ok, %Favn.SQL.Result{kind: :query}}

    @impl true
    def introspection_query(_kind, _payload, _opts), do: {:ok, "SELECT 1"}

    @impl true
    def materialization_statements(%WritePlan{}, %Favn.SQL.Capabilities{}, _opts), do: {:ok, []}
  end

  defmodule RaisingAdapter do
    @behaviour Favn.SQL.Adapter

    @impl true
    def connect(%Resolved{}, _opts), do: {:ok, :conn}

    @impl true
    def disconnect(:conn, _opts), do: :ok

    @impl true
    def capabilities(%Resolved{}, _opts),
      do: {:ok, %Favn.SQL.Capabilities{transactions: :supported}}

    @impl true
    def execute(:conn, _statement, _opts), do: {:ok, %Favn.SQL.Result{kind: :execute}}

    @impl true
    def query(:conn, _statement, _opts), do: raise("boom")

    @impl true
    def introspection_query(_kind, _payload, _opts), do: {:ok, "SELECT 1"}

    @impl true
    def materialization_statements(%WritePlan{}, %Favn.SQL.Capabilities{}, _opts), do: {:ok, []}

    @impl true
    def materialize(:conn, _plan, _opts), do: raise("boom")

    @impl true
    def relation(:conn, _ref, _opts), do: raise("boom")

    @impl true
    def columns(:conn, _ref, _opts), do: raise("boom")
  end

  setup do
    previous = Registry.list(registry_name: FavnRunner.ConnectionRegistry)

    if :ets.whereis(@events_table) != :undefined do
      :ets.delete(@events_table)
    end

    :ets.new(@events_table, [:named_table, :ordered_set, :public])

    on_exit(fn ->
      restored = Map.new(previous, fn resolved -> {resolved.name, resolved} end)
      Registry.reload(restored, registry_name: FavnRunner.ConnectionRegistry)

      if :ets.whereis(@events_table) != :undefined do
        :ets.delete(@events_table)
      end
    end)

    :ok
  end

  test "connects, queries, materializes, and introspects through runtime bridge" do
    put_resolved(:runtime_sql, Adapter)

    assert {:ok, %Session{} = session} = RuntimeBridge.connect(:runtime_sql)
    assert {:ok, %Favn.SQL.Result{kind: :query}} = RuntimeBridge.query(session, "SELECT 1", [])

    write_plan =
      %WritePlan{
        materialization: :table,
        target: %Relation{schema: "main", name: "orders", type: :table},
        select_sql: "SELECT 1 AS id"
      }

    assert {:ok, %Favn.SQL.Result{kind: :materialize}} =
             RuntimeBridge.materialize(session, write_plan, [])

    assert {:ok, %Relation{name: "orders"}} =
             RuntimeBridge.get_relation(session, %RelationRef{schema: "main", name: "orders"})

    assert {:ok, [%Favn.SQL.Column{name: "id"}]} =
             RuntimeBridge.columns(session, %RelationRef{schema: "main", name: "orders"})

    assert :ok = RuntimeBridge.disconnect(session)
    assert {:connect} in events()
    assert {:disconnect} in events()
  end

  test "connect normalizes missing connection" do
    assert {:error, %Error{type: :invalid_config, connection: :missing, operation: :connect}} =
             RuntimeBridge.connect(:missing)
  end

  test "query and materialize return invalid session errors" do
    write_plan =
      %WritePlan{
        materialization: :table,
        target: %Relation{schema: "main", name: "orders", type: :table},
        select_sql: "SELECT 1 AS id"
      }

    assert {:error, %Error{type: :invalid_config, operation: :session}} =
             RuntimeBridge.query(:not_a_session, "SELECT 1", [])

    assert {:error, %Error{type: :invalid_config, operation: :session}} =
             RuntimeBridge.materialize(:not_a_session, write_plan, [])
  end

  test "runtime bridge normalizes adapter raises as execution errors" do
    put_resolved(:raising_sql, RaisingAdapter)
    assert {:ok, session} = RuntimeBridge.connect(:raising_sql)

    write_plan =
      %WritePlan{
        materialization: :table,
        target: %Relation{schema: "main", name: "orders", type: :table},
        select_sql: "SELECT 1 AS id"
      }

    assert {:error, %Error{type: :execution_error, operation: :query}} =
             RuntimeBridge.query(session, "SELECT 1", [])

    assert {:error, %Error{type: :execution_error, operation: :materialize}} =
             RuntimeBridge.materialize(session, write_plan, [])

    assert {:error, %Error{type: :execution_error, operation: :get_relation}} =
             RuntimeBridge.get_relation(session, %RelationRef{name: "orders"})

    assert {:error, %Error{type: :execution_error, operation: :columns}} =
             RuntimeBridge.columns(session, %RelationRef{name: "orders"})
  end

  test "disconnect swallows adapter disconnect errors" do
    put_resolved(:disconnect_raise, DisconnectRaisingAdapter)
    assert {:ok, session} = RuntimeBridge.connect(:disconnect_raise)
    assert :ok = RuntimeBridge.disconnect(session)
  end

  defp put_resolved(name, adapter) do
    Registry.reload(
      %{
        name => %Resolved{
          name: name,
          adapter: adapter,
          module: __MODULE__,
          config: %{database: ":memory:"}
        }
      },
      registry_name: FavnRunner.ConnectionRegistry
    )
  end

  defp events do
    @events_table
    |> :ets.tab2list()
    |> Enum.sort_by(&elem(&1, 0))
    |> Enum.map(&elem(&1, 1))
  end
end
