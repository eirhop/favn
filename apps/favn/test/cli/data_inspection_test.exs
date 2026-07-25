defmodule Favn.CLI.DataInspectionTest do
  use ExUnit.Case, async: false

  alias Favn.CLI.DataInspection
  alias Favn.RelationRef

  setup do
    previous_modules = Application.get_env(:favn, :connection_modules)
    previous_connections = Application.get_env(:favn, :connections)

    Application.put_env(:favn, :connection_modules, [__MODULE__.WarehouseConnection])
    Application.put_env(:favn, :connections, warehouse: [])

    on_exit(fn ->
      restore_env(:connection_modules, previous_modules)
      restore_env(:connections, previous_connections)
    end)
  end

  test "parses catalog schema and name and infers single configured connection" do
    assert {:ok,
            %RelationRef{
              connection: :warehouse,
              catalog: "raw",
              schema: "sales",
              name: "orders_by_day"
            }} = DataInspection.parse_relation("raw.sales.orders_by_day")
  end

  test "uses explicit connection when multiple connections are configured" do
    Application.put_env(:favn, :connection_modules, [
      __MODULE__.WarehouseConnection,
      __MODULE__.LakeConnection
    ])

    Application.put_env(:favn, :connections, warehouse: [], lake: [])

    assert {:ok, %RelationRef{connection: :lake, schema: "sales", name: "orders"}} =
             DataInspection.parse_relation("sales.orders", connection: "lake")
  end

  test "returns a clear error when connection inference is ambiguous" do
    Application.put_env(:favn, :connection_modules, [
      __MODULE__.WarehouseConnection,
      __MODULE__.LakeConnection
    ])

    Application.put_env(:favn, :connections, warehouse: [], lake: [])

    assert {:error, message} = DataInspection.parse_relation("sales.orders")
    assert message =~ "multiple Favn SQL connections configured"
    assert message =~ "pass --connection"
  end

  test "rejects invalid relation strings" do
    assert {:error, message} = DataInspection.parse_relation("raw..orders")
    assert message =~ "relation must be"
  end

  test "inspect_relation delegates through the SQL client API" do
    assert {:ok, result} =
             DataInspection.inspect_relation("raw.sales.orders", client: __MODULE__.Client)

    assert result.relation.connection == :warehouse
    assert result.row_count == 2
    assert %Favn.SQL.Result{rows: [%{"id" => 1}, %{"id" => 2}]} = result.sample

    assert_received {:connect, :warehouse, [required_catalogs: ["raw"]]}
    assert_received {:relation, %RelationRef{name: "orders"}}
    assert_received {:columns, %RelationRef{name: "orders"}}
    assert_received {:row_count, %RelationRef{name: "orders"}}
    assert_received {:sample, %RelationRef{name: "orders"}, [limit: 50]}
    assert_received {:disconnect, :session}
  end

  defp restore_env(key, nil), do: Application.delete_env(:favn, key)
  defp restore_env(key, value), do: Application.put_env(:favn, key, value)

  defmodule WarehouseConnection do
    @behaviour Favn.Connection

    @impl true
    def definition do
      %Favn.Connection.Definition{
        name: :warehouse,
        adapter: Favn.CLI.DataInspectionTest.Adapter,
        config_schema: [%{key: :database, default: ":memory:", type: :string}]
      }
    end
  end

  defmodule LakeConnection do
    @behaviour Favn.Connection

    @impl true
    def definition do
      %Favn.Connection.Definition{
        name: :lake,
        adapter: Favn.CLI.DataInspectionTest.Adapter,
        config_schema: [%{key: :database, default: ":memory:", type: :string}]
      }
    end
  end

  defmodule Adapter do
  end

  defmodule Client do
    def connect(connection, opts) do
      send(self(), {:connect, connection, opts})
      {:ok, :session}
    end

    def disconnect(session) do
      send(self(), {:disconnect, session})
      :ok
    end

    def relation(session, relation_ref) do
      send(self(), {:relation, relation_ref})
      {:ok, %{session: session, type: :table}}
    end

    def columns(_session, relation_ref) do
      send(self(), {:columns, relation_ref})
      {:ok, [%Favn.SQL.Column{name: "id", data_type: "integer"}]}
    end

    def row_count(_session, relation_ref) do
      send(self(), {:row_count, relation_ref})
      {:ok, 2}
    end

    def sample(_session, relation_ref, opts) do
      send(self(), {:sample, relation_ref, opts})
      {:ok, %Favn.SQL.Result{columns: ["id"], rows: [%{"id" => 1}, %{"id" => 2}]}}
    end
  end
end
