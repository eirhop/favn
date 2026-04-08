defmodule Favn.ConnectionTest do
  use ExUnit.Case

  alias Favn.Connection.Definition
  alias Favn.Connection.Loader
  alias Favn.Connection.Resolved

  defmodule WarehouseConnection do
    @behaviour Favn.Connection

    @impl true
    def definition do
      %Definition{
        name: :warehouse,
        adapter: Favn.SQL.Adapter.DuckDB,
        config_schema: [
          %{key: :database, required: true, type: :path},
          %{key: :read_only, default: false, type: :boolean},
          %{key: :password, secret: true, type: :string}
        ]
      }
    end
  end

  defmodule AnalyticsConnection do
    @behaviour Favn.Connection

    @impl true
    def definition do
      %Definition{
        name: :analytics,
        adapter: Favn.SQL.Adapter.DuckDB,
        config_schema: [
          %{key: :database, required: true, type: :path}
        ]
      }
    end
  end

  defmodule DuplicateWarehouseConnection do
    @behaviour Favn.Connection

    @impl true
    def definition do
      %Definition{
        name: :warehouse,
        adapter: Favn.SQL.Adapter.DuckDB,
        config_schema: [
          %{key: :database, required: true, type: :path}
        ]
      }
    end
  end

  defmodule InvalidDefinitionConnection do
    @behaviour Favn.Connection

    @impl true
    def definition do
      %Definition{
        name: :invalid,
        adapter: Favn.SQL.Adapter.DuckDB,
        config_schema: [
          %{key: :database, required: "yes"}
        ]
      }
    end
  end

  setup do
    state = Favn.TestSetup.capture_state()

    on_exit(fn ->
      Favn.TestSetup.restore_state(state)
      Favn.Connection.Registry.reload(%{})
    end)

    :ok
  end

  test "loader resolves configured modules and merges defaults" do
    Application.put_env(:favn, :connection_modules, [WarehouseConnection, AnalyticsConnection])

    Application.put_env(:favn, :connections,
      warehouse: [database: "/tmp/warehouse.duckdb", password: "secret"],
      analytics: [database: "/tmp/analytics.duckdb"]
    )

    assert {:ok, resolved} = Loader.load()
    assert %Resolved{} = resolved.warehouse
    assert resolved.warehouse.config.database == "/tmp/warehouse.duckdb"
    assert resolved.warehouse.config.read_only == false
    assert resolved.warehouse.secret_fields == [:password]
    assert resolved.analytics.required_keys == [:database]
  end

  test "loader rejects unknown runtime keys" do
    Application.put_env(:favn, :connection_modules, [WarehouseConnection])
    Application.put_env(:favn, :connections, warehouse: [database: "/tmp/db", invalid_opt: true])

    assert {:error, errors} = Loader.load()
    assert Enum.any?(errors, &(&1.type == :unknown_keys))
  end

  test "loader rejects duplicate connection names" do
    Application.put_env(:favn, :connection_modules, [
      WarehouseConnection,
      DuplicateWarehouseConnection
    ])

    Application.put_env(:favn, :connections, warehouse: [database: "/tmp/db"])

    assert {:error, errors} = Loader.load()
    assert Enum.any?(errors, &(&1.type == :duplicate_name))
  end

  test "loader rejects invalid schema definitions" do
    Application.put_env(:favn, :connection_modules, [InvalidDefinitionConnection])

    assert {:error, errors} = Loader.load()
    assert Enum.any?(errors, &(&1.type == :invalid_definition))
  end

  test "public facade returns redacted connection inspection" do
    Application.put_env(:favn, :connection_modules, [WarehouseConnection])
    Application.put_env(:favn, :connections, warehouse: [database: "/tmp/db", password: "hidden"])

    assert {:ok, resolved} = Loader.load()
    :ok = Favn.Connection.Registry.reload(resolved)

    assert Favn.connection_registered?(:warehouse)

    assert {:ok, connection} = Favn.get_connection(:warehouse)
    assert connection.config.password == :redacted
    assert connection.config.database == "/tmp/db"

    assert [%{name: :warehouse}] = Favn.list_connections()
    assert %{name: :warehouse} = Favn.get_connection!(:warehouse)
    assert {:error, :not_found} = Favn.get_connection(:missing)
    assert_raise Favn.Connection.NotFoundError, fn -> Favn.get_connection!(:missing) end
  end
end
