defmodule FavnRunner.ConnectionLoaderTest do
  use ExUnit.Case, async: false

  alias Favn.Connection.Definition
  alias Favn.Connection.Loader

  defmodule FakeAdapter do
  end

  defmodule WarehouseConnection do
    @behaviour Favn.Connection

    @impl true
    def definition do
      %Definition{
        name: :warehouse,
        adapter: FakeAdapter,
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
        adapter: FakeAdapter,
        config_schema: [%{key: :database, required: true, type: :path}]
      }
    end
  end

  defmodule DuplicateWarehouseConnection do
    @behaviour Favn.Connection

    @impl true
    def definition do
      %Definition{
        name: :warehouse,
        adapter: FakeAdapter,
        config_schema: [%{key: :database, required: true, type: :path}]
      }
    end
  end

  defmodule InvalidDefinitionConnection do
    @behaviour Favn.Connection

    @impl true
    def definition do
      %Definition{
        name: :invalid,
        adapter: FakeAdapter,
        config_schema: [%{key: :database, required: "yes"}]
      }
    end
  end

  setup do
    previous_modules = Application.get_env(:favn, :connection_modules)
    previous_connections = Application.get_env(:favn, :connections)

    on_exit(fn ->
      restore_env(:connection_modules, previous_modules)
      restore_env(:connections, previous_connections)
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

  test "loader rejects unknown top-level runtime connection names" do
    Application.put_env(:favn, :connection_modules, [WarehouseConnection])

    Application.put_env(:favn, :connections,
      warehouse: [database: "/tmp/db"],
      ghost_connection: []
    )

    assert {:error, errors} = Loader.load()
    assert Enum.any?(errors, &(&1.message =~ "ghost_connection"))
  end

  test "loader rejects non-atom top-level runtime connection names for map config" do
    Application.put_env(:favn, :connection_modules, [WarehouseConnection])
    Application.put_env(:favn, :connections, %{"warehouse" => %{database: "/tmp/db"}})

    assert {:error, errors} = Loader.load()
    assert Enum.any?(errors, &(&1.message =~ "runtime connection name must be an atom"))
  end

  test "loader rejects duplicate provider names" do
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

  defp restore_env(key, nil), do: Application.delete_env(:favn, key)
  defp restore_env(key, value), do: Application.put_env(:favn, key, value)
end
