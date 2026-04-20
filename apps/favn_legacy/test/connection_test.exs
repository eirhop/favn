defmodule Favn.ConnectionTest do
  use ExUnit.Case

  @moduletag :legacy_execution_reference

  alias Favn.Connection.ConfigError
  alias Favn.Connection.Definition
  alias Favn.Connection.Error
  alias Favn.Connection.Info
  alias Favn.Connection.Loader
  alias Favn.Connection.NotFoundError
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

  defmodule CustomValidatorConnection do
    @behaviour Favn.Connection

    @impl true
    def definition do
      %Definition{
        name: :custom,
        adapter: Favn.SQL.Adapter.DuckDB,
        config_schema: [
          %{key: :token, required: true, type: {:custom, fn _value -> :bad_return end}}
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

  test "loader rejects duplicate top-level keyword connection names" do
    Application.put_env(:favn, :connection_modules, [WarehouseConnection])

    Application.put_env(:favn, :connections,
      warehouse: [database: "/tmp/one"],
      warehouse: [database: "/tmp/two"]
    )

    assert {:error, errors} = Loader.load()
    assert Enum.any?(errors, &(&1.message =~ "duplicate runtime connection name"))
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

  test "custom validator invalid return is normalized to invalid_type" do
    Application.put_env(:favn, :connection_modules, [CustomValidatorConnection])
    Application.put_env(:favn, :connections, custom: [token: "value"])

    assert {:error, errors} = Loader.load()

    assert Enum.any?(errors, fn error ->
             error.type == :invalid_type and
               error.details[:reason] == :invalid_custom_validator_return
           end)
  end

  test "loader rejects duplicate per-connection keyword keys" do
    Application.put_env(:favn, :connection_modules, [WarehouseConnection])

    Application.put_env(:favn, :connections,
      warehouse: [database: "/tmp/one", database: "/tmp/two"]
    )

    assert {:error, errors} = Loader.load()
    assert Enum.any?(errors, &(&1.message =~ "duplicate runtime config key"))
  end

  test "public facade returns redacted connection inspection" do
    Application.put_env(:favn, :connection_modules, [WarehouseConnection])
    Application.put_env(:favn, :connections, warehouse: [database: "/tmp/db", password: "hidden"])

    assert {:ok, resolved} = Loader.load()
    :ok = Favn.Connection.Registry.reload(resolved)

    assert Favn.connection_registered?(:warehouse)

    assert {:ok, connection} = Favn.get_connection(:warehouse)
    assert %Info{} = connection
    assert connection.config.password == :redacted
    assert connection.config.database == "/tmp/db"

    assert [%Info{name: :warehouse}] = Favn.list_connections()
    assert %Info{name: :warehouse} = Favn.get_connection!(:warehouse)
    assert {:error, :not_found} = Favn.get_connection(:missing)
    assert_raise NotFoundError, fn -> Favn.get_connection!(:missing) end
  end

  test "ConfigError exception formats multiple errors" do
    error1 = %Error{type: :missing_required, message: "database is required"}
    error2 = %Error{type: :invalid_type, message: "expected string"}

    config_error = ConfigError.exception(errors: [error1, error2])

    assert config_error.message =~ "connection configuration is invalid"
    assert config_error.message =~ "database is required"
    assert config_error.message =~ "expected string"
    assert config_error.errors == [error1, error2]
  end

  test "ConfigError exception handles empty errors" do
    config_error = ConfigError.exception(errors: [])
    assert config_error.message == "connection configuration is invalid"
    assert config_error.errors == []
  end
end
