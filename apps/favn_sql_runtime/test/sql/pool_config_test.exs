defmodule FavnSQLRuntime.SQLPoolConfigTest do
  use ExUnit.Case, async: true

  alias Favn.Connection.{Definition, Resolved, Validator}
  alias Favn.SQL.PoolConfig

  test "defaults to enabled local pooling" do
    assert {:ok, %PoolConfig{enabled: true, max_idle_per_key: 1, idle_timeout_ms: 300_000}} =
             PoolConfig.parse(nil)
  end

  test "accepts keyword and map configuration" do
    assert {:ok, %PoolConfig{enabled: false, max_idle_per_key: 0, idle_timeout_ms: 1_000}} =
             PoolConfig.parse(enabled: false, max_idle_per_key: 0, idle_timeout_ms: 1_000)

    assert {:ok, %PoolConfig{enabled: true, max_idle_per_key: 2, idle_timeout_ms: 10}} =
             PoolConfig.parse(%{max_idle_per_key: 2, idle_timeout_ms: 10})
  end

  test "rejects unknown keys" do
    assert {:error, error} = PoolConfig.parse(%{enabled: true, unknown: true})
    assert error.type == :invalid_config
    assert error.details == %{keys: [:unknown]}
  end

  test "rejects invalid values" do
    assert {:error, error} = PoolConfig.parse(enabled: :yes)
    assert error.details == %{key: :enabled}

    assert {:error, error} = PoolConfig.parse(max_idle_per_key: -1)
    assert error.details == %{key: :max_idle_per_key}

    assert {:error, error} = PoolConfig.parse(idle_timeout_ms: 0)
    assert error.details == %{key: :idle_timeout_ms}
  end

  test "connection validator reserves and normalizes pool config" do
    definition = %Definition{
      name: :warehouse,
      adapter: Favn.SQL.Adapter.DuckDB,
      module: __MODULE__,
      config_schema: [%{key: :database, required: true, type: :path}]
    }

    assert {:ok,
            %Resolved{
              config: %{pool: %PoolConfig{enabled: false, max_idle_per_key: 1}}
            }} =
             Validator.resolve(definition, %{
               database: "warehouse.duckdb",
               pool: [enabled: false]
             })
  end
end
