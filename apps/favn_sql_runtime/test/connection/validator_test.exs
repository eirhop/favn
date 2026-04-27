defmodule FavnSQLRuntime.ConnectionValidatorTest do
  use ExUnit.Case, async: true

  alias Favn.Connection.Definition
  alias Favn.Connection.Resolved
  alias Favn.Connection.Validator

  test "allows reserved runtime write concurrency config" do
    definition = %Definition{
      name: :warehouse,
      adapter: Favn.SQL.Adapter.DuckDB,
      module: __MODULE__,
      config_schema: [%{key: :database, required: true, type: :path}]
    }

    assert {:ok,
            %Resolved{
              name: :warehouse,
              config: %{database: "warehouse.duckdb", write_concurrency: 1}
            }} = Validator.resolve(definition, %{database: "warehouse.duckdb", write_concurrency: 1})
  end
end
