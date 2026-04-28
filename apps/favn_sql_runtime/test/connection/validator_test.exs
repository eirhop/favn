defmodule FavnSQLRuntime.ConnectionValidatorTest do
  use ExUnit.Case, async: true

  alias Favn.Connection.Definition
  alias Favn.Connection.Resolved
  alias Favn.Connection.Validator
  alias Favn.RuntimeConfig.Ref

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

  test "resolves nested runtime config refs and marks nested secret refs for redaction" do
    System.put_env("FAVN_TEST_DUCKLAKE_DATA_PATH", "abfss://lake@example.dfs.core.windows.net/raw")
    System.put_env("FAVN_TEST_DUCKLAKE_METADATA", "postgres://user:password@example/db")

    on_exit(fn ->
      System.delete_env("FAVN_TEST_DUCKLAKE_DATA_PATH")
      System.delete_env("FAVN_TEST_DUCKLAKE_METADATA")
    end)

    definition = %Definition{
      name: :warehouse,
      adapter: Favn.SQL.Adapter.DuckDB,
      module: __MODULE__,
      config_schema: [
        %{key: :database, required: true, type: :path},
        %{
          key: :duckdb_bootstrap,
          type: {:custom, fn value -> if is_list(value), do: :ok, else: {:error, :expected_keyword} end}
        }
      ]
    }

    assert {:ok,
            %Resolved{
              config: %{
                duckdb_bootstrap: [
                  attach: [
                    data_path: "abfss://lake@example.dfs.core.windows.net/raw",
                    metadata: "postgres://user:password@example/db"
                  ]
                ]
              },
              secret_fields: [:duckdb_bootstrap]
            }} =
             Validator.resolve(definition, %{
               database: ":memory:",
               duckdb_bootstrap: [
                 attach: [
                    data_path: Ref.env!("FAVN_TEST_DUCKLAKE_DATA_PATH"),
                    metadata: Ref.secret_env!("FAVN_TEST_DUCKLAKE_METADATA")
                 ]
               ]
             })
  end
end
