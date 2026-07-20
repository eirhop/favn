defmodule FavnSQLRuntime.ConnectionValidatorTest do
  use ExUnit.Case, async: true

  alias Favn.Connection.Definition
  alias Favn.Connection.Resolved
  alias Favn.Connection.Validator
  alias Favn.RuntimeConfig.Ref
  alias Favn.SQL.SessionScript.Config

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
            }} =
             Validator.resolve(definition, %{database: "warehouse.duckdb", write_concurrency: 1})
  end

  test "extracts circuit breaker policy instead of passing it to the adapter" do
    definition = %Definition{
      name: :warehouse,
      adapter: Favn.SQL.Adapter.DuckDB,
      module: __MODULE__,
      config_schema: [%{key: :database, required: true, type: :path}]
    }

    assert {:ok,
            %Resolved{
              circuit_breaker: %Favn.CircuitBreaker.Policy{
                failure_threshold: 3,
                probe_after_ms: 1_000
              },
              config: config
            }} =
             Validator.resolve(definition, %{
               database: "warehouse.duckdb",
               circuit_breaker: [failure_threshold: 3, probe_after_ms: 1_000]
             })

    refute Map.has_key?(config, :circuit_breaker)
  end

  test "resolves nested refs and records exact secret paths" do
    System.put_env(
      "FAVN_TEST_DUCKLAKE_DATA_PATH",
      "abfss://lake@example.dfs.core.windows.net/raw"
    )

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
          key: :duckdb,
          type:
            {:custom,
             fn value -> if is_list(value), do: :ok, else: {:error, :expected_keyword} end}
        }
      ]
    }

    assert {:ok,
            %Resolved{
              config: %{
                duckdb: [
                  resources: [
                    landing_storage: [
                      params: [
                        data_path: "abfss://lake@example.dfs.core.windows.net/raw",
                        metadata: "postgres://user:password@example/db"
                      ]
                    ]
                  ]
                ]
              },
              secret_fields: [:duckdb],
              secret_paths: [
                [:duckdb, :resources, :landing_storage, :params, :metadata]
              ]
            }} =
             Validator.resolve(definition, %{
               database: ":memory:",
               duckdb: [
                 resources: [
                   landing_storage: [
                     params: [
                       data_path: Ref.env!("FAVN_TEST_DUCKLAKE_DATA_PATH"),
                       metadata: Ref.secret_env!("FAVN_TEST_DUCKLAKE_METADATA")
                     ]
                   ]
                 ]
               ]
             })
  end

  test "custom session-script validation never exposes a resolved secret value" do
    System.put_env("FAVN_TEST_INVALID_SCRIPT_FILE", "do-not-expose-this-secret")

    on_exit(fn -> System.delete_env("FAVN_TEST_INVALID_SCRIPT_FILE") end)

    definition = %Definition{
      name: :warehouse,
      adapter: Favn.SQL.Adapter.DuckDB,
      module: __MODULE__,
      config_schema: [
        %{key: :database, required: true, type: :path},
        %{key: :duckdb, type: {:custom, &Config.validate/1}}
      ]
    }

    assert {:error, [error]} =
             Validator.resolve(definition, %{
               database: ":memory:",
               duckdb: [
                 catalogs: [
                   lake: [
                     write_concurrency: Ref.secret_env!("FAVN_TEST_INVALID_SCRIPT_FILE")
                   ]
                 ]
               ]
             })

    assert error.details.reason ==
             {:invalid_catalog_write_concurrency, :expected_positive_integer_or_unlimited}

    refute inspect(error) =~ "do-not-expose-this-secret"
  end

  test "preserves deferred runtime values and records their secret paths" do
    definition = %Definition{
      name: :warehouse,
      adapter: Favn.SQL.Adapter.DuckDB,
      module: __MODULE__,
      config_schema: [
        %{key: :database, required: true, type: :path},
        %{key: :duckdb, type: {:custom, &Config.validate/1}}
      ]
    }

    token_ref = Favn.RuntimeValue.new(__MODULE__, :request, secret?: true)

    assert {:ok, %Resolved{} = resolved} =
             Validator.resolve(definition, %{
               database: ":memory:",
               duckdb: [
                 resources: [
                   storage: [file: "/tmp/storage.sql", params: [token: token_ref]]
                 ]
               ]
             })

    assert resolved.config.duckdb[:resources][:storage][:params][:token] == token_ref

    assert resolved.secret_fields == [:duckdb]

    assert resolved.secret_paths == [
             [:duckdb, :resources, :storage, :params, :token]
           ]
  end
end
