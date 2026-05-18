defmodule FavnDuckdbADBC.SQLAdapterDuckDBADBCBootstrapTest do
  use ExUnit.Case, async: false

  alias Favn.Connection.Resolved
  alias Favn.SQL.Adapter.DuckDB.ADBC
  alias Favn.SQL.Adapter.DuckDB.ADBC.Bootstrap
  alias Favn.SQL.Error
  alias FavnDuckdbADBC.TestSupport

  defmodule FakeClient do
    use FavnDuckdbADBC.TestSupport.FakeClient

    alias FavnDuckdbADBC.TestSupport

    @impl true
    def execute(_conn_ref, sql, params) do
      TestSupport.record({:execute, sql, params})

      if TestSupport.mode(:bootstrap_fail_sql, nil) == sql do
        {:error, "failed while running #{sql}"}
      else
        {:ok, nil}
      end
    end
  end

  defmodule FakeTokenProvider do
    @behaviour Favn.Azure.PostgresEntraTokenProvider

    alias Favn.Azure.Token

    @impl true
    def fetch_token(_auth, _opts) do
      {:ok, %Token{access_token: "entra-token", expires_on: "1770000000"}}
    end
  end

  defmodule FailingTokenProvider do
    @behaviour Favn.Azure.PostgresEntraTokenProvider

    alias Favn.Azure.TokenError

    @impl true
    def fetch_token(_auth, _opts) do
      {:error,
       %TokenError{
         type: :connection_error,
         message: "managed identity token request failed",
         retryable?: true,
         details: %{status: 429, access_token: "entra-token"}
       }}
    end
  end

  setup do
    TestSupport.start_events()

    on_exit(fn ->
      TestSupport.reset()
    end)

    :ok
  end

  test "config schema fields accept open and duckdb config" do
    assert [%{key: :open, type: {:custom, open_validator}}, %{key: :duckdb, type: {:custom, validator}} | _] =
             ADBC.config_schema_fields()

    assert :ok = open_validator.(database: ":memory:")
    assert :ok = validator.([])
    assert :ok = validator.(%{})
    assert {:error, :expected_duckdb_keyword_or_map} = validator.(:invalid)
  end

  test "schema field accepts typed settings and rejects unknown settings" do
    assert %{type: {:custom, validator}} = duckdb_schema_field()

    assert :ok = validator.(settings: [azure_transport_option_type: :curl])
    assert :ok = validator.(settings: [azure_transport_option_type: "default"])

    assert {:error, {:unsupported_setting, :some_unknown_setting}} =
             validator.(settings: [some_unknown_setting: "value"])

    assert {:error, {:invalid_setting_value, :azure_transport_option_type, "bad"}} =
             validator.(settings: [azure_transport_option_type: :bad])
  end

  test "normalizes setting atom and string values into SET statements" do
    assert {:ok, atom_steps} = Bootstrap.build_steps(settings_resolved(:curl))
    assert {:ok, string_steps} = Bootstrap.build_steps(settings_resolved("default"))

    assert Enum.find(atom_steps, &(&1.id == "set_azure_transport_option_type"))
           |> Map.fetch!(:statement)
           |> IO.iodata_to_binary() == "SET azure_transport_option_type = 'curl'"

    assert Enum.find(string_steps, &(&1.id == "set_azure_transport_option_type"))
           |> Map.fetch!(:statement)
           |> IO.iodata_to_binary() == "SET azure_transport_option_type = 'default'"
  end

  test "runs DuckLake bootstrap statements in configured order" do
    {:ok, conn} = ADBC.connect(resolved(), duckdb_adbc_client: FakeClient)

    assert :ok = ADBC.bootstrap(conn, resolved(), [])

    assert statements() == [
             "LOAD ducklake",
             "LOAD postgres",
             "LOAD azure",
             "CREATE SECRET \"azure_adls\" (TYPE azure, PROVIDER credential_chain, ACCOUNT_NAME 'storageaccount')",
             "CREATE SECRET \"lakehouse_meta\" (TYPE postgres, HOST 'pg.example.com', PORT 5432, DATABASE 'ducklake', USER 'ducklake_user', PASSWORD 'super-secret')",
             "ATTACH 'ducklake:postgres:' AS \"lake\" (DATA_PATH 'abfss://lake@storageaccount.dfs.core.windows.net/raw', META_SECRET \"lakehouse_meta\")",
             ~s(USE "lake")
           ]
  end

  test "runs multiple DuckDB catalog attach statements" do
    resolved = duckdb_catalogs_resolved()
    {:ok, conn} = ADBC.connect(resolved, duckdb_adbc_client: FakeClient)

    assert :ok = ADBC.bootstrap(conn, resolved, [])

    assert statements() == [
             ~s(ATTACH '.favn/data/raw.duckdb' AS "raw"),
             ~s(ATTACH '.favn/data/mart.duckdb' AS "mart")
           ]
  end

  test "runs DuckLake bootstrap with ADLS and PostgreSQL secrets" do
    resolved = ducklake_postgres_resolved()
    {:ok, conn} = ADBC.connect(resolved, duckdb_adbc_client: FakeClient)

    assert :ok = ADBC.bootstrap(conn, resolved, [])

    assert statements() == [
             "LOAD ducklake",
             "LOAD postgres",
             "LOAD azure",
             "CREATE SECRET \"azure_adls\" (TYPE azure, PROVIDER credential_chain, ACCOUNT_NAME 'storageaccount', CHAIN 'cli;env', SCOPE 'abfss://lake@storageaccount.dfs.core.windows.net/')",
             "CREATE SECRET \"lakehouse_meta\" (TYPE postgres, HOST 'pg.example.com', PORT 5432, DATABASE 'ducklake', USER 'ducklake_user', PASSWORD 'super-secret')",
             "ATTACH 'ducklake:postgres:sslmode=require' AS \"lakehouse_lake\" (DATA_PATH 'abfss://lake@storageaccount.dfs.core.windows.net/data/', META_SECRET \"lakehouse_meta\")",
             ~s(USE "lakehouse_lake")
           ]
  end

  test "runs settings after extension load and before secrets, attach, and use" do
    resolved = ducklake_postgres_resolved(settings: [azure_transport_option_type: :curl])
    {:ok, conn} = ADBC.connect(resolved, duckdb_adbc_client: FakeClient)

    assert :ok = ADBC.bootstrap(conn, resolved, [])

    assert statements() == [
             "LOAD ducklake",
             "LOAD postgres",
             "LOAD azure",
             "SET azure_transport_option_type = 'curl'",
             "CREATE SECRET \"azure_adls\" (TYPE azure, PROVIDER credential_chain, ACCOUNT_NAME 'storageaccount', CHAIN 'cli;env', SCOPE 'abfss://lake@storageaccount.dfs.core.windows.net/')",
             "CREATE SECRET \"lakehouse_meta\" (TYPE postgres, HOST 'pg.example.com', PORT 5432, DATABASE 'ducklake', USER 'ducklake_user', PASSWORD 'super-secret')",
             "ATTACH 'ducklake:postgres:sslmode=require' AS \"lakehouse_lake\" (DATA_PATH 'abfss://lake@storageaccount.dfs.core.windows.net/data/', META_SECRET \"lakehouse_meta\")",
             ~s(USE "lakehouse_lake")
           ]
  end

  test "injects Azure PostgreSQL Entra token into temporary PostgreSQL secret" do
    resolved = ducklake_postgres_entra_resolved()
    {:ok, conn} = ADBC.connect(resolved, duckdb_adbc_client: FakeClient)

    assert :ok = ADBC.bootstrap(conn, resolved, azure_token_provider_module: FakeTokenProvider)

    assert statements() == [
             "LOAD ducklake",
             "LOAD postgres",
             "CREATE SECRET \"lakehouse_meta\" (TYPE postgres, HOST 'pg.example.com', PORT 5432, DATABASE 'ducklake', USER 'ducklake_user', PASSWORD 'entra-token')",
             "ATTACH 'ducklake:postgres:sslmode=require' AS \"lakehouse_lake\" (DATA_PATH 'abfss://lake@storageaccount.dfs.core.windows.net/data/', META_SECRET \"lakehouse_meta\")",
             ~s(USE "lakehouse_lake")
            ]
  end

  test "builds Azure PostgreSQL Entra bootstrap steps without fetching token" do
    resolved = ducklake_postgres_entra_resolved()

    assert {:ok, steps} = Bootstrap.build_steps(resolved, azure_token_provider_module: FakeTokenProvider)

    create_secret = Enum.find(steps, &(&1.id == "create_secret_lakehouse_meta"))

    refute IO.iodata_to_binary(create_secret.statement) =~ "entra-token"
    assert IO.iodata_to_binary(create_secret.safe_statement) =~ "PASSWORD 'redacted'"
    assert statements() == []
  end

  test "token acquisition failure returns redacted bootstrap error" do
    resolved = ducklake_postgres_entra_resolved()
    {:ok, conn} = ADBC.connect(resolved, duckdb_adbc_client: FakeClient)

    assert {:error,
            %Error{
              type: :connection_error,
              operation: :bootstrap,
              message: "DuckDB ADBC connection bootstrap failed at create_secret_lakehouse_meta",
              details: %{statement: safe_statement, adapter_details: adapter_details}
            }} = ADBC.bootstrap(conn, resolved, azure_token_provider_module: FailingTokenProvider)

    assert safe_statement =~ "PASSWORD 'redacted'"
    refute safe_statement =~ "entra-token"
    refute inspect(adapter_details) =~ "entra-token"
    assert adapter_details.access_token == :redacted
    assert statements() == ["LOAD ducklake", "LOAD postgres"]
  end

  test "PostgreSQL Entra secret execution failure redacts fetched token" do
    resolved = ducklake_postgres_entra_resolved()
    {:ok, conn} = ADBC.connect(resolved, duckdb_adbc_client: FakeClient)

    failing_sql =
      "CREATE SECRET \"lakehouse_meta\" (TYPE postgres, HOST 'pg.example.com', PORT 5432, DATABASE 'ducklake', USER 'ducklake_user', PASSWORD 'entra-token')"

    TestSupport.put_mode(:bootstrap_fail_sql, failing_sql)

    assert {:error,
            %Error{
              operation: :bootstrap,
              details: %{
                statement: safe_statement,
                reason: reason,
                adapter_details: adapter_details
              }
            }} = ADBC.bootstrap(conn, resolved, azure_token_provider_module: FakeTokenProvider)

    refute safe_statement =~ "entra-token"
    refute reason =~ "entra-token"
    refute inspect(adapter_details) =~ "entra-token"
    assert safe_statement =~ "PASSWORD 'redacted'"
    assert reason =~ "PASSWORD 'redacted'"
  end

  test "bootstrap failure reports failing step without exposing secret values" do
    {:ok, conn} = ADBC.connect(resolved(), duckdb_adbc_client: FakeClient)

    secret_metadata = "ducklake:postgres:"

    failing_sql =
      "ATTACH '#{secret_metadata}' AS \"lake\" (DATA_PATH 'abfss://lake@storageaccount.dfs.core.windows.net/raw', META_SECRET \"lakehouse_meta\")"

    TestSupport.put_mode(:bootstrap_fail_sql, failing_sql)

    assert {:error,
            %Error{
              operation: :bootstrap,
              connection: :warehouse,
              message: "DuckDB ADBC connection bootstrap failed at attach_lake",
              details: %{
                statement: safe_statement,
                reason: reason,
                adapter_details: adapter_details
              }
            }} = ADBC.bootstrap(conn, resolved(), [])

    refute safe_statement =~ secret_metadata
    refute safe_statement =~ "abfss://lake@storageaccount.dfs.core.windows.net/raw"
    refute reason =~ secret_metadata
    refute inspect(adapter_details) =~ secret_metadata
    assert safe_statement =~ "redacted"
    assert reason =~ "redacted"
  end

  test "setting execution failure reports bootstrap setting step" do
    resolved = settings_resolved(:curl)
    {:ok, conn} = ADBC.connect(resolved, duckdb_adbc_client: FakeClient)

    TestSupport.put_mode(:bootstrap_fail_sql, "SET azure_transport_option_type = 'curl'")

    assert {:error,
            %Error{
              operation: :bootstrap,
              details: %{
                step: "set_azure_transport_option_type",
                bootstrap_kind: :set_setting,
                statement: "SET azure_transport_option_type = 'curl'"
              }
            }} = ADBC.bootstrap(conn, resolved, [])
  end

  defp resolved do
    %Resolved{
      name: :warehouse,
      adapter: ADBC,
      module: __MODULE__,
      config: %{
        open: [database: ":memory:"],
        duckdb: [
          load: [:ducklake, :postgres, :azure],
          secrets: [
            azure_adls: [
              type: :azure,
              provider: :credential_chain,
              account_name: "storageaccount"
            ],
            lakehouse_meta: [
              type: :postgres,
              host: "pg.example.com",
              port: 5432,
              database: "ducklake",
              user: "ducklake_user",
              password: "super-secret"
            ]
          ],
          attach: [
            lake: [
              type: :ducklake,
              metadata: "ducklake:postgres:",
              meta_secret: :lakehouse_meta,
              data_path: "abfss://lake@storageaccount.dfs.core.windows.net/raw"
            ]
          ],
          use: :lake
        ]
      },
      secret_fields: [:duckdb]
    }
  end

  defp duckdb_schema_field do
    Enum.find(ADBC.config_schema_fields(), &(&1.key == :duckdb))
  end

  defp ducklake_postgres_resolved(opts \\ []) do
    settings = Keyword.get(opts, :settings, [])

    %Resolved{
      name: :warehouse,
      adapter: ADBC,
      module: __MODULE__,
      config: %{
        open: [database: ":memory:"],
        duckdb: [
          load: [:ducklake, :postgres, :azure],
          settings: settings,
          secrets: [
            azure_adls: [
              type: :azure,
              provider: :credential_chain,
              account_name: "storageaccount",
              chain: [:cli, :env],
              scope: "abfss://lake@storageaccount.dfs.core.windows.net/"
            ],
            lakehouse_meta: [
              type: :postgres,
              host: "pg.example.com",
              port: 5432,
              database: "ducklake",
              user: "ducklake_user",
              password: "super-secret",
              sslmode: :require
            ]
          ],
          attach: [
            lakehouse_lake: [
              type: :ducklake,
              metadata: "ducklake:postgres:",
              meta_secret: :lakehouse_meta,
              data_path: "abfss://lake@storageaccount.dfs.core.windows.net/data/"
            ]
          ],
          use: :lakehouse_lake
        ]
      },
      secret_fields: [:duckdb]
    }
  end

  defp duckdb_catalogs_resolved do
    %Resolved{
      name: :important_lakehouse,
      adapter: ADBC,
      module: __MODULE__,
      config: %{
        open: [database: ":memory:"],
        duckdb: [
          attach: [
            raw: [type: :duckdb, path: ".favn/data/raw.duckdb"],
            mart: [type: :duckdb, path: ".favn/data/mart.duckdb"]
          ]
        ]
      },
      secret_fields: []
    }
  end

  defp settings_resolved(value) do
    %Resolved{
      name: :warehouse,
      adapter: ADBC,
      module: __MODULE__,
      config: %{
        open: [database: ":memory:"],
        duckdb: [
          load: [:azure],
          settings: [azure_transport_option_type: value]
        ]
      },
      secret_fields: [:duckdb]
    }
  end

  defp ducklake_postgres_entra_resolved do
    %Resolved{
      name: :warehouse,
      adapter: ADBC,
      module: __MODULE__,
      config: %{
        open: [database: ":memory:"],
        duckdb: [
          load: [:ducklake, :postgres],
          secrets: [
            lakehouse_meta: [
              type: :postgres,
              host: "pg.example.com",
              port: 5432,
              database: "ducklake",
              user: "ducklake_user",
              auth: [type: :azure_postgres_entra, provider: :azure_cli],
              sslmode: :require
            ]
          ],
          attach: [
            lakehouse_lake: [
              type: :ducklake,
              metadata: "ducklake:postgres:",
              meta_secret: :lakehouse_meta,
              data_path: "abfss://lake@storageaccount.dfs.core.windows.net/data/"
            ]
          ],
          use: :lakehouse_lake
        ]
      },
      secret_fields: [:duckdb]
    }
  end

  defp statements do
    TestSupport.events()
    |> Enum.flat_map(fn
      {:execute, sql, []} -> [sql]
      _event -> []
    end)
  end
end
