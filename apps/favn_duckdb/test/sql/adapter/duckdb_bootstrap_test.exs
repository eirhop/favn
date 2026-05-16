defmodule FavnDuckdb.SQLAdapterDuckDBBootstrapTest do
  use ExUnit.Case, async: false

  alias Favn.Connection.Resolved
  alias Favn.SQL.Adapter.DuckDB
  alias Favn.SQL.Adapter.DuckDB.Bootstrap
  alias Favn.SQL.Error
  alias FavnDuckdb.TestSupport

  defmodule FakeClient do
    use FavnDuckdb.TestSupport.FakeClient

    alias FavnDuckdb.TestSupport

    @impl true
    def query(_conn_ref, sql, params) do
      TestSupport.record({:query, sql, params})

      if TestSupport.mode(:bootstrap_fail_sql, nil) == sql do
        {:error, "failed while running #{sql}"}
      else
        {:ok, make_ref()}
      end
    end

    @impl true
    def release(resource) do
      TestSupport.record({:release, resource})
      :ok
    end
  end

  defmodule FakeTokenProvider do
    @behaviour Favn.Azure.PostgresEntraTokenProvider

    alias Favn.Azure.Token
    alias FavnDuckdb.TestSupport

    @impl true
    def fetch_token(auth, _opts) do
      TestSupport.record({:token_auth, auth})
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

  test "schema field accepts keyword or map bootstrap config" do
    assert %{key: :duckdb_bootstrap, type: {:custom, validator}} = DuckDB.bootstrap_schema_field()
    assert :ok = validator.([])
    assert :ok = validator.(%{})
    assert {:error, :expected_duckdb_bootstrap_keyword_or_map} = validator.(:invalid)
  end

  test "schema field accepts arbitrary extension identifiers as atoms or binaries" do
    assert %{type: {:custom, validator}} = DuckDB.bootstrap_schema_field()

    assert :ok =
             validator.(extensions: [install: ["ducklake", :json], load: ["postgres", "azure"]])

    assert {:error, {:invalid_identifier, "invalid extension"}} =
             validator.(extensions: [load: ["invalid extension"]])
  end

  test "schema field accepts typed settings and rejects unknown settings" do
    assert %{type: {:custom, validator}} = DuckDB.bootstrap_schema_field()

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

  test "schema field validates Azure credential chain and scope" do
    assert %{type: {:custom, validator}} = DuckDB.bootstrap_schema_field()

    assert :ok =
             validator.(
               secrets: [
                 azure_adls: [
                   type: :azure,
                   provider: :credential_chain,
                   account_name: "storageaccount",
                   chain: [:cli, "env"],
                   scope: "abfss://lake@storageaccount.dfs.core.windows.net/"
                 ]
               ]
             )

    assert {:error, {:invalid_azure_credential_chain, "bad"}} =
             validator.(
               secrets: [
                 azure_adls: [
                   type: :azure,
                   provider: :credential_chain,
                   account_name: "storageaccount",
                   chain: [:cli, "bad"]
                 ]
               ]
             )

    assert {:error, {:invalid_azure_scope, :missing_trailing_slash}} =
             validator.(
               secrets: [
                 azure_adls: [
                   type: :azure,
                   provider: :credential_chain,
                   account_name: "storageaccount",
                   scope: "abfss://lake@storageaccount.dfs.core.windows.net"
                 ]
               ]
             )
  end

  test "schema field redacts invalid PostgreSQL password values" do
    assert %{type: {:custom, validator}} = DuckDB.bootstrap_schema_field()

    assert {:error, {:invalid_secret_field, "oceanos_meta", :password}} =
             validator.(
               secrets: [
                 oceanos_meta: [
                   type: :postgres,
                   host: "pg.example.com",
                   port: 5432,
                   database: "ducklake",
                   user: "ducklake_user",
                   password: {:raw_secret, "super-secret"}
                 ]
               ]
             )
  end

  test "schema field accepts Azure PostgreSQL Entra auth and rejects password conflicts" do
    assert %{type: {:custom, validator}} = DuckDB.bootstrap_schema_field()

    assert :ok =
             validator.(
               secrets: [
                 oceanos_meta: [
                   type: :postgres,
                   host: "pg.example.com",
                   port: 5432,
                   database: "ducklake",
                   user: "ducklake_user",
                   auth: [
                     type: :azure_postgres_entra,
                     provider: :managed_identity,
                     client_id: "client-1",
                     endpoint: :auto
                   ]
                 ]
               ]
             )

    assert {:error, {:conflicting_secret_fields, "oceanos_meta", [:password, :auth]}} =
             validator.(
               secrets: [
                 oceanos_meta: [
                   type: :postgres,
                   host: "pg.example.com",
                   port: 5432,
                   database: "ducklake",
                   user: "ducklake_user",
                   password: "super-secret",
                   auth: [type: :azure_postgres_entra, provider: :azure_cli]
                 ]
               ]
             )
  end

  test "runs DuckLake bootstrap statements in configured order" do
    {:ok, conn} = DuckDB.connect(resolved(), duckdb_client: FakeClient)

    assert :ok = DuckDB.bootstrap(conn, resolved(), [])

    assert statements() == [
             "INSTALL ducklake",
             "INSTALL postgres",
             "INSTALL azure",
             "LOAD ducklake",
             "LOAD postgres",
             "LOAD azure",
             "CREATE SECRET \"azure_adls\" (TYPE azure, PROVIDER credential_chain, ACCOUNT_NAME 'storageaccount')",
             "ATTACH 'postgres://user:password@localhost:5432/ducklake' AS \"lake\" (TYPE ducklake, DATA_PATH 'abfss://lake@storageaccount.dfs.core.windows.net/raw')",
             ~s(USE "lake")
           ]
  end

  test "runs multiple DuckDB catalog attach statements" do
    resolved = duckdb_catalogs_resolved()
    {:ok, conn} = DuckDB.connect(resolved, duckdb_client: FakeClient)

    assert :ok = DuckDB.bootstrap(conn, resolved, [])

    assert statements() == [
             ~s(ATTACH '.favn/data/raw.duckdb' AS "raw"),
             ~s(ATTACH '.favn/data/mart.duckdb' AS "mart")
           ]
  end

  test "runs DuckLake bootstrap with ADLS and PostgreSQL secrets" do
    resolved = ducklake_postgres_resolved()
    {:ok, conn} = DuckDB.connect(resolved, duckdb_client: FakeClient)

    assert :ok = DuckDB.bootstrap(conn, resolved, [])

    assert statements() == [
             "LOAD ducklake",
             "LOAD postgres",
             "LOAD azure",
             "CREATE SECRET \"azure_adls\" (TYPE azure, PROVIDER credential_chain, ACCOUNT_NAME 'storageaccount', CHAIN 'cli;env', SCOPE 'abfss://lake@storageaccount.dfs.core.windows.net/')",
             "CREATE SECRET \"oceanos_meta\" (TYPE postgres, HOST 'pg.example.com', PORT 5432, DATABASE 'ducklake', USER 'ducklake_user', PASSWORD 'super-secret')",
             "ATTACH 'ducklake:postgres:sslmode=require' AS \"oceanos_lake\" (DATA_PATH 'abfss://lake@storageaccount.dfs.core.windows.net/data/', META_SECRET \"oceanos_meta\")",
             ~s(USE "oceanos_lake")
           ]
  end

  test "runs settings after extension load and before secrets, attach, and use" do
    resolved = ducklake_postgres_resolved(settings: [azure_transport_option_type: :curl])
    {:ok, conn} = DuckDB.connect(resolved, duckdb_client: FakeClient)

    assert :ok = DuckDB.bootstrap(conn, resolved, [])

    assert statements() == [
             "LOAD ducklake",
             "LOAD postgres",
             "LOAD azure",
             "SET azure_transport_option_type = 'curl'",
             "CREATE SECRET \"azure_adls\" (TYPE azure, PROVIDER credential_chain, ACCOUNT_NAME 'storageaccount', CHAIN 'cli;env', SCOPE 'abfss://lake@storageaccount.dfs.core.windows.net/')",
             "CREATE SECRET \"oceanos_meta\" (TYPE postgres, HOST 'pg.example.com', PORT 5432, DATABASE 'ducklake', USER 'ducklake_user', PASSWORD 'super-secret')",
             "ATTACH 'ducklake:postgres:sslmode=require' AS \"oceanos_lake\" (DATA_PATH 'abfss://lake@storageaccount.dfs.core.windows.net/data/', META_SECRET \"oceanos_meta\")",
             ~s(USE "oceanos_lake")
           ]
  end

  test "injects Azure PostgreSQL Entra token into temporary PostgreSQL secret" do
    resolved = ducklake_postgres_entra_resolved()
    {:ok, conn} = DuckDB.connect(resolved, duckdb_client: FakeClient)

    assert :ok = DuckDB.bootstrap(conn, resolved, azure_token_provider_module: FakeTokenProvider)

    assert {:token_auth,
            [
              type: :azure_postgres_entra,
              provider: :managed_identity,
              client_id: "client-1",
              endpoint: :auto
            ]} in TestSupport.events()

    assert statements() == [
             "LOAD ducklake",
             "LOAD postgres",
             "CREATE SECRET \"oceanos_meta\" (TYPE postgres, HOST 'pg.example.com', PORT 5432, DATABASE 'ducklake', USER 'ducklake_user', PASSWORD 'entra-token')",
             "ATTACH 'ducklake:postgres:sslmode=require' AS \"oceanos_lake\" (DATA_PATH 'abfss://lake@storageaccount.dfs.core.windows.net/data/', META_SECRET \"oceanos_meta\")",
             ~s(USE "oceanos_lake")
           ]
  end

  test "builds Azure PostgreSQL Entra bootstrap steps without fetching token" do
    resolved = ducklake_postgres_entra_resolved()

    assert {:ok, steps} =
             Bootstrap.build_steps(resolved, azure_token_provider_module: FakeTokenProvider)

    refute {:token_auth,
            [
              type: :azure_postgres_entra,
              provider: :managed_identity,
              client_id: "client-1",
              endpoint: :auto
            ]} in TestSupport.events()

    create_secret = Enum.find(steps, &(&1.id == "create_secret_oceanos_meta"))

    refute IO.iodata_to_binary(create_secret.statement) =~ "entra-token"
    assert IO.iodata_to_binary(create_secret.safe_statement) =~ "PASSWORD 'redacted'"
  end

  test "token acquisition failure returns redacted bootstrap error" do
    resolved = ducklake_postgres_entra_resolved()
    {:ok, conn} = DuckDB.connect(resolved, duckdb_client: FakeClient)

    assert {:error,
            %Error{
              type: :connection_error,
              operation: :bootstrap,
              message: "DuckDB connection bootstrap failed at create_secret_oceanos_meta",
              details: %{statement: safe_statement, adapter_details: adapter_details}
            }} =
             DuckDB.bootstrap(conn, resolved, azure_token_provider_module: FailingTokenProvider)

    assert safe_statement =~ "PASSWORD 'redacted'"
    refute safe_statement =~ "entra-token"
    refute inspect(adapter_details) =~ "entra-token"
    assert adapter_details.access_token == :redacted
    assert statements() == ["LOAD ducklake", "LOAD postgres"]
  end

  test "PostgreSQL Entra secret execution failure redacts fetched token" do
    resolved = ducklake_postgres_entra_resolved()
    {:ok, conn} = DuckDB.connect(resolved, duckdb_client: FakeClient)

    failing_sql =
      "CREATE SECRET \"oceanos_meta\" (TYPE postgres, HOST 'pg.example.com', PORT 5432, DATABASE 'ducklake', USER 'ducklake_user', PASSWORD 'entra-token')"

    TestSupport.put_mode(:bootstrap_fail_sql, failing_sql)

    assert {:error,
            %Error{
              operation: :bootstrap,
              details: %{
                statement: safe_statement,
                reason: reason,
                adapter_details: adapter_details
              }
            }} = DuckDB.bootstrap(conn, resolved, azure_token_provider_module: FakeTokenProvider)

    refute safe_statement =~ "entra-token"
    refute reason =~ "entra-token"
    refute inspect(adapter_details) =~ "entra-token"
    assert safe_statement =~ "PASSWORD 'redacted'"
    assert reason =~ "PASSWORD 'redacted'"
  end

  test "bootstrap failure reports failing step without exposing secret values" do
    {:ok, conn} = DuckDB.connect(resolved(), duckdb_client: FakeClient)

    secret_metadata = "postgres://user:password@localhost:5432/ducklake"

    failing_sql =
      "ATTACH '#{secret_metadata}' AS \"lake\" (TYPE ducklake, DATA_PATH 'abfss://lake@storageaccount.dfs.core.windows.net/raw')"

    TestSupport.put_mode(:bootstrap_fail_sql, failing_sql)

    assert {:error,
            %Error{
              operation: :bootstrap,
              connection: :warehouse,
              message: "DuckDB connection bootstrap failed at attach_lake",
              details: %{
                step: "attach_lake",
                bootstrap_kind: :ducklake_attach,
                statement: safe_statement,
                reason: reason,
                adapter_details: adapter_details
              }
            }} = DuckDB.bootstrap(conn, resolved(), [])

    refute safe_statement =~ secret_metadata
    refute safe_statement =~ "abfss://lake@storageaccount.dfs.core.windows.net/raw"
    refute reason =~ secret_metadata
    refute inspect(adapter_details) =~ secret_metadata
    assert safe_statement =~ "redacted"
    assert reason =~ "redacted"
  end

  test "setting execution failure reports bootstrap setting step" do
    resolved = settings_resolved(:curl)
    {:ok, conn} = DuckDB.connect(resolved, duckdb_client: FakeClient)

    TestSupport.put_mode(:bootstrap_fail_sql, "SET azure_transport_option_type = 'curl'")

    assert {:error,
            %Error{
              operation: :bootstrap,
              details: %{
                step: "set_azure_transport_option_type",
                bootstrap_kind: :set_setting,
                statement: "SET azure_transport_option_type = 'curl'"
              }
            }} = DuckDB.bootstrap(conn, resolved, [])
  end

  test "PostgreSQL secret bootstrap failure redacts password only" do
    resolved = ducklake_postgres_resolved()
    {:ok, conn} = DuckDB.connect(resolved, duckdb_client: FakeClient)

    failing_sql =
      "CREATE SECRET \"oceanos_meta\" (TYPE postgres, HOST 'pg.example.com', PORT 5432, DATABASE 'ducklake', USER 'ducklake_user', PASSWORD 'super-secret')"

    TestSupport.put_mode(:bootstrap_fail_sql, failing_sql)

    assert {:error,
            %Error{
              operation: :bootstrap,
              details: %{
                statement: safe_statement,
                reason: reason,
                adapter_details: adapter_details
              }
            }} = DuckDB.bootstrap(conn, resolved, [])

    refute safe_statement =~ "super-secret"
    refute reason =~ "super-secret"
    refute inspect(adapter_details) =~ "super-secret"
    assert safe_statement =~ "HOST 'pg.example.com'"
    assert safe_statement =~ "USER 'ducklake_user'"
    assert safe_statement =~ "PASSWORD 'redacted'"
  end

  test "invalid bootstrap config returns structured invalid config error" do
    {:ok, conn} = DuckDB.connect(resolved(), duckdb_client: FakeClient)

    resolved = %Resolved{
      resolved()
      | config: %{
          database: ":memory:",
          duckdb_bootstrap: [extensions: [load: ["invalid extension"]]]
        }
    }

    assert {:error,
            %Error{
              type: :invalid_config,
              operation: :bootstrap,
              connection: :warehouse,
              details: %{reason: reason}
            }} = DuckDB.bootstrap(conn, resolved, [])

    assert reason =~ "invalid_identifier"
  end

  test "malformed bootstrap config returns structured invalid config errors" do
    {:ok, conn} = DuckDB.connect(resolved(), duckdb_client: FakeClient)

    invalid_configs = [
      [extensions: :bad],
      [secrets: [azure_adls: :bad]],
      [:not_a_keyword_tuple],
      [secrets: [:not_a_secret_tuple]],
      [attach: :bad]
    ]

    for duckdb_bootstrap <- invalid_configs do
      resolved = %Resolved{
        resolved()
        | config: %{database: ":memory:", duckdb_bootstrap: duckdb_bootstrap}
      }

      assert {:error,
              %Error{
                type: :invalid_config,
                operation: :bootstrap,
                connection: :warehouse,
                details: %{reason: reason}
              }} = DuckDB.bootstrap(conn, resolved, [])

      assert is_binary(reason)
    end
  end

  defp resolved do
    %Resolved{
      name: :warehouse,
      adapter: DuckDB,
      module: __MODULE__,
      config: %{
        database: ":memory:",
        duckdb_bootstrap: [
          extensions: [
            install: [:ducklake, :postgres, :azure],
            load: [:ducklake, :postgres, :azure]
          ],
          secrets: [
            azure_adls: [
              type: :azure,
              provider: :credential_chain,
              account_name: "storageaccount"
            ]
          ],
          attach: [
            name: :lake,
            type: :ducklake,
            metadata: "postgres://user:password@localhost:5432/ducklake",
            data_path: "abfss://lake@storageaccount.dfs.core.windows.net/raw"
          ],
          use: :lake
        ]
      },
      secret_fields: [:duckdb_bootstrap]
    }
  end

  defp ducklake_postgres_resolved(opts \\ []) do
    settings = Keyword.get(opts, :settings, [])

    %Resolved{
      name: :warehouse,
      adapter: DuckDB,
      module: __MODULE__,
      config: %{
        database: ":memory:",
        duckdb_bootstrap: [
          extensions: [load: [:ducklake, :postgres, :azure]],
          settings: settings,
          secrets: [
            azure_adls: [
              type: :azure,
              provider: :credential_chain,
              account_name: "storageaccount",
              chain: [:cli, :env],
              scope: "abfss://lake@storageaccount.dfs.core.windows.net/"
            ],
            oceanos_meta: [
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
            name: :oceanos_lake,
            type: :ducklake,
            metadata: [type: :postgres, secret: :oceanos_meta],
            data_path: "abfss://lake@storageaccount.dfs.core.windows.net/data/"
          ],
          use: :oceanos_lake
        ]
      },
      secret_fields: [:duckdb_bootstrap]
    }
  end

  defp duckdb_catalogs_resolved do
    %Resolved{
      name: :important_lakehouse,
      adapter: DuckDB,
      module: __MODULE__,
      config: %{
        database: ":memory:",
        duckdb_bootstrap: [
          attach: [
            [type: :duckdb, name: :raw, path: ".favn/data/raw.duckdb"],
            [type: :duckdb, name: :mart, path: ".favn/data/mart.duckdb"]
          ]
        ]
      },
      secret_fields: []
    }
  end

  defp settings_resolved(value) do
    %Resolved{
      name: :warehouse,
      adapter: DuckDB,
      module: __MODULE__,
      config: %{
        database: ":memory:",
        duckdb_bootstrap: [
          extensions: [load: [:azure]],
          settings: [azure_transport_option_type: value]
        ]
      },
      secret_fields: [:duckdb_bootstrap]
    }
  end

  defp ducklake_postgres_entra_resolved do
    %Resolved{
      name: :warehouse,
      adapter: DuckDB,
      module: __MODULE__,
      config: %{
        database: ":memory:",
        duckdb_bootstrap: [
          extensions: [load: [:ducklake, :postgres]],
          secrets: [
            oceanos_meta: [
              type: :postgres,
              host: "pg.example.com",
              port: 5432,
              database: "ducklake",
              user: "ducklake_user",
              auth: [
                type: :azure_postgres_entra,
                provider: :managed_identity,
                client_id: "client-1",
                endpoint: :auto
              ],
              sslmode: :require
            ]
          ],
          attach: [
            name: :oceanos_lake,
            type: :ducklake,
            metadata: [type: :postgres, secret: :oceanos_meta],
            data_path: "abfss://lake@storageaccount.dfs.core.windows.net/data/"
          ],
          use: :oceanos_lake
        ]
      },
      secret_fields: [:duckdb_bootstrap]
    }
  end

  defp statements do
    TestSupport.events()
    |> Enum.flat_map(fn
      {:query, sql, []} -> [sql]
      _event -> []
    end)
  end
end
