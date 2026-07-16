defmodule FavnDuckdbADBC.SQLAdapterDuckDBADBCAzurePoolTest do
  use ExUnit.Case, async: false

  alias Favn.Azure.Credentials
  alias Favn.Azure.Credentials.Supervisor, as: CredentialsSupervisor
  alias Favn.Connection.{Registry, Resolved}
  alias Favn.SQL.Adapter.DuckDB.ADBC
  alias Favn.SQL.{Client, PoolConfig, SessionPool}
  alias FavnDuckdbADBC.TestSupport

  @moduletag :tmp_dir
  @postgres_resource "https://ossrdbms-aad.database.windows.net"
  @provider_env :azure_pool_test_provider

  defmodule RotatingManagedIdentityProvider do
    @behaviour Favn.Azure.CredentialProvider

    alias Favn.Azure.{Token, TokenError}

    @impl true
    def fetch_token(request, _opts) do
      provider = Application.fetch_env!(:favn_duckdb_adbc, :azure_pool_test_provider)

      Agent.get_and_update(provider, fn
        %{responses: [{access_token, expires_at} | rest], calls: calls} = state ->
          result =
            if request.resource == "https://ossrdbms-aad.database.windows.net" do
              Token.new(access_token, expires_at)
            else
              {:error,
               %TokenError{
                 type: :invalid_config,
                 message: "unexpected Azure resource",
                 details: %{reason: :unexpected_resource}
               }}
            end

          {result, %{state | responses: rest, calls: calls + 1}}
      end)
    end
  end

  defmodule FakeClient do
    use FavnDuckdbADBC.TestSupport.FakeClient

    alias FavnDuckdbADBC.TestSupport

    @impl true
    def open(database, opts) do
      db_ref = make_ref()
      TestSupport.record({:open, db_ref, database, opts})
      {:ok, db_ref}
    end

    @impl true
    def connection(db_ref) do
      conn_ref = make_ref()
      TestSupport.record({:connection, db_ref, conn_ref})
      {:ok, conn_ref}
    end

    @impl true
    def execute(conn_ref, sql, params) do
      TestSupport.record({:execute, conn_ref, sql, params})
      {:ok, nil}
    end
  end

  setup do
    :ok = SessionPool.reset()
    TestSupport.start_events()
    now = DateTime.utc_now()
    {:ok, clock} = Agent.start_link(fn -> now end)

    {:ok, provider} =
      Agent.start_link(fn ->
        %{
          responses: [
            {"managed-identity-token-1", DateTime.add(now, 60, :second)},
            {"managed-identity-token-2", DateTime.add(now, 3_600, :second)}
          ],
          calls: 0
        }
      end)

    Application.put_env(:favn_duckdb_adbc, @provider_env, provider)

    start_supervised!(
      {CredentialsSupervisor,
       refresh_before_seconds: 0, clock: fn -> Agent.get(clock, & &1) end}
    )

    on_exit(fn ->
      SessionPool.reset()
      Application.delete_env(:favn_duckdb_adbc, @provider_env)
      TestSupport.reset()
    end)

    {:ok, provider: provider, clock: clock}
  end

  test "PostgreSQL Entra token refresh replaces an idle ADBC physical session", %{
    tmp_dir: tmp_dir,
    provider: provider,
    clock: clock
  } do
    script = write_postgres_secret_script!(tmp_dir)
    resolved = resolved_connection(script)
    registry_name = unique_name("Registry")

    start_supervised!({Registry, name: registry_name, connections: %{warehouse: resolved}})

    connect_opts = [
      registry_name: registry_name,
      required_resources: [:source_metadata],
      duckdb_adbc_client: FakeClient
    ]

    assert {:ok, first} = Client.connect(:warehouse, connect_opts)
    first_conn_ref = first.conn.conn_ref
    assert :ok = Client.disconnect(first)
    assert provider_calls(provider) == 1
    assert %{idle: 1} = SessionPool.diagnostics()

    assert {:ok, reused} = Client.connect(:warehouse, connect_opts)
    assert reused.conn.conn_ref == first_conn_ref
    assert :ok = Client.disconnect(reused)
    assert provider_calls(provider) == 1

    Agent.update(clock, &DateTime.add(&1, 61, :second))

    assert {:ok, refreshed} = Client.connect(:warehouse, connect_opts)
    refute refreshed.conn.conn_ref == first_conn_ref
    assert provider_calls(provider) == 2
    assert :ok = Client.disconnect(refreshed)

    assert postgres_secret_statements() == [
             postgres_secret_statement("managed-identity-token-1"),
             postgres_secret_statement("managed-identity-token-2")
           ]
  end

  defp resolved_connection(script) do
    token =
      Credentials.token_ref(@postgres_resource,
        provider: RotatingManagedIdentityProvider
      )

    %Resolved{
      name: :warehouse,
      adapter: ADBC,
      module: __MODULE__,
      config: %{
        open: %{database: ":memory:"},
        duckdb: %{
          resources: %{
            source_metadata: %{
              file: script,
              params: %{
                host: "postgres.example.com",
                port: 5432,
                database: "metadata",
                user: "favn-runner",
                access_token: token,
                data_path: "az://data/source"
              }
            }
          }
        },
        pool: %PoolConfig{enabled: true, max_idle_per_key: 1, idle_timeout_ms: 60_000},
        admission_timeout_ms: 2_000
      }
    }
  end

  defp write_postgres_secret_script!(tmp_dir) do
    path = Path.join(tmp_dir, "ducklake-postgres-entra.sql")
    File.write!(path, postgres_secret_template())
    path
  end

  defp postgres_secret_template do
    """
    CREATE OR REPLACE SECRET source_metadata (
      TYPE postgres,
      HOST @host,
      PORT @port,
      DATABASE @database,
      USER @user,
      PASSWORD @access_token
    );
    ATTACH 'ducklake:postgres:sslmode=require' AS source (
      DATA_PATH @data_path,
      META_SECRET source_metadata
    );
    """
    |> String.trim()
  end

  defp postgres_secret_statement(token) do
    postgres_secret_template()
    |> String.replace("@host", "'postgres.example.com'")
    |> String.replace("@port", "5432")
    |> String.replace("@database", "'metadata'")
    |> String.replace("@user", "'favn-runner'")
    |> String.replace("@access_token", "'#{token}'")
    |> String.replace("@data_path", "'az://data/source'")
  end

  defp postgres_secret_statements do
    TestSupport.events()
    |> Enum.flat_map(fn
      {:execute, _conn_ref, sql, []} when is_binary(sql) -> [sql]
      _event -> []
    end)
  end

  defp provider_calls(provider), do: Agent.get(provider, & &1.calls)

  defp unique_name(suffix) do
    Module.concat(__MODULE__, "#{suffix}#{System.unique_integer([:positive])}")
  end
end
