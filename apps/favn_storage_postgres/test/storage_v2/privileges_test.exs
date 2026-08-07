defmodule FavnStoragePostgres.StorageV2.PrivilegesTest do
  use ExUnit.Case, async: false

  alias Ecto.Adapters.SQL
  alias FavnStoragePostgres.Config
  alias FavnStoragePostgres.Repo
  alias FavnStoragePostgres.RuntimePrivileges
  alias FavnStoragePostgres.StorageV2.Migrations

  @password "runtime-test-password"

  setup_all do
    url =
      System.get_env("FAVN_DATABASE_URL") ||
        raise "FAVN_DATABASE_URL is required for PostgreSQL storage tests"

    {:ok, options} = Config.repo_options(url: url, ssl_mode: :disable, pool_size: 2)
    start_supervised!({Repo, options})
    :ok = Migrations.migrate!(Repo)

    role = "favn_runtime_test_" <> random_id()
    quoted_role = RuntimePrivileges.quote_identifier!(role)

    SQL.query!(
      Repo,
      "CREATE ROLE #{quoted_role} LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT PASSWORD '#{@password}'",
      []
    )

    :ok = RuntimePrivileges.grant_runtime!(Repo, role)

    {:ok, role: role, url: url}
  end

  test "runtime role can perform DML but cannot mutate schema authority or create objects",
       context do
    quoted_role = RuntimePrivileges.quote_identifier!(context.role)
    %{rows: [[database]]} = SQL.query!(Repo, "SELECT current_database()", [])
    quoted_database = RuntimePrivileges.quote_identifier!(database)

    SQL.query!(Repo, "GRANT MAINTAIN ON favn_control.workspaces TO #{quoted_role}", [])

    SQL.query!(
      Repo,
      "GRANT CONNECT ON DATABASE #{quoted_database} TO #{quoted_role} WITH GRANT OPTION",
      []
    )

    SQL.query!(
      Repo,
      "GRANT SELECT ON favn_control.workspaces TO #{quoted_role} WITH GRANT OPTION",
      []
    )

    SQL.query!(Repo, "CREATE SEQUENCE favn_control.runtime_privilege_probe", [])

    SQL.query!(
      Repo,
      "GRANT UPDATE ON SEQUENCE favn_control.runtime_privilege_probe TO #{quoted_role}",
      []
    )

    SQL.query!(
      Repo,
      "ALTER DEFAULT PRIVILEGES IN SCHEMA favn_control GRANT SELECT ON TABLES TO PUBLIC",
      []
    )

    SQL.query!(Repo, "GRANT SELECT ON favn_control.workspaces TO PUBLIC", [])
    refute RuntimePrivileges.diagnostics(Repo, context.role).safe?
    assert {:ok, :converged} = RuntimePrivileges.converge_runtime(Repo, context.role)

    {:ok, connection} = Postgrex.start_link(connection_options(context.url, context.role))

    try do
      assert {:ok, %{rows: [[1]]}} = Postgrex.query(connection, "SELECT 1", [])

      assert {:ok, _result} =
               Postgrex.query(
                 connection,
                 "UPDATE favn_control.workspaces SET updated_at = updated_at WHERE false",
                 []
               )

      assert {:error, %Postgrex.Error{postgres: %{code: :insufficient_privilege}}} =
               Postgrex.query(
                 connection,
                 "DELETE FROM favn_control.schema_migrations WHERE false",
                 []
               )

      assert {:error, %Postgrex.Error{postgres: %{code: :insufficient_privilege}}} =
               Postgrex.query(
                 connection,
                 "CREATE TABLE favn_control.runtime_role_must_not_create (id integer)",
                 []
               )

      assert {:error, %Postgrex.Error{postgres: %{code: :insufficient_privilege}}} =
               Postgrex.query(
                 connection,
                 "CREATE TABLE public.runtime_role_must_not_create (id integer)",
                 []
               )

      assert {:error, %Postgrex.Error{postgres: %{code: :insufficient_privilege}}} =
               Postgrex.query(
                 connection,
                 "CREATE TEMP TABLE runtime_role_must_not_create (id integer)",
                 []
               )

      assert {:error, %Postgrex.Error{postgres: %{code: :insufficient_privilege}}} =
               Postgrex.query(connection, "TRUNCATE favn_control.workspaces", [])

      assert %{safe?: true, least_privilege?: true} =
               RuntimePrivileges.diagnostics(Repo, context.role)
    after
      GenServer.stop(connection)
      SQL.query!(Repo, "DROP SEQUENCE IF EXISTS favn_control.runtime_privilege_probe", [])
      cleanup_role(context.role)
    end
  end

  defp cleanup_role(role) do
    quoted_role = RuntimePrivileges.quote_identifier!(role)
    SQL.query!(Repo, "DROP OWNED BY #{quoted_role}", [])
    SQL.query!(Repo, "DROP ROLE #{quoted_role}", [])
  end

  defp connection_options(url, role) do
    uri = URI.parse(url)

    [
      hostname: uri.host,
      port: uri.port || 5432,
      database: String.trim_leading(uri.path || "", "/"),
      username: role,
      password: @password,
      ssl: false
    ]
  end

  defp random_id, do: :crypto.strong_rand_bytes(5) |> Base.encode16(case: :lower)
end
