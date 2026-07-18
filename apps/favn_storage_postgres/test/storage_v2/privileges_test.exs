defmodule FavnStoragePostgres.StorageV2.PrivilegesTest do
  use ExUnit.Case, async: false

  alias Ecto.Adapters.SQL
  alias FavnStoragePostgres.Config
  alias FavnStoragePostgres.Privileges
  alias FavnStoragePostgres.Repo
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
    parent_role = role <> "_parent"
    quoted_role = Privileges.quote_identifier!(role)
    quoted_parent_role = Privileges.quote_identifier!(parent_role)

    %{rows: [[database]]} = SQL.query!(Repo, "SELECT current_database()", [])
    quoted_database = Privileges.quote_identifier!(database)

    SQL.query!(
      Repo,
      "CREATE ROLE #{quoted_role} LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT PASSWORD '#{@password}'",
      []
    )

    SQL.query!(Repo, "CREATE ROLE #{quoted_parent_role} NOLOGIN", [])
    SQL.query!(Repo, "GRANT #{quoted_parent_role} TO #{quoted_role}", [])
    SQL.query!(Repo, "GRANT CREATE ON DATABASE #{quoted_database} TO #{quoted_role}", [])
    SQL.query!(Repo, "GRANT CREATE ON SCHEMA public TO #{quoted_role}", [])
    SQL.query!(Repo, "GRANT ALL ON SCHEMA favn_control TO #{quoted_role}", [])
    SQL.query!(Repo, "GRANT ALL ON ALL TABLES IN SCHEMA favn_control TO #{quoted_role}", [])
    SQL.query!(Repo, "GRANT ALL ON ALL SEQUENCES IN SCHEMA favn_control TO #{quoted_role}", [])

    :ok = Privileges.grant_runtime!(Repo, role)

    {:ok, role: role, parent_role: parent_role, url: url}
  end

  test "runtime role can perform DML but cannot mutate schema authority or create objects",
       context do
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
               Postgrex.query(connection, "TRUNCATE favn_control.workspaces", [])

      assert {:error, %Postgrex.Error{postgres: %{code: :insufficient_privilege}}} =
               Postgrex.query(connection, "SET ROLE #{context.parent_role}", [])

      assert %{safe?: true, least_privilege?: true} = Privileges.diagnostics(Repo, context.role)
    after
      GenServer.stop(connection)
      cleanup_roles(context.role, context.parent_role)
    end
  end

  defp cleanup_roles(role, parent_role) do
    quoted_role = Privileges.quote_identifier!(role)
    quoted_parent_role = Privileges.quote_identifier!(parent_role)
    SQL.query!(Repo, "DROP OWNED BY #{quoted_role}", [])
    SQL.query!(Repo, "DROP ROLE #{quoted_role}", [])
    SQL.query!(Repo, "DROP ROLE #{quoted_parent_role}", [])
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
