defmodule FavnStoragePostgres.Bootstrap.RolePolicyTest do
  use ExUnit.Case, async: false

  alias FavnStoragePostgres.Bootstrap.RolePolicy

  setup do
    url =
      System.get_env("FAVN_DATABASE_URL") ||
        raise "FAVN_DATABASE_URL is required for PostgreSQL storage tests"

    options = url |> Ecto.Repo.Supervisor.parse_url() |> Keyword.put(:ssl, false)
    connection = start_supervised!({Postgrex, options})

    role = "favn_role_policy_" <> random_id()
    parent = role <> "_parent"
    {:ok, quoted_role} = RolePolicy.quote_identifier(role)
    {:ok, quoted_parent} = RolePolicy.quote_identifier(parent)

    Postgrex.query!(connection, "CREATE ROLE #{quoted_parent} NOLOGIN", [])

    Postgrex.query!(
      connection,
      "CREATE ROLE #{quoted_role} LOGIN CREATEDB CREATEROLE INHERIT NOREPLICATION NOBYPASSRLS",
      []
    )

    Postgrex.query!(connection, "GRANT #{quoted_parent} TO #{quoted_role}", [])

    on_exit(fn ->
      Postgrex.query!(connection, "DROP ROLE IF EXISTS #{quoted_role}", [])
      Postgrex.query!(connection, "DROP ROLE IF EXISTS #{quoted_parent}", [])
    end)

    {:ok, connection: connection, role: role}
  end

  test "bootstrap hardening removes CREATEROLE and memberships without granting new authority",
       context do
    assert {:ok,
            %{
              create_database?: true,
              create_role?: true,
              inherit?: true,
              memberships: [_parent]
            }} = RolePolicy.status(context.connection, context.role)

    assert {:ok, :hardened} = RolePolicy.ensure_hardened(context.connection, context.role)

    assert {:ok,
            %{
              safe?: true,
              login?: true,
              superuser?: false,
              create_database?: false,
              create_role?: false,
              inherit?: false,
              replication?: false,
              bypass_rls?: false,
              memberships: []
            }} = RolePolicy.status(context.connection, context.role)

    assert {:ok, :exact} = RolePolicy.ensure_hardened(context.connection, context.role)
  end

  test "invalid role identifiers are rejected before SQL is assembled" do
    assert {:error, :invalid_role} = RolePolicy.quote_identifier("role; DROP ROLE postgres")
    assert {:error, :invalid_role} = RolePolicy.quote_identifier("UpperCase")
  end

  test "pre-mapping hardening preserves NOLOGIN until identity attachment", context do
    {:ok, quoted_role} = RolePolicy.quote_identifier(context.role)
    Postgrex.query!(context.connection, "ALTER ROLE #{quoted_role} NOLOGIN", [])

    assert {:ok, :hardened} =
             RolePolicy.ensure_safe_before_identity_mapping(context.connection, context.role)

    assert {:ok,
            %{
              login?: false,
              create_database?: false,
              create_role?: false,
              memberships: []
            }} = RolePolicy.status(context.connection, context.role)

    assert {:ok, :hardened} = RolePolicy.ensure_hardened(context.connection, context.role)

    assert {:ok, %{login?: true, safe?: true}} =
             RolePolicy.status(context.connection, context.role)
  end

  defp random_id, do: :crypto.strong_rand_bytes(5) |> Base.encode16(case: :lower)
end
