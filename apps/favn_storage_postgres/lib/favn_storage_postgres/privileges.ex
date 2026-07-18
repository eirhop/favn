defmodule FavnStoragePostgres.Privileges do
  @moduledoc false

  alias Ecto.Adapters.SQL

  @spec grant_runtime!(module() | pid(), String.t()) :: :ok
  def grant_runtime!(repo, role) when is_binary(role) do
    quoted_role = quote_identifier!(role)
    database = current_database!(repo)
    quoted_database = quote_catalog_identifier(database)

    revoke_memberships!(repo, role, quoted_role)

    [
      "ALTER ROLE #{quoted_role} NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT",
      "REVOKE CREATE ON DATABASE #{quoted_database} FROM PUBLIC",
      "REVOKE CREATE ON DATABASE #{quoted_database} FROM #{quoted_role}",
      "GRANT CONNECT ON DATABASE #{quoted_database} TO #{quoted_role}",
      "REVOKE CREATE ON SCHEMA public FROM PUBLIC",
      "REVOKE ALL ON SCHEMA public FROM #{quoted_role}",
      "REVOKE ALL ON SCHEMA favn_control FROM PUBLIC",
      "REVOKE ALL ON ALL TABLES IN SCHEMA favn_control FROM PUBLIC",
      "REVOKE ALL ON ALL SEQUENCES IN SCHEMA favn_control FROM PUBLIC",
      "REVOKE ALL ON SCHEMA favn_control FROM #{quoted_role}",
      "REVOKE ALL ON ALL TABLES IN SCHEMA favn_control FROM #{quoted_role}",
      "REVOKE ALL ON ALL SEQUENCES IN SCHEMA favn_control FROM #{quoted_role}",
      "ALTER DEFAULT PRIVILEGES IN SCHEMA favn_control REVOKE ALL ON TABLES FROM #{quoted_role}",
      "ALTER DEFAULT PRIVILEGES IN SCHEMA favn_control REVOKE ALL ON SEQUENCES FROM #{quoted_role}",
      "GRANT USAGE ON SCHEMA favn_control TO #{quoted_role}",
      "GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA favn_control TO #{quoted_role}",
      "GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA favn_control TO #{quoted_role}",
      "REVOKE INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER ON favn_control.schema_migrations FROM #{quoted_role}",
      "GRANT SELECT ON favn_control.schema_migrations TO #{quoted_role}",
      "ALTER DEFAULT PRIVILEGES IN SCHEMA favn_control GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO #{quoted_role}",
      "ALTER DEFAULT PRIVILEGES IN SCHEMA favn_control GRANT USAGE, SELECT ON SEQUENCES TO #{quoted_role}"
    ]
    |> Enum.each(&SQL.query!(repo, &1, []))

    case diagnostics(repo, role) do
      %{safe?: true} -> :ok
      diagnostics -> raise "runtime role privilege convergence failed: #{inspect(diagnostics)}"
    end
  end

  @spec current_role_diagnostics(module() | pid()) :: map()
  def current_role_diagnostics(repo) do
    %{rows: [[role]]} = SQL.query!(repo, "SELECT current_user", [])
    diagnostics(repo, role)
  end

  @spec diagnostics(module() | pid(), String.t()) :: map()
  def diagnostics(repo, role) when is_binary(role) do
    %{rows: [row]} =
      SQL.query!(
        repo,
        """
        SELECT r.rolname,
               r.rolsuper, r.rolcreatedb, r.rolcreaterole, r.rolinherit,
               has_database_privilege(r.oid, current_database(), 'CONNECT'),
               has_database_privilege(r.oid, current_database(), 'CREATE'),
               has_schema_privilege(r.oid, 'public', 'CREATE'),
               has_schema_privilege(r.oid, 'favn_control', 'USAGE'),
               has_schema_privilege(r.oid, 'favn_control', 'CREATE'),
               EXISTS (SELECT 1 FROM pg_catalog.pg_auth_members m WHERE m.member = r.oid),
               COALESCE((
                 SELECT bool_and(
                   has_table_privilege(r.oid, format('%I.%I', n.nspname, c.relname), 'SELECT')
                 )
                 FROM pg_catalog.pg_class c
                 JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                 WHERE n.nspname = 'favn_control' AND c.relkind IN ('r', 'p')
               ), false),
               COALESCE((
                 SELECT bool_and(
                   has_table_privilege(r.oid, format('%I.%I', n.nspname, c.relname),
                     'INSERT,UPDATE,DELETE')
                 )
                 FROM pg_catalog.pg_class c
                 JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                 WHERE n.nspname = 'favn_control' AND c.relkind IN ('r', 'p')
                   AND c.relname <> 'schema_migrations'
               ), false),
               has_table_privilege(r.oid, 'favn_control.schema_migrations',
                 'INSERT,UPDATE,DELETE,TRUNCATE'),
               COALESCE((
                 SELECT bool_and(
                   has_sequence_privilege(r.oid, format('%I.%I', n.nspname, c.relname),
                     'USAGE,SELECT')
                 )
                 FROM pg_catalog.pg_class c
                 JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                 WHERE n.nspname = 'favn_control' AND c.relkind = 'S'
               ), true)
        FROM pg_catalog.pg_roles r
        WHERE r.rolname = $1
        """,
        [role]
      )

    [
      role,
      superuser?,
      create_database?,
      create_role?,
      inherit?,
      connect?,
      database_create?,
      public_create?,
      control_usage?,
      control_create?,
      member_of_roles?,
      table_select?,
      table_dml?,
      migrations_dml?,
      sequence_use?
    ] = row

    safe? =
      connect? and control_usage? and table_select? and table_dml? and sequence_use? and
        not superuser? and not create_database? and not create_role? and not inherit? and
        not database_create? and not public_create? and not control_create? and
        not member_of_roles? and not migrations_dml?

    %{
      role: role,
      safe?: safe?,
      connect?: connect?,
      least_privilege?:
        not superuser? and not create_database? and not create_role? and not inherit? and
          not database_create? and not public_create? and not control_create? and
          not member_of_roles?,
      schema_access?: control_usage? and table_select? and table_dml? and sequence_use?,
      schema_migrations_read_only?: not migrations_dml?
    }
  end

  @spec quote_identifier!(String.t()) :: String.t()
  def quote_identifier!(value) when is_binary(value) do
    if Regex.match?(~r/^[a-z_][a-z0-9_]{0,62}$/, value) do
      ~s("#{value}")
    else
      raise ArgumentError, "invalid PostgreSQL role name"
    end
  end

  defp current_database!(repo) do
    %{rows: [[database]]} = SQL.query!(repo, "SELECT current_database()", [])
    database
  end

  defp revoke_memberships!(repo, role, quoted_role) do
    %{rows: rows} =
      SQL.query!(
        repo,
        """
        SELECT parent.rolname
        FROM pg_catalog.pg_auth_members membership
        JOIN pg_catalog.pg_roles member ON member.oid = membership.member
        JOIN pg_catalog.pg_roles parent ON parent.oid = membership.roleid
        WHERE member.rolname = $1
        ORDER BY parent.rolname
        """,
        [role]
      )

    Enum.each(rows, fn [parent_role] ->
      SQL.query!(repo, "REVOKE #{quote_catalog_identifier(parent_role)} FROM #{quoted_role}", [])
    end)
  end

  defp quote_catalog_identifier(value) when is_binary(value),
    do: ~s("#{String.replace(value, "\"", "\"\"")}")
end
