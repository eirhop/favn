defmodule FavnStoragePostgres.RuntimePrivileges do
  @moduledoc false

  alias Ecto.Adapters.SQL

  @spec grant_runtime!(module() | pid(), String.t()) :: :ok
  def grant_runtime!(repo, role) when is_binary(role) do
    case converge_runtime(repo, role) do
      {:ok, _status} ->
        :ok

      {:error, diagnostics} ->
        raise "runtime privilege convergence failed: #{inspect(diagnostics)}"
    end
  end

  @spec converge_runtime(module() | pid(), String.t()) ::
          {:ok, :exact | :converged} | {:error, map()}
  def converge_runtime(repo, role) when is_binary(role) do
    case diagnostics(repo, role) do
      %{safe?: true} ->
        {:ok, :exact}

      _not_ready ->
        apply_runtime_grants(repo, role)

        case diagnostics(repo, role) do
          %{safe?: true} -> {:ok, :converged}
          diagnostics -> {:error, diagnostics}
        end
    end
  end

  defp apply_runtime_grants(repo, role) do
    quoted_role = quote_identifier!(role)

    [
      "REVOKE ALL ON SCHEMA favn_control FROM PUBLIC",
      "REVOKE ALL ON ALL TABLES IN SCHEMA favn_control FROM PUBLIC",
      "REVOKE ALL ON ALL SEQUENCES IN SCHEMA favn_control FROM PUBLIC",
      "REVOKE ALL ON SCHEMA favn_control FROM #{quoted_role}",
      "REVOKE ALL ON ALL TABLES IN SCHEMA favn_control FROM #{quoted_role}",
      "REVOKE ALL ON ALL SEQUENCES IN SCHEMA favn_control FROM #{quoted_role}",
      "ALTER DEFAULT PRIVILEGES IN SCHEMA favn_control REVOKE ALL ON TABLES FROM #{quoted_role}",
      "ALTER DEFAULT PRIVILEGES IN SCHEMA favn_control REVOKE ALL ON SEQUENCES FROM #{quoted_role}",
      "ALTER DEFAULT PRIVILEGES IN SCHEMA favn_control REVOKE ALL ON TABLES FROM PUBLIC",
      "ALTER DEFAULT PRIVILEGES IN SCHEMA favn_control REVOKE ALL ON SEQUENCES FROM PUBLIC",
      "GRANT USAGE ON SCHEMA favn_control TO #{quoted_role}",
      "GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA favn_control TO #{quoted_role}",
      "GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA favn_control TO #{quoted_role}",
      "REVOKE INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER ON favn_control.schema_migrations FROM #{quoted_role}",
      "GRANT SELECT ON favn_control.schema_migrations TO #{quoted_role}",
      "ALTER DEFAULT PRIVILEGES IN SCHEMA favn_control GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO #{quoted_role}",
      "ALTER DEFAULT PRIVILEGES IN SCHEMA favn_control GRANT USAGE, SELECT ON SEQUENCES TO #{quoted_role}"
    ]
    |> Enum.each(&SQL.query!(repo, &1, []))
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
               r.rolreplication, r.rolbypassrls,
               EXISTS (
                 SELECT 1
                 FROM pg_catalog.pg_database database
                 CROSS JOIN LATERAL pg_catalog.aclexplode(
                   COALESCE(database.datacl, pg_catalog.acldefault('d', database.datdba))
                 ) acl
                 WHERE database.datname = pg_catalog.current_database()
                   AND acl.grantee = r.oid
                   AND acl.privilege_type = 'CONNECT'
                   AND NOT acl.is_grantable
               ),
               has_database_privilege(r.oid, current_database(), 'CREATE'),
               has_database_privilege(r.oid, current_database(), 'TEMPORARY'),
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
                   has_table_privilege(r.oid, format('%I.%I', n.nspname, c.relname), 'INSERT')
                   AND has_table_privilege(r.oid, format('%I.%I', n.nspname, c.relname), 'UPDATE')
                   AND has_table_privilege(r.oid, format('%I.%I', n.nspname, c.relname), 'DELETE')
                 )
                 FROM pg_catalog.pg_class c
                 JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                 WHERE n.nspname = 'favn_control' AND c.relkind IN ('r', 'p')
                   AND c.relname <> 'schema_migrations'
               ), false),
               has_table_privilege(r.oid, 'favn_control.schema_migrations',
                 'INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER,MAINTAIN'),
               COALESCE((
                 SELECT bool_or(
                   has_table_privilege(r.oid, format('%I.%I', n.nspname, c.relname),
                     'TRUNCATE,REFERENCES,TRIGGER,MAINTAIN')
                 )
                 FROM pg_catalog.pg_class c
                 JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                 WHERE n.nspname = 'favn_control' AND c.relkind IN ('r', 'p')
               ), false),
               COALESCE((
                 SELECT bool_and(
                   has_sequence_privilege(r.oid, format('%I.%I', n.nspname, c.relname), 'USAGE')
                   AND has_sequence_privilege(r.oid, format('%I.%I', n.nspname, c.relname), 'SELECT')
                 )
                 FROM pg_catalog.pg_class c
                 JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                 WHERE n.nspname = 'favn_control' AND c.relkind = 'S'
               ), true),
               COALESCE((
                 SELECT bool_or(
                   has_sequence_privilege(r.oid, format('%I.%I', n.nspname, c.relname), 'UPDATE')
                 )
                 FROM pg_catalog.pg_class c
                 JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                 WHERE n.nspname = 'favn_control' AND c.relkind = 'S'
               ), false),
               EXISTS (
                 SELECT 1
                 FROM pg_catalog.pg_default_acl default_acl
                 JOIN pg_catalog.pg_namespace n ON n.oid = default_acl.defaclnamespace
                 CROSS JOIN LATERAL pg_catalog.aclexplode(default_acl.defaclacl) acl
                 WHERE n.nspname = 'favn_control'
                   AND default_acl.defaclobjtype = 'r'
                   AND default_acl.defaclrole = n.nspowner
                   AND acl.grantee = r.oid
                   AND acl.privilege_type = 'SELECT'
               ) AND EXISTS (
                 SELECT 1
                 FROM pg_catalog.pg_default_acl default_acl
                 JOIN pg_catalog.pg_namespace n ON n.oid = default_acl.defaclnamespace
                 CROSS JOIN LATERAL pg_catalog.aclexplode(default_acl.defaclacl) acl
                 WHERE n.nspname = 'favn_control'
                   AND default_acl.defaclobjtype = 'r'
                   AND default_acl.defaclrole = n.nspowner
                   AND acl.grantee = r.oid
                   AND acl.privilege_type = 'INSERT'
               ) AND EXISTS (
                 SELECT 1
                 FROM pg_catalog.pg_default_acl default_acl
                 JOIN pg_catalog.pg_namespace n ON n.oid = default_acl.defaclnamespace
                 CROSS JOIN LATERAL pg_catalog.aclexplode(default_acl.defaclacl) acl
                 WHERE n.nspname = 'favn_control'
                   AND default_acl.defaclobjtype = 'r'
                   AND default_acl.defaclrole = n.nspowner
                   AND acl.grantee = r.oid
                   AND acl.privilege_type = 'UPDATE'
               ) AND EXISTS (
                 SELECT 1
                 FROM pg_catalog.pg_default_acl default_acl
                 JOIN pg_catalog.pg_namespace n ON n.oid = default_acl.defaclnamespace
                 CROSS JOIN LATERAL pg_catalog.aclexplode(default_acl.defaclacl) acl
                 WHERE n.nspname = 'favn_control'
                   AND default_acl.defaclobjtype = 'r'
                   AND default_acl.defaclrole = n.nspowner
                   AND acl.grantee = r.oid
                   AND acl.privilege_type = 'DELETE'
               ),
               EXISTS (
                 SELECT 1
                 FROM pg_catalog.pg_default_acl default_acl
                 JOIN pg_catalog.pg_namespace n ON n.oid = default_acl.defaclnamespace
                 CROSS JOIN LATERAL pg_catalog.aclexplode(default_acl.defaclacl) acl
                 WHERE n.nspname = 'favn_control'
                   AND default_acl.defaclobjtype = 'S'
                   AND default_acl.defaclrole = n.nspowner
                   AND acl.grantee = r.oid
                   AND acl.privilege_type = 'USAGE'
               ) AND EXISTS (
                 SELECT 1
                 FROM pg_catalog.pg_default_acl default_acl
                 JOIN pg_catalog.pg_namespace n ON n.oid = default_acl.defaclnamespace
                 CROSS JOIN LATERAL pg_catalog.aclexplode(default_acl.defaclacl) acl
                 WHERE n.nspname = 'favn_control'
                   AND default_acl.defaclobjtype = 'S'
                   AND default_acl.defaclrole = n.nspowner
                   AND acl.grantee = r.oid
                   AND acl.privilege_type = 'SELECT'
               ),
               EXISTS (
                 SELECT 1
                 FROM pg_catalog.pg_default_acl default_acl
                 JOIN pg_catalog.pg_namespace n ON n.oid = default_acl.defaclnamespace
                 CROSS JOIN LATERAL pg_catalog.aclexplode(default_acl.defaclacl) acl
                 WHERE n.nspname = 'favn_control'
                   AND default_acl.defaclrole = n.nspowner
                   AND acl.grantee = r.oid
                   AND (
                     (default_acl.defaclobjtype = 'r' AND
                       acl.privilege_type NOT IN ('SELECT', 'INSERT', 'UPDATE', 'DELETE'))
                     OR (default_acl.defaclobjtype = 'S' AND
                       acl.privilege_type NOT IN ('USAGE', 'SELECT'))
                     OR acl.is_grantable
                   )
               ),
               EXISTS (
                 SELECT 1
                 FROM pg_catalog.pg_database database
                 CROSS JOIN LATERAL pg_catalog.aclexplode(
                   COALESCE(database.datacl, pg_catalog.acldefault('d', database.datdba))
                 ) acl
                 WHERE database.datname = pg_catalog.current_database()
                   AND acl.grantee = r.oid
                   AND acl.is_grantable
               ) OR EXISTS (
                 SELECT 1
                 FROM pg_catalog.pg_namespace n
                 CROSS JOIN LATERAL pg_catalog.aclexplode(
                   COALESCE(n.nspacl, pg_catalog.acldefault('n', n.nspowner))
                 ) acl
                 WHERE n.nspname = 'favn_control'
                   AND acl.grantee = r.oid
                   AND acl.is_grantable
               ) OR EXISTS (
                 SELECT 1
                 FROM pg_catalog.pg_class c
                 JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                 CROSS JOIN LATERAL pg_catalog.aclexplode(
                   COALESCE(c.relacl, pg_catalog.acldefault(
                     CASE WHEN c.relkind = 'S' THEN 'S'::"char" ELSE 'r'::"char" END,
                     c.relowner
                   ))
                 ) acl
                 WHERE n.nspname = 'favn_control'
                   AND c.relkind IN ('r', 'p', 'S')
                   AND acl.grantee = r.oid
                   AND acl.is_grantable
               ),
               EXISTS (
                 SELECT 1
                 FROM pg_catalog.pg_default_acl default_acl
                 JOIN pg_catalog.pg_namespace n ON n.oid = default_acl.defaclnamespace
                 CROSS JOIN LATERAL pg_catalog.aclexplode(default_acl.defaclacl) acl
                 WHERE n.nspname = 'favn_control'
                   AND default_acl.defaclrole = n.nspowner
                   AND default_acl.defaclobjtype IN ('r', 'S')
                   AND acl.grantee = 0
               ),
               EXISTS (
                 SELECT 1
                 FROM pg_catalog.pg_namespace n
                 CROSS JOIN LATERAL pg_catalog.aclexplode(
                   COALESCE(n.nspacl, pg_catalog.acldefault('n', n.nspowner))
                 ) acl
                 WHERE n.nspname = 'favn_control' AND acl.grantee = 0
               ) OR EXISTS (
                 SELECT 1
                 FROM pg_catalog.pg_class c
                 JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                 CROSS JOIN LATERAL pg_catalog.aclexplode(
                   COALESCE(c.relacl, pg_catalog.acldefault(
                     CASE WHEN c.relkind = 'S' THEN 'S'::"char" ELSE 'r'::"char" END,
                     c.relowner
                   ))
                 ) acl
                 WHERE n.nspname = 'favn_control'
                   AND c.relkind IN ('r', 'p', 'S')
                   AND acl.grantee = 0
               )
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
      replication?,
      bypass_rls?,
      connect?,
      database_create?,
      database_temporary?,
      public_create?,
      control_usage?,
      control_create?,
      member_of_roles?,
      table_select?,
      table_dml?,
      migrations_dml?,
      unsafe_table_privileges?,
      sequence_use?,
      sequence_update?,
      default_table_dml?,
      default_sequence_use?,
      unsafe_default_privileges?,
      grant_options?,
      public_default_privileges?,
      public_object_privileges?
    ] = row

    safe? =
      connect? and control_usage? and table_select? and table_dml? and sequence_use? and
        default_table_dml? and default_sequence_use? and
        not superuser? and not create_database? and not create_role? and not inherit? and
        not replication? and not bypass_rls? and
        not database_create? and not database_temporary? and not public_create? and
        not control_create? and
        not member_of_roles? and not migrations_dml? and not unsafe_table_privileges? and
        not sequence_update? and not unsafe_default_privileges? and not grant_options? and
        not public_default_privileges? and not public_object_privileges?

    %{
      role: role,
      safe?: safe?,
      connect?: connect?,
      superuser?: superuser?,
      create_database?: create_database?,
      create_role?: create_role?,
      inherit?: inherit?,
      replication?: replication?,
      bypass_rls?: bypass_rls?,
      database_create?: database_create?,
      database_temporary?: database_temporary?,
      public_schema_create?: public_create?,
      control_schema_usage?: control_usage?,
      control_schema_create?: control_create?,
      member_of_roles?: member_of_roles?,
      table_select?: table_select?,
      table_dml?: table_dml?,
      schema_migrations_dml?: migrations_dml?,
      unsafe_table_privileges?: unsafe_table_privileges?,
      sequence_use?: sequence_use?,
      sequence_update?: sequence_update?,
      default_table_dml?: default_table_dml?,
      default_sequence_use?: default_sequence_use?,
      unsafe_default_privileges?: unsafe_default_privileges?,
      grant_options?: grant_options?,
      public_default_privileges?: public_default_privileges?,
      public_object_privileges?: public_object_privileges?,
      least_privilege?:
        not superuser? and not create_database? and not create_role? and not inherit? and
          not replication? and not bypass_rls? and
          not database_create? and not database_temporary? and not public_create? and
          not control_create? and
          not member_of_roles?,
      schema_access?:
        control_usage? and table_select? and table_dml? and sequence_use? and
          not unsafe_table_privileges? and not sequence_update? and not grant_options? and
          not public_object_privileges?,
      default_privileges?:
        default_table_dml? and default_sequence_use? and not unsafe_default_privileges? and
          not public_default_privileges?,
      schema_migrations_read_only?: not migrations_dml?
    }
  end

  @doc false
  @spec findings(map()) :: [map()]
  def findings(diagnostics) when is_map(diagnostics) do
    role = Map.get(diagnostics, :role)

    [
      {not diagnostics.connect?, :runtime_database_connect_missing, :missing},
      {diagnostics.superuser?, :runtime_role_superuser, :unsafe},
      {diagnostics.create_database?, :runtime_role_createdb, :unsafe},
      {diagnostics.create_role?, :runtime_role_createrole, :unsafe},
      {diagnostics.inherit?, :runtime_role_inherit, :unsafe},
      {diagnostics.replication?, :runtime_role_replication, :unsafe},
      {diagnostics.bypass_rls?, :runtime_role_bypass_rls, :unsafe},
      {diagnostics.database_create?, :runtime_database_create, :unsafe},
      {diagnostics.database_temporary?, :runtime_database_temporary, :unsafe},
      {diagnostics.public_schema_create?, :runtime_public_schema_create, :unsafe},
      {not diagnostics.control_schema_usage?, :runtime_schema_usage_missing, :missing},
      {diagnostics.control_schema_create?, :runtime_schema_create, :unsafe},
      {diagnostics.member_of_roles?, :runtime_role_memberships, :unsafe},
      {not diagnostics.table_select?, :runtime_table_select_missing, :missing},
      {not diagnostics.table_dml?, :runtime_table_dml_missing, :missing},
      {diagnostics.schema_migrations_dml?, :runtime_schema_migrations_write, :unsafe},
      {diagnostics.unsafe_table_privileges?, :runtime_unsafe_table_privileges, :unsafe},
      {not diagnostics.sequence_use?, :runtime_sequence_access_missing, :missing},
      {diagnostics.sequence_update?, :runtime_sequence_update, :unsafe},
      {not diagnostics.default_table_dml?, :runtime_default_table_grants_missing, :missing},
      {not diagnostics.default_sequence_use?, :runtime_default_sequence_grants_missing, :missing},
      {diagnostics.unsafe_default_privileges?, :runtime_unsafe_default_privileges, :unsafe},
      {diagnostics.grant_options?, :runtime_grant_options, :unsafe},
      {diagnostics.public_default_privileges?, :public_default_privileges, :unsafe},
      {diagnostics.public_object_privileges?, :public_control_privileges, :unsafe}
    ]
    |> Enum.flat_map(fn
      {true, code, category} ->
        [
          %{
            code: code,
            stage: :runtime_grants,
            details: %{expected_role: role, category: category}
          }
        ]

      {false, _code, _category} ->
        []
    end)
  end

  @spec quote_identifier!(String.t()) :: String.t()
  def quote_identifier!(value) when is_binary(value) do
    if Regex.match?(~r/^[a-z_][a-z0-9_]{0,62}$/, value) do
      ~s("#{value}")
    else
      raise ArgumentError, "invalid PostgreSQL role name"
    end
  end
end
