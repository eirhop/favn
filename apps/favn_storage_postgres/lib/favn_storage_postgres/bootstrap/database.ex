defmodule FavnStoragePostgres.Bootstrap.Database do
  @moduledoc false

  alias FavnStoragePostgres.Bootstrap.RolePolicy

  @spec exists?(pid(), String.t()) :: {:ok, boolean()} | {:error, atom()}
  def exists?(connection, database) when is_pid(connection) and is_binary(database) do
    case Postgrex.query(
           connection,
           "SELECT EXISTS(SELECT 1 FROM pg_catalog.pg_database WHERE datname = $1)",
           [database]
         ) do
      {:ok, %{rows: [[exists?]]}} -> {:ok, exists?}
      {:error, _reason} -> {:error, :database_inspection_failed}
    end
  end

  @spec ensure_exists(pid(), String.t()) :: {:ok, :exact | :created} | {:error, atom()}
  def ensure_exists(connection, database) do
    with {:ok, exists?} <- exists?(connection, database) do
      if exists? do
        {:ok, :exact}
      else
        create_database(connection, database)
      end
    end
  end

  @spec policy_status(pid(), String.t(), String.t()) :: {:ok, map()} | {:error, atom()}
  def policy_status(connection, migrator_role, runtime_role) do
    with {:ok,
          %{
            rows: [
              [
                database,
                owner,
                migrator_connect_only?,
                runtime_connect_only?,
                public_connect?,
                public_create?,
                public_temporary?,
                migrator_create?,
                migrator_temporary?,
                runtime_create?,
                runtime_temporary?
              ]
            ]
          }} <-
           Postgrex.query(
             connection,
             """
             SELECT database.datname,
                    owner.rolname,
                    COALESCE((
                      SELECT count(*) = 1 AND bool_and(
                        acl.privilege_type = 'CONNECT' AND NOT acl.is_grantable
                      )
                      FROM pg_catalog.aclexplode(
                        COALESCE(database.datacl, '{}'::aclitem[])
                      ) acl
                      JOIN pg_catalog.pg_roles grantee ON grantee.oid = acl.grantee
                      WHERE grantee.rolname = $1
                    ), false),
                    COALESCE((
                      SELECT count(*) = 1 AND bool_and(
                        acl.privilege_type = 'CONNECT' AND NOT acl.is_grantable
                      )
                      FROM pg_catalog.aclexplode(
                        COALESCE(database.datacl, '{}'::aclitem[])
                      ) acl
                      JOIN pg_catalog.pg_roles grantee ON grantee.oid = acl.grantee
                      WHERE grantee.rolname = $2
                    ), false),
                    COALESCE((
                      SELECT bool_or(acl.grantee = 0 AND acl.privilege_type = 'CONNECT')
                      FROM pg_catalog.aclexplode(
                        COALESCE(database.datacl, pg_catalog.acldefault('d', database.datdba))
                      ) acl
                    ), false),
                    COALESCE((
                      SELECT bool_or(acl.grantee = 0 AND acl.privilege_type = 'CREATE')
                      FROM pg_catalog.aclexplode(
                        COALESCE(database.datacl, pg_catalog.acldefault('d', database.datdba))
                      ) acl
                    ), false),
                    COALESCE((
                      SELECT bool_or(acl.grantee = 0 AND acl.privilege_type = 'TEMPORARY')
                      FROM pg_catalog.aclexplode(
                        COALESCE(database.datacl, pg_catalog.acldefault('d', database.datdba))
                      ) acl
                    ), false),
                    pg_catalog.has_database_privilege($1, database.datname, 'CREATE'),
                    pg_catalog.has_database_privilege($1, database.datname, 'TEMPORARY'),
                    pg_catalog.has_database_privilege($2, database.datname, 'CREATE'),
                    pg_catalog.has_database_privilege($2, database.datname, 'TEMPORARY')
             FROM pg_catalog.pg_database database
             JOIN pg_catalog.pg_roles owner ON owner.oid = database.datdba
             WHERE database.datname = pg_catalog.current_database()
             """,
             [migrator_role, runtime_role]
           ),
         {:ok, schema} <- schema_status(connection),
         {:ok, public_schema_create?} <- public_schema_create?(connection),
         {:ok, migrator_outside?} <- owns_outside_control?(connection, migrator_role),
         {:ok, foreign_control_ownership?} <-
           control_objects_owned_by_others?(connection, migrator_role),
         {:ok, runtime_anything?} <- owns_anything?(connection, runtime_role) do
      safe? =
        owner not in [migrator_role, runtime_role] and migrator_connect_only? and
          runtime_connect_only? and not public_connect? and
          not public_create? and
          not public_temporary? and not migrator_create? and not migrator_temporary? and
          not runtime_create? and not runtime_temporary? and not public_schema_create? and
          schema == %{exists?: true, owner: migrator_role} and not migrator_outside? and
          not foreign_control_ownership? and not runtime_anything?

      {:ok,
       %{
         safe?: safe?,
         database: database,
         database_owner: owner,
         migrator_database_connect_only?: migrator_connect_only?,
         runtime_database_connect_only?: runtime_connect_only?,
         public_database_connect?: public_connect?,
         public_database_create?: public_create?,
         public_database_temporary?: public_temporary?,
         migrator_database_create?: migrator_create?,
         migrator_database_temporary?: migrator_temporary?,
         runtime_database_create?: runtime_create?,
         runtime_database_temporary?: runtime_temporary?,
         public_schema_create?: public_schema_create?,
         control_schema: schema,
         migrator_owns_outside_control?: migrator_outside?,
         control_objects_owned_by_others?: foreign_control_ownership?,
         runtime_owns_objects?: runtime_anything?
       }}
    else
      {:error, _reason} -> {:error, :database_policy_inspection_failed}
    end
  end

  @doc false
  @spec findings(map(), String.t(), String.t()) :: [map()]
  def findings(status, migrator_role, runtime_role) when is_map(status) do
    [
      {status.database_owner in [migrator_role, runtime_role], :database_owned_by_normal_role,
       :database},
      {not status.migrator_database_connect_only?, :migrator_database_acl_not_exact, :migrator},
      {not status.runtime_database_connect_only?, :runtime_database_acl_not_exact, :runtime},
      {status.public_database_connect?, :public_database_connect, :public},
      {status.public_database_create?, :public_database_create, :public},
      {status.public_database_temporary?, :public_database_temporary, :public},
      {status.migrator_database_create?, :migrator_database_create, :migrator},
      {status.migrator_database_temporary?, :migrator_database_temporary, :migrator},
      {status.runtime_database_create?, :runtime_database_create, :runtime},
      {status.runtime_database_temporary?, :runtime_database_temporary, :runtime},
      {status.public_schema_create?, :public_schema_create, :public},
      {not status.control_schema.exists?, :control_schema_missing, :schema},
      {status.control_schema.exists? and status.control_schema.owner != migrator_role,
       :control_schema_owner_mismatch, :schema},
      {status.migrator_owns_outside_control?, :migrator_owns_outside_control, :migrator},
      {status.control_objects_owned_by_others?, :control_object_owner_mismatch, :schema},
      {status.runtime_owns_objects?, :runtime_owns_objects, :runtime}
    ]
    |> Enum.flat_map(fn
      {true, code, subject} ->
        [
          %{
            code: code,
            stage: :database_policy,
            details: %{
              subject: subject,
              expected_role: expected_role(subject, migrator_role, runtime_role)
            }
          }
        ]

      {false, _code, _subject} ->
        []
    end)
  end

  @spec ensure_policy(pid(), String.t(), String.t()) ::
          {:ok, :exact | :converged} | {:error, atom()}
  def ensure_policy(connection, migrator_role, runtime_role) do
    result =
      Postgrex.transaction(connection, fn transaction ->
        case converge_policy(transaction, migrator_role, runtime_role) do
          {:ok, _convergence} = success -> success
          {:error, reason} -> Postgrex.rollback(transaction, reason)
        end
      end)

    case result do
      {:ok, {:ok, convergence}} -> {:ok, convergence}
      {:error, reason} when is_atom(reason) -> {:error, reason}
      {:error, %DBConnection.ConnectionError{}} -> {:error, :unknown_outcome}
      {:error, _reason} -> {:error, :unknown_outcome}
    end
  end

  defp converge_policy(connection, migrator_role, runtime_role) do
    with {:ok, before} <- policy_status(connection, migrator_role, runtime_role),
         :ok <- reject_unsafe_ownership(before, migrator_role, runtime_role),
         {:ok, migrator} <- RolePolicy.quote_identifier(migrator_role),
         {:ok, runtime} <- RolePolicy.quote_identifier(runtime_role),
         {:ok, database} <- RolePolicy.quote_identifier(before.database),
         :ok <-
           maybe_execute(
             connection,
             before.database_owner in [migrator_role, runtime_role],
             "ALTER DATABASE #{database} OWNER TO CURRENT_USER"
           ),
         :ok <-
           maybe_execute(
             connection,
             before.public_database_connect?,
             "REVOKE CONNECT ON DATABASE #{database} FROM PUBLIC"
           ),
         :ok <-
           maybe_execute(
             connection,
             before.public_database_create?,
             "REVOKE CREATE ON DATABASE #{database} FROM PUBLIC"
           ),
         :ok <-
           maybe_execute(
             connection,
             before.public_database_temporary?,
             "REVOKE TEMPORARY ON DATABASE #{database} FROM PUBLIC"
           ),
         :ok <-
           maybe_execute(
             connection,
             before.migrator_database_create?,
             "REVOKE CREATE ON DATABASE #{database} FROM #{migrator}"
           ),
         :ok <-
           maybe_execute(
             connection,
             before.migrator_database_temporary?,
             "REVOKE TEMPORARY ON DATABASE #{database} FROM #{migrator}"
           ),
         :ok <-
           maybe_execute(
             connection,
             before.runtime_database_create?,
             "REVOKE CREATE ON DATABASE #{database} FROM #{runtime}"
           ),
         :ok <-
           maybe_execute(
             connection,
             before.runtime_database_temporary?,
             "REVOKE TEMPORARY ON DATABASE #{database} FROM #{runtime}"
           ),
         :ok <-
           maybe_set_connect_only(
             connection,
             before.migrator_database_connect_only?,
             database,
             migrator
           ),
         :ok <-
           maybe_set_connect_only(
             connection,
             before.runtime_database_connect_only?,
             database,
             runtime
           ),
         :ok <-
           maybe_execute(
             connection,
             before.public_schema_create?,
             "REVOKE CREATE ON SCHEMA public FROM PUBLIC"
           ),
         :ok <- revoke_public_schema_create(connection, migrator_role, migrator),
         :ok <- revoke_public_schema_create(connection, runtime_role, runtime),
         {:ok, after_status} <- policy_status(connection, migrator_role, runtime_role),
         true <- after_status.safe? do
      if before.safe?, do: {:ok, :exact}, else: {:ok, :converged}
    else
      false -> {:error, :database_policy_not_converged}
      {:error, code} -> {:error, code}
    end
  end

  defp create_database(connection, database) do
    with {:ok, quoted} <- RolePolicy.quote_identifier(database) do
      case Postgrex.query(connection, "CREATE DATABASE #{quoted}", []) do
        {:ok, _result} ->
          case exists?(connection, database) do
            {:ok, true} -> {:ok, :created}
            _unknown -> {:error, :unknown_outcome}
          end

        {:error, %Postgrex.Error{postgres: %{code: :insufficient_privilege}}} ->
          {:error, :database_creation_not_authorized}

        {:error, _reason} ->
          {:error, :unknown_outcome}
      end
    end
  end

  defp schema_status(connection) do
    case Postgrex.query(
           connection,
           """
           SELECT owner.rolname
           FROM pg_catalog.pg_namespace namespace
           JOIN pg_catalog.pg_roles owner ON owner.oid = namespace.nspowner
           WHERE namespace.nspname = 'favn_control'
           """,
           []
         ) do
      {:ok, %{rows: []}} -> {:ok, %{exists?: false, owner: nil}}
      {:ok, %{rows: [[owner]]}} -> {:ok, %{exists?: true, owner: owner}}
      {:error, _reason} -> {:error, :schema_inspection_failed}
    end
  end

  defp public_schema_create?(connection) do
    case Postgrex.query(
           connection,
           """
           SELECT COALESCE(bool_or(acl.grantee = 0 AND acl.privilege_type = 'CREATE'), false)
           FROM pg_catalog.pg_namespace namespace
           LEFT JOIN LATERAL pg_catalog.aclexplode(
             COALESCE(namespace.nspacl, pg_catalog.acldefault('n', namespace.nspowner))
           ) acl ON true
           WHERE namespace.nspname = 'public'
           """,
           []
         ) do
      {:ok, %{rows: [[create?]]}} -> {:ok, create?}
      {:error, _reason} -> {:error, :database_policy_inspection_failed}
    end
  end

  defp set_connect_only(connection, quoted_database, quoted_role) do
    with :ok <-
           execute(
             connection,
             "REVOKE ALL PRIVILEGES ON DATABASE #{quoted_database} FROM #{quoted_role}"
           ) do
      execute(connection, "GRANT CONNECT ON DATABASE #{quoted_database} TO #{quoted_role}")
    end
  end

  defp maybe_set_connect_only(_connection, true, _database, _role), do: :ok

  defp maybe_set_connect_only(connection, false, database, role),
    do: set_connect_only(connection, database, role)

  defp revoke_public_schema_create(connection, role, quoted_role) do
    case Postgrex.query(
           connection,
           "SELECT pg_catalog.has_schema_privilege($1, 'public', 'CREATE')",
           [role]
         ) do
      {:ok, %{rows: [[true]]}} ->
        execute(connection, "REVOKE CREATE ON SCHEMA public FROM #{quoted_role}")

      {:ok, %{rows: [[false]]}} ->
        :ok

      {:error, _reason} ->
        {:error, :database_policy_inspection_failed}
    end
  end

  @spec ensure_control_schema(pid(), String.t(), String.t(), (-> :ok | {:error, atom()})) ::
          {:ok, :exact | :created} | {:error, atom()}
  def ensure_control_schema(connection, database, migrator_role, create_as_migrator)
      when is_function(create_as_migrator, 0) do
    with {:ok, schema} <- schema_status(connection) do
      case schema do
        %{exists?: true, owner: ^migrator_role} ->
          {:ok, :exact}

        %{exists?: true} ->
          {:error, :unsafe_control_ownership}

        %{exists?: false} ->
          create_control_schema(connection, database, migrator_role, create_as_migrator)
      end
    end
  end

  defp create_control_schema(connection, database, migrator_role, create_as_migrator) do
    with {:ok, quoted_database} <- RolePolicy.quote_identifier(database),
         {:ok, quoted_migrator} <- RolePolicy.quote_identifier(migrator_role),
         :ok <-
           execute(
             connection,
             "GRANT CONNECT, CREATE ON DATABASE #{quoted_database} TO #{quoted_migrator}"
           ) do
      creation = safely_create_schema(create_as_migrator)

      revocation =
        execute(
          connection,
          "REVOKE CREATE ON DATABASE #{quoted_database} FROM #{quoted_migrator}"
        )

      reconcile_schema_creation(connection, migrator_role, creation, revocation)
    end
  end

  defp safely_create_schema(create_as_migrator) do
    create_as_migrator.()
  rescue
    _exception -> {:error, :unknown_outcome}
  catch
    _kind, _reason -> {:error, :unknown_outcome}
  end

  defp reconcile_schema_creation(_connection, _migrator_role, _creation, {:error, _reason}),
    do: {:error, :unknown_outcome}

  defp reconcile_schema_creation(connection, migrator_role, creation, :ok) do
    case schema_status(connection) do
      {:ok, %{exists?: true, owner: ^migrator_role}} ->
        {:ok, :created}

      {:ok, %{exists?: true}} ->
        {:error, :unsafe_control_ownership}

      {:ok, %{exists?: false}} ->
        case creation do
          {:error, code} -> {:error, code}
          _result -> {:error, :database_policy_not_converged}
        end

      {:error, _code} ->
        {:error, :unknown_outcome}
    end
  end

  defp reject_unsafe_ownership(status, migrator, _runtime) do
    cond do
      status.migrator_owns_outside_control? ->
        {:error, :unsafe_migrator_ownership}

      status.control_schema.exists? and status.control_schema.owner != migrator ->
        {:error, :unsafe_control_ownership}

      status.control_objects_owned_by_others? ->
        {:error, :unsafe_control_ownership}

      status.runtime_owns_objects? ->
        {:error, :unsafe_runtime_ownership}

      true ->
        :ok
    end
  end

  defp owns_outside_control?(connection, role) do
    ownership_dependency_exists?(connection, role, true)
  end

  defp owns_anything?(connection, role) do
    ownership_dependency_exists?(connection, role, false)
  end

  defp control_objects_owned_by_others?(connection, migrator_role) do
    case Postgrex.query(
           connection,
           """
           SELECT EXISTS(
             SELECT 1
             FROM pg_catalog.pg_shdepend dependency
             JOIN pg_catalog.pg_roles owner ON owner.oid = dependency.refobjid
             CROSS JOIN LATERAL pg_catalog.pg_identify_object(
               dependency.classid,
               dependency.objid,
               dependency.objsubid
             ) identified
             WHERE dependency.refclassid = 'pg_catalog.pg_authid'::regclass
               AND dependency.deptype = 'o'
               AND dependency.dbid = (
                 SELECT oid
                 FROM pg_catalog.pg_database
                 WHERE datname = pg_catalog.current_database()
               )
               AND (
                 identified.schema = 'favn_control' OR
                 (identified.type = 'schema' AND identified.name = 'favn_control') OR
                 (
                   dependency.classid = 'pg_catalog.pg_default_acl'::regclass AND
                   EXISTS (
                     SELECT 1
                     FROM pg_catalog.pg_default_acl default_acl
                     JOIN pg_catalog.pg_namespace namespace
                       ON namespace.oid = default_acl.defaclnamespace
                     WHERE default_acl.oid = dependency.objid
                       AND namespace.nspname = 'favn_control'
                   )
                 )
               )
               AND owner.rolname <> $1
           )
           """,
           [migrator_role]
         ) do
      {:ok, %{rows: [[exists?]]}} -> {:ok, exists?}
      {:error, _reason} -> {:error, :database_policy_inspection_failed}
    end
  end

  defp ownership_dependency_exists?(connection, role, allow_control?) do
    case Postgrex.query(
           connection,
           """
           SELECT EXISTS(
             SELECT 1
             FROM pg_catalog.pg_shdepend dependency
             JOIN pg_catalog.pg_roles owner ON owner.oid = dependency.refobjid
             CROSS JOIN LATERAL pg_catalog.pg_identify_object(
               dependency.classid,
               dependency.objid,
               dependency.objsubid
             ) identified
             WHERE dependency.refclassid = 'pg_catalog.pg_authid'::regclass
               AND dependency.deptype = 'o'
               AND owner.rolname = $1
               AND (
                 dependency.dbid = 0 OR dependency.dbid = (
                   SELECT oid
                   FROM pg_catalog.pg_database
                   WHERE datname = pg_catalog.current_database()
                 )
               )
               AND NOT (
                 $2 AND dependency.dbid <> 0 AND (
                   identified.schema = 'favn_control' OR
                   (identified.type = 'schema' AND identified.name = 'favn_control') OR
                   (
                     dependency.classid = 'pg_catalog.pg_default_acl'::regclass AND
                     EXISTS (
                       SELECT 1
                       FROM pg_catalog.pg_default_acl default_acl
                       JOIN pg_catalog.pg_namespace namespace
                         ON namespace.oid = default_acl.defaclnamespace
                       WHERE default_acl.oid = dependency.objid
                         AND namespace.nspname = 'favn_control'
                     )
                   )
                 )
               )
           )
           """,
           [role, allow_control?]
         ) do
      {:ok, %{rows: [[exists?]]}} -> {:ok, exists?}
      {:error, _reason} -> {:error, :database_policy_inspection_failed}
    end
  end

  defp maybe_execute(_connection, false, _sql), do: :ok
  defp maybe_execute(connection, true, sql), do: execute(connection, sql)

  defp execute(connection, sql) do
    case Postgrex.query(connection, sql, []) do
      {:ok, _result} ->
        :ok

      {:error, %Postgrex.Error{postgres: %{code: :insufficient_privilege}}} ->
        {:error, :database_policy_not_authorized}

      {:error, %Postgrex.Error{}} ->
        {:error, :database_policy_failed}

      {:error, _reason} ->
        {:error, :unknown_outcome}
    end
  end

  defp expected_role(:migrator, migrator_role, _runtime_role), do: migrator_role
  defp expected_role(:runtime, _migrator_role, runtime_role), do: runtime_role
  defp expected_role(:schema, migrator_role, _runtime_role), do: migrator_role
  defp expected_role(_subject, _migrator_role, _runtime_role), do: nil
end
