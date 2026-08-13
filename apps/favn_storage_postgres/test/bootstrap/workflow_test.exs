defmodule FavnStoragePostgres.Bootstrap.WorkflowTest do
  use ExUnit.Case, async: false

  alias FavnStoragePostgres.Bootstrap
  alias FavnStoragePostgres.Bootstrap.{Database, Lock, RolePolicy}

  @bootstrap_password "bootstrap-test-password"
  @migrator_password "migrator-bootstrap-test-password"
  @runtime_password "runtime-bootstrap-test-password"

  setup do
    base_url =
      System.get_env("FAVN_DATABASE_URL") ||
        raise "FAVN_DATABASE_URL is required for PostgreSQL storage tests"

    suffix = random_id()
    database = "favn_bootstrap_#{suffix}"
    bootstrap_role = "favn_bootstrap_admin_#{suffix}"
    migrator_role = "favn_migrator_#{suffix}"
    runtime_role = "favn_runtime_#{suffix}"
    transition_parent_role = "favn_transition_parent_#{suffix}"
    maintenance_url = replace_database(base_url, "postgres")
    admin = start_supervised!({Postgrex, connection_options(maintenance_url)})
    quoted_bootstrap = quote!(bootstrap_role)

    Postgrex.query!(
      admin,
      "CREATE ROLE #{quoted_bootstrap} LOGIN CREATEDB CREATEROLE PASSWORD '#{@bootstrap_password}'",
      []
    )

    Postgrex.query!(admin, "ALTER ROLE #{quoted_bootstrap} SET log_parameter_max_length = 0", [])

    Postgrex.query!(admin, "ALTER ROLE #{quoted_bootstrap} SET scram_iterations = 8192", [])

    Postgrex.query!(
      admin,
      "ALTER ROLE #{quoted_bootstrap} SET log_parameter_max_length_on_error = 0",
      []
    )

    Postgrex.query!(
      admin,
      "CREATE ROLE #{quote!(migrator_role)} LOGIN CREATEDB CREATEROLE PASSWORD '#{@migrator_password}'",
      []
    )

    Postgrex.query!(
      admin,
      "CREATE ROLE #{quote!(runtime_role)} LOGIN INHERIT PASSWORD '#{@runtime_password}'",
      []
    )

    Postgrex.query!(admin, "CREATE ROLE #{quote!(transition_parent_role)} NOLOGIN", [])

    Postgrex.query!(
      admin,
      "GRANT #{quote!(migrator_role)}, #{quote!(runtime_role)}, #{quote!(transition_parent_role)} TO #{quoted_bootstrap} WITH ADMIN OPTION",
      []
    )

    Postgrex.query!(admin, "SET ROLE #{quoted_bootstrap}", [])

    Postgrex.query!(
      admin,
      "GRANT #{quote!(transition_parent_role)} TO #{quote!(migrator_role)}",
      []
    )

    Postgrex.query!(admin, "RESET ROLE", [])

    bootstrap_url = role_url(maintenance_url, "postgres", bootstrap_role, @bootstrap_password)

    env =
      password_env(
        bootstrap_url,
        database,
        migrator_role,
        runtime_role,
        "bootstrap-workspace-#{suffix}"
      )

    on_exit(fn ->
      with_cleanup_connection(maintenance_url, fn cleanup ->
        Postgrex.query!(
          cleanup,
          "DROP DATABASE IF EXISTS #{quote!(database)} WITH (FORCE)",
          []
        )

        Postgrex.query!(cleanup, "DROP ROLE IF EXISTS #{quote!(runtime_role)}", [])
        Postgrex.query!(cleanup, "DROP ROLE IF EXISTS #{quote!(migrator_role)}", [])
        Postgrex.query!(cleanup, "DROP ROLE IF EXISTS #{quote!(transition_parent_role)}", [])
        Postgrex.query!(cleanup, "DROP ROLE IF EXISTS #{quoted_bootstrap}", [])
      end)
    end)

    {:ok,
     admin: admin,
     bootstrap_role: bootstrap_role,
     database: database,
     env: env,
     maintenance_url: maintenance_url,
     migrator_role: migrator_role,
     runtime_role: runtime_role}
  end

  test "one bootstrap command converges a fresh database and is safe to run again", context do
    assert {:ok,
            %{
              operation: :status,
              outcome: :changes_required,
              state: :database_missing,
              runtime_verified: false,
              findings: initial_findings
            }} = Bootstrap.status(context.env)

    initial_codes = MapSet.new(initial_findings, & &1.code)
    assert :database_missing in initial_codes
    assert :role_createdb in initial_codes
    assert :role_createrole in initial_codes
    assert :role_inherit in initial_codes

    assert Enum.any?(initial_findings, fn finding ->
             finding.code == :role_memberships and
               finding.details.required_action ==
                 :make_parent_memberships_revocable_by_bootstrap
           end)

    assert {:ok,
            %{
              operation: :bootstrap,
              outcome: :ready,
              state: :ready,
              runtime_verified: true,
              completed_stages: [
                :database,
                :identities,
                :roles,
                :database_policy,
                :migrations,
                :runtime_grants,
                :workspace_administrator,
                :runtime_verification
              ]
            }} = Bootstrap.bootstrap(context.env)

    assert {:ok, %{safe?: true, create_role?: false}} =
             RolePolicy.status(context.admin, context.migrator_role)

    assert {:ok, %{safe?: true, create_role?: false}} =
             RolePolicy.status(context.admin, context.runtime_role)

    target_admin =
      start_supervised!(
        {Postgrex,
         connection_options(replace_database(context.maintenance_url, context.database))},
        id: :bootstrap_target_admin
      )

    assert {:ok,
            %{
              safe?: true,
              public_database_connect?: false,
              public_database_temporary?: false,
              control_schema: %{owner: owner}
            }} =
             Database.policy_status(
               target_admin,
               context.migrator_role,
               context.runtime_role
             )

    assert owner == context.migrator_role

    %{rows: [[database_xmin]]} =
      Postgrex.query!(
        target_admin,
        "SELECT xmin::text FROM pg_catalog.pg_database WHERE datname = $1",
        [context.database]
      )

    assert {:ok, %{state: :ready, runtime_verified: true}} = Bootstrap.bootstrap(context.env)

    assert %{rows: [[^database_xmin]]} =
             Postgrex.query!(
               target_admin,
               "SELECT xmin::text FROM pg_catalog.pg_database WHERE datname = $1",
               [context.database]
             )

    {:ok, quoted_runtime} = RolePolicy.quote_identifier(context.runtime_role)
    {:ok, quoted_migrator} = RolePolicy.quote_identifier(context.migrator_role)

    Postgrex.query!(
      target_admin,
      "CREATE FUNCTION favn_control.runtime_owned_probe() RETURNS integer LANGUAGE sql AS 'SELECT 1'",
      []
    )

    Postgrex.query!(
      target_admin,
      "ALTER FUNCTION favn_control.runtime_owned_probe() OWNER TO #{quoted_runtime}",
      []
    )

    Postgrex.query!(target_admin, "CREATE DOMAIN favn_control.runtime_owned_domain AS text", [])

    Postgrex.query!(
      target_admin,
      "ALTER DOMAIN favn_control.runtime_owned_domain OWNER TO #{quoted_runtime}",
      []
    )

    assert {:ok, %{safe?: false, runtime_owns_objects?: true}} =
             Database.policy_status(target_admin, context.migrator_role, context.runtime_role)

    Postgrex.query!(
      target_admin,
      "ALTER FUNCTION favn_control.runtime_owned_probe() OWNER TO #{quoted_migrator}",
      []
    )

    Postgrex.query!(
      target_admin,
      "ALTER DOMAIN favn_control.runtime_owned_domain OWNER TO #{quoted_migrator}",
      []
    )

    Postgrex.query!(target_admin, "DROP FUNCTION favn_control.runtime_owned_probe()", [])
    Postgrex.query!(target_admin, "DROP DOMAIN favn_control.runtime_owned_domain", [])

    %{rows: [[large_object_oid]]} =
      Postgrex.query!(target_admin, "SELECT pg_catalog.lo_create(0)", [])

    Postgrex.query!(
      target_admin,
      "ALTER LARGE OBJECT #{large_object_oid} OWNER TO #{quoted_runtime}",
      []
    )

    assert {:ok, %{safe?: false, runtime_owns_objects?: true}} =
             Database.policy_status(target_admin, context.migrator_role, context.runtime_role)

    Postgrex.query!(target_admin, "SELECT pg_catalog.lo_unlink($1)", [large_object_oid])

    assert {:ok, %{safe?: true}} =
             Database.policy_status(target_admin, context.migrator_role, context.runtime_role)

    assert {:ok, %{state: :ready, runtime_verified: true}} = Bootstrap.status(context.env)

    Postgrex.query!(context.admin, "ALTER ROLE #{quoted_migrator} CREATEROLE", [])

    assert {:ok, %{state: :unsafe_authority, runtime_verified: false, findings: findings}} =
             Bootstrap.status(context.env)

    assert Enum.any?(findings, &(&1.code == :role_createrole))

    upgrade_env =
      Map.drop(context.env, [
        "FAVN_DATABASE_BOOTSTRAP_AUTH_MODE",
        "FAVN_DATABASE_BOOTSTRAP_URL",
        "FAVN_WORKSPACE_ID",
        "FAVN_WORKSPACE_SLUG",
        "FAVN_WORKSPACE_NAME"
      ])

    assert {:error, %{state: :bootstrap_required, runtime_verified: false}} =
             Bootstrap.upgrade(upgrade_env)

    assert {:ok, %{state: :ready, runtime_verified: true}} = Bootstrap.bootstrap(context.env)

    Postgrex.query!(
      target_admin,
      "GRANT CREATE ON DATABASE #{quote!(context.database)} TO #{quoted_runtime}",
      []
    )

    assert {:ok, %{state: :unsafe_authority, findings: findings}} =
             Bootstrap.status(context.env)

    assert Enum.any?(findings, &(&1.code == :runtime_database_create))
    assert {:ok, %{state: :ready}} = Bootstrap.bootstrap(context.env)

    Postgrex.query!(
      target_admin,
      "REVOKE CONNECT ON DATABASE #{quote!(context.database)} FROM #{quoted_runtime}",
      []
    )

    assert {:ok, %{state: :unsafe_authority, findings: findings}} =
             Bootstrap.status(context.env)

    assert Enum.any?(findings, &(&1.code == :runtime_database_acl_not_exact))
    assert {:ok, %{state: :ready}} = Bootstrap.bootstrap(context.env)

    bad_runtime_env =
      Map.put(
        context.env,
        "FAVN_DATABASE_RUNTIME_URL",
        role_url(
          context.maintenance_url,
          context.database,
          context.runtime_role,
          "wrong-runtime-password"
        )
      )

    Postgrex.query!(context.admin, "ALTER ROLE #{quoted_migrator} CREATEROLE", [])

    assert {:error,
            %{
              state: :authentication_rejected,
              safe_to_retry: true,
              findings: authentication_findings
            }} =
             Bootstrap.status(bad_runtime_env)

    assert Enum.any?(authentication_findings, &(&1.code == :role_createrole))
    assert Enum.any?(authentication_findings, &(&1.code == :authentication_rejected))

    authentication_finding =
      Enum.find(authentication_findings, &(&1.code == :authentication_rejected))

    assert authentication_finding.stage == :runtime_connection
    assert authentication_finding.details.failure_kind == :error
    assert authentication_finding.details.failure_class == "authentication_rejected"
    assert authentication_finding.details.diagnostic_id =~ ~r/^diag_[0-9a-f]{16}$/
    assert {:ok, %{state: :ready}} = Bootstrap.bootstrap(context.env)

    DBConnection.run(target_admin, fn lock_connection ->
      :ok = Lock.acquire(lock_connection, context.database)

      assert {:error,
              %{
                state: :operation_in_progress,
                safe_to_retry: true,
                runtime_verified: false
              }} = Bootstrap.upgrade(upgrade_env)

      :ok = Lock.release(lock_connection, context.database)
    end)

    assert {:ok,
            %{
              operation: :upgrade,
              state: :ready,
              runtime_verified: true,
              completed_stages: [:migrations, :runtime_grants, :runtime_verification]
            }} = Bootstrap.upgrade(upgrade_env)

    Postgrex.query!(
      target_admin,
      "UPDATE favn_control.workspaces SET display_name = 'Conflicting workspace'",
      []
    )

    Postgrex.query!(context.admin, "ALTER ROLE #{quoted_migrator} CREATEROLE", [])

    Postgrex.query!(
      target_admin,
      "GRANT CONNECT ON DATABASE #{quote!(context.database)} TO PUBLIC",
      []
    )

    assert {:ok,
            %{state: :unsafe_authority, runtime_verified: false, findings: combined_findings}} =
             Bootstrap.status(context.env)

    combined_codes = MapSet.new(combined_findings, & &1.code)
    assert :role_createrole in combined_codes
    assert :public_database_connect in combined_codes
    assert :workspace_conflict in combined_codes

    assert {:error,
            %{
              state: :workspace_conflict,
              runtime_verified: false,
              completed_stages: [
                :database,
                :identities,
                :roles,
                :database_policy,
                :migrations,
                :runtime_grants,
                :workspace_administrator
              ]
            }} = Bootstrap.bootstrap(context.env)
  end

  test "fresh bootstrap supports protected local-password administrator input", context do
    if match?({:unix, _name}, :os.type()) do
      password = "bootstrap-local-admin-password-634"
      env = with_password_workspace_provisioning(context.env, password)

      assert {:ok, %{state: :ready, runtime_verified: true}} = Bootstrap.bootstrap(env)
      assert {:ok, %{state: :ready, runtime_verified: true}} = Bootstrap.status(env)
    end
  end

  test "one command creates missing password roles from SCRAM verifiers", context do
    Postgrex.query!(context.admin, "DROP ROLE #{quote!(context.runtime_role)}", [])
    Postgrex.query!(context.admin, "DROP ROLE #{quote!(context.migrator_role)}", [])

    assert {:ok, %{state: :ready, runtime_verified: true}} = Bootstrap.bootstrap(context.env)

    assert {:ok, %{safe?: true}} = RolePolicy.status(context.admin, context.migrator_role)
    assert {:ok, %{safe?: true}} = RolePolicy.status(context.admin, context.runtime_role)
  end

  test "status closes the bootstrap profile before starting the migrator profile", context do
    assert {:ok, %{state: :ready}} = Bootstrap.bootstrap(context.env)

    handler = "status-profile-order-#{System.unique_integer([:positive])}"
    parent = self()

    :telemetry.attach(
      handler,
      [:favn, :storage_postgres, :database_workflow, :stage],
      fn _event, _measurements, metadata, _config ->
        if metadata.operation == :status do
          send(parent, {:status_stage, metadata.stage, metadata.outcome})
        end
      end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler) end)

    assert {:ok, %{state: :ready, completed_stages: []}} = Bootstrap.status(context.env)

    stages = collect_status_stages([])
    bootstrap_complete = Enum.find_index(stages, &(&1 == {:bootstrap_connection, :complete}))
    migrator_started = Enum.find_index(stages, &(&1 == {:migrator_connection, :started}))

    assert is_integer(bootstrap_complete)
    assert is_integer(migrator_started)
    assert bootstrap_complete < migrator_started
  end

  test "managed identity bootstrap opens target auth while maintenance auth remains alive",
       context do
    {endpoint, endpoint_server} = start_identity_endpoint(@bootstrap_password, self())
    on_exit(fn -> stop_identity_endpoint(endpoint_server) end)

    azure_env = managed_identity_bootstrap_env(context)

    with_identity_endpoint(endpoint, fn ->
      for _attempt <- 1..2 do
        assert {:error,
                %{
                  state: :bootstrap_required,
                  code: :identity_provider_prerequisite,
                  completed_stages: [:database],
                  findings: [finding]
                } = result} = Bootstrap.bootstrap(azure_env)

        assert finding.stage == :identities
        refute inspect(result) =~ "authentication_lifecycle_conflict"

        for _request <- 1..2 do
          assert_receive {:identity_endpoint_request, request}, 2_000
          assert request =~ "client_id=11111111-1111-1111-1111-111111111111"
        end
      end
    end)
  end

  test "runtime lifecycle conflicts remain authentication errors", context do
    assert {:ok, %{state: :ready}} = Bootstrap.bootstrap(context.env)

    credentials_supervisor =
      FavnStoragePostgres.RuntimeOperationAuth.CredentialsSupervisor

    assert {:ok, occupied} = Agent.start_link(fn -> :occupied end, name: credentials_supervisor)
    on_exit(fn -> if Process.alive?(occupied), do: Agent.stop(occupied) end)

    assert {:error,
            %{
              state: :authentication_unavailable,
              code: :authentication_lifecycle_conflict,
              completed_stages: [],
              findings: findings
            } = result} = Bootstrap.status(runtime_managed_identity_status_env(context))

    finding = Enum.find(findings, &(&1.code == :authentication_lifecycle_conflict))
    assert finding.stage == :runtime_connection
    assert finding.details.failure_class == "authentication_lifecycle_conflict"
    assert finding.details.diagnostic_id =~ ~r/^diag_[0-9a-f]{16}$/
    refute inspect(result) =~ "credential-canary"
  end

  test "repeat bootstrap does not require normal-role access to the maintenance database",
       context do
    Postgrex.query!(
      context.admin,
      "GRANT CONNECT ON DATABASE postgres TO #{quote!(context.bootstrap_role)}",
      []
    )

    Postgrex.query!(context.admin, "REVOKE CONNECT ON DATABASE postgres FROM PUBLIC", [])

    on_exit(fn ->
      with_cleanup_connection(context.maintenance_url, fn cleanup ->
        Postgrex.query!(
          cleanup,
          "REVOKE CONNECT ON DATABASE postgres FROM #{quote!(context.bootstrap_role)}",
          []
        )

        Postgrex.query!(cleanup, "GRANT CONNECT ON DATABASE postgres TO PUBLIC", [])
      end)
    end)

    assert {:ok, %{state: :ready, runtime_verified: true}} = Bootstrap.bootstrap(context.env)

    target_admin =
      start_supervised!(
        {Postgrex,
         connection_options(replace_database(context.maintenance_url, context.database))},
        id: :maintenance_acl_target_admin
      )

    {:ok, quoted_runtime} = RolePolicy.quote_identifier(context.runtime_role)

    Postgrex.query!(
      target_admin,
      "REVOKE CONNECT ON DATABASE #{quote!(context.database)} FROM #{quoted_runtime}",
      []
    )

    Postgrex.query!(context.admin, "ALTER ROLE #{quoted_runtime} CREATEROLE", [])

    bad_env =
      Map.put(
        context.env,
        "FAVN_DATABASE_RUNTIME_URL",
        role_url(
          context.maintenance_url,
          context.database,
          context.runtime_role,
          "wrong-runtime-password"
        )
      )

    assert {:error, %{state: :authentication_rejected}} = Bootstrap.bootstrap(bad_env)

    assert {:ok, %{create_role?: true}} =
             RolePolicy.status(context.admin, context.runtime_role)

    assert {:ok, %{state: :ready, runtime_verified: true}} = Bootstrap.bootstrap(context.env)

    assert {:ok, %{safe?: true, runtime_database_connect_only?: true}} =
             Database.policy_status(
               target_admin,
               context.migrator_role,
               context.runtime_role
             )
  end

  test "bootstrap refuses to take over an existing schema owned by another role", context do
    assert {:ok, :created} = Database.ensure_exists(context.admin, context.database)

    target_admin =
      start_supervised!(
        {Postgrex,
         connection_options(replace_database(context.maintenance_url, context.database))},
        id: :foreign_schema_target_admin
      )

    Postgrex.query!(target_admin, "CREATE SCHEMA favn_control", [])
    %{rows: [[owner_before]]} = schema_owner(target_admin)

    assert {:error, %{state: :unsafe_authority, code: :unsafe_control_ownership}} =
             Bootstrap.bootstrap(context.env)

    assert %{rows: [[^owner_before]]} = schema_owner(target_admin)
  end

  test "wrong existing password is rejected before role hardening", context do
    bad_env =
      Map.put(
        context.env,
        "FAVN_DATABASE_MIGRATOR_URL",
        role_url(
          context.maintenance_url,
          context.database,
          context.migrator_role,
          "wrong-migrator-password"
        )
      )

    assert {:error, %{state: :authentication_rejected}} = Bootstrap.bootstrap(bad_env)

    assert {:ok, %{create_database?: true, create_role?: true}} =
             RolePolicy.status(context.admin, context.migrator_role)
  end

  test "temporary schema CREATE is revoked when the migrator callback exits", context do
    assert {:ok, :created} = Database.ensure_exists(context.admin, context.database)

    target_admin =
      start_supervised!(
        {Postgrex,
         connection_options(replace_database(context.maintenance_url, context.database))},
        id: :schema_cleanup_target_admin
      )

    assert {:error, :unknown_outcome} =
             Database.ensure_control_schema(
               target_admin,
               context.database,
               context.migrator_role,
               fn -> exit(:schema_creator_crashed) end
             )

    assert %{rows: [[false]]} =
             Postgrex.query!(
               target_admin,
               "SELECT pg_catalog.has_database_privilege($1, current_database(), 'CREATE')",
               [context.migrator_role]
             )
  end

  test "loss of the advisory-lock connection aborts with an unknown outcome", context do
    handler = "bootstrap-lock-loss-#{System.unique_integer([:positive])}"
    parent = self()

    :telemetry.attach(
      handler,
      [:favn, :storage_postgres, :database_workflow, :stage],
      fn _event, _measurements, metadata, _config ->
        if metadata.operation == :bootstrap and metadata.stage == :database and
             metadata.outcome == :complete do
          result =
            Postgrex.query!(
              context.admin,
              """
              SELECT pg_catalog.pg_terminate_backend(pid)
              FROM pg_catalog.pg_stat_activity
              WHERE usename = $1 AND datname = 'postgres'
              """,
              [context.bootstrap_role]
            )

          send(parent, {:lock_holder_terminated, result.num_rows})
        end
      end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler) end)

    assert {:error, %{state: :unknown_outcome, safe_to_retry: false}} =
             Bootstrap.bootstrap(context.env)

    assert_receive {:lock_holder_terminated, 1}
  end

  defp password_env(bootstrap_url, database, migrator_role, runtime_role, workspace_id) do
    %{
      "FAVN_DEPLOYMENT_MODE" => "production",
      "FAVN_DATABASE_SSL_MODE" => "disable",
      "FAVN_DATABASE_BOOTSTRAP_AUTH_MODE" => "password",
      "FAVN_DATABASE_BOOTSTRAP_URL" => bootstrap_url,
      "FAVN_DATABASE_MIGRATOR_AUTH_MODE" => "password",
      "FAVN_DATABASE_MIGRATOR_URL" =>
        role_url(bootstrap_url, database, migrator_role, @migrator_password),
      "FAVN_DATABASE_RUNTIME_AUTH_MODE" => "password",
      "FAVN_DATABASE_RUNTIME_URL" =>
        role_url(bootstrap_url, database, runtime_role, @runtime_password),
      "FAVN_WORKSPACE_ID" => workspace_id,
      "FAVN_WORKSPACE_SLUG" => workspace_id,
      "FAVN_WORKSPACE_NAME" => "Bootstrap workflow test"
    }
    |> with_workspace_provisioning(workspace_id, "Bootstrap workflow test")
  end

  defp managed_identity_bootstrap_env(context) do
    uri = URI.parse(context.maintenance_url)

    %{
      "FAVN_DEPLOYMENT_MODE" => "production",
      "FAVN_DATABASE_SSL_MODE" => "disable",
      "FAVN_DATABASE_HOST" => uri.host,
      "FAVN_DATABASE_PORT" => Integer.to_string(uri.port || 5432),
      "FAVN_DATABASE_NAME" => context.database,
      "FAVN_DATABASE_MAINTENANCE_NAME" => "postgres",
      "FAVN_DATABASE_BOOTSTRAP_AUTH_MODE" => "azure_managed_identity",
      "FAVN_DATABASE_BOOTSTRAP_USERNAME" => context.bootstrap_role,
      "FAVN_DATABASE_BOOTSTRAP_AZURE_MANAGED_IDENTITY_CLIENT_ID" =>
        "11111111-1111-1111-1111-111111111111",
      "FAVN_DATABASE_MIGRATOR_AUTH_MODE" => "azure_managed_identity",
      "FAVN_DATABASE_MIGRATOR_USERNAME" => context.migrator_role,
      "FAVN_DATABASE_MIGRATOR_AZURE_MANAGED_IDENTITY_CLIENT_ID" =>
        "22222222-2222-2222-2222-222222222222",
      "FAVN_DATABASE_MIGRATOR_AZURE_OBJECT_ID" => "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
      "FAVN_DATABASE_RUNTIME_AUTH_MODE" => "azure_managed_identity",
      "FAVN_DATABASE_RUNTIME_USERNAME" => context.runtime_role,
      "FAVN_DATABASE_RUNTIME_AZURE_MANAGED_IDENTITY_CLIENT_ID" =>
        "33333333-3333-3333-3333-333333333333",
      "FAVN_DATABASE_RUNTIME_AZURE_OBJECT_ID" => "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
      "FAVN_WORKSPACE_ID" => "managed-identity-workflow",
      "FAVN_WORKSPACE_SLUG" => "managed-identity-workflow",
      "FAVN_WORKSPACE_NAME" => "Managed identity workflow"
    }
    |> with_workspace_provisioning(
      "managed-identity-workflow",
      "Managed identity workflow"
    )
  end

  defp with_workspace_provisioning(env, workspace_id, workspace_name) do
    path =
      Path.join(
        System.tmp_dir!(),
        "favn-bootstrap-workflow-#{System.unique_integer([:positive])}.json"
      )

    File.write!(
      path,
      Jason.encode!(%{
        contract_version: 1,
        operation_id: "bootstrap-#{workspace_id}",
        workspace: %{id: workspace_id, slug: workspace_id, display_name: workspace_name},
        administrator: %{
          mode: "entra",
          username: "admin-#{workspace_id}",
          display_name: "Initial administrator",
          tenant_id: "11111111-1111-1111-1111-111111111111",
          object_id: "22222222-2222-2222-2222-222222222222"
        }
      })
    )

    Map.merge(env, %{
      "FAVN_WORKSPACE_PROVISIONING_CONFIG_FILE" => path,
      "FAVN_OPERATOR_COMMAND_HMAC_SECRET" => "bootstrap-test-hmac-secret-00000001"
    })
  end

  defp with_password_workspace_provisioning(env, password) do
    workspace_id = Map.fetch!(env, "FAVN_WORKSPACE_ID")
    workspace_name = Map.fetch!(env, "FAVN_WORKSPACE_NAME")
    directory = Path.join(System.tmp_dir!(), "favn-password-bootstrap-#{random_id()}")
    File.mkdir_p!(directory)
    config_path = Path.join(directory, "workspace-bootstrap.json")
    password_path = Path.join(directory, "initial-admin-password")
    File.write!(password_path, password)
    File.chmod!(password_path, 0o600)

    File.write!(
      config_path,
      Jason.encode!(%{
        contract_version: 1,
        operation_id: "password-bootstrap-#{workspace_id}",
        workspace: %{id: workspace_id, slug: workspace_id, display_name: workspace_name},
        administrator: %{
          mode: "password",
          username: "local-admin-#{random_id()}",
          display_name: "Local administrator"
        }
      })
    )

    Map.merge(env, %{
      "FAVN_WORKSPACE_PROVISIONING_CONFIG_FILE" => config_path,
      "FAVN_WORKSPACE_ADMIN_PASSWORD_FILE" => password_path
    })
  end

  defp runtime_managed_identity_status_env(context) do
    uri = URI.parse(context.maintenance_url)

    context.env
    |> Map.drop([
      "FAVN_DATABASE_BOOTSTRAP_AUTH_MODE",
      "FAVN_DATABASE_BOOTSTRAP_URL",
      "FAVN_DATABASE_RUNTIME_URL"
    ])
    |> Map.merge(%{
      "FAVN_DATABASE_HOST" => uri.host,
      "FAVN_DATABASE_PORT" => Integer.to_string(uri.port || 5432),
      "FAVN_DATABASE_NAME" => context.database,
      "FAVN_DATABASE_RUNTIME_AUTH_MODE" => "azure_managed_identity",
      "FAVN_DATABASE_RUNTIME_USERNAME" => context.runtime_role,
      "FAVN_DATABASE_RUNTIME_AZURE_MANAGED_IDENTITY_CLIENT_ID" =>
        "33333333-3333-3333-3333-333333333333",
      "FAVN_DATABASE_RUNTIME_AZURE_OBJECT_ID" => "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
    })
  end

  defp start_identity_endpoint(password, owner) do
    {:ok, listener} =
      :gen_tcp.listen(0, [:binary, active: false, packet: :raw, reuseaddr: true])

    {:ok, {_address, port}} = :inet.sockname(listener)

    server =
      spawn(fn -> identity_endpoint_accept(listener, password, owner) end)

    {"http://127.0.0.1:#{port}/token", {listener, server}}
  end

  defp identity_endpoint_accept(listener, password, owner) do
    case :gen_tcp.accept(listener) do
      {:ok, socket} ->
        serve_identity_token(socket, password, owner)
        identity_endpoint_accept(listener, password, owner)

      {:error, :closed} ->
        :ok

      {:error, _reason} ->
        :ok
    end
  end

  defp serve_identity_token(socket, password, owner) do
    with {:ok, request} <- :gen_tcp.recv(socket, 0, 5_000) do
      send(owner, {:identity_endpoint_request, request})

      body =
        Jason.encode!(%{
          "access_token" => password,
          "expires_on" =>
            DateTime.utc_now()
            |> DateTime.add(3_600, :second)
            |> DateTime.to_unix()
            |> Integer.to_string()
        })

      :ok =
        :gen_tcp.send(
          socket,
          "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: #{byte_size(body)}\r\nconnection: close\r\n\r\n#{body}"
        )
    end

    :gen_tcp.close(socket)
  end

  defp with_identity_endpoint(endpoint, function) do
    names = ["IDENTITY_ENDPOINT", "IDENTITY_HEADER"]
    previous = Map.new(names, &{&1, System.get_env(&1)})

    System.put_env("IDENTITY_ENDPOINT", endpoint)
    System.put_env("IDENTITY_HEADER", "test-header")

    try do
      function.()
    after
      Enum.each(previous, fn
        {name, nil} -> System.delete_env(name)
        {name, value} -> System.put_env(name, value)
      end)
    end
  end

  defp stop_identity_endpoint({listener, server}) do
    :gen_tcp.close(listener)
    if Process.alive?(server), do: Process.exit(server, :kill)
  end

  defp collect_status_stages(stages) do
    receive do
      {:status_stage, stage, outcome} -> collect_status_stages([{stage, outcome} | stages])
    after
      0 -> Enum.reverse(stages)
    end
  end

  defp role_url(base_url, database, role, password) do
    uri = URI.parse(base_url)
    URI.to_string(%{uri | userinfo: "#{role}:#{password}", path: "/#{database}"})
  end

  defp replace_database(url, database) do
    uri = URI.parse(url)
    URI.to_string(%{uri | path: "/#{database}"})
  end

  defp connection_options(url) do
    url
    |> Ecto.Repo.Supervisor.parse_url()
    |> Keyword.put(:ssl, false)
  end

  defp with_cleanup_connection(url, function) do
    {:ok, connection} = Postgrex.start_link(connection_options(url))

    try do
      function.(connection)
    after
      GenServer.stop(connection)
    end
  end

  defp schema_owner(connection) do
    Postgrex.query!(
      connection,
      """
      SELECT owner.rolname
      FROM pg_catalog.pg_namespace namespace
      JOIN pg_catalog.pg_roles owner ON owner.oid = namespace.nspowner
      WHERE namespace.nspname = 'favn_control'
      """,
      []
    )
  end

  defp quote!(identifier) do
    {:ok, quoted} = RolePolicy.quote_identifier(identifier)
    quoted
  end

  defp random_id, do: :crypto.strong_rand_bytes(5) |> Base.encode16(case: :lower)
end
