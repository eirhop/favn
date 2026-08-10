defmodule FavnStoragePostgres.Bootstrap do
  @moduledoc """
  Idempotent PostgreSQL bootstrap, status, and upgrade workflows for one-off Jobs.

  The resident control plane never invokes this module. Every stage connects as
  the exact bootstrap, migrator, or runtime identity configured for that stage.
  """

  alias FavnOrchestrator.Persistence.Commands.ProvisionWorkspace
  alias FavnOrchestrator.Persistence.Error, as: PersistenceError
  alias FavnOrchestrator.Persistence.PlatformContext
  alias FavnStoragePostgres.Authentication

  alias FavnStoragePostgres.Bootstrap.{
    Config,
    Connection,
    Database,
    Lock,
    Profile,
    Result,
    RolePolicy,
    WorkflowRunner
  }

  alias FavnStoragePostgres.Config, as: DatabaseConfig
  alias FavnStoragePostgres.Registry.Store
  alias FavnStoragePostgres.RuntimePrivileges
  alias FavnStoragePostgres.Schemas.Workspace
  alias FavnStoragePostgres.StorageV2.Migrations

  @type operation :: :status | :bootstrap | :upgrade
  @type result :: {:ok, map()} | {:error, map()}
  @status_change_states [
    :database_missing,
    :identity_mapping_missing,
    :identity_mapping_conflict,
    :unsafe_authority,
    :role_hardening_required,
    :schema_upgrade_required,
    :runtime_grants_missing,
    :workspace_missing,
    :workspace_conflict,
    :bootstrap_required
  ]

  @doc "Returns bounded read-only state derived from current PostgreSQL/provider evidence."
  @spec status(map()) :: result()
  def status(env \\ System.get_env()) when is_map(env) do
    execute(:status, env, &do_status/1)
  end

  @doc "Creates or safely resumes all Favn-owned PostgreSQL setup."
  @spec bootstrap(map()) :: result()
  def bootstrap(env \\ System.get_env()) when is_map(env) do
    execute(:bootstrap, env, &do_bootstrap/1)
  end

  @doc "Runs normal migrations/grants and verifies through the runtime identity."
  @spec upgrade(map()) :: result()
  def upgrade(env \\ System.get_env()) when is_map(env) do
    execute(:upgrade, env, &do_upgrade/1)
  end

  defp execute(operation, env, function) do
    started_at = System.monotonic_time(:millisecond)
    emit(:start, operation, %{system_time: System.system_time()}, %{})

    result =
      case Config.from_env(operation, env) do
        {:ok, config} ->
          execute_configured(operation, config, function)

        {:error, _reason} ->
          Result.error(
            operation,
            :invalid_configuration,
            :invalid_database_bootstrap_configuration,
            :configuration
          )
      end

    duration_ms = max(System.monotonic_time(:millisecond) - started_at, 0)
    result = put_result_duration(result, duration_ms)
    metadata = result_metadata(result)
    emit(:stop, operation, %{duration_ms: duration_ms}, metadata)

    result
  end

  defp execute_configured(operation, config, function) do
    WorkflowRunner.run(operation, fn -> function.(config) end)
  end

  defp do_status(%Config{bootstrap: %Profile{}} = config) do
    with_admin_connection(config, fn maintenance ->
      with {:ok, database_exists?} <-
             WorkflowRunner.track_stage(:database, fn ->
               Database.exists?(maintenance, config.target_database)
             end),
           {:ok, identity_findings} <-
             WorkflowRunner.track_stage(:provider_identity, fn ->
               identity_findings(maintenance, config)
             end),
           {:ok, role_findings} <-
             WorkflowRunner.track_stage(:role_policy, fn ->
               roles_findings(maintenance, config.migrator.role, config.runtime.role)
             end) do
        findings =
          database_findings(database_exists?) ++ identity_findings ++ role_findings

        if status_dependency_blocked?(findings) do
          Result.status_findings(:status, status_state(findings), findings)
        else
          target_status(config, findings)
        end
      else
        {:error, code} ->
          status_result(code, :provider_identity)
      end
    end)
    |> normalize_status_connection(:bootstrap_connection)
  end

  defp do_status(%Config{} = config), do: target_status(config, [])

  defp target_status(config, carried_findings) do
    Connection.with_raw(
      config,
      config.migrator,
      config.target_database,
      :migrator_lock,
      fn connection ->
        with {:ok, role_findings} <-
               WorkflowRunner.track_stage(:role_policy, fn ->
                 roles_findings(connection, config.migrator.role, config.runtime.role)
               end) do
          role_findings = merge_findings(carried_findings, role_findings)

          if Enum.any?(role_findings, &(&1.code == :role_missing)) do
            Result.status_findings(:status, status_state(role_findings), role_findings)
          else
            case WorkflowRunner.track_stage(:database_policy, fn ->
                   Database.policy_status(
                     connection,
                     config.migrator.role,
                     config.runtime.role
                   )
                 end) do
              {:ok, policy} -> policy_status_result(policy, config, role_findings)
              {:error, code} -> status_result(code, :database_policy)
            end
          end
        else
          {:error, code} ->
            status_result(code, :role_policy)
        end
      end
    )
    |> normalize_status_connection(:migrator_connection)
  end

  defp schema_status(config, carried_findings) do
    result =
      Connection.with_repo(
        config,
        config.migrator,
        config.target_database,
        :migrator_operation,
        fn repo ->
          WorkflowRunner.track_stage(:schema, fn ->
            case Migrations.pending_versions(repo) do
              {:ok, []} ->
                privileges = RuntimePrivileges.diagnostics(repo, config.runtime.role)

                findings =
                  merge_findings(carried_findings, RuntimePrivileges.findings(privileges))

                {:verify_runtime, findings}

              {:ok, _pending} ->
                findings =
                  merge_findings(carried_findings, [
                    %{code: :schema_upgrade_required, stage: :migrations, details: %{}}
                  ])

                status_with_findings(status_state(findings), findings, :migrations)

              {:error, _reason} ->
                findings =
                  merge_findings(carried_findings, [
                    %{code: :schema_inspection_failed, stage: :schema, details: %{}}
                  ])

                status_with_findings(status_state(findings), findings, :schema)
            end
          end)
        end
      )

    case result do
      {:verify_runtime, findings} -> runtime_status(config, findings)
      other -> normalize_status_connection(other, :migrator_connection)
    end
  end

  defp runtime_status(config, carried_findings) do
    result =
      Connection.with_repo(
        config,
        config.runtime,
        config.target_database,
        :runtime_operation,
        fn repo ->
          WorkflowRunner.track_stage(:runtime, fn -> verify_runtime(repo, config) end)
        end
      )

    case result do
      :ok when carried_findings == [] ->
        Result.ready(:status, [:configuration, :database, :roles, :schema, :runtime])

      :ok ->
        Result.status_findings(:status, status_state(carried_findings), carried_findings)

      {:error, code} when code in [:workspace_missing, :workspace_conflict] ->
        findings =
          merge_findings(carried_findings, [
            %{code: code, stage: :workspace, details: %{}}
          ])

        Result.status_findings(:status, status_state(findings), findings)

      {:error, :runtime_grants_missing} ->
        findings =
          merge_findings(carried_findings, [
            %{code: :runtime_grants_missing, stage: :runtime, details: %{}}
          ])

        Result.status_findings(:status, status_state(findings), findings)

      {:error, code}
      when code in [
             :authentication_rejected,
             :authentication_unavailable,
             :server_unreachable,
             :dependency_unavailable
           ] ->
        status_error_with_findings(code, :runtime_connection, carried_findings)

      {:error, code} ->
        findings =
          merge_findings(carried_findings, [
            %{code: code, stage: :runtime, details: %{}}
          ])

        Result.status_findings(:status, status_state(findings), findings)
    end
  end

  defp do_bootstrap(config) do
    with_admin_connection(config, fn maintenance ->
      with_session_lock(maintenance, config.target_database, :bootstrap, [], fn ->
        with {:ok, stages} <-
               step([], :database, fn ->
                 Database.ensure_exists(maintenance, config.target_database)
               end) do
          bootstrap_target(config, maintenance, stages)
        else
          {:error, code, stage, stages} ->
            workflow_error(:bootstrap, code, stage, stages)
        end
      end)
    end)
    |> normalize_workflow_connection(:bootstrap, :bootstrap_connection, [])
  end

  defp bootstrap_target(config, maintenance, stages) do
    Connection.with_raw(
      config,
      config.bootstrap,
      config.target_database,
      :bootstrap_target,
      fn target ->
        with_session_lock(target, config.target_database, :bootstrap, stages, fn ->
          with {:ok, stages} <-
                 step(stages, :identities, fn ->
                   with :ok <- preflight_existing_roles(maintenance, config),
                        :ok <- prepare_existing_roles(maintenance, config) do
                     ensure_identities(maintenance, config)
                   end
                 end),
               {:ok, stages} <-
                 step(stages, :roles, fn -> ensure_roles(maintenance, config) end),
               {:ok, stages} <-
                 step(stages, :database_policy, fn ->
                   with {:ok, _schema} <- ensure_control_schema(target, config),
                        {:ok, _policy} <-
                          Database.ensure_policy(
                            target,
                            config.migrator.role,
                            config.runtime.role
                          ) do
                     :ok
                   end
                 end),
               {:ok, stages} <- migrate_and_grant(config, stages),
               {:ok, stages} <- ensure_workspace(config, stages),
               {:ok, stages} <- verify_fresh_runtime(config, stages) do
            Result.ready(:bootstrap, stages)
          else
            {:error, code, stage, completed} ->
              workflow_error(:bootstrap, code, stage, completed)
          end
        end)
      end
    )
    |> normalize_workflow_connection(:bootstrap, :bootstrap_target, stages)
  end

  defp ensure_control_schema(admin_connection, config) do
    Database.ensure_control_schema(
      admin_connection,
      config.target_database,
      config.migrator.role,
      fn -> create_schema_as_migrator(config) end
    )
  end

  defp create_schema_as_migrator(config) do
    caller = self()
    reference = make_ref()

    {worker, monitor} =
      spawn_monitor(fn ->
        result =
          Connection.with_raw(
            config,
            config.migrator,
            config.target_database,
            :migrator_operation,
            fn migrator_connection ->
              case Postgrex.query(
                     migrator_connection,
                     "CREATE SCHEMA favn_control AUTHORIZATION CURRENT_USER",
                     []
                   ) do
                {:ok, _result} -> :ok
                {:error, %DBConnection.ConnectionError{}} -> {:error, :unknown_outcome}
                {:error, _reason} -> {:error, :schema_creation_failed}
              end
            end
          )

        send(caller, {reference, result})
      end)

    receive do
      {^reference, result} ->
        Process.demonitor(monitor, [:flush])
        result

      {:DOWN, ^monitor, :process, ^worker, _reason} ->
        {:error, :unknown_outcome}
    end
  end

  defp do_upgrade(config) do
    Connection.with_raw(
      config,
      config.migrator,
      config.target_database,
      :migrator_lock,
      fn connection ->
        with :ok <-
               WorkflowRunner.track_stage(:preflight, fn ->
                 require_upgrade_policy(connection, config)
               end) do
          with_session_lock(connection, config.target_database, :upgrade, [], fn ->
            with {:ok, stages} <- migrate_and_grant(config, []),
                 {:ok, stages} <- verify_fresh_runtime(config, stages) do
              Result.ready(:upgrade, stages)
            else
              {:error, code, stage, completed} ->
                workflow_error(:upgrade, code, stage, completed)
            end
          end)
        else
          {:error, :bootstrap_required} ->
            Result.error(:upgrade, :bootstrap_required, :bootstrap_required, :preflight)
        end
      end
    )
    |> normalize_workflow_connection(:upgrade, :migrator_connection, [])
  end

  defp with_session_lock(connection, database, operation, completed_stages, function) do
    case Lock.acquire(connection, database) do
      :ok ->
        try do
          result = function.()

          case Lock.release(connection, database) do
            :ok ->
              result

            {:error, :lock_lost} ->
              workflow_error(
                operation,
                :unknown_outcome,
                :lock,
                result_completed_stages(result, completed_stages)
              )
          end
        catch
          kind, reason ->
            _release = Lock.release(connection, database)
            :erlang.raise(kind, reason, __STACKTRACE__)
        end

      {:error, :operation_in_progress} ->
        Result.error(operation, :operation_in_progress, :operation_in_progress, :lock)

      {:error, :lock_failed} ->
        workflow_error(operation, :unknown_outcome, :lock, completed_stages)
    end
  end

  defp result_completed_stages({_tag, %{completed_stages: stages}}, _fallback)
       when is_list(stages),
       do: stages

  defp result_completed_stages(_result, fallback), do: fallback

  defp with_admin_connection(config, function) do
    Connection.with_raw(
      config,
      config.bootstrap,
      config.maintenance_database,
      :bootstrap_maintenance,
      function
    )
  end

  defp ensure_identities(connection, config) do
    if azure_profiles?(config) and config.maintenance_database != "postgres" do
      {:error, :identity_provider_prerequisite}
    else
      Enum.reduce_while([config.migrator, config.runtime], :ok, fn profile, :ok ->
        result =
          case profile.authentication_mode do
            :password -> :ok
            :azure_managed_identity -> ensure_identity(connection, config, profile)
          end

        case result do
          :ok -> {:cont, :ok}
          {:error, code} -> {:halt, {:error, code}}
        end
      end)
    end
  end

  defp prepare_existing_roles(connection, config) do
    Enum.reduce_while([config.migrator, config.runtime], :ok, fn profile, :ok ->
      case RolePolicy.status(connection, profile.role) do
        {:ok, %{exists?: true, superuser?: true}} ->
          {:halt, {:error, :unsafe_authority}}

        {:ok, %{exists?: false}} ->
          {:cont, :ok}

        {:ok, %{exists?: true}} ->
          hardening =
            case profile.authentication_mode do
              :azure_managed_identity ->
                RolePolicy.ensure_safe_before_identity_mapping(connection, profile.role)

              :password ->
                RolePolicy.ensure_hardened(connection, profile.role)
            end

          case hardening do
            {:ok, _status} -> {:cont, :ok}
            {:error, code} -> {:halt, {:error, code}}
          end

        {:error, code} ->
          {:halt, {:error, code}}
      end
    end)
  end

  defp preflight_existing_roles(connection, config) do
    Enum.reduce_while([config.migrator, config.runtime], :ok, fn profile, :ok ->
      result =
        case RolePolicy.status(connection, profile.role) do
          {:ok, %{exists?: true, superuser?: true}} ->
            {:error, :unsafe_authority}

          {:ok, %{exists?: false}} ->
            :ok

          {:ok, %{exists?: true}} ->
            preflight_existing_identity(connection, config, profile)

          {:error, code} ->
            {:error, code}
        end

      case result do
        :ok -> {:cont, :ok}
        {:error, code} -> {:halt, {:error, code}}
      end
    end)
  end

  defp preflight_existing_identity(
         _connection,
         config,
         %Profile{
           authentication_mode: :password
         } = profile
       ) do
    case attempt_password_identity(config, profile, config.target_database) do
      {:error, :database_access_rejected} ->
        case attempt_password_identity(config, profile, config.maintenance_database) do
          # PostgreSQL checks the password before its database CONNECT ACL, so
          # two post-authentication access rejections still prove the credential.
          {:error, :database_access_rejected} -> :ok
          result -> result
        end

      result ->
        result
    end
  end

  defp preflight_existing_identity(
         connection,
         config,
         %Profile{
           authentication_mode: :azure_managed_identity
         } = profile
       ) do
    case identity_status(connection, config, profile) do
      :ok -> :ok
      {:error, :identity_mapping_missing} -> {:error, :identity_mapping_conflict}
      {:error, code} -> {:error, code}
    end
  end

  defp attempt_password_identity(config, profile, database) do
    Connection.with_raw(
      config,
      profile,
      database,
      preflight_lifecycle(profile),
      fn connection ->
        case RolePolicy.current_role(connection) do
          {:ok, role} when role == profile.role -> :ok
          {:ok, _other_role} -> {:error, :role_identity_mismatch}
          {:error, code} -> {:error, code}
        end
      end
    )
  end

  defp preflight_lifecycle(%Profile{purpose: :migrator}), do: :migrator_operation
  defp preflight_lifecycle(%Profile{purpose: :runtime}), do: :runtime_operation

  defp ensure_identity(connection, config, profile) do
    with {:ok, authentication} <- authentication(config, profile, :migrator_operation),
         {:ok, _status} <-
           Authentication.ensure_identity(
             authentication,
             executor(connection),
             identity_mapping(profile)
           ) do
      :ok
    end
  end

  defp identity_status(
         connection,
         config,
         %Profile{authentication_mode: :azure_managed_identity} = profile
       ) do
    with {:ok, authentication} <- authentication(config, profile, :migrator_operation),
         {:ok, :exact} <-
           Authentication.identity_status(
             authentication,
             executor(connection),
             identity_mapping(profile)
           ) do
      :ok
    else
      {:ok, :missing} -> {:error, :identity_mapping_missing}
      {:ok, :conflict} -> {:error, :identity_mapping_conflict}
      {:error, code} -> {:error, code}
    end
  end

  defp identity_findings(connection, config) do
    Enum.reduce_while([config.migrator, config.runtime], {:ok, []}, fn profile, {:ok, findings} ->
      case profile_identity_findings(connection, config, profile) do
        {:ok, profile_findings} -> {:cont, {:ok, findings ++ profile_findings}}
        {:error, code} -> {:halt, {:error, code}}
      end
    end)
  end

  defp profile_identity_findings(_connection, _config, %Profile{
         authentication_mode: :password
       }),
       do: {:ok, []}

  defp profile_identity_findings(
         connection,
         config,
         %Profile{
           authentication_mode: :azure_managed_identity
         } = profile
       ) do
    with {:ok, authentication} <- authentication(config, profile, :migrator_operation) do
      case Authentication.identity_status(
             authentication,
             executor(connection),
             identity_mapping(profile)
           ) do
        {:ok, :exact} ->
          {:ok, []}

        {:ok, state} when state in [:missing, :conflict] ->
          {:ok,
           [
             %{
               code:
                 if(state == :missing,
                   do: :identity_mapping_missing,
                   else: :identity_mapping_conflict
                 ),
               stage: profile.purpose,
               details: %{expected_role: profile.role}
             }
           ]}

        {:error, code} ->
          {:error, code}
      end
    end
  end

  defp ensure_roles(connection, config) do
    Enum.reduce_while([config.migrator, config.runtime], :ok, fn profile, :ok ->
      result =
        case profile.authentication_mode do
          :password ->
            RolePolicy.ensure_password_role(
              connection,
              profile.role,
              Profile.password(profile)
            )

          :azure_managed_identity ->
            RolePolicy.ensure_hardened(connection, profile.role)
        end

      case result do
        {:ok, _status} -> {:cont, :ok}
        {:error, code} -> {:halt, {:error, code}}
      end
    end)
  end

  defp roles_status(connection, migrator_role, runtime_role) do
    with {:ok, findings} <- roles_findings(connection, migrator_role, runtime_role) do
      if findings == [], do: :ok, else: {:error, {:role_findings, findings}}
    end
  end

  defp roles_findings(connection, migrator_role, runtime_role) do
    with {:ok, migrator} <- RolePolicy.status(connection, migrator_role),
         {:ok, runtime} <- RolePolicy.status(connection, runtime_role) do
      {:ok, role_findings(migrator, :migrator) ++ role_findings(runtime, :runtime)}
    end
  end

  defp role_findings(%{exists?: false, role: role}, purpose) do
    [role_finding(:role_missing, purpose, role)]
  end

  defp role_findings(status, purpose) do
    [
      {status.superuser?, :role_superuser},
      {status.create_database?, :role_createdb},
      {status.create_role?, :role_createrole},
      {status.inherit?, :role_inherit},
      {status.replication?, :role_replication},
      {status.bypass_rls?, :role_bypass_rls},
      {not status.login?, :role_login_missing},
      {status.memberships != [], :role_memberships}
    ]
    |> Enum.flat_map(fn
      {true, :role_memberships} ->
        [
          role_finding(:role_memberships, purpose, status.role)
          |> put_in([:details, :parent_roles], Enum.take(status.memberships, 8))
          |> put_in(
            [:details, :required_action],
            :make_parent_memberships_revocable_by_bootstrap
          )
        ]

      {true, code} ->
        [role_finding(code, purpose, status.role)]

      {false, _code} ->
        []
    end)
  end

  defp role_finding(code, purpose, role) do
    %{code: code, stage: purpose, details: %{expected_role: role}}
  end

  defp require_upgrade_policy(connection, config) do
    with :ok <- roles_status(connection, config.migrator.role, config.runtime.role),
         {:ok, %{safe?: true}} <-
           Database.policy_status(connection, config.migrator.role, config.runtime.role) do
      :ok
    else
      _not_ready -> {:error, :bootstrap_required}
    end
  end

  defp policy_status_result(policy, config, role_findings) do
    findings =
      merge_findings(
        role_findings,
        Database.findings(policy, config.migrator.role, config.runtime.role)
      )

    if Enum.any?(findings, &schema_access_blocked?/1) do
      status_with_findings(status_state(findings), findings, :database_policy)
    else
      schema_status(config, findings)
    end
  end

  defp status_with_findings(state, [], stage), do: Result.status(:status, state, stage)

  defp status_with_findings(state, findings, _stage) do
    Result.status_findings(:status, state, findings)
  end

  defp database_findings(true), do: []

  defp database_findings(false) do
    [%{code: :database_missing, stage: :database, details: %{}}]
  end

  defp status_state(findings) do
    codes = MapSet.new(findings, & &1.code)

    cond do
      :database_missing in codes ->
        :database_missing

      :identity_mapping_conflict in codes ->
        :identity_mapping_conflict

      :identity_mapping_missing in codes ->
        :identity_mapping_missing

      Enum.any?(findings, &unsafe_finding?/1) ->
        :unsafe_authority

      Enum.any?(
        codes,
        &(&1 in [:control_schema_missing, :schema_upgrade_required, :schema_inspection_failed])
      ) ->
        :schema_upgrade_required

      Enum.any?(findings, &(Map.get(&1.details, :category) == :missing)) ->
        :runtime_grants_missing

      :workspace_conflict in codes ->
        :workspace_conflict

      :workspace_missing in codes ->
        :workspace_missing

      :runtime_grants_missing in codes ->
        :runtime_grants_missing

      true ->
        :role_hardening_required
    end
  end

  defp unsafe_finding?(%{details: %{category: :unsafe}}), do: true
  defp unsafe_finding?(%{details: %{category: :missing}}), do: false

  defp unsafe_finding?(finding) do
    finding.code not in [
      :database_missing,
      :identity_mapping_missing,
      :role_missing,
      :role_login_missing,
      :control_schema_missing,
      :schema_upgrade_required,
      :schema_inspection_failed,
      :runtime_grants_missing,
      :workspace_missing,
      :workspace_conflict
    ]
  end

  defp schema_access_blocked?(finding) do
    finding.code in [
      :control_schema_missing,
      :control_schema_owner_mismatch,
      :control_object_owner_mismatch
    ]
  end

  defp status_dependency_blocked?(findings) do
    Enum.any?(findings, fn finding ->
      finding.code in [
        :database_missing,
        :identity_mapping_missing,
        :identity_mapping_conflict,
        :role_missing,
        :role_login_missing
      ]
    end)
  end

  defp merge_findings(left, right) do
    Enum.uniq_by(left ++ right, &{&1.code, &1.stage, &1.details})
  end

  defp status_error_with_findings(code, stage, findings) do
    Result.error_findings(:status, state_for(code), code, stage, findings)
  end

  defp migrate_and_grant(config, stages) do
    Connection.with_repo(
      config,
      config.migrator,
      config.target_database,
      :migrator_operation,
      fn repo ->
        with {:ok, stages} <-
               step(stages, :migrations, fn ->
                 case Migrations.pending_versions(repo) do
                   {:ok, []} ->
                     {:ok, :exact}

                   {:ok, _pending} ->
                     migrate_pending(repo)

                   {:error, {:future_migration_versions, _versions}} ->
                     {:error, :future_migration_versions}

                   {:error, _reason} ->
                     {:error, :migration_inspection_failed}
                 end
               end),
             {:ok, stages} <-
               step(stages, :runtime_grants, fn ->
                 RuntimePrivileges.converge_runtime(repo, config.runtime.role)
               end) do
          {:ok, stages}
        end
      end
    )
    |> normalize_stage_connection(:migrator_connection, stages)
  end

  defp ensure_workspace(config, stages) do
    Connection.with_repo(
      config,
      config.runtime,
      config.target_database,
      :runtime_operation,
      fn repo ->
        step(stages, :workspace, fn -> ensure_workspace_row(repo, config.workspace) end)
      end
    )
    |> normalize_stage_connection(:runtime_connection, stages)
  end

  defp ensure_workspace_row(repo, workspace) do
    try do
      case repo.get(Workspace, workspace.workspace_id) do
        nil ->
          provision_workspace(workspace)

        %Workspace{} = existing ->
          if existing.slug == workspace.slug and
               existing.display_name == workspace.display_name and
               existing.status == "active" do
            {:ok, :exact}
          else
            {:error, :workspace_conflict}
          end
      end
    rescue
      _exception -> {:error, :unknown_outcome}
    catch
      _kind, _reason -> {:error, :unknown_outcome}
    end
  end

  defp migrate_pending(repo) do
    try do
      :ok = Migrations.migrate_existing_schema!(repo)
      verify_migration_completion(repo)
    rescue
      _exception -> {:error, :unknown_outcome}
    catch
      _kind, _reason -> {:error, :unknown_outcome}
    end
  end

  defp verify_migration_completion(repo) do
    case Migrations.pending_versions(repo) do
      {:ok, []} -> {:ok, :migrated}
      _not_verified -> {:error, :unknown_outcome}
    end
  end

  defp provision_workspace(workspace) do
    with {:ok, context} <-
           PlatformContext.new(
             "release:database-bootstrap",
             "release:database-bootstrap",
             [:platform_admin]
           ),
         :ok <-
           Store.provision_workspace(%ProvisionWorkspace{
             platform_context: context,
             workspace_id: workspace.workspace_id,
             slug: workspace.slug,
             display_name: workspace.display_name,
             occurred_at: DateTime.utc_now()
           }) do
      {:ok, :created}
    else
      {:error, %PersistenceError{} = failure} -> {:error, classify_workspace_failure(failure)}
    end
  end

  @doc false
  @spec classify_workspace_failure(PersistenceError.t()) :: atom()
  def classify_workspace_failure(%PersistenceError{kind: :conflict}), do: :workspace_conflict

  def classify_workspace_failure(%PersistenceError{kind: kind})
      when kind in [:timeout, :unavailable, :internal],
      do: :unknown_outcome

  def classify_workspace_failure(%PersistenceError{}), do: :workspace_provision_failed

  defp verify_fresh_runtime(config, stages) do
    Connection.with_repo(
      config,
      config.runtime,
      config.target_database,
      :runtime_operation,
      fn repo -> step(stages, :runtime_verification, fn -> verify_runtime(repo, config) end) end
    )
    |> normalize_stage_connection(:runtime_connection, stages)
  end

  defp verify_runtime(repo, config) do
    with %{rows: [[current_role]]} <- Ecto.Adapters.SQL.query!(repo, "SELECT current_user", []),
         true <- current_role == config.runtime.role,
         {:ok, %{ready?: true, runtime_role: %{safe?: true}}} <- Migrations.diagnostics(repo),
         :ok <- verify_workspace(repo, config.workspace) do
      :ok
    else
      false -> {:error, :runtime_role_mismatch}
      {:ok, _diagnostics} -> {:error, :runtime_grants_missing}
      {:error, code} -> {:error, code}
      _failure -> {:error, :runtime_verification_failed}
    end
  end

  defp verify_workspace(_repo, nil), do: :ok

  defp verify_workspace(repo, workspace) do
    case repo.get(Workspace, workspace.workspace_id) do
      nil ->
        {:error, :workspace_missing}

      %Workspace{} = existing ->
        if existing.slug == workspace.slug and existing.display_name == workspace.display_name and
             existing.status == "active",
           do: :ok,
           else: {:error, :workspace_conflict}
    end
  end

  defp authentication(config, profile, lifecycle) do
    env = Config.connection_env(config, profile, config.target_database, lifecycle)

    case DatabaseConfig.connection_config_from_env(env) do
      {:ok, connection_config} -> {:ok, connection_config.authentication}
      {:error, _reason} -> {:error, :invalid_database_configuration}
    end
  end

  defp identity_mapping(profile),
    do: %{role: profile.role, object_id: profile.object_id, purpose: profile.purpose}

  defp executor(connection) do
    fn sql, params ->
      case Postgrex.query(connection, sql, params) do
        {:error, %Postgrex.Error{postgres: %{code: :undefined_function}}} ->
          {:error, :identity_provider_prerequisite}

        {:error, %Postgrex.Error{postgres: %{code: :insufficient_privilege}}} ->
          {:error, :identity_mapping_not_authorized}

        {:error, %DBConnection.ConnectionError{}} ->
          {:error, :dependency_unavailable}

        result ->
          result
      end
    end
  end

  defp azure_profiles?(config) do
    Enum.any?(
      [config.migrator, config.runtime],
      &(&1.authentication_mode == :azure_managed_identity)
    )
  end

  defp step(stages, stage, function) do
    case WorkflowRunner.track_stage(stage, function) do
      :ok -> complete_stage(stages, stage)
      {:ok, _details} -> complete_stage(stages, stage)
      {:error, code} when is_atom(code) -> {:error, code, stage, stages}
      _invalid -> {:error, :invalid_stage_result, stage, stages}
    end
  end

  defp complete_stage(stages, stage) do
    completed_stages = stages ++ [stage]
    :ok = WorkflowRunner.record_completed(completed_stages)
    {:ok, completed_stages}
  end

  defp normalize_stage_connection({:error, code}, stage, stages),
    do: {:error, code, stage, stages}

  defp normalize_stage_connection(result, _stage, _stages), do: result

  defp normalize_status_connection({:error, :database_missing}, stage),
    do: Result.status(:status, :database_missing, stage)

  defp normalize_status_connection(
         {:error, %{operation: :status, status: :error}} = result,
         _stage
       ),
       do: result

  defp normalize_status_connection({:error, code}, stage), do: status_result(code, stage)
  defp normalize_status_connection(result, _stage), do: result

  defp normalize_workflow_connection(
         {:error, %{operation: operation, status: :error}} = result,
         operation,
         _stage,
         _stages
       ),
       do: result

  defp normalize_workflow_connection({:error, code}, operation, stage, stages),
    do: workflow_error(operation, code, stage, stages)

  defp normalize_workflow_connection(result, _operation, _stage, _stages), do: result

  defp status_result(code, stage) do
    state = state_for(code)

    if state in @status_change_states,
      do: Result.status(:status, state, stage),
      else: Result.error(:status, state, code, stage)
  end

  defp workflow_error(operation, code, stage, stages) do
    state = state_for(code)
    Result.error(operation, state, code, stage, stages)
  end

  defp state_for(code)
       when code in [:authentication_unavailable, :dependency_unavailable],
       do: :authentication_unavailable

  defp state_for(:authentication_rejected), do: :authentication_rejected
  defp state_for(:server_unreachable), do: :server_unreachable
  defp state_for(:database_missing), do: :database_missing
  defp state_for(:database_creation_not_authorized), do: :database_missing
  defp state_for(:database_access_rejected), do: :bootstrap_required
  defp state_for(:identity_mapping_missing), do: :identity_mapping_missing
  defp state_for(:identity_mapping_conflict), do: :identity_mapping_conflict
  defp state_for(:identity_provider_prerequisite), do: :bootstrap_required
  defp state_for(:scram_policy_unsupported), do: :bootstrap_required
  defp state_for(:scram_policy_inspection_failed), do: :bootstrap_required
  defp state_for(:role_hardening_required), do: :role_hardening_required
  defp state_for(:unsafe_authority), do: :unsafe_authority
  defp state_for(:workspace_conflict), do: :workspace_conflict
  defp state_for(:workspace_missing), do: :workspace_missing
  defp state_for(:operation_in_progress), do: :operation_in_progress
  defp state_for(:bootstrap_required), do: :bootstrap_required
  defp state_for(:unknown_outcome), do: :unknown_outcome

  defp state_for(code)
       when code in [
              :role_hardening_unsupported,
              :role_membership_hardening_unsupported,
              :role_membership_hardening_not_authorized,
              :role_creation_not_authorized,
              :identity_mapping_not_authorized,
              :unsafe_database_ownership,
              :unsafe_migrator_ownership,
              :unsafe_control_ownership,
              :unsafe_runtime_ownership,
              :database_policy_not_authorized
            ],
       do: :unsafe_authority

  defp state_for(_code), do: :operation_failed

  defp result_metadata({_, result}) do
    %{outcome: result.outcome, state: result.state}
  end

  defp put_result_duration({tag, result}, duration_ms) when tag in [:ok, :error],
    do: {tag, Map.put(result, :duration_ms, duration_ms)}

  defp emit(suffix, operation, measurements, metadata) do
    :telemetry.execute(
      [:favn, :storage_postgres, :database_workflow, suffix],
      measurements,
      Map.put(metadata, :operation, operation)
    )
  end
end
