defmodule FavnStoragePostgres.Release do
  @moduledoc """
  Release-safe PostgreSQL operations for one-off control-plane containers.

  Every operation uses `FavnStoragePostgres.Config`, returns a stable redacted
  result, and starts the repository only for the duration of the call when the
  application has not already started it. Normal application startup never
  calls `migrate/0`.
  """

  require Logger

  alias Ecto.Adapters.SQL
  alias Favn.DeploymentMode
  alias Favn.RuntimeInput.KeyringConfig
  alias FavnOrchestrator.AdminLifecycle
  alias FavnOrchestrator.Operator.Audit
  alias FavnOrchestrator.Persistence.Error, as: PersistenceError
  alias FavnOrchestrator.Redaction
  alias FavnOrchestrator.WorkspaceProvisioning
  alias FavnStoragePostgres.Authentication
  alias FavnStoragePostgres.Bootstrap, as: DatabaseBootstrap
  alias FavnStoragePostgres.Config
  alias FavnStoragePostgres.Identity.Store, as: IdentityStore
  alias FavnStoragePostgres.Instrumented.WorkspaceProvisioning, as: WorkspaceProvisioningStore
  alias FavnStoragePostgres.Repo
  alias FavnStoragePostgres.RuntimePrivileges
  alias FavnStoragePostgres.RuntimeInputKeyInventory
  alias FavnStoragePostgres.Bootstrap.Result, as: OperationResult
  alias FavnStoragePostgres.Schemas.Workspace
  alias FavnStoragePostgres.StorageV2.Migrations

  @default_runtime_role "favn_runtime"
  @max_compaction_versions 100
  @restore_timeout_ms 600_000

  @type operation ::
          :bootstrap
          | :status
          | :upgrade
          | :migrate
          | :admin_bootstrap
          | :admin_recover
          | :admin_password_reset
          | :admin_actor_status
          | :admin_entra_identity
          | :verify_schema
          | :verify_workspace
          | :verify_restore
          | :grant_runtime
          | :provision_workspace
          | :workspace_status
          | :runtime_input_key_inventory
          | :compact_runtime_input_keys

  @type success :: %{
          required(:operation) => operation(),
          required(:status) => :ok,
          atom() => term()
        }
  @type failure :: %{
          required(:operation) => operation(),
          required(:status) => :error,
          required(:code) => atom(),
          atom() => term()
        }
  @type result :: {:ok, success()} | {:error, failure()}

  @doc "Inspects current provider and PostgreSQL bootstrap state without writing."
  @spec database_status(map()) :: DatabaseBootstrap.result()
  def database_status(env \\ System.get_env()) when is_map(env),
    do: DatabaseBootstrap.status(env)

  @doc "Creates or safely resumes all Favn-owned PostgreSQL setup."
  @spec bootstrap(map()) :: DatabaseBootstrap.result()
  def bootstrap(env \\ System.get_env()) when is_map(env),
    do: DatabaseBootstrap.bootstrap(env)

  @doc "Migrates, converges runtime grants, and verifies through the runtime identity."
  @spec upgrade(map()) :: DatabaseBootstrap.result()
  def upgrade(env \\ System.get_env()) when is_map(env),
    do: DatabaseBootstrap.upgrade(env)

  @doc "Applies every known Storage V2 migration with an elevated database role."
  @spec migrate() :: result()
  @spec migrate(map()) :: result()
  def migrate(env \\ System.get_env()) when is_map(env) do
    database_operation(:migrate, env, fn ->
      with :ok <- require_elevated_role(:migrate, env),
           :ok <- Migrations.migrate_existing_schema!(Repo) do
        versions = Migrations.expected_versions()
        Logger.info("favn.release.postgres_migrated migration_versions=#{inspect(versions)}")
        ok(:migrate, migration_versions: versions)
      end
    end)
  end

  @doc "Verifies the exact PostgreSQL schema, migration, projection, and grant contract."
  @spec verify_schema() :: result()
  @spec verify_schema(map()) :: result()
  def verify_schema(env \\ System.get_env()) when is_map(env) do
    verify_schema(env, false)
  end

  @doc "Verifies the exact schema through the configured least-privilege runtime role."
  @spec verify_runtime_schema() :: result()
  @spec verify_runtime_schema(map()) :: result()
  def verify_runtime_schema(env \\ System.get_env()) when is_map(env) do
    verify_schema(env, true)
  end

  defp verify_schema(env, require_runtime_role?) do
    database_operation(:verify_schema, env, fn ->
      case Migrations.diagnostics(Repo) do
        {:ok, diagnostics} ->
          cond do
            require_runtime_role? and not runtime_role_ready?(diagnostics, env) ->
              error(:verify_schema, :runtime_role_not_ready,
                diagnostics: schema_diagnostics(diagnostics)
              )

            diagnostics.ready? ->
              ok(:verify_schema,
                schema: diagnostics.schema,
                engine: diagnostics.engine,
                definition_fingerprint: diagnostics.actual_definition_fingerprint
              )

            true ->
              error(:verify_schema, :schema_not_ready,
                diagnostics: schema_diagnostics(diagnostics)
              )
          end

        {:error, reason} ->
          database_error(:verify_schema, reason)
      end
    end)
  end

  defp runtime_role_ready?(diagnostics, env) do
    expected_role = Map.get(env, "FAVN_DATABASE_RUNTIME_ROLE", @default_runtime_role)
    diagnostics.runtime_role.safe? and diagnostics.runtime_role.role == expected_role
  end

  @doc """
  Verifies that one explicitly provisioned workspace exists.

  Source-development startup uses this read-only check after schema
  verification. It never provisions the workspace implicitly.
  """
  @spec verify_workspace(String.t()) :: result()
  def verify_workspace(workspace_id) when is_binary(workspace_id) and workspace_id != "" do
    database_operation(:verify_workspace, fn ->
      case Repo.get(Workspace, workspace_id) do
        %Workspace{status: "active"} ->
          ok(:verify_workspace, workspace_id: workspace_id)

        %Workspace{} ->
          error(:verify_workspace, :workspace_not_active, workspace_id: workspace_id)

        nil ->
          error(:verify_workspace, :workspace_not_found, workspace_id: workspace_id)
      end
    end)
  end

  def verify_workspace(_workspace_id) do
    release_operation(:verify_workspace, fn ->
      error(:verify_workspace, :invalid_workspace, reason: :workspace_id_required)
    end)
  end

  @doc "Creates the one-time initial administrator from an explicit release command."
  @spec admin_bootstrap(map()) :: result()
  def admin_bootstrap(input) when is_map(input) do
    database_operation(:admin_bootstrap, fn ->
      with {:ok, workspace_ids} <- fetch_string_list(input, :workspace_ids),
           {:ok, username} <- fetch_string(input, :username),
           {:ok, password} <- fetch_string(input, :password),
           {:ok, display_name} <- fetch_string(input, :display_name),
           {:ok, actor} <-
             AdminLifecycle.bootstrap(workspace_ids, username, password, display_name,
               identity_store: IdentityStore
             ) do
        Logger.info(
          "favn.release.administrator_bootstrapped actor_id=#{actor.actor_id} workspaces=#{length(workspace_ids)}"
        )

        ok(:admin_bootstrap,
          actor_id: actor.actor_id,
          username: actor.username,
          workspace_ids: workspace_ids
        )
      else
        {:error, %PersistenceError{} = failure} ->
          persistence_error(:admin_bootstrap, failure)

        {:error, reason} ->
          error(:admin_bootstrap, :invalid_admin_bootstrap, reason: safe_reason(reason))
      end
    end)
  end

  def admin_bootstrap(_input) do
    release_operation(:admin_bootstrap, fn ->
      error(:admin_bootstrap, :invalid_admin_bootstrap, reason: :map_required)
    end)
  end

  @doc """
  Secure interactive bootstrap entry point for a packaged release.

  The password is read without echo from the release's standard input. It is
  never part of an argument, environment variable, or returned result.
  """
  @spec admin_bootstrap_from_stdin([String.t()], String.t(), String.t()) :: result()
  def admin_bootstrap_from_stdin(workspace_ids, username, display_name)
      when is_list(workspace_ids) and is_binary(username) and is_binary(display_name) do
    with {:ok, password} <- read_password("New administrator password") do
      admin_bootstrap(%{
        workspace_ids: workspace_ids,
        username: username,
        display_name: display_name,
        password: password
      })
    else
      {:error, reason} ->
        release_operation(:admin_bootstrap, fn ->
          error(:admin_bootstrap, :password_input_failed, reason: reason)
        end)
    end
  end

  def admin_bootstrap_from_stdin(_workspace_ids, _username, _display_name) do
    release_operation(:admin_bootstrap, fn ->
      error(:admin_bootstrap, :invalid_admin_bootstrap, reason: :arguments_required)
    end)
  end

  @doc "Rotates one existing administrator credential and revokes all sessions."
  @spec admin_recover(map()) :: result()
  def admin_recover(input) when is_map(input) do
    database_operation(:admin_recover, fn ->
      with {:ok, username} <- fetch_string(input, :username),
           {:ok, password} <- fetch_string(input, :password),
           {:ok, actor} <-
             AdminLifecycle.recover(username, password, identity_store: IdentityStore) do
        Logger.warning("favn.release.administrator_recovered actor_id=#{actor.actor_id}")

        ok(:admin_recover,
          actor_id: actor.actor_id,
          username: actor.username,
          sessions_revoked: true
        )
      else
        {:error, %PersistenceError{} = failure} ->
          persistence_error(:admin_recover, failure)

        {:error, reason} ->
          error(:admin_recover, :invalid_admin_recovery, reason: safe_reason(reason))
      end
    end)
  end

  def admin_recover(_input) do
    release_operation(:admin_recover, fn ->
      error(:admin_recover, :invalid_admin_recovery, reason: :map_required)
    end)
  end

  @doc """
  Secure interactive recovery entry point for a packaged release.

  The replacement password is read without echo from the release's standard
  input. Recovery revokes every existing session for the administrator.
  """
  @spec admin_recover_from_stdin(String.t()) :: result()
  def admin_recover_from_stdin(username) when is_binary(username) do
    with {:ok, password} <- read_password("Replacement administrator password") do
      admin_recover(%{username: username, password: password})
    else
      {:error, reason} ->
        release_operation(:admin_recover, fn ->
          error(:admin_recover, :password_input_failed, reason: reason)
        end)
    end
  end

  def admin_recover_from_stdin(_username) do
    release_operation(:admin_recover, fn ->
      error(:admin_recover, :invalid_admin_recovery, reason: :username_required)
    end)
  end

  @doc "Rotates one exact global actor credential and revokes all sessions."
  @spec admin_password_reset(map()) :: result()
  def admin_password_reset(input) when is_map(input) do
    database_operation(:admin_password_reset, fn ->
      with {:ok, username} <- fetch_string(input, :username),
           {:ok, password} <- fetch_string(input, :password),
           {:ok, actor} <-
             AdminLifecycle.reset_actor_credential(username, password,
               identity_store: IdentityStore
             ) do
        Logger.warning("favn.release.actor_password_reset actor_id=#{actor.actor_id}")

        ok(:admin_password_reset,
          actor_id: actor.actor_id,
          username: actor.username,
          sessions_revoked: true
        )
      else
        {:error, %PersistenceError{} = failure} ->
          persistence_error(:admin_password_reset, failure)

        {:error, reason} ->
          error(:admin_password_reset, :invalid_actor_credential_reset,
            reason: safe_reason(reason)
          )
      end
    end)
  end

  def admin_password_reset(_input) do
    release_operation(:admin_password_reset, fn ->
      error(:admin_password_reset, :invalid_actor_credential_reset, reason: :map_required)
    end)
  end

  @doc """
  Secure interactive password-reset entry point for a packaged release.

  The replacement password is read without echo. The actor's status and grants
  are preserved, while all of its sessions are revoked.
  """
  @spec admin_password_reset_from_stdin(String.t()) :: result()
  def admin_password_reset_from_stdin(username) when is_binary(username) do
    case read_password("Replacement actor password") do
      {:ok, password} ->
        admin_password_reset(%{username: username, password: password})

      {:error, reason} ->
        release_operation(:admin_password_reset, fn ->
          error(:admin_password_reset, :password_input_failed, reason: reason)
        end)
    end
  end

  def admin_password_reset_from_stdin(_username) do
    release_operation(:admin_password_reset, fn ->
      error(:admin_password_reset, :invalid_actor_credential_reset, reason: :username_required)
    end)
  end

  @doc "Enables or disables one exact global actor from a trusted release command."
  @spec admin_actor_status(map()) :: result()
  def admin_actor_status(input) when is_map(input) do
    database_operation(:admin_actor_status, fn ->
      with {:ok, username} <- fetch_string(input, :username),
           {:ok, status} <- fetch_actor_status(input),
           {:ok, actor} <-
             AdminLifecycle.set_actor_status(username, status, identity_store: IdentityStore) do
        Logger.warning(
          "favn.release.actor_status_changed actor_id=#{actor.actor_id} status=#{actor.status}"
        )

        ok(:admin_actor_status,
          actor_id: actor.actor_id,
          username: actor.username,
          status: actor.status,
          sessions_revoked: actor.sessions_revoked
        )
      else
        {:error, %PersistenceError{} = failure} ->
          persistence_error(:admin_actor_status, failure)

        {:error, reason} ->
          error(:admin_actor_status, :invalid_actor_status, reason: safe_reason(reason))
      end
    end)
  end

  def admin_actor_status(_input) do
    release_operation(:admin_actor_status, fn ->
      error(:admin_actor_status, :invalid_actor_status, reason: :map_required)
    end)
  end

  @doc "Links or unlinks one Entra object ID for one exact global actor."
  @spec admin_entra_identity(map()) :: result()
  def admin_entra_identity(input) when is_map(input) do
    database_operation(:admin_entra_identity, fn ->
      with {:ok, username} <- fetch_string(input, :username),
           {:ok, tenant_id} <- fetch_string(input, :tenant_id),
           {:ok, object_id} <- fetch_string(input, :object_id),
           {:ok, action} <- fetch_entra_action(input),
           {:ok, actor} <-
             AdminLifecycle.configure_entra_identity(
               username,
               tenant_id,
               object_id,
               action,
               identity_store: IdentityStore
             ) do
        Logger.warning(
          "favn.release.entra_identity_changed actor_id=#{actor.actor_id} action=#{actor.action}"
        )

        ok(:admin_entra_identity,
          actor_id: actor.actor_id,
          username: actor.username,
          provider: actor.provider,
          action: actor.action
        )
      else
        {:error, %PersistenceError{} = failure} ->
          persistence_error(:admin_entra_identity, failure)

        {:error, reason} ->
          error(:admin_entra_identity, :invalid_entra_identity, reason: safe_reason(reason))
      end
    end)
  end

  def admin_entra_identity(_input) do
    release_operation(:admin_entra_identity, fn ->
      error(:admin_entra_identity, :invalid_entra_identity, reason: :map_required)
    end)
  end

  @doc "Verifies exact schema readiness plus authoritative restore relationships."
  @spec verify_restore() :: result()
  def verify_restore do
    database_operation(:verify_restore, fn ->
      with_restore_timeout(fn ->
        verify_restore_contract()
      end)
    end)
  end

  defp verify_restore_contract do
    with {:ok, %{ready?: true} = diagnostics} <- Migrations.diagnostics(Repo),
         :ok <- verify_restore_authority() do
      ok(:verify_restore,
        schema: diagnostics.schema,
        definition_fingerprint: diagnostics.actual_definition_fingerprint,
        statement_timeout_ms: @restore_timeout_ms
      )
    else
      {:ok, diagnostics} ->
        error(:verify_restore, :schema_not_ready, diagnostics: schema_diagnostics(diagnostics))

      {:error, %{operation: :verify_restore} = failure} ->
        {:error, failure}

      {:error, reason} ->
        database_error(:verify_restore, reason)
    end
  end

  @doc "Converges Favn schema grants for an already-hardened runtime role."
  @spec grant_runtime() :: result()
  @spec grant_runtime(map()) :: result()
  def grant_runtime(env \\ System.get_env()) when is_map(env) do
    database_operation(:grant_runtime, env, fn ->
      role = Map.get(env, "FAVN_DATABASE_RUNTIME_ROLE", @default_runtime_role)

      with :ok <- require_elevated_role(:grant_runtime, env),
           :ok <- validate_role(:grant_runtime, role),
           :ok <- RuntimePrivileges.grant_runtime!(Repo, role) do
        Logger.info("favn.release.postgres_runtime_granted role=#{role}")
        ok(:grant_runtime, role: role)
      end
    end)
  end

  @doc "Atomically provisions one workspace with its selected initial administrator."
  @spec provision_workspace_administrator(map(), map()) :: result()
  def provision_workspace_administrator(input, env \\ System.get_env())

  def provision_workspace_administrator(input, env) when is_map(input) and is_map(env) do
    database_operation(:provision_workspace, env, fn ->
      with {:ok, fingerprint_key} <- workspace_provisioning_fingerprint_key(env),
           {:ok, result} <-
             WorkspaceProvisioning.provision(input,
               store: WorkspaceProvisioningStore,
               fingerprint_key: fingerprint_key
             ) do
        Logger.info(
          "favn.release.workspace_administrator_provisioned workspace_id=#{result.workspace_id} authentication_mode=#{result.authentication_mode}"
        )

        OperationResult.ready(:provision_workspace, provisioning_stages(result))
        |> merge_operation_fields(provisioning_fields(result))
      else
        {:error, %PersistenceError{} = failure} ->
          provisioning_failure(:provision_workspace, failure)

        {:error, reason} ->
          OperationResult.error(
            :provision_workspace,
            :invalid_configuration,
            :invalid_workspace_provisioning,
            :configuration,
            [],
            %{failure_kind: safe_reason(reason)}
          )
      end
    end)
  end

  def provision_workspace_administrator(_input, _env) do
    OperationResult.error(
      :provision_workspace,
      :invalid_configuration,
      :invalid_workspace_provisioning,
      :configuration
    )
  end

  @doc "Returns authoritative readiness for one provisioned workspace administrator."
  @spec workspace_status(String.t(), map()) :: result()
  def workspace_status(workspace_id, env \\ System.get_env())

  def workspace_status(workspace_id, env) when is_binary(workspace_id) and is_map(env) do
    database_operation(:workspace_status, env, fn ->
      case WorkspaceProvisioning.status(workspace_id, store: WorkspaceProvisioningStore) do
        {:ok, result} ->
          OperationResult.ready(:workspace_status, [:workspace_administrator_verification])
          |> merge_operation_fields(provisioning_fields(result))

        {:error, %PersistenceError{kind: kind} = failure}
        when kind in [:not_found, :constraint] ->
          OperationResult.status(
            :workspace_status,
            :workspace_administrator_missing,
            :workspace_administrator,
            [],
            Map.put(failure.details, :workspace_id, workspace_id)
          )

        {:error, %PersistenceError{} = failure} ->
          provisioning_failure(:workspace_status, failure)
      end
    end)
  end

  def workspace_status(_workspace_id, _env) do
    OperationResult.error(
      :workspace_status,
      :invalid_configuration,
      :invalid_workspace,
      :configuration
    )
  end

  @doc "Lists persisted key versions, pin counts, and redacted configured-version metadata."
  @spec runtime_input_key_inventory() :: result()
  def runtime_input_key_inventory do
    release_operation(:runtime_input_key_inventory, fn ->
      with {:ok, keyring} <- runtime_input_keyring(:runtime_input_key_inventory) do
        with_repo(:runtime_input_key_inventory, fn ->
          case RuntimeInputKeyInventory.list(Repo) do
            {:ok, inventory} ->
              configured = KeyringConfig.diagnostics(keyring)

              ok(:runtime_input_key_inventory,
                inventory: inventory,
                current_version: configured.current_version,
                retained_versions: configured.retained_versions,
                invalid_versions: []
              )

            {:error, reason} ->
              database_error(:runtime_input_key_inventory, reason)
          end
        end)
      end
    end)
  end

  @doc "Removes explicitly requested unreferenced, non-current key versions."
  @spec compact_runtime_input_keys(pos_integer() | [pos_integer()]) :: result()
  def compact_runtime_input_keys(versions) do
    release_operation(:compact_runtime_input_keys, fn ->
      with {:ok, requested} <- normalize_versions(versions),
           {:ok, keyring} <- runtime_input_keyring(:compact_runtime_input_keys),
           :ok <- reject_current_key_version(requested, keyring.current_version) do
        with_repo(:compact_runtime_input_keys, fn ->
          case RuntimeInputKeyInventory.compact(Repo, requested) do
            {:ok, removed} ->
              Logger.info(
                "favn.release.runtime_input_keys_compacted removed_versions=#{inspect(removed)}"
              )

              ok(:compact_runtime_input_keys,
                requested_versions: requested,
                removed_versions: removed
              )

            {:error, {:runtime_input_key_versions_still_referenced, referenced}} ->
              error(:compact_runtime_input_keys, :key_versions_still_referenced,
                referenced_versions: referenced
              )

            {:error, reason} ->
              database_error(:compact_runtime_input_keys, reason)
          end
        end)
      end
    end)
  end

  defp database_operation(operation, function) do
    release_operation(operation, fn -> with_repo(operation, function) end)
  end

  defp database_operation(operation, env, function) do
    release_operation(operation, fn -> with_repo(operation, env, function) end)
  end

  defp release_operation(operation, function) do
    started_at = System.monotonic_time(:millisecond)

    :telemetry.execute(
      [:favn, :storage_postgres, :release_operation, :start],
      %{system_time: System.system_time()},
      %{operation: operation}
    )

    Logger.info("favn.release.postgres_operation_started operation=#{operation}")

    result =
      try do
        function.()
      rescue
        exception -> database_error(operation, exception)
      catch
        kind, reason ->
          error(operation, :operation_failed, failure_kind: kind, reason: safe_reason(reason))
      end

    emit_operation_completed(operation, result, started_at)
    result
  end

  defp with_repo(operation, function) do
    with_repo(operation, System.get_env(), function)
  end

  defp with_repo(operation, env, function) do
    with :ok <- ensure_dependencies(operation),
         {:ok, connection_config} <- configured_connection_config(operation, env),
         :ok <- ensure_authentication_dependencies(operation, connection_config.authentication),
         {:ok, authentication_state} <-
           start_authentication(operation, connection_config.authentication),
         {:ok, repo_state} <-
           start_repo(operation, connection_config.repo_options, authentication_state) do
      try do
        function.()
      rescue
        exception -> database_error(operation, exception)
      catch
        kind, reason ->
          error(operation, :operation_failed, failure_kind: kind, reason: safe_reason(reason))
      after
        stop_repo(repo_state)
        stop_authentication(authentication_state)
      end
    end
  end

  defp emit_operation_completed(operation, result, started_at) do
    duration_ms = max(System.monotonic_time(:millisecond) - started_at, 0)

    metadata =
      case result do
        {:ok, _success} -> %{operation: operation, status: :ok}
        {:error, failure} -> %{operation: operation, status: :error, code: failure.code}
      end

    :telemetry.execute(
      [:favn, :storage_postgres, :release_operation, :stop],
      %{duration_ms: duration_ms},
      metadata
    )

    Logger.log(
      if(metadata.status == :ok, do: :info, else: :warning),
      "favn.release.postgres_operation_completed operation=#{operation} " <>
        "status=#{metadata.status} duration_ms=#{duration_ms}" <>
        if(Map.has_key?(metadata, :code), do: " code=#{metadata.code}", else: "")
    )
  end

  defp ensure_dependencies(operation) do
    with {:ok, _applications} <- Application.ensure_all_started(:ecto_sql),
         {:ok, _applications} <- Application.ensure_all_started(:postgrex) do
      :ok
    else
      {:error, {_application, _reason}} -> error(operation, :dependency_start_failed)
    end
  end

  defp configured_connection_config(operation, env) do
    case Config.connection_config_from_env(env) do
      {:ok, connection_config} ->
        {:ok, connection_config}

      {:error, reason} ->
        error(operation, :invalid_database_configuration, reason: safe_reason(reason))
    end
  end

  defp start_repo(operation, options, authentication_state) do
    case Repo.start_link(options) do
      {:ok, pid} ->
        {:ok, %{pid: pid, owned?: true}}

      {:error, {:already_started, _pid}} ->
        stop_authentication(authentication_state)
        error(operation, :repo_already_started)

      {:error, reason} ->
        stop_authentication(authentication_state)
        database_error(operation, reason)
    end
  end

  defp stop_repo(%{pid: pid, owned?: true}) do
    if Process.alive?(pid), do: GenServer.stop(pid)
  end

  defp ensure_authentication_dependencies(operation, authentication) do
    with {:ok, applications} <- Authentication.applications(authentication) do
      Enum.reduce_while(applications, :ok, fn application, :ok ->
        case Application.ensure_all_started(application) do
          {:ok, _started} ->
            {:cont, :ok}

          {:error, {_application, _reason}} ->
            {:halt, error(operation, :dependency_start_failed)}
        end
      end)
    else
      {:error, _reason} -> error(operation, :invalid_database_authentication_provider)
    end
  end

  defp start_authentication(operation, authentication) do
    with {:ok, child_specs} <- Authentication.child_specs(authentication) do
      case child_specs do
        [] ->
          {:ok, %{pid: nil}}

        child_specs ->
          case Supervisor.start_link(child_specs, strategy: :one_for_one) do
            {:ok, pid} -> {:ok, %{pid: pid}}
            {:error, _reason} -> error(operation, :database_authentication_start_failed)
          end
      end
    else
      {:error, _reason} -> error(operation, :invalid_database_authentication_provider)
    end
  end

  defp stop_authentication(%{pid: nil}), do: :ok

  defp stop_authentication(%{pid: pid}) do
    if Process.alive?(pid), do: Supervisor.stop(pid)
  end

  defp require_elevated_role(operation, env) do
    case DeploymentMode.from_env(env) do
      {:ok, :production} -> require_production_migrator(operation, env)
      {:error, _reason} -> error(operation, :invalid_database_configuration)
    end
  end

  defp require_production_migrator(operation, env) do
    %{
      rows: [
        [
          current_role,
          superuser?,
          create_database?,
          create_role?,
          inherit?,
          replication?,
          bypass_rls?,
          memberships?
        ]
      ]
    } =
      SQL.query!(
        Repo,
        """
        SELECT role.rolname, role.rolsuper, role.rolcreatedb, role.rolcreaterole,
               role.rolinherit, role.rolreplication, role.rolbypassrls,
               EXISTS (
                 SELECT 1
                 FROM pg_catalog.pg_auth_members membership
                 WHERE membership.member = role.oid
               )
        FROM pg_catalog.pg_roles role
        WHERE role.rolname = CURRENT_USER
        """,
        []
      )

    runtime_role = Map.get(env, "FAVN_DATABASE_RUNTIME_ROLE", @default_runtime_role)

    cond do
      current_role == runtime_role ->
        error(operation, :restricted_runtime_role, role: current_role)

      superuser? or create_database? or create_role? or inherit? or replication? or bypass_rls? or
          memberships? ->
        error(operation, :unsafe_migrator_authority, role: current_role)

      true ->
        :ok
    end
  end

  defp validate_role(operation, role) do
    RuntimePrivileges.quote_identifier!(role)
    :ok
  rescue
    ArgumentError -> error(operation, :invalid_runtime_role)
  end

  defp fetch_string(input, key) do
    case Map.get(input, key, Map.get(input, Atom.to_string(key))) do
      value when is_binary(value) and value != "" -> {:ok, value}
      _missing -> {:error, {key, :required}}
    end
  end

  defp fetch_string_list(input, key) do
    case Map.get(input, key, Map.get(input, Atom.to_string(key))) do
      values when is_list(values) and values != [] and length(values) <= 100 ->
        if Enum.all?(values, &(is_binary(&1) and &1 != "")),
          do: {:ok, values},
          else: {:error, {key, :invalid}}

      _missing ->
        {:error, {key, :required}}
    end
  end

  defp fetch_actor_status(input) do
    case Map.get(input, :status, Map.get(input, "status")) do
      :active -> {:ok, :active}
      :disabled -> {:ok, :disabled}
      "active" -> {:ok, :active}
      "disabled" -> {:ok, :disabled}
      _invalid -> {:error, {:status, :invalid}}
    end
  end

  defp fetch_entra_action(input) do
    case Map.get(input, :action, Map.get(input, "action")) do
      :link -> {:ok, :link}
      :unlink -> {:ok, :unlink}
      "link" -> {:ok, :link}
      "unlink" -> {:ok, :unlink}
      _invalid -> {:error, {:action, :invalid}}
    end
  end

  defp normalize_versions(version) when is_integer(version), do: normalize_versions([version])

  defp normalize_versions(versions)
       when is_list(versions) and versions != [] and length(versions) <= @max_compaction_versions do
    if Enum.all?(versions, &(is_integer(&1) and &1 > 0)) do
      {:ok, versions |> Enum.uniq() |> Enum.sort()}
    else
      error(:compact_runtime_input_keys, :invalid_key_versions)
    end
  end

  defp normalize_versions(_versions),
    do: error(:compact_runtime_input_keys, :invalid_key_versions)

  defp runtime_input_keyring(operation) do
    case KeyringConfig.from_env() do
      {:ok, keyring} -> {:ok, keyring}
      {:error, _reason} -> error(operation, :invalid_runtime_input_key_configuration)
    end
  end

  defp reject_current_key_version(versions, current_version) do
    if current_version in versions do
      error(:compact_runtime_input_keys, :current_key_version_requested,
        current_version: current_version
      )
    else
      :ok
    end
  end

  defp verify_restore_authority do
    with {:ok, %{rows: [[0]]}} <-
           SQL.query(
             Repo,
             """
             SELECT
               (SELECT count(*) FROM favn_control.runs run
                LEFT JOIN favn_control.workspaces workspace USING (workspace_id)
                WHERE workspace.workspace_id IS NULL) +
               (SELECT count(*) FROM favn_control.run_events event
                LEFT JOIN favn_control.runs run USING (workspace_id, run_id)
                WHERE run.run_id IS NULL) +
               (SELECT count(*) FROM favn_control.workspace_deployments deployment
                LEFT JOIN favn_control.manifest_versions manifest USING (manifest_version_id)
                WHERE manifest.manifest_version_id IS NULL) +
               (SELECT count(*) FROM favn_control.run_targets target
                LEFT JOIN favn_control.workspace_deployment_targets catalog
                  USING (workspace_id, deployment_id, target_kind, target_id)
                WHERE catalog.target_id IS NULL)
             """,
             [],
             timeout: @restore_timeout_ms
           ),
         {:ok, %{rows: [[0]]}} <-
           SQL.query(
             Repo,
             """
             SELECT count(*)
             FROM favn_control.projection_cursors cursor
             WHERE cursor.last_publication_id >
               (SELECT last_publication_id
                FROM favn_control.outbox_publication_state
                WHERE singleton_id = 1)
             """,
             [],
             timeout: @restore_timeout_ms
           ) do
      :ok
    else
      {:ok, %{rows: [[count]]}} ->
        error(:verify_restore, :restore_authority_invalid, invalid_row_count: count)

      {:error, reason} ->
        database_error(:verify_restore, reason)
    end
  end

  defp with_restore_timeout(function) do
    Repo.checkout(
      fn ->
        with {:ok, %{rows: [[previous_timeout]]}} <-
               SQL.query(Repo, "SHOW statement_timeout", [], timeout: @restore_timeout_ms),
             {:ok, _result} <-
               SQL.query(Repo, "SET statement_timeout = '10min'", [],
                 timeout: @restore_timeout_ms
               ) do
          try do
            function.()
          after
            restore_statement_timeout(previous_timeout)
          end
        else
          {:error, reason} -> database_error(:verify_restore, reason)
        end
      end,
      timeout: @restore_timeout_ms
    )
  end

  defp restore_statement_timeout(previous_timeout) do
    case SQL.query(
           Repo,
           "SELECT pg_catalog.set_config('statement_timeout', $1, false)",
           [previous_timeout],
           timeout: @restore_timeout_ms
         ) do
      {:ok, _result} -> :ok
      {:error, _reason} -> Logger.warning("favn.release.restore_timeout_reset_failed")
    end
  end

  defp schema_diagnostics(diagnostics) do
    Map.take(diagnostics, [
      :status,
      :engine,
      :schema,
      :missing_tables,
      :missing_critical_indexes,
      :missing_columns,
      :unexpected_columns,
      :missing_critical_constraints,
      :missing_migration_versions,
      :future_migration_versions,
      :definition_fingerprint_matches?,
      :expected_definition_fingerprint,
      :actual_definition_fingerprint,
      :runtime_role
    ])
  end

  defp workspace_provisioning_fingerprint_key(env) do
    case Application.get_env(:favn_orchestrator, :operator_command_hmac_key) do
      key when is_binary(key) and byte_size(key) >= 32 ->
        {:ok, key}

      _missing ->
        case Map.get(env, "FAVN_OPERATOR_COMMAND_HMAC_SECRET") do
          secret when is_binary(secret) and byte_size(secret) >= 32 ->
            {:ok, Audit.derive_command_hmac_key(secret)}

          _invalid ->
            {:error, :operator_command_hmac_key_unavailable}
        end
    end
  end

  defp provisioning_stages(%{authentication_mode: mode}) do
    [:workspace, :actor, :membership, :platform_grant, mode, :audit, :receipt]
  end

  defp provisioning_fields(result) do
    %{
      actor_id: result.actor_id,
      authentication_mode: result.authentication_mode,
      operation_id: result.operation_id,
      platform_roles: result.platform_roles,
      replayed: result.replayed?,
      slug: result.slug,
      username: result.username,
      workspace_id: result.workspace_id,
      workspace_name: result.workspace_name,
      workspace_roles: result.workspace_roles
    }
  end

  defp merge_operation_fields({tag, details}, fields), do: {tag, Map.merge(details, fields)}

  defp provisioning_failure(operation, %PersistenceError{} = failure) do
    details = Redaction.redact_operational_bounded(failure.details)

    if failure.kind in [:timeout, :unavailable, :internal] do
      OperationResult.error(
        operation,
        :unknown_outcome,
        :workspace_provisioning_outcome_unknown,
        :workspace_administrator,
        [],
        Map.put(details, :failure_kind, failure.kind)
      )
    else
      OperationResult.error(
        operation,
        :operation_failed,
        failure.kind,
        :workspace_administrator,
        [],
        details
      )
    end
  end

  defp read_password(prompt) do
    IO.write(:stderr, prompt <> ": ")

    case :io.get_password() do
      password when is_list(password) ->
        password = List.to_string(password)

        cond do
          password == "" -> {:error, :empty_password}
          byte_size(password) > 1_024 -> {:error, :password_too_long}
          String.contains?(password, <<0>>) -> {:error, :password_contains_null}
          true -> {:ok, password}
        end

      :eof ->
        {:error, :end_of_input}

      {:error, _reason} ->
        {:error, :stdin_unavailable}
    end
  end

  defp persistence_error(operation, %PersistenceError{} = failure) do
    error(operation, failure.kind,
      retryable?: failure.retryable?,
      details: Redaction.redact_operational_bounded(failure.details)
    )
  end

  defp database_error(operation, %Postgrex.Error{postgres: postgres}) do
    error(operation, :database_error, database_code: Map.get(postgres || %{}, :code))
  end

  defp database_error(operation, %Ecto.ConstraintError{type: type, constraint: constraint}) do
    error(operation, :database_constraint, constraint_type: type, constraint: constraint)
  end

  defp database_error(operation, reason) do
    Logger.error("favn.release.postgres_operation_failed operation=#{operation}")
    error(operation, :operation_failed, reason: safe_reason(reason))
  end

  defp safe_reason(reason) when is_atom(reason), do: reason
  defp safe_reason({reason, value}) when is_atom(reason) and is_atom(value), do: {reason, value}
  defp safe_reason({_reason, _value}), do: :redacted
  defp safe_reason(_reason), do: :redacted

  defp ok(operation, fields) do
    {:ok, fields |> Map.new() |> Map.merge(%{operation: operation, status: :ok})}
  end

  defp error(operation, code, fields \\ []) do
    {:error,
     fields
     |> Map.new()
     |> Map.merge(%{operation: operation, status: :error, code: code})}
  end
end
