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
  alias Favn.Manifest.Compatibility
  alias FavnOrchestrator.Persistence.Commands.ProvisionWorkspace
  alias FavnOrchestrator.Persistence.Error, as: PersistenceError
  alias FavnOrchestrator.Persistence.PlatformContext
  alias FavnStoragePostgres.Config
  alias FavnStoragePostgres.Privileges
  alias FavnStoragePostgres.Registry.Store
  alias FavnStoragePostgres.Repo
  alias FavnStoragePostgres.RuntimeInputKeyInventory
  alias FavnStoragePostgres.RuntimeInputKeys
  alias FavnStoragePostgres.StorageV2.Migrations

  @current_manifest_schema Compatibility.current_schema_version()
  @default_runtime_role "favn_runtime"
  @max_compaction_versions 100
  @preflight_blocker_sample_limit 100
  @restore_timeout_ms 600_000

  @type operation ::
          :migrate
          | :verify_schema
          | :verify_restore
          | :grant_runtime
          | :provision_workspace
          | :runtime_input_key_inventory
          | :compact_runtime_input_keys
          | :preflight_upgrade

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

  @doc "Applies every known Storage V2 migration with an elevated database role."
  @spec migrate() :: result()
  def migrate do
    with_repo(:migrate, fn ->
      with :ok <- require_elevated_role(:migrate),
           :ok <- Migrations.migrate!(Repo) do
        versions = Migrations.expected_versions()
        Logger.info("favn.release.postgres_migrated migration_versions=#{inspect(versions)}")
        ok(:migrate, migration_versions: versions)
      end
    end)
  end

  @doc "Verifies the exact PostgreSQL schema, migration, projection, and grant contract."
  @spec verify_schema() :: result()
  def verify_schema do
    with_repo(:verify_schema, fn ->
      case Migrations.diagnostics(Repo) do
        {:ok, %{ready?: true} = diagnostics} ->
          ok(:verify_schema,
            schema: diagnostics.schema,
            engine: diagnostics.engine,
            definition_fingerprint: diagnostics.actual_definition_fingerprint
          )

        {:ok, diagnostics} ->
          error(:verify_schema, :schema_not_ready, diagnostics: schema_diagnostics(diagnostics))

        {:error, reason} ->
          database_error(:verify_schema, reason)
      end
    end)
  end

  @doc "Verifies exact schema readiness plus authoritative restore relationships."
  @spec verify_restore() :: result()
  def verify_restore do
    with_repo(:verify_restore, fn ->
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

  @doc "Converges least-privilege grants for the configured runtime role."
  @spec grant_runtime() :: result()
  def grant_runtime do
    with_repo(:grant_runtime, fn ->
      role = System.get_env("FAVN_DATABASE_RUNTIME_ROLE", @default_runtime_role)

      with :ok <- require_elevated_role(:grant_runtime),
           :ok <- validate_role(:grant_runtime, role),
           :ok <- Privileges.grant_runtime!(Repo, role) do
        Logger.info("favn.release.postgres_runtime_granted role=#{role}")
        ok(:grant_runtime, role: role)
      end
    end)
  end

  @doc "Idempotently provisions one workspace from an atom-keyed map or keyword list."
  @spec provision_workspace(map() | keyword()) :: result()
  def provision_workspace(input) when is_map(input) or is_list(input) do
    with_repo(:provision_workspace, fn ->
      with {:ok, workspace} <- normalize_workspace(input),
           {:ok, context} <-
             PlatformContext.new(
               "release:workspace-provisioner",
               "release:workspace-provisioner",
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
        Logger.info("favn.release.workspace_provisioned workspace_id=#{workspace.workspace_id}")

        ok(:provision_workspace,
          workspace_id: workspace.workspace_id,
          slug: workspace.slug
        )
      else
        {:error, %PersistenceError{} = failure} ->
          persistence_error(:provision_workspace, failure)

        {:error, %{operation: :provision_workspace} = failure} ->
          {:error, failure}

        {:error, reason} ->
          error(:provision_workspace, :invalid_workspace, reason: safe_reason(reason))
      end
    end)
  end

  def provision_workspace(_input),
    do: error(:provision_workspace, :invalid_workspace, reason: :map_or_keyword_required)

  @doc "Lists persisted key versions, pin counts, and redacted configured-version metadata."
  @spec runtime_input_key_inventory() :: result()
  def runtime_input_key_inventory do
    with_repo(:runtime_input_key_inventory, fn ->
      case RuntimeInputKeyInventory.list(Repo) do
        {:ok, inventory} ->
          configured = RuntimeInputKeys.diagnostics()

          ok(:runtime_input_key_inventory,
            inventory: inventory,
            current_version: configured.current_version,
            retained_versions: configured.retained_versions,
            invalid_versions: configured.invalid_versions
          )

        {:error, reason} ->
          database_error(:runtime_input_key_inventory, reason)
      end
    end)
  end

  @doc "Removes explicitly requested unreferenced, non-current key versions."
  @spec compact_runtime_input_keys(pos_integer() | [pos_integer()]) :: result()
  def compact_runtime_input_keys(versions) do
    with {:ok, requested} <- normalize_versions(versions),
         :ok <- reject_current_key_version(requested) do
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
  end

  @doc "Reports active deployments that still use a pre-runner-identity manifest schema."
  @spec preflight_upgrade() :: result()
  def preflight_upgrade do
    with_repo(:preflight_upgrade, fn ->
      case SQL.query(
             Repo,
             """
             WITH blockers AS (
               SELECT runtime.workspace_id, deployment.deployment_id,
                      manifest.manifest_version_id, manifest.schema_version
               FROM favn_control.workspace_runtime_state AS runtime
               JOIN favn_control.workspace_deployments AS deployment
                 ON deployment.workspace_id = runtime.workspace_id
                AND deployment.deployment_id = runtime.active_deployment_id
               JOIN favn_control.manifest_versions AS manifest
                 ON manifest.manifest_version_id = deployment.manifest_version_id
               WHERE runtime.active_deployment_id IS NOT NULL
                 AND manifest.schema_version < $1
             )
             SELECT workspace_id, deployment_id, manifest_version_id, schema_version,
                    count(*) OVER () AS blocker_count
             FROM blockers
             ORDER BY workspace_id
             LIMIT $2
             """,
             [@current_manifest_schema, @preflight_blocker_sample_limit]
           ) do
        {:ok, %{rows: []}} ->
          ok(:preflight_upgrade,
            current_manifest_schema: @current_manifest_schema,
            blocker_count: 0,
            blocker_sample_limit: @preflight_blocker_sample_limit,
            truncated?: false,
            active_legacy_manifests: []
          )

        {:ok, %{rows: rows}} ->
          blocker_count = rows |> hd() |> List.last()

          blockers =
            Enum.map(rows, fn [
                                workspace_id,
                                deployment_id,
                                manifest_version_id,
                                schema_version,
                                _blocker_count
                              ] ->
              %{
                workspace_id: workspace_id,
                deployment_id: deployment_id,
                manifest_version_id: manifest_version_id,
                schema_version: schema_version
              }
            end)

          error(:preflight_upgrade, :active_legacy_manifests,
            current_manifest_schema: @current_manifest_schema,
            blocker_count: blocker_count,
            blocker_sample_limit: @preflight_blocker_sample_limit,
            truncated?: blocker_count > length(blockers),
            active_legacy_manifests: blockers
          )

        {:error, reason} ->
          database_error(:preflight_upgrade, reason)
      end
    end)
  end

  defp with_repo(operation, function) do
    with :ok <- ensure_dependencies(operation),
         {:ok, options} <- configured_repo_options(operation),
         {:ok, repo_state} <- start_repo(operation, options) do
      try do
        function.()
      rescue
        exception -> database_error(operation, exception)
      catch
        kind, reason ->
          error(operation, :operation_failed, failure_kind: kind, reason: safe_reason(reason))
      after
        stop_repo(repo_state)
      end
    end
  end

  defp ensure_dependencies(operation) do
    with {:ok, _applications} <- Application.ensure_all_started(:ecto_sql),
         {:ok, _applications} <- Application.ensure_all_started(:postgrex) do
      :ok
    else
      {:error, {_application, _reason}} -> error(operation, :dependency_start_failed)
    end
  end

  defp configured_repo_options(operation) do
    case Config.repo_options() do
      {:ok, options} ->
        {:ok, options}

      {:error, reason} ->
        error(operation, :invalid_database_configuration, reason: safe_reason(reason))
    end
  end

  defp start_repo(operation, options) do
    case Repo.start_link(options) do
      {:ok, pid} -> {:ok, %{pid: pid, owned?: true}}
      {:error, {:already_started, pid}} -> {:ok, %{pid: pid, owned?: false}}
      {:error, reason} -> database_error(operation, reason)
    end
  end

  defp stop_repo(%{pid: pid, owned?: true}) do
    if Process.alive?(pid), do: GenServer.stop(pid)
  end

  defp stop_repo(%{owned?: false}), do: :ok

  defp require_elevated_role(operation) do
    %{rows: [[current_role]]} = SQL.query!(Repo, "SELECT current_user", [])
    runtime_role = System.get_env("FAVN_DATABASE_RUNTIME_ROLE", @default_runtime_role)

    if current_role == runtime_role do
      error(operation, :restricted_runtime_role, role: current_role)
    else
      :ok
    end
  end

  defp validate_role(operation, role) do
    Privileges.quote_identifier!(role)
    :ok
  rescue
    ArgumentError -> error(operation, :invalid_runtime_role)
  end

  defp normalize_workspace(input) do
    values = if is_list(input), do: Map.new(input), else: input
    workspace_id = Map.get(values, :workspace_id)
    slug = Map.get(values, :slug, workspace_id)
    display_name = Map.get(values, :display_name, workspace_id)

    if valid_identifier?(workspace_id) and valid_identifier?(slug) and
         valid_identifier?(display_name) do
      {:ok, %{workspace_id: workspace_id, slug: slug, display_name: display_name}}
    else
      error(:provision_workspace, :invalid_workspace, reason: :invalid_identifier)
    end
  end

  defp valid_identifier?(value),
    do: is_binary(value) and value != "" and byte_size(value) <= 255

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

  defp reject_current_key_version(versions) do
    current = RuntimeInputKeys.diagnostics().current_version

    if current in versions do
      error(:compact_runtime_input_keys, :current_key_version_requested, current_version: current)
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

  defp persistence_error(operation, %PersistenceError{} = failure) do
    error(operation, failure.kind,
      retryable?: failure.retryable?,
      details: failure.details
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
