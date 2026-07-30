defmodule FavnStoragePostgres.StorageV2.ExternalIdentityMigrationTest do
  use ExUnit.Case, async: false

  @moduletag :slow

  alias Ecto.Adapters.SQL
  alias FavnStoragePostgres.Config
  alias FavnStoragePostgres.Migrations.AddExternalIdentitiesV2
  alias FavnStoragePostgres.StorageV2.Migrations

  @migration_version 20_260_730_030_000
  @migration {@migration_version, AddExternalIdentitiesV2}

  defmodule UpgradeRepo do
    use Ecto.Repo,
      otp_app: :favn_storage_postgres,
      adapter: Ecto.Adapters.Postgres
  end

  test "upgrades the previous auth schema and downgrades cleanly" do
    source_url =
      System.get_env("FAVN_DATABASE_URL") ||
        raise "FAVN_DATABASE_URL is required for PostgreSQL migration tests"

    database = "favn_external_identity_upgrade_#{random_suffix()}"
    target_url = replace_database(source_url, database)
    {:ok, admin} = Postgrex.start_link(postgrex_options(source_url))
    Process.unlink(admin)

    assert %Postgrex.Result{} = Postgrex.query!(admin, "CREATE DATABASE #{database}", [])

    {:ok, options} = Config.repo_options(url: target_url, ssl_mode: :disable, pool_size: 2)
    {:ok, repo} = UpgradeRepo.start_link(options)

    on_exit(fn ->
      if Process.alive?(repo) do
        try do
          GenServer.stop(repo)
        catch
          :exit, _reason -> :ok
        end
      end

      Postgrex.query!(admin, "DROP DATABASE IF EXISTS #{database} WITH (FORCE)", [])
      GenServer.stop(admin)
    end)

    assert :ok = Migrations.migrate!(UpgradeRepo)
    assert external_identity_table?()
    assert external_session_columns?()
    assert auth_session_constraint() =~ "azure_container_apps_entra"
    seed_entra_session_with_intent()
    assert operator_intent_present?()

    assert [@migration_version] = migrate(:down)
    refute external_identity_table?()
    refute external_session_columns?()
    refute auth_session_constraint() =~ "azure_container_apps_entra"
    refute operator_intent_present?()

    assert [@migration_version] = migrate(:up)
    assert external_identity_table?()
    assert external_session_columns?()
    assert auth_session_constraint() =~ "azure_container_apps_entra"
  end

  defp migrate(direction) do
    Ecto.Migrator.run(
      UpgradeRepo,
      [@migration],
      direction,
      all: true,
      prefix: "favn_control"
    )
  end

  defp external_identity_table? do
    scalar("""
    SELECT count(*)
    FROM information_schema.tables
    WHERE table_schema = 'favn_control'
      AND table_name = 'auth_external_identities'
    """) == 1
  end

  defp external_session_columns? do
    scalar("""
    SELECT count(*)
    FROM information_schema.columns
    WHERE table_schema = 'favn_control'
      AND table_name = 'auth_sessions'
      AND column_name IN ('external_tenant_id', 'external_subject_id')
    """) == 2
  end

  defp auth_session_constraint do
    scalar("""
    SELECT pg_get_constraintdef(constraint_row.oid)
    FROM pg_constraint constraint_row
    JOIN pg_class table_row ON table_row.oid = constraint_row.conrelid
    JOIN pg_namespace namespace_row ON namespace_row.oid = table_row.relnamespace
    WHERE namespace_row.nspname = 'favn_control'
      AND table_row.relname = 'auth_sessions'
      AND constraint_row.conname = 'auth_sessions_values_valid'
    """)
  end

  defp seed_entra_session_with_intent do
    statements = [
      """
      INSERT INTO favn_control.workspaces
        (workspace_id, slug, display_name, status, version, inserted_at, updated_at)
      VALUES
        ('ws-entra-migration', 'entra-migration', 'Entra migration', 'active', 1, now(), now())
      """,
      """
      INSERT INTO favn_control.auth_actors
        (actor_id, username, normalized_username, display_name, creation_command_id,
         creation_hash, status, version, inserted_at, updated_at)
      VALUES
        ('act-entra-migration', 'entra-migration', 'entra-migration', 'Entra migration',
         'actor-create-entra-migration', decode(repeat('ab', 32), 'hex'), 'active', 1,
         now(), now())
      """,
      """
      INSERT INTO favn_control.auth_workspace_memberships
        (workspace_id, actor_id, roles, status, version, inserted_at, updated_at)
      VALUES
        ('ws-entra-migration', 'act-entra-migration', ARRAY['customer_reader'], 'active',
         1, now(), now())
      """,
      """
      INSERT INTO favn_control.auth_external_identities
        (provider, tenant_id, subject_id, actor_id, linked_at, inserted_at)
      VALUES
        ('azure_container_apps_entra', '11111111-1111-4111-8111-111111111111',
         '22222222-2222-4222-8222-222222222222', 'act-entra-migration', now(), now())
      """,
      """
      INSERT INTO favn_control.auth_sessions
        (session_id, actor_id, workspace_id, creation_command_id, token_hash, provider,
         external_tenant_id, external_subject_id, status, expires_at, inserted_at, updated_at)
      VALUES
        ('ses-entra-migration', 'act-entra-migration', 'ws-entra-migration',
         'session-create-entra-migration', decode(repeat('cd', 32), 'hex'),
         'azure_container_apps_entra', '11111111-1111-4111-8111-111111111111',
         '22222222-2222-4222-8222-222222222222', 'active', now() + interval '1 hour',
         now(), now())
      """,
      """
      INSERT INTO favn_control.auth_operator_commands
        (intent_id, workspace_id, actor_id, session_id, operation, resource_type, resource_id,
         key_hash, request_fingerprint, status, expires_at, inserted_at, updated_at)
      VALUES
        ('intent-entra-migration', 'ws-entra-migration', 'act-entra-migration',
         'ses-entra-migration', 'run.cancel', 'run', 'run-entra-migration',
         'key-entra-migration', 'request-entra-migration', 'pending',
         now() + interval '1 hour', now(), now())
      """
    ]

    Enum.each(statements, &SQL.query!(UpgradeRepo, &1, []))
  end

  defp operator_intent_present? do
    scalar("""
    SELECT count(*)
    FROM favn_control.auth_operator_commands
    WHERE intent_id = 'intent-entra-migration'
    """) == 1
  end

  defp scalar(query) do
    %{rows: [[value]]} = SQL.query!(UpgradeRepo, query, [])
    value
  end

  defp replace_database(url, database) do
    uri = URI.parse(url)
    URI.to_string(%{uri | path: "/" <> database})
  end

  defp postgrex_options(url) do
    uri = URI.parse(url)
    [username, password] = String.split(uri.userinfo, ":", parts: 2)

    [
      hostname: uri.host,
      port: uri.port || 5432,
      username: URI.decode(username),
      password: URI.decode(password),
      database: String.trim_leading(uri.path, "/")
    ]
  end

  defp random_suffix, do: :crypto.strong_rand_bytes(8) |> Base.encode16(case: :lower)
end
