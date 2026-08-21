defmodule FavnStoragePostgres.StorageV2.ScheduleOccurrenceRunReferenceMigrationTest do
  use ExUnit.Case, async: false

  @moduletag :slow

  alias Ecto.Adapters.SQL
  alias FavnStoragePostgres.Config
  alias FavnStoragePostgres.Migrations.RebindScheduleOccurrenceRunReferenceV2
  alias FavnStoragePostgres.StorageV2.Migrations

  @migration_version 20_260_821_000_000
  @migration {@migration_version, RebindScheduleOccurrenceRunReferenceV2}

  defmodule UpgradeRepo do
    use Ecto.Repo,
      otp_app: :favn_storage_postgres,
      adapter: Ecto.Adapters.Postgres
  end

  test "rebinds scheduled run identity to its durable submission and downgrades when safe" do
    source_url =
      System.get_env("FAVN_DATABASE_URL") ||
        raise "FAVN_DATABASE_URL is required for PostgreSQL migration tests"

    database = "favn_schedule_reference_upgrade_#{random_suffix()}"
    target_url = replace_database(source_url, database)
    source_tool_url = postgres_tool_url(source_url)

    assert {_, 0} =
             System.cmd("createdb", ["--maintenance-db", source_tool_url, database],
               stderr_to_stdout: true
             )

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

      System.cmd(
        "dropdb",
        ["--if-exists", "--force", "--maintenance-db", source_tool_url, database],
        stderr_to_stdout: true
      )
    end)

    assert :ok = Migrations.migrate!(UpgradeRepo)
    assert occurrence_reference() == {"run_submissions", false, false}
    assert diagnostics_ready?()

    assert [@migration_version] = migrate(:down)
    assert occurrence_reference() == {"runs", true, true}

    assert [@migration_version] = migrate(:up)
    assert occurrence_reference() == {"run_submissions", false, false}
    assert diagnostics_ready?()

    assert :ok = commit_occurrence_before_run()

    assert %{rows: [[1, 1, 0]]} =
             SQL.query!(
               UpgradeRepo,
               """
               SELECT
                 (SELECT count(*) FROM favn_control.schedule_occurrences
                  WHERE workspace_id = 'schedule-commit-workspace'),
                 (SELECT count(*) FROM favn_control.run_submissions
                  WHERE workspace_id = 'schedule-commit-workspace'),
                 (SELECT count(*) FROM favn_control.runs
                  WHERE workspace_id = 'schedule-commit-workspace')
               """,
               []
             )
  end

  defp commit_occurrence_before_run do
    UpgradeRepo.transaction(fn ->
      query!("""
      INSERT INTO favn_control.workspaces
        (workspace_id, slug, display_name, status, version, inserted_at, updated_at)
      VALUES
        ('schedule-commit-workspace', 'schedule-commit-workspace', 'Schedule commit',
         'active', 1, clock_timestamp(), clock_timestamp())
      """)

      query!("""
      INSERT INTO favn_control.manifest_versions
        (manifest_version_id, content_hash, schema_version, runner_contract_version,
         runner_releases, payload_version, manifest, inserted_at,
         asset_count, pipeline_count, schedule_count)
      VALUES
        ('schedule-commit-manifest', decode(repeat('11', 32), 'hex'), 1, 1,
         '{"default":"schedule-commit-release"}'::jsonb, 1, '{}'::jsonb,
         clock_timestamp(), 0, 1, 1)
      """)

      query!("""
      INSERT INTO favn_control.workspace_deployments
        (workspace_id, deployment_id, manifest_version_id, configuration,
         configuration_fingerprint, target_catalog_fingerprint, configuration_version,
         inserted_at)
      VALUES
        ('schedule-commit-workspace', 'schedule-commit-deployment',
         'schedule-commit-manifest', '{}'::jsonb, decode(repeat('22', 32), 'hex'),
         decode(repeat('33', 32), 'hex'), 1, clock_timestamp())
      """)

      query!("""
      INSERT INTO favn_control.workspace_deployment_targets
        (workspace_id, deployment_id, target_kind, target_id, selection_source,
         customer_visible, descriptor, inserted_at)
      VALUES
        ('schedule-commit-workspace', 'schedule-commit-deployment', 'pipeline',
         'schedule-commit-pipeline', 'common', true,
         '{"target_id":"schedule-commit-pipeline"}'::jsonb, clock_timestamp())
      """)

      query!("""
      INSERT INTO favn_control.schedule_cursors
        (workspace_id, deployment_id, target_kind, pipeline_target_id, schedule_id,
         next_due_at, cursor, version, claim_generation, updated_at,
         schedule_fingerprint, definition)
      VALUES
        ('schedule-commit-workspace', 'schedule-commit-deployment', 'pipeline',
         'schedule-commit-pipeline', 'daily', clock_timestamp() - interval '1 second',
         '{}'::jsonb, 1, 0, clock_timestamp(), 'schedule-commit-fingerprint', '{}'::jsonb)
      """)

      query!("""
      INSERT INTO favn_control.run_submissions
        (workspace_id, submission_id, source, idempotency_key, request_hash, authority,
         deployment_id, manifest_version_id, target_kind, target_id, run_id, intent,
         status, attempt, claim_generation, retry_root_id, enqueued_at, available_at,
         inserted_at, updated_at)
      VALUES
        ('schedule-commit-workspace', 'schedule-commit-submission', 'scheduler',
         'schedule-commit-idempotency', decode(repeat('44', 32), 'hex'),
         '{"workspace_id":"schedule-commit-workspace","principal_id":"scheduler","roles":["customer_operator"],"request_id":null}'::jsonb,
         'schedule-commit-deployment', 'schedule-commit-manifest', 'pipeline',
         'schedule-commit-pipeline', 'schedule-commit-run', '{}'::jsonb, 'queued',
         0, 0, 'schedule-commit-submission', clock_timestamp(), clock_timestamp(),
         clock_timestamp(), clock_timestamp())
      """)

      query!("""
      INSERT INTO favn_control.schedule_occurrences
        (workspace_id, occurrence_id, occurrence_key, evaluation_command_id,
         deployment_id, pipeline_target_id, schedule_id, due_at, payload, status,
         claim_generation, run_id, attempt_count, inserted_at, updated_at)
      VALUES
        ('schedule-commit-workspace', 'schedule-commit-occurrence',
         decode(repeat('55', 32), 'hex'), 'schedule-commit-evaluation',
         'schedule-commit-deployment', 'schedule-commit-pipeline', 'daily',
         clock_timestamp() - interval '1 second', '{}'::jsonb, 'completed', 1,
         'schedule-commit-run', 1, clock_timestamp(), clock_timestamp())
      """)

      :ok
    end)
    |> case do
      {:ok, :ok} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  defp query!(statement), do: SQL.query!(UpgradeRepo, statement, [])

  defp migrate(direction) do
    Ecto.Migrator.run(
      UpgradeRepo,
      [@migration],
      direction,
      all: true,
      prefix: "favn_control"
    )
  end

  defp occurrence_reference do
    %{rows: [[referenced_table, deferrable?, deferred?]]} =
      SQL.query!(
        UpgradeRepo,
        """
        SELECT referenced.relname, constraint_row.condeferrable, constraint_row.condeferred
        FROM pg_constraint constraint_row
        JOIN pg_class source ON source.oid = constraint_row.conrelid
        JOIN pg_class referenced ON referenced.oid = constraint_row.confrelid
        JOIN pg_namespace namespace_row ON namespace_row.oid = source.relnamespace
        WHERE namespace_row.nspname = 'favn_control'
          AND source.relname = 'schedule_occurrences'
          AND constraint_row.conname IN (
            'schedule_occurrences_run_fk',
            'schedule_occurrences_run_submission_fk'
          )
        """,
        []
      )

    {referenced_table, deferrable?, deferred?}
  end

  defp diagnostics_ready? do
    match?(
      {:ok,
       %{
         ready?: true,
         missing_critical_constraints: [],
         missing_migration_versions: [],
         definition_fingerprint_matches?: true
       }},
      Migrations.diagnostics(UpgradeRepo)
    )
  end

  defp replace_database(url, database) do
    uri = URI.parse(url)
    URI.to_string(%{uri | path: "/" <> database})
  end

  defp postgres_tool_url("ecto://" <> rest), do: "postgresql://" <> rest
  defp postgres_tool_url(url), do: url

  defp random_suffix, do: :crypto.strong_rand_bytes(8) |> Base.encode16(case: :lower)
end
