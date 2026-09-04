defmodule FavnStoragePostgres.StorageV2.RunExecutionCheckpointMigrationTest do
  use ExUnit.Case, async: false

  @moduletag :slow

  alias Ecto.Adapters.SQL
  alias FavnStoragePostgres.Config
  alias FavnStoragePostgres.Migrations.AddRunExecutionCheckpointsV2
  alias FavnStoragePostgres.Migrations.IncreaseRunnerTaskPayloadBoundV2
  alias FavnStoragePostgres.Migrations.IncreaseRunnerTaskOrchestrationContextBoundV2
  alias FavnStoragePostgres.StorageV2.Migrations

  @payload_migration_version 20_260_904_000_000
  @payload_migration {@payload_migration_version, IncreaseRunnerTaskPayloadBoundV2}

  @migration_version 20_260_729_000_000
  @migration {@migration_version, AddRunExecutionCheckpointsV2}
  @bound_migration_version 20_260_729_010_000
  @bound_migration {@bound_migration_version, IncreaseRunnerTaskOrchestrationContextBoundV2}

  defmodule UpgradeRepo do
    use Ecto.Repo,
      otp_app: :favn_storage_postgres,
      adapter: Ecto.Adapters.Postgres
  end

  test "adds the exact checkpoint authority surface and downgrades cleanly" do
    source_url =
      System.get_env("FAVN_DATABASE_URL") ||
        raise "FAVN_DATABASE_URL is required for PostgreSQL migration tests"

    database = "favn_checkpoint_upgrade_#{random_suffix()}"
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
    assert table_present?()
    assert runner_context_bound() == 8 * 1_024 * 1_024
    assert diagnostics_ready?()

    assert runner_bound("payload") == 12 * 1_024 * 1_024
    assert [@payload_migration_version] = migrate(@payload_migration, :down)
    assert runner_bound("payload") == 2 * 1_024 * 1_024
    assert runner_context_bound() == 8 * 1_024 * 1_024

    assert [@bound_migration_version] = migrate(@bound_migration, :down)
    assert runner_context_bound() == 2 * 1_024 * 1_024
    assert [@bound_migration_version] = migrate(@bound_migration, :up)

    assert [@migration_version] = migrate(:down)
    refute table_present?()

    assert [@migration_version] = migrate(:up)
    assert [@payload_migration_version] = migrate(@payload_migration, :up)
    assert runner_bound("payload") == 12 * 1_024 * 1_024
    assert runner_context_bound() == 8 * 1_024 * 1_024
    assert table_present?()
    assert diagnostics_ready?()
  end

  defp migrate(direction), do: migrate(@migration, direction)

  defp migrate(migration, direction) do
    Ecto.Migrator.run(
      UpgradeRepo,
      [migration],
      direction,
      all: true,
      prefix: "favn_control"
    )
  end

  defp runner_context_bound, do: runner_bound("orchestration_context")

  defp runner_bound(column) do
    %{rows: [[definition]]} =
      SQL.query!(
        UpgradeRepo,
        """
        SELECT pg_get_constraintdef(constraint_row.oid)
        FROM pg_constraint constraint_row
        JOIN pg_class table_row ON table_row.oid = constraint_row.conrelid
        JOIN pg_namespace namespace_row ON namespace_row.oid = table_row.relnamespace
        WHERE namespace_row.nspname = 'favn_control'
          AND table_row.relname = 'runner_tasks'
          AND constraint_row.conname = 'runner_tasks_payload_valid'
        """,
        []
      )

    [bound] =
      Regex.run(
        Regex.compile!("pg_column_size\\(#{column}\\) <= ([0-9]+)"),
        definition,
        capture: :all_but_first
      )

    String.to_integer(bound)
  end

  defp table_present? do
    %{rows: [[count]]} =
      SQL.query!(
        UpgradeRepo,
        """
        SELECT count(*)
        FROM information_schema.tables
        WHERE table_schema = 'favn_control'
          AND table_name = 'run_execution_checkpoints'
        """,
        []
      )

    count == 1
  end

  defp diagnostics_ready? do
    match?(
      {:ok,
       %{
         ready?: true,
         missing_columns: [],
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
