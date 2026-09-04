defmodule FavnStoragePostgres.StorageV2.RunExecutionCheckpointMigrationTest do
  use ExUnit.Case, async: false

  @moduletag :slow

  alias Ecto.Adapters.SQL
  alias FavnStoragePostgres.Config
  alias FavnStoragePostgres.Migrations.AddCrashSafeRunnerTasksV2
  alias FavnStoragePostgres.StorageV2.Migrations

  @current_migration_version 20_260_904_020_000
  @current_migration {@current_migration_version, AddCrashSafeRunnerTasksV2}

  defmodule UpgradeRepo do
    use Ecto.Repo,
      otp_app: :favn_storage_postgres,
      adapter: Ecto.Adapters.Postgres
  end

  test "current recovery schema has exact bounds and rejects in-place downgrade" do
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
    assert runner_context_bound() == 4 * 4 * 1_024 * 1_024 + 8_192

    current_constraint = runner_constraint_definition()
    assert current_constraint =~ "THEN 33562624"
    assert current_constraint =~ "ELSE 4202496"
    assert diagnostics_ready?()

    assert_raise RuntimeError, ~r/requires a separate compatible development database/, fn ->
      Ecto.Migrator.run(
        UpgradeRepo,
        [@current_migration],
        :down,
        all: true,
        prefix: "favn_control"
      )
    end

    assert diagnostics_ready?()
  end

  defp runner_context_bound, do: runner_bound("orchestration_context")

  defp runner_bound(column) do
    definition = runner_constraint_definition()

    [bound] =
      Regex.run(
        Regex.compile!("pg_column_size\\(#{column}\\) <= ([0-9]+)"),
        definition,
        capture: :all_but_first
      )

    String.to_integer(bound)
  end

  defp runner_constraint_definition do
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

    definition
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
