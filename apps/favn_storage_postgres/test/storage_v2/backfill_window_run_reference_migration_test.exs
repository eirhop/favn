defmodule FavnStoragePostgres.StorageV2.BackfillWindowRunReferenceMigrationTest do
  use ExUnit.Case, async: false

  @moduletag :slow

  alias Ecto.Adapters.SQL
  alias FavnStoragePostgres.Config
  alias FavnStoragePostgres.Migrations.RebindBackfillWindowRunReferenceV2
  alias FavnStoragePostgres.StorageV2.Migrations

  @migration_version 20_260_825_000_000
  @migration {@migration_version, RebindBackfillWindowRunReferenceV2}

  defmodule UpgradeRepo do
    use Ecto.Repo,
      otp_app: :favn_storage_postgres,
      adapter: Ecto.Adapters.Postgres
  end

  test "rebinds backfill child identity to its durable submission and downgrades when safe" do
    source_url =
      System.get_env("FAVN_DATABASE_URL") ||
        raise "FAVN_DATABASE_URL is required for PostgreSQL migration tests"

    database = "favn_backfill_reference_upgrade_#{random_suffix()}"
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
    assert window_reference() == {"run_submissions", false, false}
    assert diagnostics_ready?()

    seed_dangling_window("missing-run")

    assert_raise Postgrex.Error,
                 ~r/backfill window downgrade requires durable runs for 1 linked windows/,
                 fn -> migrate(:down) end

    delete_dangling_window()
    assert [@migration_version] = migrate(:down)
    assert window_reference() == {"runs", true, true}

    seed_dangling_window("missing-submission")

    assert_raise Postgrex.Error,
                 ~r/backfill window migration requires durable submissions for 1 linked windows/,
                 fn -> migrate(:up) end

    delete_dangling_window()
    assert [@migration_version] = migrate(:up)
    assert window_reference() == {"run_submissions", false, false}
    assert diagnostics_ready?()
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

  defp window_reference do
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
          AND source.relname = 'backfill_windows'
          AND constraint_row.conname IN (
            'backfill_windows_run_fk',
            'backfill_windows_run_submission_fk'
          )
        """,
        []
      )

    {referenced_table, deferrable?, deferred?}
  end

  defp seed_dangling_window(run_id) do
    UpgradeRepo.transaction(fn ->
      SQL.query!(UpgradeRepo, "SET LOCAL session_replication_role = replica", [])

      SQL.query!(
        UpgradeRepo,
        """
        INSERT INTO favn_control.backfill_windows
          (workspace_id, backfill_id, window_id, batch_index, window_key,
           window_start, window_end, status, fencing_token, run_id, attempt_count,
           payload, version, inserted_at, updated_at)
        VALUES
          ('migration-guard-workspace', 'migration-guard-backfill',
           'migration-guard-window', 0, 'migration-guard-window',
           clock_timestamp() - interval '1 hour', clock_timestamp(), 'succeeded',
           1, $1, 1, '{}'::jsonb, 1, clock_timestamp(), clock_timestamp())
        """,
        [run_id]
      )
    end)
  end

  defp delete_dangling_window do
    SQL.query!(
      UpgradeRepo,
      "DELETE FROM favn_control.backfill_windows WHERE workspace_id = 'migration-guard-workspace'",
      []
    )
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
