defmodule FavnStoragePostgres.StorageV2.GenerationPublicationMigrationTest do
  use ExUnit.Case, async: false

  alias Ecto.Adapters.SQL
  alias FavnStoragePostgres.{Config, Repo}
  alias FavnStoragePostgres.StorageV2.Migrations
  alias FavnStoragePostgres.TestSupport.IsolatedDatabase

  setup do
    url = IsolatedDatabase.create!(System.fetch_env!("FAVN_DATABASE_URL"))
    {:ok, options} = Config.repo_options(url: url, ssl_mode: :disable, pool_size: 2)
    start_supervised!({Repo, options})
    :ok = Migrations.migrate!(Repo)
    :ok
  end

  test "fresh schema is ready with bounded assignment identity and no target repair table" do
    assert {:ok, diagnostics} = Migrations.diagnostics(Repo)
    assert diagnostics.ready?
    assert diagnostics.definition_fingerprint_matches?

    assert %{rows: [[nil]]} =
             SQL.query!(Repo, "SELECT to_regclass('favn_control.target_recovery_operations')", [])

    assert %{rows: [[definition]]} =
             SQL.query!(
               Repo,
               "SELECT pg_get_constraintdef(oid) FROM pg_constraint WHERE conname='runner_tasks_generation_precondition_bounded'",
               []
             )

    assert definition =~ "32768"
  end

  test "retirement refuses existing repair evidence without deleting it" do
    # Reintroduce only the predecessor table needed to exercise the refusal guard.
    SQL.query!(
      Repo,
      "CREATE TABLE favn_control.target_recovery_operations (operation_id text)",
      []
    )

    SQL.query!(
      Repo,
      "INSERT INTO favn_control.target_recovery_operations VALUES ('preserved-repair')",
      []
    )

    SQL.query!(
      Repo,
      "DELETE FROM favn_control.schema_migrations WHERE version=20260924010000",
      []
    )

    assert_raise Postgrex.Error, ~r/requires a fresh environment/, fn ->
      Migrations.migrate!(Repo)
    end

    assert %{rows: [["preserved-repair"]]} =
             SQL.query!(
               Repo,
               "SELECT operation_id FROM favn_control.target_recovery_operations",
               []
             )

    assert {:ok, [20_260_924_010_000]} = Migrations.pending_versions(Repo)
  end
end
