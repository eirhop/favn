defmodule FavnStoragePostgres.StorageV2.RunnerReleaseMigrationTest do
  use ExUnit.Case, async: false

  @moduletag :slow

  alias Ecto.Adapters.SQL
  alias FavnStoragePostgres.Config
  alias FavnStoragePostgres.Migrations.AddRunnerReleaseIdentityV2
  alias FavnStoragePostgres.Migrations.NormalizeResourceCircuitDefinitionsV2
  alias FavnStoragePostgres.StorageV2.Migrations

  @normalization_migration_version 20_260_720_020_000
  @runner_release_migration_version 20_260_721_000_000
  @current_migrations [
    {@normalization_migration_version, NormalizeResourceCircuitDefinitionsV2},
    {@runner_release_migration_version, AddRunnerReleaseIdentityV2}
  ]
  @valid_runner_release_id "rr_#{String.duplicate("a", 64)}"

  defmodule UpgradeRepo do
    use Ecto.Repo,
      otp_app: :favn_storage_postgres,
      adapter: Ecto.Adapters.Postgres
  end

  test "upgrades the previous schema, preserves history, enforces current identity, and downgrades" do
    source_url =
      System.get_env("FAVN_DATABASE_URL") ||
        raise "FAVN_DATABASE_URL is required for PostgreSQL migration tests"

    database = "favn_runner_release_upgrade_#{random_suffix()}"
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
    migrate_current_pair(:down)
    refute column_present?()

    assert {:ok,
            %{
              ready?: false,
              missing_migration_versions: missing_versions,
              definition_fingerprint_matches?: false
            }} = Migrations.diagnostics(UpgradeRepo)

    assert missing_versions == [
             @normalization_migration_version,
             @runner_release_migration_version
           ]

    legacy_id = "legacy-before-runner-release"
    legacy_hash = :crypto.hash(:sha256, legacy_id)

    assert {:ok, _result} =
             SQL.query(
               UpgradeRepo,
               """
               INSERT INTO favn_control.manifest_versions
                 (manifest_version_id, content_hash, schema_version,
                  runner_contract_version, payload_version, asset_count,
                  pipeline_count, schedule_count, atom_strings, manifest, inserted_at)
               VALUES ($1, $2, 9, 9, 1, 0, 0, 0, ARRAY[]::text[],
                       jsonb_build_object('assets', jsonb_build_array(),
                                          'pipelines', jsonb_build_array(),
                                          'schedules', jsonb_build_array()),
                       clock_timestamp())
               """,
               [legacy_id, legacy_hash]
             )

    assert :ok = Migrations.migrate!(UpgradeRepo)
    assert column_present?()

    assert %{rows: [[nil]]} =
             SQL.query!(
               UpgradeRepo,
               """
               SELECT required_runner_release_id
               FROM favn_control.manifest_versions
               WHERE manifest_version_id = $1
               """,
               [legacy_id]
             )

    assert_runner_release_constraint(10, nil, "current-null")
    assert_runner_release_constraint(10, "rr_invalid", "current-malformed")
    assert_runner_release_constraint(9, @valid_runner_release_id, "legacy-bound")

    assert {:ok, _result} =
             assert_manifest_insert(10, @valid_runner_release_id, "current-valid")

    assert_current_schema_ready()

    migrate_current_pair(:down)
    refute column_present?()

    invalid_current_id = "current-missing-runner-release"

    assert {:ok, _result} = insert_pre_migration_current(invalid_current_id)

    assert_raise Postgrex.Error,
                 ~r/current manifest rows cannot be bound to a valid runner release identity/,
                 fn -> Migrations.migrate!(UpgradeRepo) end

    SQL.query!(
      UpgradeRepo,
      "DELETE FROM favn_control.manifest_versions WHERE manifest_version_id = $1",
      [invalid_current_id]
    )

    assert :ok = Migrations.migrate!(UpgradeRepo)
    assert column_present?()

    assert %{rows: [[nil]]} =
             SQL.query!(
               UpgradeRepo,
               """
               SELECT required_runner_release_id
               FROM favn_control.manifest_versions
               WHERE manifest_version_id = $1
               """,
               [legacy_id]
             )

    assert %{rows: [[@valid_runner_release_id]]} =
             SQL.query!(
               UpgradeRepo,
               """
               SELECT required_runner_release_id
               FROM favn_control.manifest_versions
               WHERE manifest_version_id = $1
               """,
               ["migration-current-valid"]
             )

    assert_current_schema_ready()
  end

  defp assert_current_schema_ready do
    assert {:ok,
            %{
              ready?: true,
              missing_columns: [],
              missing_critical_constraints: [],
              missing_migration_versions: [],
              future_migration_versions: [],
              definition_fingerprint_matches?: true
            }} = Migrations.diagnostics(UpgradeRepo)
  end

  defp migrate_current_pair(:down) do
    assert [@runner_release_migration_version, @normalization_migration_version] =
             run_current_migrations(:down)
  end

  defp run_current_migrations(direction) do
    Ecto.Migrator.run(
      UpgradeRepo,
      @current_migrations,
      direction,
      all: true,
      prefix: "favn_control"
    )
  end

  defp column_present? do
    %{rows: [[count]]} =
      SQL.query!(
        UpgradeRepo,
        """
        SELECT count(*)
        FROM information_schema.columns
        WHERE table_schema = 'favn_control'
          AND table_name = 'manifest_versions'
          AND column_name = 'required_runner_release_id'
        """,
        []
      )

    count == 1
  end

  defp assert_runner_release_constraint(schema_version, runner_release_id, suffix) do
    assert {:error,
            %Postgrex.Error{
              postgres: %{
                code: :check_violation,
                constraint: "manifest_versions_runner_release_valid"
              }
            }} = assert_manifest_insert(schema_version, runner_release_id, suffix)
  end

  defp assert_manifest_insert(schema_version, runner_release_id, suffix) do
    manifest_version_id = "migration-#{suffix}"

    SQL.query(
      UpgradeRepo,
      """
      INSERT INTO favn_control.manifest_versions
        (manifest_version_id, content_hash, schema_version,
         runner_contract_version, required_runner_release_id,
         payload_version, asset_count, pipeline_count, schedule_count,
         atom_strings, manifest, inserted_at)
      VALUES ($1, $2, $3, 10, $4::varchar, 1, 0, 0, 0, ARRAY[]::text[],
              jsonb_build_object('assets', jsonb_build_array(),
                                 'pipelines', jsonb_build_array(),
                                 'schedules', jsonb_build_array(),
                                 'required_runner_release_id', $4::varchar),
              clock_timestamp())
      """,
      [
        manifest_version_id,
        :crypto.hash(:sha256, manifest_version_id),
        schema_version,
        runner_release_id
      ]
    )
  end

  defp insert_pre_migration_current(manifest_version_id) do
    SQL.query(
      UpgradeRepo,
      """
      INSERT INTO favn_control.manifest_versions
        (manifest_version_id, content_hash, schema_version,
         runner_contract_version, payload_version, asset_count,
         pipeline_count, schedule_count, atom_strings, manifest, inserted_at)
      VALUES ($1, $2, 10, 10, 1, 0, 0, 0, ARRAY[]::text[],
              jsonb_build_object('assets', jsonb_build_array(),
                                 'pipelines', jsonb_build_array(),
                                 'schedules', jsonb_build_array()),
              clock_timestamp())
      """,
      [manifest_version_id, :crypto.hash(:sha256, manifest_version_id)]
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
