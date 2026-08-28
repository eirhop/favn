defmodule FavnStoragePostgres.StorageV2.AssetEvidenceBindingMigrationTest do
  use ExUnit.Case, async: false

  @moduletag :slow

  alias Ecto.Adapters.SQL
  alias FavnStoragePostgres.Config
  alias FavnStoragePostgres.Migrations.AddAssetEvidenceBindingsV2
  alias FavnStoragePostgres.StorageV2.Migrations

  @migration_version 20_260_728_010_000
  @migration {@migration_version, AddAssetEvidenceBindingsV2}
  @evidence_generation_id "ag_#{String.duplicate("a", 64)}"
  @runner_release_id "rr_#{String.duplicate("b", 64)}"
  @workspace_id "evidence-migration"
  @manifest_id "evidence-manifest"
  @deployment_id "evidence-deployment"
  @target_id "asset:Elixir.Example.Asset:orders"

  defmodule UpgradeRepo do
    use Ecto.Repo,
      otp_app: :favn_storage_postgres,
      adapter: Ecto.Adapters.Postgres
  end

  test "seeds active non-persisted assets and downgrades cleanly" do
    source_url =
      System.get_env("FAVN_DATABASE_URL") ||
        raise "FAVN_DATABASE_URL is required for PostgreSQL migration tests"

    database = "favn_asset_evidence_upgrade_#{random_suffix()}"
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
    assert [@migration_version] = migrate(:down)
    refute table_present?()

    insert_active_deployment()

    assert :ok = Migrations.migrate!(UpgradeRepo)
    assert table_present?()

    assert %{rows: [[@evidence_generation_id, @manifest_id]]} =
             SQL.query!(
               UpgradeRepo,
               """
               SELECT evidence_generation_id, initial_manifest_id
               FROM favn_control.asset_evidence_bindings
               WHERE workspace_id = $1 AND target_id = $2
               """,
               [@workspace_id, @target_id]
             )

    assert {:ok,
            %{
              ready?: true,
              missing_columns: [],
              missing_critical_constraints: [],
              missing_migration_versions: [],
              definition_fingerprint_matches?: true
            }} = Migrations.diagnostics(UpgradeRepo)

    assert [@migration_version] = migrate(:down)
    refute table_present?()
  end

  defp insert_active_deployment do
    hash = :crypto.hash(:sha256, @manifest_id)

    SQL.query!(
      UpgradeRepo,
      """
      INSERT INTO favn_control.workspaces
        (workspace_id, slug, display_name, status, version, inserted_at, updated_at)
      VALUES ($1, $1, 'Evidence Migration', 'active', 1, clock_timestamp(), clock_timestamp())
      """,
      [@workspace_id]
    )

    SQL.query!(
      UpgradeRepo,
      """
      WITH payload AS (
        SELECT jsonb_build_object(
          'assets', jsonb_build_array(
            jsonb_build_object(
              'ref', jsonb_build_array('Elixir.Example.Asset', 'orders'),
              'target_descriptor', NULL,
              'semantic_generation_id', $4::text
            )
          ),
          'pipelines', jsonb_build_array(),
          'schedules', jsonb_build_array(),
          'runner_releases', jsonb_build_object('default', $3::text)
        ) AS manifest
      )
      INSERT INTO favn_control.manifest_versions
        (manifest_version_id, content_hash, schema_version, runner_contract_version,
         runner_releases, payload_version, asset_count, pipeline_count,
         schedule_count, atom_strings, manifest, manifest_index_bytes, inserted_at)
      SELECT
        $1, $2, 14, 13, jsonb_build_object('default', $3::text),
        1, 1, 0, 0, ARRAY[]::text[],
        payload.manifest,
        octet_length(payload.manifest::text),
        clock_timestamp()
      FROM payload
      """,
      [@manifest_id, hash, @runner_release_id, @evidence_generation_id]
    )

    SQL.query!(
      UpgradeRepo,
      """
      INSERT INTO favn_control.workspace_deployments
        (workspace_id, deployment_id, manifest_version_id, configuration,
         configuration_fingerprint, target_catalog_fingerprint, configuration_version,
         deployed_by_actor_id, inserted_at)
      VALUES (
        $1, $2, $3, '{}'::jsonb, $4, $4, 1, 'migration-test', clock_timestamp()
      )
      """,
      [@workspace_id, @deployment_id, @manifest_id, hash]
    )

    SQL.query!(
      UpgradeRepo,
      """
      INSERT INTO favn_control.workspace_deployment_targets
        (workspace_id, deployment_id, target_kind, target_id, selection_source,
         customer_visible, descriptor, inserted_at)
      VALUES (
        $1, $2, 'asset', $3::text, 'explicit', true,
        jsonb_build_object('target_id', $3::text, 'label', 'Example.Asset.orders'),
        clock_timestamp()
      )
      """,
      [@workspace_id, @deployment_id, @target_id]
    )

    SQL.query!(
      UpgradeRepo,
      """
      INSERT INTO favn_control.workspace_runtime_state
        (workspace_id, active_deployment_id, revision, activated_by_actor_id,
         activated_at, updated_at)
      VALUES ($1, $2, 1, 'migration-test', clock_timestamp(), clock_timestamp())
      """,
      [@workspace_id, @deployment_id]
    )
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

  defp table_present? do
    %{rows: [[count]]} =
      SQL.query!(
        UpgradeRepo,
        """
        SELECT count(*)
        FROM information_schema.tables
        WHERE table_schema = 'favn_control'
          AND table_name = 'asset_evidence_bindings'
        """,
        []
      )

    count == 1
  end

  defp replace_database(url, database) do
    uri = URI.parse(url)
    URI.to_string(%{uri | path: "/" <> database})
  end

  defp postgres_tool_url("ecto://" <> rest), do: "postgresql://" <> rest
  defp postgres_tool_url(url), do: url

  defp random_suffix, do: :crypto.strong_rand_bytes(8) |> Base.encode16(case: :lower)
end
