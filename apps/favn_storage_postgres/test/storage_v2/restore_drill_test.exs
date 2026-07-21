defmodule FavnStoragePostgres.StorageV2.RestoreDrillTest do
  use ExUnit.Case, async: false

  @moduletag :slow

  alias Ecto.Adapters.SQL
  alias FavnStoragePostgres.Config
  alias FavnStoragePostgres.Repo
  alias FavnStoragePostgres.StorageV2.Migrations

  defmodule RestoreRepo do
    use Ecto.Repo,
      otp_app: :favn_storage_postgres,
      adapter: Ecto.Adapters.Postgres
  end

  @counted_tables ~w(workspaces manifest_versions workspace_deployments runs run_events outbox_events)

  setup_all do
    source_url =
      System.get_env("FAVN_DATABASE_URL") ||
        raise "FAVN_DATABASE_URL is required for PostgreSQL restore tests"

    {:ok, options} = Config.repo_options(url: source_url, ssl_mode: :disable, pool_size: 2)
    start_supervised!({Repo, options})
    :ok = Migrations.migrate!(Repo)
    {:ok, source_url: source_url}
  end

  test "a custom-format backup restores into an isolated compatible database", %{
    source_url: source_url
  } do
    manifest_version_id =
      "restore-runner-release-#{System.unique_integer([:positive, :monotonic])}"

    runner_release_id = "rr_#{String.duplicate("b", 64)}"

    SQL.query!(
      Repo,
      """
      INSERT INTO favn_control.manifest_versions
        (manifest_version_id, content_hash, schema_version,
         runner_contract_version, required_runner_release_id,
         payload_version, asset_count, pipeline_count, schedule_count,
         atom_strings, manifest, inserted_at)
      VALUES ($1, $2, 10, 10, $3, 1, 0, 0, 0, ARRAY[]::text[],
              jsonb_build_object('assets', jsonb_build_array(),
                                 'pipelines', jsonb_build_array(),
                                 'schedules', jsonb_build_array()),
              clock_timestamp())
      """,
      [manifest_version_id, :crypto.hash(:sha256, manifest_version_id), runner_release_id]
    )

    on_exit(fn ->
      SQL.query!(
        Repo,
        "DELETE FROM favn_control.manifest_versions WHERE manifest_version_id = $1",
        [manifest_version_id]
      )
    end)

    database = "favn_restore_#{System.unique_integer([:positive, :monotonic])}"
    target_url = replace_database(source_url, database)
    source_tool_url = postgres_tool_url(source_url)
    target_tool_url = postgres_tool_url(target_url)
    artifact = Path.join(System.tmp_dir!(), database <> ".dump")

    on_exit(fn ->
      System.cmd(
        "dropdb",
        ["--if-exists", "--force", "--maintenance-db", source_tool_url, database],
        stderr_to_stdout: true
      )

      File.rm(artifact)
    end)

    assert {_, 0} =
             System.cmd("createdb", ["--maintenance-db", source_tool_url, database],
               stderr_to_stdout: true
             )

    assert {_, 0} =
             System.cmd(
               "pg_dump",
               [
                 "--dbname",
                 source_tool_url,
                 "--format=custom",
                 "--schema=favn_control",
                 "--no-owner",
                 "--no-acl",
                 "--file",
                 artifact
               ],
               stderr_to_stdout: true
             )

    assert File.stat!(artifact).size > 0

    assert {_, 0} =
             System.cmd(
               "pg_restore",
               ["--dbname", target_tool_url, "--no-owner", "--no-acl", artifact],
               stderr_to_stdout: true
             )

    {:ok, restore_options} =
      Config.repo_options(url: target_url, ssl_mode: :disable, pool_size: 2)

    {:ok, restore_repo} = RestoreRepo.start_link(restore_options)

    try do
      assert {:ok, %{ready?: true}} = Migrations.diagnostics(RestoreRepo)

      assert table_counts(Repo) == table_counts(RestoreRepo)

      assert %{rows: [[^runner_release_id]]} =
               SQL.query!(
                 RestoreRepo,
                 """
                 SELECT required_runner_release_id
                 FROM favn_control.manifest_versions
                 WHERE manifest_version_id = $1
                 """,
                 [manifest_version_id]
               )

      assert {:ok, %{rows: [[0]]}} =
               SQL.query(
                 RestoreRepo,
                 """
                 SELECT count(*) FROM favn_control.run_events event
                 LEFT JOIN favn_control.runs run USING (workspace_id, run_id)
                 WHERE run.run_id IS NULL
                 """,
                 []
               )
    after
      GenServer.stop(restore_repo)
    end
  end

  defp table_counts(repo_or_connection) do
    @counted_tables
    |> Enum.map_join(" UNION ALL ", fn table ->
      "SELECT '#{table}', count(*) FROM favn_control.#{table}"
    end)
    |> then(fn query -> SQL.query!(repo_or_connection, query, []).rows end)
    |> Map.new(fn [table, count] -> {table, count} end)
  end

  defp replace_database(url, database) do
    uri = URI.parse(url)
    URI.to_string(%{uri | path: "/" <> database})
  end

  defp postgres_tool_url("ecto://" <> rest), do: "postgresql://" <> rest
  defp postgres_tool_url(url), do: url
end
