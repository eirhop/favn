defmodule FavnStoragePostgres.StorageV2.TaskPackageMigrationTest do
  use ExUnit.Case, async: false
  @moduletag :slow
  alias Ecto.Adapters.SQL
  alias Favn.Contracts.RunnerTask.PersistenceCodec
  alias FavnStoragePostgres.Config
  alias FavnStoragePostgres.StorageV2.Migrations

  defmodule UpgradeRepo do
    use Ecto.Repo, otp_app: :favn_storage_postgres, adapter: Ecto.Adapters.Postgres
  end

  test "empty adoption, all reset predicates, rollback and ordinary restart" do
    source = System.fetch_env!("FAVN_DATABASE_URL")
    tool_url = String.replace_prefix(source, "ecto://", "postgresql://")
    database = "favn_705_upgrade_" <> Base.encode16(:crypto.strong_rand_bytes(6), case: :lower)

    assert {_, 0} =
             System.cmd("createdb", ["--maintenance-db", tool_url, database],
               stderr_to_stdout: true
             )

    on_exit(fn ->
      System.cmd("dropdb", ["--if-exists", "--force", "--maintenance-db", tool_url, database],
        stderr_to_stdout: true
      )
    end)

    url = URI.to_string(%{URI.parse(source) | path: "/" <> database})
    {:ok, opts} = Config.repo_options(url: url, ssl_mode: :disable, pool_size: 2)
    start_supervised!({UpgradeRepo, opts})
    assert :ok = Migrations.migrate!(UpgradeRepo)
    assert {:ok, %{ready?: true}} = Migrations.diagnostics(UpgradeRepo)
    current = constraint()
    assert current =~ "payload_version = #{PersistenceCodec.payload_version()}"

    # Recreate just the previous constraint/version in this disposable database.
    prior = String.replace(current, "payload_version = 2", "payload_version = 13")
    sql("ALTER TABLE favn_control.runner_tasks DROP CONSTRAINT runner_tasks_payload_valid")

    sql(
      "ALTER TABLE favn_control.runner_tasks ADD CONSTRAINT runner_tasks_payload_valid #{prior}"
    )

    sql("DELETE FROM favn_control.schema_migrations WHERE version = 20260915000000")

    fixtures = [
      {"workspaces",
       "INSERT INTO favn_control.workspaces (workspace_id,slug,display_name,inserted_at,updated_at) VALUES ('guard','guard','Guard',now(),now())"},
      {"runner_task_commands",
       "INSERT INTO favn_control.runner_task_commands VALUES ('guard','guard','claim',decode(repeat('aa',32),'hex'),'{}',now(),now())"},
      {"runner_capacity_demands",
       "INSERT INTO favn_control.runner_capacity_demands (runner_pool,required_runner_release_id,updated_at) VALUES ('guard','rr_' || repeat('a',64),now())"},
      {"runner_sessions",
       "INSERT INTO favn_control.runner_sessions (session_id,runner_instance_id,runner_boot_id,session_generation,control_plane_boot_id,runner_pool,required_runner_release_id,beam_node,protocol_version,lifecycle_mode,registered_at,inserted_at,updated_at) VALUES ('rs_' || repeat('a',32),'guard','guard',1,'guard','guard','rr_' || repeat('a',64),'guard@host',13,'elastic',now(),now(),now())"}
    ]

    for {table, insert} <- fixtures do
      sql(insert)

      assert_raise Postgrex.Error, ~r/explicit environment reset/, fn ->
        Migrations.migrate!(UpgradeRepo)
      end

      assert constraint() == prior
      assert %{rows: [[1]]} = sql("SELECT count(*) FROM favn_control.#{table}")
      assert {:ok, [20_260_915_000_000]} = Migrations.pending_versions(UpgradeRepo)
      sql("DELETE FROM favn_control.#{table}")
    end

    assert :ok = Migrations.migrate!(UpgradeRepo)
    assert constraint() == current
    sql(elem(hd(fixtures), 1))
    assert :ok = Migrations.migrate!(UpgradeRepo)
    assert {:ok, %{ready?: true}} = Migrations.diagnostics(UpgradeRepo)
    assert %{rows: [[1]]} = sql("SELECT count(*) FROM favn_control.workspaces")
  end

  defp sql(query), do: SQL.query!(UpgradeRepo, query, [])

  defp constraint do
    %{rows: [[definition]]} =
      sql(
        "SELECT pg_get_constraintdef(oid) FROM pg_constraint WHERE conrelid='favn_control.runner_tasks'::regclass AND conname='runner_tasks_payload_valid'"
      )

    definition
  end
end
