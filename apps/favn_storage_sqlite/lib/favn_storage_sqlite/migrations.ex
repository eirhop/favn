defmodule FavnStorageSqlite.Migrations do
  @moduledoc false

  alias Ecto.Adapters.SQL
  alias FavnStorageSqlite.Migrations.AddBackfillState
  alias FavnStorageSqlite.Migrations.CreateFoundation

  @migrations [
    {20_260_415_000_000, CreateFoundation},
    {20_260_428_100_000, AddBackfillState}
  ]
  @required_tables [
    "favn_manifest_versions",
    "favn_runtime_settings",
    "favn_runs",
    "favn_run_events",
    "favn_scheduler_cursors",
    "favn_pipeline_coverage_baselines",
    "favn_backfill_windows",
    "favn_asset_window_states",
    "favn_counters"
  ]
  @expected_versions Enum.map(@migrations, fn {version, _module} -> to_string(version) end)

  @spec migrate!(module()) :: :ok
  def migrate!(repo) when is_atom(repo) do
    {:ok, _migrated, _apps} =
      Ecto.Migrator.with_repo(repo, fn migrator_repo ->
        Ecto.Migrator.run(migrator_repo, @migrations, :up, all: true)
      end)

    :ok
  end

  @spec schema_ready?(module()) :: boolean()
  def schema_ready?(repo) when is_atom(repo) do
    schema_objects_ready?(repo) and migration_versions_ready?(repo)
  end

  defp schema_objects_ready?(repo) do
    placeholders = Enum.map_join(1..length(@required_tables), ",", &"?#{&1}")

    sql =
      "SELECT COUNT(*) = ?#{length(@required_tables) + 1} FROM sqlite_master WHERE type = 'table' AND name IN (#{placeholders})"

    params = @required_tables ++ [length(@required_tables)]

    case SQL.query(repo, sql, params) do
      {:ok, %{rows: [[1]]}} -> true
      {:ok, %{rows: [[true]]}} -> true
      _ -> false
    end
  end

  defp migration_versions_ready?(repo) do
    placeholders = Enum.map_join(1..length(@expected_versions), ",", &"?#{&1}")

    sql =
      "SELECT COUNT(*) = ?#{length(@expected_versions) + 1} FROM schema_migrations WHERE version IN (#{placeholders})"

    params = @expected_versions ++ [length(@expected_versions)]

    case SQL.query(repo, sql, params) do
      {:ok, %{rows: [[1]]}} -> true
      {:ok, %{rows: [[true]]}} -> true
      _ -> false
    end
  end
end
