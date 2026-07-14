defmodule FavnStoragePostgres.Migrations do
  @moduledoc false

  alias Ecto.Adapters.SQL
  alias FavnStoragePostgres.Migrations.AddAssetFreshnessState
  alias FavnStoragePostgres.Migrations.AddExecutionAdmissionWaiters
  alias FavnStoragePostgres.Migrations.AddExecutionGroupSummaries
  alias FavnStoragePostgres.Migrations.AddExecutionLeases
  alias FavnStoragePostgres.Migrations.AddExecutionOwnerships
  alias FavnStoragePostgres.Migrations.AddLogEntries
  alias FavnStoragePostgres.Migrations.AddMaterializationClaims
  alias FavnStoragePostgres.Migrations.AddBackfillState
  alias FavnStoragePostgres.Migrations.AddBackfillProgress
  alias FavnStoragePostgres.Migrations.AddRunEventGlobalSequence
  alias FavnStoragePostgres.Migrations.AddRunEventStepIdentity
  alias FavnStoragePostgres.Migrations.AddRunEventType
  alias FavnStoragePostgres.Migrations.AddRunGroupQueryColumns
  alias FavnStoragePostgres.Migrations.AddRunPipelineQueryColumn
  alias FavnStoragePostgres.Migrations.AddTargetStatuses
  alias FavnStoragePostgres.Migrations.CreateFoundation
  alias FavnStoragePostgres.Migrations.RebuildBackfillReadModelsForDtos

  @migrations [
    {20_260_415_100_000, CreateFoundation},
    {20_260_428_100_000, AddBackfillState},
    {20_260_503_120_000, AddRunEventGlobalSequence},
    {20_260_505_100_000, RebuildBackfillReadModelsForDtos},
    {20_260_509_100_000, AddAssetFreshnessState},
    {20_260_510_100_000, AddLogEntries},
    {20_260_520_100_000, AddExecutionLeases},
    {20_260_520_110_000, AddExecutionAdmissionWaiters},
    {20_260_521_100_000, AddMaterializationClaims},
    {20_260_521_200_000, AddRunGroupQueryColumns},
    {20_260_522_100_000, AddBackfillProgress},
    {20_260_524_100_000, AddExecutionGroupSummaries},
    {20_260_525_100_000, AddTargetStatuses},
    {20_260_525_110_000, AddRunPipelineQueryColumn},
    {20_260_525_120_000, AddExecutionOwnerships},
    {20_260_714_100_000, AddRunEventType},
    {20_260_714_110_000, AddRunEventStepIdentity}
  ]
  @required_tables [
    "public.favn_manifest_versions",
    "public.favn_runtime_settings",
    "public.favn_runs",
    "public.favn_run_events",
    "public.favn_scheduler_cursors",
    "public.favn_pipeline_coverage_baselines",
    "public.favn_backfill_windows",
    "public.favn_backfill_progress",
    "public.favn_asset_window_states",
    "public.favn_asset_freshness_states",
    "public.favn_run_write_seq",
    "public.favn_run_event_global_seq",
    "public.favn_log_entries",
    "public.favn_log_global_seq",
    "public.favn_execution_leases",
    "public.favn_execution_lease_scopes",
    "public.favn_execution_admission_waiters",
    "public.favn_materialization_claims",
    "public.favn_execution_group_summaries",
    "public.favn_target_statuses",
    "public.favn_execution_ownerships"
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

  @spec schema_diagnostics(module() | pid()) :: {:ok, map()} | {:error, term()}
  def schema_diagnostics(repo) do
    with {:ok, missing_tables} <- missing_required_tables(repo),
         {:ok, missing_versions} <- missing_migration_versions(repo) do
      status =
        cond do
          missing_tables == [] and missing_versions == [] -> :ready
          length(missing_tables) == length(@required_tables) -> :empty_database
          true -> :upgrade_required
        end

      {:ok,
       %{
         status: status,
         ready?: status == :ready,
         missing_tables: missing_tables,
         missing_migration_versions: missing_versions,
         expected_migration_count: length(@expected_versions)
       }}
    end
  end

  defp schema_objects_ready?(repo) do
    placeholders = Enum.map_join(1..length(@required_tables), ",", &"$#{&1}")

    sql =
      "SELECT bool_and(to_regclass(name) IS NOT NULL) FROM unnest(ARRAY[#{placeholders}]::text[]) AS name"

    case SQL.query(repo, sql, @required_tables) do
      {:ok, %{rows: [[true]]}} -> true
      _ -> false
    end
  end

  defp missing_required_tables(repo) do
    placeholders = Enum.map_join(1..length(@required_tables), ",", &"$#{&1}")

    sql =
      "SELECT name FROM unnest(ARRAY[#{placeholders}]::text[]) AS name WHERE to_regclass(name) IS NULL ORDER BY name"

    case SQL.query(repo, sql, @required_tables) do
      {:ok, %{rows: rows}} -> {:ok, Enum.map(rows, fn [name] -> name end)}
      {:error, reason} -> {:error, reason}
    end
  end

  defp migration_versions_ready?(repo) do
    placeholders = Enum.map_join(1..length(@expected_versions), ",", &"$#{&1}")

    sql =
      "SELECT COUNT(*) = $#{length(@expected_versions) + 1} FROM schema_migrations WHERE version IN (#{placeholders})"

    params = @expected_versions ++ [length(@expected_versions)]

    case SQL.query(repo, sql, params) do
      {:ok, %{rows: [[true]]}} -> true
      _ -> false
    end
  end

  defp missing_migration_versions(repo) do
    placeholders = Enum.map_join(1..length(@expected_versions), ",", &"$#{&1}")

    sql =
      "SELECT expected.version FROM unnest(ARRAY[#{placeholders}]::bigint[]) AS expected(version) " <>
        "LEFT JOIN schema_migrations sm ON sm.version = expected.version " <>
        "WHERE sm.version IS NULL ORDER BY expected.version"

    versions = Enum.map(@expected_versions, &String.to_integer/1)

    case SQL.query(repo, sql, versions) do
      {:ok, %{rows: rows}} -> {:ok, Enum.map(rows, fn [version] -> to_string(version) end)}
      {:error, %Postgrex.Error{postgres: %{code: :undefined_table}}} -> {:ok, @expected_versions}
      {:error, reason} -> {:error, reason}
    end
  end
end
