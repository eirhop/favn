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

  @type schema_status ::
          :empty_database
          | :ready
          | :schema_missing
          | :upgrade_required
          | :schema_newer_than_release
          | :schema_inconsistent

  @type schema_diagnostics :: %{
          status: schema_status(),
          tables: [String.t()],
          required_tables: [String.t()],
          missing_tables: [String.t()],
          applied_versions: [String.t()],
          expected_versions: [String.t()],
          missing_versions: [String.t()],
          future_versions: [String.t()]
        }

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
    case schema_diagnostics(repo) do
      {:ok, %{status: :ready}} -> true
      _ -> false
    end
  end

  @spec schema_diagnostics(module()) :: {:ok, schema_diagnostics()} | {:error, term()}
  def schema_diagnostics(repo) when is_atom(repo) do
    with {:ok, tables} <- list_tables(repo),
         {:ok, applied_versions} <- list_applied_versions(repo) do
      missing_tables = @required_tables -- tables
      missing_versions = @expected_versions -- applied_versions
      future_versions = applied_versions -- @expected_versions

      status =
        classify_schema(
          tables,
          missing_tables,
          applied_versions,
          missing_versions,
          future_versions
        )

      {:ok,
       %{
         status: status,
         tables: tables,
         required_tables: @required_tables,
         missing_tables: missing_tables,
         applied_versions: applied_versions,
         expected_versions: @expected_versions,
         missing_versions: missing_versions,
         future_versions: future_versions
       }}
    end
  end

  defp classify_schema([], _missing_tables, [], _missing_versions, []), do: :empty_database

  defp classify_schema(_tables, _missing_tables, _applied_versions, _missing_versions, [_ | _]),
    do: :schema_newer_than_release

  defp classify_schema(_tables, [], _applied_versions, [], []), do: :ready

  defp classify_schema(_tables, [_ | _], _applied_versions, [], []), do: :schema_inconsistent

  defp classify_schema(_tables, _missing_tables, [], _missing_versions, []), do: :schema_missing

  defp classify_schema(_tables, _missing_tables, _applied_versions, [_ | _], []),
    do: :upgrade_required

  defp list_tables(repo) do
    sql =
      "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%' ORDER BY name"

    case SQL.query(repo, sql, []) do
      {:ok, %{rows: rows}} -> {:ok, Enum.map(rows, fn [name] -> name end)}
      {:error, reason} -> {:error, {:schema_tables_query_failed, reason}}
    end
  end

  defp list_applied_versions(repo) do
    case SQL.query(repo, "SELECT version FROM schema_migrations ORDER BY version", []) do
      {:ok, %{rows: rows}} ->
        {:ok, Enum.map(rows, fn [version] -> to_string(version) end)}

      {:error, %{message: message}} when is_binary(message) ->
        handle_missing_schema_migrations(message)

      {:error, reason} ->
        {:error, {:schema_migrations_query_failed, reason}}
    end
  end

  defp handle_missing_schema_migrations(message) do
    if String.contains?(message, "no such table: schema_migrations") do
      {:ok, []}
    else
      {:error, {:schema_migrations_query_failed, message}}
    end
  end
end
