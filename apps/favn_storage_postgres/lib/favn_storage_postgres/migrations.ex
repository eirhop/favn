defmodule FavnStoragePostgres.Migrations do
  @moduledoc false

  alias Ecto.Adapters.SQL
  alias FavnStoragePostgres.Migrations.CreateFoundation

  @migrations [{20_260_415_100_000, CreateFoundation}]
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
    required = [
      "public.favn_manifest_versions",
      "public.favn_runtime_settings",
      "public.favn_runs",
      "public.favn_run_events",
      "public.favn_scheduler_cursors",
      "public.favn_run_write_seq"
    ]

    placeholders = Enum.map_join(1..length(required), ",", &"$#{&1}")

    sql =
      "SELECT bool_and(to_regclass(name) IS NOT NULL) FROM unnest(ARRAY[#{placeholders}]::text[]) AS name"

    case SQL.query(repo, sql, required) do
      {:ok, %{rows: [[true]]}} -> true
      _ -> false
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
end
