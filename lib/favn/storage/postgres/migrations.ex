defmodule Favn.Storage.Postgres.Migrations do
  @moduledoc """
  Runs PostgreSQL migrations for the Postgres storage adapter.
  """

  alias Ecto.Adapters.SQL
  alias Favn.Storage.Postgres.Migrations.CreateFoundation

  @migrations [{20_260_413_100_000, CreateFoundation}]

  @spec migrate!(module()) :: :ok
  def migrate!(repo) do
    {:ok, _migrated, _apps} =
      Ecto.Migrator.with_repo(repo, fn migrator_repo ->
        Ecto.Migrator.run(migrator_repo, @migrations, :up, all: true)
      end)

    :ok
  end

  @spec schema_ready?(module()) :: boolean()
  def schema_ready?(repo) do
    required = [
      "public.favn_runs",
      "public.favn_run_nodes",
      "public.favn_asset_window_latest",
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
end
