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
    case SQL.query(repo, "SELECT to_regclass('public.favn_runs')", []) do
      {:ok, %{rows: [[nil]]}} -> false
      {:ok, %{rows: [[_name]]}} -> true
      _ -> false
    end
  end
end
