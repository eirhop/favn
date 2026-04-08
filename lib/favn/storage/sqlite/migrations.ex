defmodule Favn.Storage.SQLite.Migrations do
  @moduledoc """
  Runs SQLite migrations for the durable storage adapter.
  """

  alias Favn.Storage.SQLite.Migrations.CreateRuns

  @migrations [{20_260_330_000_000, CreateRuns}]

  @spec migrate!(module()) :: :ok
  def migrate!(repo) do
    {:ok, _migrated, _apps} =
      Ecto.Migrator.with_repo(repo, fn migrator_repo ->
        Ecto.Migrator.run(migrator_repo, @migrations, :up, all: true)
      end)

    :ok
  end
end
