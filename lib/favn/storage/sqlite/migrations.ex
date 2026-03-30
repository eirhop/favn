defmodule Favn.Storage.SQLite.Migrations do
  @moduledoc """
  Runs SQLite migrations for the durable storage adapter.
  """

  @spec migrate!(module()) :: :ok
  def migrate!(repo) do
    path = Application.app_dir(:favn, "priv/favn/storage/sqlite/migrations")

    {:ok, _migrated, _apps} =
      Ecto.Migrator.with_repo(repo, fn migrator_repo ->
        Ecto.Migrator.run(migrator_repo, path, :up, all: true)
      end)

    :ok
  end
end
