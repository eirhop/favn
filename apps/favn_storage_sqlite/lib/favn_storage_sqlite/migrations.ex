defmodule FavnStorageSqlite.Migrations do
  @moduledoc false

  alias FavnStorageSqlite.Migrations.CreateFoundation

  @migrations [{20_260_415_000_000, CreateFoundation}]

  @spec migrate!(module()) :: :ok
  def migrate!(repo) when is_atom(repo) do
    {:ok, _migrated, _apps} =
      Ecto.Migrator.with_repo(repo, fn migrator_repo ->
        Ecto.Migrator.run(migrator_repo, @migrations, :up, all: true)
      end)

    :ok
  end
end
