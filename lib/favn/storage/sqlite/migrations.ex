defmodule Favn.Storage.SQLite.Migrations do
  @moduledoc """
  Runs SQLite migrations for the durable storage adapter.
  """

  alias Favn.Storage.SQLite.Migrations.CreateRuns
  alias Favn.Storage.SQLite.Migrations.CreateSchedulerStates

  @migrations [
    {20_260_330_000_000, CreateRuns},
    {20_260_408_010_000, CreateSchedulerStates}
  ]

  @spec migrate!(module()) :: :ok
  def migrate!(repo) do
    {:ok, _migrated, _apps} =
      Ecto.Migrator.with_repo(repo, fn migrator_repo ->
        Ecto.Migrator.run(migrator_repo, @migrations, :up, all: true)
      end)

    :ok
  end
end
