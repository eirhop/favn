defmodule Favn.Storage.SQLite.Migrations.CreateRuns do
  use Ecto.Migration

  def change do
    create table(:runs, primary_key: false) do
      add :id, :text, primary_key: true
      add :status, :text, null: false
      add :started_at, :utc_datetime
      add :finished_at, :utc_datetime
      add :inserted_at, :utc_datetime, null: false
      add :updated_at, :utc_datetime, null: false
      add :run_blob, :binary, null: false
    end

    create index(:runs, [:status])
    create index(:runs, [:updated_at, :inserted_at, :id])
  end
end
