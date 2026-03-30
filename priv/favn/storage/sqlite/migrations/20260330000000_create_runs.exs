defmodule Favn.Storage.SQLite.Migrations.CreateRuns do
  use Ecto.Migration

  def change do
    create table(:runs, primary_key: false) do
      add :id, :text, primary_key: true
      add :status, :text, null: false
      add :started_at, :utc_datetime
      add :finished_at, :utc_datetime
      add :inserted_at_us, :bigint, null: false
      add :updated_at_us, :bigint, null: false
      add :updated_seq, :bigint, null: false
      add :run_blob, :binary, null: false
    end

    create index(:runs, [:status])
    create index(:runs, [:updated_seq, :updated_at_us, :id])
  end
end
