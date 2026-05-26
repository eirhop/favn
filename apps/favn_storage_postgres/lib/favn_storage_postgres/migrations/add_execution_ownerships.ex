defmodule FavnStoragePostgres.Migrations.AddExecutionOwnerships do
  @moduledoc false

  use Ecto.Migration

  def change do
    create_if_not_exists table(:favn_execution_ownerships, primary_key: false) do
      add(:ownership_id, :string, primary_key: true)
      add(:run_id, :string, null: false)
      add(:asset_step_id, :string, null: false)
      add(:runner_execution_id, :string)
      add(:status, :string, null: false)
      add(:inserted_at, :utc_datetime_usec, null: false)
      add(:updated_at, :utc_datetime_usec, null: false)
      add(:ownership_payload, :text, null: false)
    end

    create_if_not_exists(index(:favn_execution_ownerships, [:run_id, :status]))
    create_if_not_exists(index(:favn_execution_ownerships, [:runner_execution_id]))
  end
end
