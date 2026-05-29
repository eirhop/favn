defmodule FavnStoragePostgres.Migrations.AddAuditEvents do
  @moduledoc false

  use Ecto.Migration

  def change do
    create_if_not_exists table(:favn_audit_events, primary_key: false) do
      add(:audit_id, :string, primary_key: true)
      add(:occurred_at, :utc_datetime_usec, null: false)
      add(:action, :string, null: false)
      add(:outcome, :string, null: false)
      add(:actor_id, :string)
      add(:session_id, :string)
      add(:browser_session_id, :string)
      add(:source, :string, null: false)
      add(:manifest_version_id, :string)
      add(:target_type, :string)
      add(:target_id, :string)
      add(:resource_type, :string)
      add(:resource_id, :string)
      add(:event_blob, :binary, null: false)
    end

    create_if_not_exists(index(:favn_audit_events, [:occurred_at, :audit_id]))
    create_if_not_exists(index(:favn_audit_events, [:actor_id, :occurred_at]))
    create_if_not_exists(index(:favn_audit_events, [:session_id, :occurred_at]))
    create_if_not_exists(index(:favn_audit_events, [:action, :occurred_at]))

    create_if_not_exists(
      index(:favn_audit_events, [:manifest_version_id, :target_type, :target_id, :occurred_at])
    )

    create_if_not_exists(index(:favn_audit_events, [:resource_type, :resource_id]))
  end
end
