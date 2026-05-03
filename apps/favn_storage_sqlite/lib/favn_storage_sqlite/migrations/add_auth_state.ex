defmodule FavnStorageSqlite.Migrations.AddAuthState do
  @moduledoc false

  use Ecto.Migration

  def change do
    create table(:favn_auth_actors, primary_key: false) do
      add(:actor_id, :string, primary_key: true)
      add(:username, :string, null: false)
      add(:display_name, :string, null: false)
      add(:roles_blob, :binary, null: false)
      add(:status, :string, null: false)
      add(:inserted_at, :text, null: false)
      add(:updated_at, :text, null: false)
    end

    create(unique_index(:favn_auth_actors, [:username]))

    create table(:favn_auth_credentials, primary_key: false) do
      add(:actor_id, :string, primary_key: true)
      add(:credential_blob, :binary, null: false)
      add(:updated_at, :text, null: false)
    end

    create table(:favn_auth_sessions, primary_key: false) do
      add(:session_id, :string, primary_key: true)
      add(:token_hash, :string, null: false)
      add(:actor_id, :string, null: false)
      add(:provider, :string, null: false)
      add(:issued_at, :text, null: false)
      add(:expires_at, :text, null: false)
      add(:revoked_at, :text)
    end

    create(unique_index(:favn_auth_sessions, [:token_hash]))
    create(index(:favn_auth_sessions, [:actor_id, :revoked_at]))

    create table(:favn_auth_audits, primary_key: false) do
      add(:audit_id, :string, primary_key: true)
      add(:occurred_at, :text, null: false)
      add(:action, :string)
      add(:actor_id, :string)
      add(:session_id, :string)
      add(:outcome, :string)
      add(:entry_blob, :binary, null: false)
    end

    create(index(:favn_auth_audits, [:occurred_at]))
  end
end
