defmodule Favn.Storage.SQLite.Migrations.AddFavnCountersAndDropRunWriteOrders do
  use Ecto.Migration

  def up do
    create table(:favn_counters, primary_key: false) do
      add :name, :text, primary_key: true
      add :value, :bigint, null: false
    end

    execute("""
    INSERT INTO favn_counters (name, value)
    VALUES ('run_write_order', 0)
    ON CONFLICT(name) DO NOTHING
    """)

    drop_if_exists table(:run_write_orders)
  end

  def down do
    create table(:run_write_orders) do
    end

    drop_if_exists table(:favn_counters)
  end
end
