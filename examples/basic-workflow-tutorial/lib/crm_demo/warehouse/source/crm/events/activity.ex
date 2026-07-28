defmodule CrmDemo.Warehouse.Source.Crm.Events.Activity do
  @moduledoc "Typed CRM activity events for one landed day."

  use CrmDemo.SQL.SourceMetadata
  use Favn.SQLAsset

  alias CrmDemo.Landing.Crm.Daily

  settings(landing_dataset: "activities")
  depends({Daily, :activities})
  relation(name: "activity")
  materialized({:incremental, strategy: :delete_insert, window_column: :occurred_at})
  execution_pool(:duckdb)

  contract do
    grain(by: [:activity_id], description: "one CRM activity event")

    column(:activity_id, :string, null: false)
    column(:account_id, :string, null: false)
    column(:activity_type, :string, null: false)
    column(:occurred_at, :datetime, null: false)

    include(CrmDemo.Contracts.SourceMetadata)

    row_count(equals: param(:expected_row_count), on_violation: :fail)
  end

  query(file: "activity.sql")
end
