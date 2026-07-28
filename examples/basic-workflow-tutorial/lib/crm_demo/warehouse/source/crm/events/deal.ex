defmodule CrmDemo.Warehouse.Source.Crm.Events.Deal do
  @moduledoc """
  Typed CRM deal events for one landed day.

  The window, coverage, and freshness policy come from the `Events` namespace.
  Landing already filtered the day, so the SQL does not filter again - the
  incremental write plan uses `occurred_at` to replace exactly that day in the
  target.
  """

  use CrmDemo.SQL.SourceMetadata
  use Favn.SQLAsset

  alias CrmDemo.Landing.Crm.Daily

  settings(landing_dataset: "deals")
  depends({Daily, :deals})
  relation(name: "deal")
  materialized({:incremental, strategy: :delete_insert, window_column: :occurred_at})
  execution_pool(:duckdb)

  contract do
    grain(by: [:deal_id], description: "one CRM deal event")

    column(:deal_id, :string, null: false)
    column(:account_id, :string, null: false)
    column(:stage, :string, null: false)
    column(:amount_cents, :integer, null: false)
    column(:occurred_at, :datetime, null: false)

    include(CrmDemo.Contracts.SourceMetadata)

    row_count(equals: param(:expected_row_count), on_violation: :fail)
  end

  query(file: "deal.sql")
end
