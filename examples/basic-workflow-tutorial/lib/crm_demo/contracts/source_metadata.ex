defmodule CrmDemo.Contracts.SourceMetadata do
  @moduledoc """
  Technical columns every Source relation publishes.

  This fragment is position-matched with `CrmDemo.SQL.SourceMetadata`: the
  columns declared here appear in the same order the SQL helper projects them.
  Fragments hold technical metadata only - grain, business columns, keys, and
  checks stay visible in each asset.
  """

  use Favn.SQL.ContractFragment

  column(:_landing_run_id, :string, null: false, description: "landing run that produced the row")
  column(:_extracted_at, :datetime, null: false, description: "when the source was read")
  column(:_row_hash, :string, null: false, description: "hash of the raw source record")
  column(:_favn_run_id, :string, null: false, description: "Favn run that published the row")
end
