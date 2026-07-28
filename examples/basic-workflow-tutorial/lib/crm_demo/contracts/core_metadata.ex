defmodule CrmDemo.Contracts.CoreMetadata do
  @moduledoc """
  Technical columns every Core model publishes.

  Position-matched with `CrmDemo.SQL.CoreMetadata`.
  """

  use Favn.SQL.ContractFragment

  column(:_processed_at, :datetime, null: false, description: "start time of the Favn run")
  column(:_favn_run_id, :string, null: false, description: "Favn run that published the row")
end
