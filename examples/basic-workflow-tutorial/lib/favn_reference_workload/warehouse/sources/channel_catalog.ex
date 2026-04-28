defmodule FavnReferenceWorkload.Warehouse.Sources.ChannelCatalog do
  @moduledoc """
  Deterministic channel lookup seed used by synthetic order generation.

  Like `CountryRegions`, this is a small Elixir seed asset. It creates the
  lookup table that downstream raw assets read when validating synthetic orders.

  In plain terms: this is a declared lookup table for valid acquisition
  channels, and this asset owns creating it for the tutorial workload.

  Alternative:

  - Use `Favn.Source` when the data is externally managed and should not be
    created by the workload.
  - Convert it to a SQL asset if you want to author the seed data as SQL.
  """

  use Favn.Namespace
  use Favn.Asset

  alias FavnReferenceWorkload.Client.DuckDBJSONLoader

  @meta owner: "reference-workload", category: :reference_data, tags: [:seed]
  @relation true
  def asset(ctx) do
    DuckDBJSONLoader.replace_relation_from_sql(ctx.asset.relation, """
    select channel_code
    from (
      values ('organic_search'), ('paid_social'), ('email'), ('affiliate')
    ) as t(channel_code)
    """)
  end
end
