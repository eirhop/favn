defmodule FavnReferenceWorkload.Warehouse.Sources.ChannelCatalog do
  @moduledoc """
  External channel lookup source used by synthetic order generation.

  Like `CountryRegions`, this is a `Favn.Source` module. It models a source
  relation in the graph so downstream SQL can depend on it.

  In plain terms: this is a declared lookup table for valid acquisition
  channels.

  Alternative:

  - Keep it as a source relation when the data is externally managed.
  - Convert it to a SQL asset if you want Favn to generate/populate it.
  """

  use Favn.Namespace
  use Favn.Source

  @meta owner: "reference-workload", category: :reference_data, tags: [:source]
  @relation true
end
