defmodule Favn.SQLAsset.Definition do
  @moduledoc """
  Internal compiled SQL asset definition used by the SQL asset frontend.
  """

  alias Favn.Asset
  alias Favn.SQLAsset.Materialization

  @enforce_keys [:module, :asset, :sql, :materialization]
  defstruct [:module, :asset, :sql, :materialization, raw_asset: nil]

  @type t :: %__MODULE__{
          module: module(),
          asset: Asset.t(),
          sql: String.t(),
          materialization: Materialization.t(),
          raw_asset: map() | nil
        }
end
