defmodule Favn.Manifest.Asset do
  @moduledoc """
  Manifest entry for one compiled asset.
  """

  alias Favn.Asset

  @type t :: %__MODULE__{
          ref: Favn.Ref.t(),
          module: module(),
          name: atom(),
          type: :elixir | :sql | :source,
          asset: Asset.t()
        }

  defstruct [:ref, :module, :name, :type, :asset]

  @spec from_asset(Asset.t()) :: t()
  def from_asset(%Asset{} = asset) do
    %__MODULE__{
      ref: asset.ref,
      module: asset.module,
      name: asset.name,
      type: asset.type,
      asset: asset
    }
  end
end
