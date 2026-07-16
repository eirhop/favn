defmodule Favn.Test.Fixtures.Assets.Basic.SampleAssets do
  @moduledoc false
  use Favn.MultiAsset

  asset :extract_orders do
    description("Extract raw orders")
  end

  asset :normalize_orders do
    description("Normalize extracted orders")
    depends(:extract_orders)
    meta(tags: [:sales])
  end

  def asset(_ctx), do: :ok
end

defmodule Favn.Test.Fixtures.Assets.Basic.CrossModuleAssets do
  @moduledoc false
  use Favn.MultiAsset

  alias Favn.Test.Fixtures.Assets.Basic.SampleAssets

  asset :publish_orders do
    description("Publish normalized orders")
    depends({SampleAssets, :normalize_orders})
    meta(tags: [:reporting])
  end

  def asset(_ctx), do: :ok
end

defmodule Favn.Test.Fixtures.Assets.Basic.SpoofedAssets do
  @moduledoc false
  def __favn_assets__, do: :oops
end

defmodule Favn.Test.Fixtures.Assets.Basic.AdditionalAssets do
  @moduledoc false
  use Favn.MultiAsset

  alias Favn.Test.Fixtures.Assets.Basic.CrossModuleAssets

  asset :archive_orders do
    description("Archive published orders")
    depends({CrossModuleAssets, :publish_orders})
  end

  def asset(_ctx), do: :ok
end
