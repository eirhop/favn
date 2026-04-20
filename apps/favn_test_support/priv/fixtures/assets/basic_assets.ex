defmodule Favn.Test.Fixtures.Assets.Basic.SampleAssets do
  @moduledoc false
  use Favn.Assets

  @doc "Extract raw orders"
  @asset true
  def extract_orders(_ctx), do: :ok

  @doc "Normalize extracted orders"
  @asset true
  @depends :extract_orders
  @meta tags: [:sales]
  def normalize_orders(_ctx), do: :ok
end

defmodule Favn.Test.Fixtures.Assets.Basic.CrossModuleAssets do
  @moduledoc false
  use Favn.Assets

  alias Favn.Test.Fixtures.Assets.Basic.SampleAssets

  @doc "Publish normalized orders"
  @asset true
  @depends {SampleAssets, :normalize_orders}
  @meta tags: [:reporting]
  def publish_orders(_ctx), do: :ok
end

defmodule Favn.Test.Fixtures.Assets.Basic.SpoofedAssets do
  @moduledoc false
  def __favn_assets__, do: :oops
end

defmodule Favn.Test.Fixtures.Assets.Basic.AdditionalAssets do
  @moduledoc false
  use Favn.Assets

  alias Favn.Test.Fixtures.Assets.Basic.CrossModuleAssets

  @doc "Archive published orders"
  @asset true
  @depends {CrossModuleAssets, :publish_orders}
  def archive_orders(_ctx), do: :ok
end
