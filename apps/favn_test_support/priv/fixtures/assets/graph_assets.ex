defmodule Favn.Test.Fixtures.Assets.Graph.SourceAssets do
  @moduledoc false
  use Favn.MultiAsset

  asset :raw_orders do
    description("Raw orders")
  end

  asset :raw_customers do
    description("Raw customers")
  end

  def asset(_ctx), do: :ok
end

defmodule Favn.Test.Fixtures.Assets.Graph.WarehouseAssets do
  @moduledoc false
  use Favn.MultiAsset

  alias Favn.Test.Fixtures.Assets.Graph.SourceAssets

  asset :normalize_orders do
    description("Normalize orders")
    depends({SourceAssets, :raw_orders})
    meta(tags: [:warehouse])
  end

  asset :normalize_customers do
    description("Normalize customers")
    depends({SourceAssets, :raw_customers})
    meta(tags: [:warehouse])
  end

  asset :fact_sales do
    description("Build sales fact")
    depends(:normalize_orders)
    depends(:normalize_customers)
    meta(tags: [:finance])
  end

  def asset(_ctx), do: :ok
end

defmodule Favn.Test.Fixtures.Assets.Graph.ReportingAssets do
  @moduledoc false
  use Favn.MultiAsset

  alias Favn.Test.Fixtures.Assets.Graph.WarehouseAssets

  asset :dashboard do
    description("Build dashboard")
    depends({WarehouseAssets, :fact_sales})
    depends({WarehouseAssets, :normalize_orders})
  end

  def asset(_ctx), do: :ok
end

defmodule Favn.Test.Fixtures.Assets.Graph.BronzeAssets do
  @moduledoc false
  use Favn.MultiAsset

  asset :raw_orders do
  end

  asset :raw_customers do
  end

  def asset(_ctx), do: :ok
end

defmodule Favn.Test.Fixtures.Assets.Graph.SilverAssets do
  @moduledoc false
  use Favn.MultiAsset

  alias Favn.Test.Fixtures.Assets.Graph.BronzeAssets

  asset :nightly_orders do
    depends({BronzeAssets, :raw_orders})
  end

  asset :monthly_customers do
    depends({BronzeAssets, :raw_customers})
  end

  def asset(_ctx), do: :ok
end

defmodule Favn.Test.Fixtures.Assets.Graph.GoldAssets do
  @moduledoc false
  use Favn.MultiAsset

  alias Favn.Test.Fixtures.Assets.Graph.SilverAssets

  asset :gold_sales do
    depends({SilverAssets, :nightly_orders})
    depends({SilverAssets, :monthly_customers})
  end

  asset :gold_finance do
    depends({SilverAssets, :nightly_orders})
  end

  def asset(_ctx), do: :ok
end
