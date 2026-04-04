defmodule Favn.Test.Fixtures.Assets.Graph.SourceAssets do
  use Favn.Assets

  @doc "Raw orders"
  @asset true
  def raw_orders(_ctx), do: :ok

  @doc "Raw customers"
  @asset true
  def raw_customers(_ctx), do: :ok
end

defmodule Favn.Test.Fixtures.Assets.Graph.WarehouseAssets do
  use Favn.Assets

  alias Favn.Test.Fixtures.Assets.Graph.SourceAssets

  @doc "Normalize orders"
  @asset true
  @depends {SourceAssets, :raw_orders}
  @meta tags: [:warehouse]
  def normalize_orders(_ctx), do: :ok

  @doc "Normalize customers"
  @asset true
  @depends {SourceAssets, :raw_customers}
  @meta tags: [:warehouse]
  def normalize_customers(_ctx), do: :ok

  @doc "Build sales fact"
  @asset true
  @depends :normalize_orders
  @depends :normalize_customers
  @meta tags: [:finance]
  def fact_sales(_ctx), do: :ok
end

defmodule Favn.Test.Fixtures.Assets.Graph.ReportingAssets do
  use Favn.Assets

  alias Favn.Test.Fixtures.Assets.Graph.WarehouseAssets

  @doc "Build dashboard"
  @asset true
  @depends {WarehouseAssets, :fact_sales}
  @depends {WarehouseAssets, :normalize_orders}
  def dashboard(_ctx), do: :ok
end

defmodule Favn.Test.Fixtures.Assets.Graph.BronzeAssets do
  use Favn.Assets

  @asset true
  def raw_orders(_ctx), do: :ok

  @asset true
  def raw_customers(_ctx), do: :ok
end

defmodule Favn.Test.Fixtures.Assets.Graph.SilverAssets do
  use Favn.Assets

  alias Favn.Test.Fixtures.Assets.Graph.BronzeAssets

  @asset true
  @depends {BronzeAssets, :raw_orders}
  def nightly_orders(_ctx), do: :ok

  @asset true
  @depends {BronzeAssets, :raw_customers}
  def monthly_customers(_ctx), do: :ok
end

defmodule Favn.Test.Fixtures.Assets.Graph.GoldAssets do
  use Favn.Assets

  alias Favn.Test.Fixtures.Assets.Graph.SilverAssets

  @asset true
  @depends {SilverAssets, :nightly_orders}
  @depends {SilverAssets, :monthly_customers}
  def gold_sales(_ctx), do: :ok

  @asset true
  @depends {SilverAssets, :nightly_orders}
  def gold_finance(_ctx), do: :ok
end
