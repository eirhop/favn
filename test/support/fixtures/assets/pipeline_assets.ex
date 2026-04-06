defmodule Favn.Test.Fixtures.Assets.Pipeline.CtxRecorder do
  use Agent

  def start_link(_opts) do
    Agent.start_link(fn -> [] end, name: __MODULE__)
  end

  def reset do
    Agent.update(__MODULE__, fn _ -> [] end)
  end

  def record(ctx) do
    Agent.update(__MODULE__, fn list -> [ctx | list] end)
  end

  def all do
    Agent.get(__MODULE__, & &1)
  end
end

defmodule Favn.Test.Fixtures.Assets.Pipeline.SalesAssets do
  use Favn.Assets

  alias Favn.Test.Fixtures.Assets.Pipeline.CtxRecorder

  @asset true
  @meta tags: [:daily], category: :bronze
  def extract_orders(_ctx), do: :ok

  @asset true
  @depends :extract_orders
  @meta tags: [:daily], category: :silver
  def normalize_orders(_ctx), do: :ok

  @asset true
  @depends :normalize_orders
  @meta tags: [:daily], category: :gold
  def sales_daily(ctx) do
    CtxRecorder.record(ctx)
    :ok
  end

  @asset true
  @depends :normalize_orders
  @meta tags: [:inventory], category: :gold
  def inventory_daily(_ctx), do: :ok
end

defmodule Favn.Test.Fixtures.Assets.Pipeline.ReportingAssets do
  use Favn.Assets

  @asset true
  @meta tags: [:daily], category: :gold
  def marketing_snapshot(_ctx), do: :ok
end

defmodule Favn.Test.Fixtures.Pipelines.SimplePipeline do
  use Favn.Pipeline

  alias Favn.Test.Fixtures.Assets.Pipeline.SalesAssets

  pipeline :daily_sales do
    asset({SalesAssets, :sales_daily})
    deps(:all)

    config(timezone: "UTC", notify: false)
    meta(owner: "data-platform", domain: :sales)

    schedule(:daily_default)
    partition(:calendar_day)
    source(:snowflake_primary)
    outputs([:warehouse_gold])
  end
end

defmodule Favn.Test.Fixtures.Pipelines.SelectPipeline do
  use Favn.Pipeline

  alias Favn.Test.Fixtures.Assets.Pipeline.ReportingAssets

  pipeline :daily_gold do
    select do
      tag(:daily)
      category(:gold)
      module(ReportingAssets)
    end

    deps(:none)
    outputs([:warehouse_gold])
  end
end

defmodule Favn.Test.Fixtures.Pipelines.AssetsShorthandPipeline do
  use Favn.Pipeline

  alias Favn.Test.Fixtures.Assets.Pipeline.SalesAssets

  pipeline :multi_daily do
    assets([
      {SalesAssets, :sales_daily},
      {SalesAssets, :inventory_daily}
    ])

    deps(:none)
  end
end
