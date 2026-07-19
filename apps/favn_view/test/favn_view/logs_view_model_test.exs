defmodule FavnView.LogsViewModelTest do
  use ExUnit.Case, async: true

  alias FavnView.LogsViewModel

  test "uses the module name for canonical single-asset refs" do
    assert LogsViewModel.display_name({MyApp.Assets.ElixirOrders, :asset}) == "ElixirOrders"

    assert LogsViewModel.display_name(%{
             module: "Elixir.MyApp.Assets.SqlOrders",
             name: "asset"
           }) == "SqlOrders"

    assert LogsViewModel.display_name("MyApp.Assets.DailyRevenue.asset") == "DailyRevenue"
    assert LogsViewModel.display_name("MyApp.Assets.MonthlyRevenue:asset") == "MonthlyRevenue"
  end

  test "uses an explicitly named asset ref name" do
    assert LogsViewModel.display_name({MyApp.Assets.Orders, :daily_orders}) == "daily_orders"
    assert LogsViewModel.display_name("MyApp.Assets.Orders.daily_orders") == "daily_orders"
  end
end
