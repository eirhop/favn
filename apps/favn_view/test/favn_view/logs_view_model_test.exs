defmodule FavnView.LogsViewModelTest do
  use ExUnit.Case, async: true

  alias FavnView.LogsViewModel

  test "includes the explicit workspace timezone in log timestamps" do
    entry =
      LogsViewModel.entry(
        %{
          id: "log-1",
          occurred_at: ~U[2026-07-01 06:45:56Z],
          level: :info,
          source: :runner,
          message: "started"
        },
        "Europe/Oslo"
      )

    assert entry.timestamp == "Jul 1 08:45:56 CEST"
  end

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
