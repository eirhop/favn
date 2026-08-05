defmodule FavnView.LogsViewModelTest do
  use ExUnit.Case, async: false

  alias FavnView.LogsViewModel

  setup do
    previous = Application.get_env(:favn, :default_timezone)

    on_exit(fn ->
      if is_nil(previous) do
        Application.delete_env(:favn, :default_timezone)
      else
        Application.put_env(:favn, :default_timezone, previous)
      end
    end)

    :ok
  end

  test "includes the configured timezone in log timestamps" do
    Application.put_env(:favn, :default_timezone, "Europe/Oslo")

    entry =
      LogsViewModel.entry(%{
        id: "log-1",
        occurred_at: ~U[2026-07-01 06:45:56Z],
        level: :info,
        source: :runner,
        message: "started"
      })

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
