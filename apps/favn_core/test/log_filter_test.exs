defmodule Favn.Log.FilterTest do
  use ExUnit.Case, async: true

  alias Favn.Log.Filter

  test "normalizes map and keyword filters" do
    since = ~U[2026-05-01 00:00:00Z]
    until_time = ~U[2026-05-01 01:00:00Z]

    filter =
      Filter.normalize(%{
        "run_id" => "run_1",
        "asset_step_id" => "asset_step_1",
        "node_key" => "node-a",
        "asset_ref" => {MyApp.Asset, :daily},
        "levels" => ["info", :error],
        "sources" => "runner",
        "since" => since,
        "until" => until_time
      })

    assert filter == %Filter{
             run_id: "run_1",
             asset_step_id: "asset_step_1",
             node_key: "node-a",
             asset_ref: "asset:Elixir.MyApp.Asset:daily",
             levels: [:info, :error],
             sources: [:runner],
             since: since,
             until: until_time
           }

    assert %Filter{levels: [:warning]} = Filter.normalize(levels: :warning)
  end

  test "rejects unknown levels and sources" do
    assert_raise ArgumentError, fn -> Filter.normalize(levels: [:notice]) end
    assert_raise ArgumentError, fn -> Filter.normalize(sources: ["database"]) end
  end
end
