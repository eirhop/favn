defmodule Favn.Assets.PlannerWindowTest do
  use ExUnit.Case, async: true

  alias Favn.Assets.GraphIndex
  alias Favn.Assets.Planner
  alias Favn.Window.Anchor
  alias Favn.Window.Spec

  test "expands planner runtime windows from calendar anchor and lookback" do
    ref = {MyApp.Daily, :asset}
    spec = Spec.new!(:day, lookback: 1, timezone: "Europe/Oslo")

    assert {:ok, index} =
             GraphIndex.build_index([
               %{ref: ref, module: MyApp.Daily, name: :asset, depends_on: [], window_spec: spec}
             ])

    anchor =
      Anchor.new!(
        :day,
        oslo_datetime!(~N[2026-03-29 00:00:00]),
        oslo_datetime!(~N[2026-03-30 00:00:00]),
        timezone: "Europe/Oslo"
      )

    assert {:ok, plan} = Planner.plan(ref, graph_index: index, anchor_window: anchor)

    windows = plan.nodes |> Map.values() |> Enum.map(& &1.window) |> Enum.sort_by(& &1.start_at)

    assert Enum.map(windows, & &1.start_at) == [
             oslo_datetime!(~N[2026-03-28 00:00:00]),
             oslo_datetime!(~N[2026-03-29 00:00:00])
           ]

    assert Enum.map(windows, & &1.end_at) == [
             oslo_datetime!(~N[2026-03-29 00:00:00]),
             oslo_datetime!(~N[2026-03-30 00:00:00])
           ]

    assert Enum.map(plan.target_node_keys, fn {^ref, key} -> key end) ==
             Enum.map(windows, & &1.key)

    assert Enum.all?(windows, &(&1.anchor_key == anchor.key))
  end

  test "copies asset execution pool onto planned nodes" do
    ref = {MyApp.ExternalApi, :asset}

    assert {:ok, index} =
             GraphIndex.build_index([
               %{
                 ref: ref,
                 module: MyApp.ExternalApi,
                 name: :asset,
                 depends_on: [],
                 execution_pool: :github_api
               }
             ])

    assert {:ok, plan} = Planner.plan(ref, graph_index: index)
    assert [%{execution_pool: :github_api}] = Map.values(plan.nodes)
  end

  defp oslo_datetime!(naive) do
    DateTime.from_naive!(naive, "Europe/Oslo", Favn.Timezone.database!())
  end
end
