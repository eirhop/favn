defmodule Favn.Assets.PlannerWindowTest do
  use ExUnit.Case, async: true

  alias Favn.Assets.GraphIndex
  alias Favn.Assets.Planner
  alias Favn.Window.Anchor
  alias Favn.Window.Policy
  alias Favn.Window.Spec

  test "maps a calendar anchor without asset-level lookback expansion" do
    ref = {MyApp.Daily, :asset}
    spec = Spec.new!(:day, timezone: "Europe/Oslo")

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
             oslo_datetime!(~N[2026-03-29 00:00:00])
           ]

    assert Enum.map(windows, & &1.end_at) == [
             oslo_datetime!(~N[2026-03-30 00:00:00])
           ]

    assert Enum.map(plan.target_node_keys, fn {^ref, key} -> key end) ==
             Enum.map(windows, & &1.key)

    assert Enum.all?(windows, &(&1.anchor_key == anchor.key))
  end

  test "monthly asset planning maps exactly the supplied pipeline anchor" do
    ref = {MyApp.MonthlyRefresh, :asset}
    spec = Spec.new!(:month, timezone: "Europe/Oslo")

    assert {:ok, index} =
             GraphIndex.build_index([
               %{
                 ref: ref,
                 module: MyApp.MonthlyRefresh,
                 name: :asset,
                 depends_on: [],
                 window_spec: spec
               }
             ])

    due_at = oslo_datetime!(~N[2026-07-17 02:00:00])

    assert {:ok, previous_anchor} =
             Policy.resolve_scheduled(
               Policy.new!(:monthly,
                 anchor: :previous_complete_period,
                 timezone: "Europe/Oslo"
               ),
               due_at,
               "Europe/Oslo"
             )

    assert {:ok, current_anchor} =
             Policy.resolve_scheduled(
               Policy.new!(:monthly, anchor: :current_period, timezone: "Europe/Oslo"),
               due_at,
               "Europe/Oslo"
             )

    assert {:ok, previous_plan} =
             Planner.plan(ref, graph_index: index, anchor_window: previous_anchor)

    assert {:ok, current_plan} =
             Planner.plan(ref, graph_index: index, anchor_window: current_anchor)

    assert window_starts(previous_plan) == [
             oslo_datetime!(~N[2026-06-01 00:00:00])
           ]

    assert window_starts(current_plan) == [
             oslo_datetime!(~N[2026-07-01 00:00:00])
           ]
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

  test "expands runtime windows from persisted asset window maps" do
    ref = {MyApp.Monthly, :asset}

    assert {:ok, index} =
             GraphIndex.build_index([
               %{
                 ref: ref,
                 module: MyApp.Monthly,
                 name: :asset,
                 depends_on: [],
                 window: %{"kind" => "month", "refresh_from" => "day", "timezone" => "Etc/UTC"}
               }
             ])

    anchor = Anchor.new!(:month, ~U[2026-01-01 00:00:00Z], ~U[2026-02-01 00:00:00Z])

    assert {:ok, plan} = Planner.plan(ref, graph_index: index, anchor_window: anchor)

    assert [%{window: %{kind: :month, start_at: ~U[2026-01-01 00:00:00Z]}}] =
             Map.values(plan.nodes)
  end

  defp oslo_datetime!(naive) do
    DateTime.from_naive!(naive, "Europe/Oslo", Favn.Timezone.database!())
  end

  defp window_starts(plan) do
    plan.nodes
    |> Map.values()
    |> Enum.map(& &1.window.start_at)
    |> Enum.sort_by(&DateTime.to_unix(&1, :microsecond))
  end
end
