defmodule Favn.Assets.PlannerWindowTest do
  use ExUnit.Case, async: true

  alias Favn.Assets.GraphIndex
  alias Favn.Assets.Planner
  alias Favn.Window.Anchor
  alias Favn.Window.Policy
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

  test "expands runtime windows from policy-shaped asset windows" do
    ref = {MyApp.PolicyWindow, :asset}

    assert {:ok, index} =
             GraphIndex.build_index([
               %{
                 ref: ref,
                 module: MyApp.PolicyWindow,
                 name: :asset,
                 depends_on: [],
                 window: Policy.new!(:daily, timezone: "Etc/UTC")
               }
             ])

    anchor = Anchor.new!(:day, ~U[2026-04-26 00:00:00Z], ~U[2026-04-27 00:00:00Z])

    assert {:ok, plan} = Planner.plan(ref, graph_index: index, anchor_window: anchor)
    assert [%{window: %{kind: :day, start_at: ~U[2026-04-26 00:00:00Z]}}] = Map.values(plan.nodes)
  end

  test "expands runtime windows from policy-shaped asset windows with default timezone" do
    ref = {MyApp.PolicyWindowDefaultTimezone, :asset}

    assert {:ok, index} =
             GraphIndex.build_index([
               %{
                 ref: ref,
                 module: MyApp.PolicyWindowDefaultTimezone,
                 name: :asset,
                 depends_on: [],
                 window: Policy.new!(:daily)
               }
             ])

    anchor = Anchor.new!(:day, ~U[2026-04-26 00:00:00Z], ~U[2026-04-27 00:00:00Z])

    assert {:ok, plan} = Planner.plan(ref, graph_index: index, anchor_window: anchor)
    assert [%{window: %{kind: :day, start_at: ~U[2026-04-26 00:00:00Z]}}] = Map.values(plan.nodes)
  end

  defp oslo_datetime!(naive) do
    DateTime.from_naive!(naive, "Europe/Oslo", Favn.Timezone.database!())
  end
end
