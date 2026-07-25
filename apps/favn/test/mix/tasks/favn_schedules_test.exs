defmodule Mix.Tasks.Favn.SchedulesTest do
  use ExUnit.Case, async: true

  alias Mix.Tasks.Favn.Schedules

  test "parses schedule inspection commands" do
    assert {:ok, {:list, []}} = Schedules.parse_args(["list"])

    assert {:ok, {:show, "schedule-v2:id:name", []}} =
             Schedules.parse_args(["show", "schedule-v2:id:name"])

    assert {:ok, {:preview, "schedule-v2:id:name", [limit: 3]}} =
             Schedules.parse_args(["preview", "schedule-v2:id:name", "--limit", "3"])
  end

  test "requires a reason for activation changes" do
    assert {:error, "missing required option: --reason"} =
             Schedules.parse_args(["activate", "schedule-v2:id:name"])

    assert {:ok, {:deactivate, "schedule-v2:id:name", [reason: "maintenance"]}} =
             Schedules.parse_args([
               "deactivate",
               "schedule-v2:id:name",
               "--reason",
               "maintenance"
             ])
  end
end
