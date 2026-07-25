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

    assert {:ok,
            {:activate, "schedule-v2:id:name",
             [reason: "reviewed", idempotency_key: "schedule-change-42"]}} =
             Schedules.parse_args([
               "activate",
               "schedule-v2:id:name",
               "--reason",
               "reviewed",
               "--idempotency-key",
               "schedule-change-42"
             ])

    assert {:error, "--idempotency-key must contain between 1 and 200 bytes"} =
             Schedules.parse_args([
               "activate",
               "schedule-v2:id:name",
               "--reason",
               "reviewed",
               "--idempotency-key",
               ""
             ])
  end

  test "formats preview response keys without dynamic atom conversion" do
    assert [
             ~s(due_at=2026-07-25T12:00:00Z status=due window=%{"key" => "day:2026-07-24"})
           ] =
             Schedules.preview_lines([
               %{
                 "due_at" => "2026-07-25T12:00:00Z",
                 "status" => "due",
                 "window" => %{"key" => "day:2026-07-24"}
               }
             ])

    assert ["due_at=2026-07-25T12:00:00Z status=due window=%{key: \"day:2026-07-24\"}"] =
             Schedules.preview_lines([
               %{
                 due_at: "2026-07-25T12:00:00Z",
                 status: "due",
                 window: %{key: "day:2026-07-24"}
               }
             ])
  end
end
