defmodule Favn.ScheduleTest do
  use ExUnit.Case, async: true

  alias Favn.Triggers.Schedule

  test "schedule constructors apply default timezone" do
    assert {:ok, unresolved} = Schedule.new_inline(cron: "0 3 * * *")
    assert {:ok, resolved} = Schedule.apply_default_timezone(unresolved, "Etc/UTC")
    assert resolved.timezone == "Etc/UTC"
  end

  test "schedule cron validation accepts five-field and six-field expressions" do
    assert {:ok, schedule} = Schedule.new_inline(cron: "0 3 * * *")
    assert schedule.cron == "0 3 * * *"

    assert {:ok, schedule} = Schedule.new_inline(cron: "15 */10 * * * *")
    assert schedule.cron == "15 */10 * * * *"

    assert {:error, {:invalid_schedule_cron, "60 * * * * *"}} =
             Schedule.new_inline(cron: "60 * * * * *")

    assert {:error, {:invalid_schedule_cron, "*/0 * * * * *"}} =
             Schedule.new_inline(cron: "*/0 * * * * *")

    assert {:error, {:invalid_schedule_cron, "0 0 0 * * * *"}} =
             Schedule.new_inline(cron: "0 0 0 * * * *")
  end
end
